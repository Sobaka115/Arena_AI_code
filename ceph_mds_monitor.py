#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ceph_mds_monitor.py
===================

Обходное решение для Ceph: мониторит статус кластера и при необходимости
перезапускает проблемный MDS (Metadata Server).

Сценарий (по заданию):
  * Проверяет статус ceph каждые N секунд.
  * Реагирует ТОЛЬКО на HEALTH_WARN. HEALTH_ERR игнорируется.
  * При WARN + хранилище (CephFS mount) доступно — просто ждём/логируем,
    надеясь на самовосстановление.
  * При WARN + хранилище НЕДОСТУПНО — определяет проблемный MDS из текста
    ошибки (slow request) и перезапускает именно его.
  * После перезапуска: если в течение 3 минут статус всё ещё WARN —
    больше не перезапускать MDS, пока статус HEALTH_OK не продержится
    подряд 20 минут.

Требования к окружению (сервер mst1):
  * ceph развёрнут в Docker на этом же хосте;
  * службы MDS (ceph-mds@<instance>) работают напрямую на хосте systemd;
  * CephFS примонтирован на этом же хосте.

Запуск:
  python3 ceph_mds_monitor.py          # цикличный мониторинг (systemd)
  python3 ceph_mds_monitor.py --once   # единичная проверка (отладка)

Все настройки — через переменные окружения (см. ceph-mds-monitor.env.example)
или аргументы командной строки.
"""

import argparse
import json
import logging
import logging.handlers
import os
import re
import shlex
import signal
import subprocess
import sys
import time
from datetime import datetime
from typing import List, Optional, Set, Tuple, Dict, Any

# ---------------------------------------------------------------------------
# Конфигурация (по умолчанию — из переменных окружения)
# ---------------------------------------------------------------------------
def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)

CEPH_CONTAINER   = _env("CEPH_CONTAINER",   "")          # имя контейнера с ceph CLI ("" — ceph на хосте)
CEPH_BIN         = _env("CEPH_BIN",         "ceph")
CEPH_EXTRA_ARGS  = shlex.split(_env("CEPH_EXTRA_ARGS", ""))  # напр. --cluster ceph --keyring ...
CEPH_FS          = _env("CEPH_FS",          "cephfs")
CEPH_MOUNT       = _env("CEPH_MOUNT",       "/mnt/cephfs")
CHECK_INTERVAL   = int(_env("CHECK_INTERVAL", "30"))      # сек между проверками
LOG_FILE         = _env("LOG_FILE",         "/var/log/ceph-mds-monitor.log")
LOG_LEVEL        = _env("LOG_LEVEL",        "INFO")
MDS_RESTART_CMD  = _env("MDS_RESTART_CMD",  "systemctl restart ceph-mds@{daemon}")
COOLDOWN_WARN_MIN = int(_env("COOLDOWN_WARN_MIN", "3"))   # мин Beobachtки после перезапуска
HEALTH_OK_MIN    = int(_env("HEALTH_OK_MIN",   "20"))     # мин HEALTH_OK для окончания паузы
STORAGE_TIMEOUT  = int(_env("STORAGE_TIMEOUT", "15"))     # сек на проверку доступа к хранилищу

COOLDOWN_WARN_SEC = COOLDOWN_WARN_MIN * 60
HEALTH_OK_SEC     = HEALTH_OK_MIN * 60

# ---------------------------------------------------------------------------
# Логирование
# ---------------------------------------------------------------------------
logger = logging.getLogger("ceph-mds-monitor")


def setup_logging(log_file: Optional[str], level: str) -> None:
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    if log_file:
        try:
            fh = logging.handlers.RotatingFileHandler(
                log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
            )
            fh.setFormatter(fmt)
            logger.addHandler(fh)
        except OSError as exc:
            logger.error("Не удалось открыть файл лога %s: %s", log_file, exc)


# ---------------------------------------------------------------------------
# Обёртка над вызовами ceph (через docker exec или напрямую)
# ---------------------------------------------------------------------------
def run_cmd(cmd: List[str], timeout: int = 30) -> Tuple[int, str, str]:
    """Выполнить команду, вернуть (rc, stdout, stderr)."""
    try:
        proc = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout
        )
        return proc.returncode, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired:
        return -1, "", f"command timed out after {timeout}s: {' '.join(cmd)}"
    except FileNotFoundError:
        return -1, "", f"command not found: {cmd[0]}"
    except Exception as exc:  # pragma: no cover - защитная ветка
        return -1, "", f"exception running {' '.join(cmd)}: {exc}"


def ceph_raw(args: List[str], timeout: int = 30) -> Tuple[int, str, str]:
    """Выполнить ceph CLI с JSON-выводом (внутри контейнера или на хосте)."""
    cmd: List[str] = []
    if CEPH_CONTAINER:
        cmd += ["docker", "exec", CEPH_CONTAINER]
    cmd += [CEPH_BIN] + CEPH_EXTRA_ARGS + args + ["--format=json"]
    return run_cmd(cmd, timeout)


def get_ceph_health() -> Dict[str, Any]:
    """Получить общий статус здоровья кластера (словарь health из ceph)."""
    # Предпочитаем подробный вывод health detail, где есть тексты ошибок.
    rc, out, err = ceph_raw(["health", "detail"])
    if rc != 0 or not out.strip():
        logger.debug("ceph health detail не удался (rc=%s): %s", rc, err.strip())
        rc, out, err = ceph_raw(["-s"])
        if rc != 0 or not out.strip():
            raise RuntimeError(f"ceph status failed: {err.strip()}")
    try:
        data = json.loads(out)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"cannot parse ceph JSON output: {exc}")

    health = data.get("health", {})
    if not health and "health" not in data:
        # Некоторые обёртки возвращают корень сразу со статусом.
        health = data
    return health


def get_overall_status(health: Dict[str, Any]) -> str:
    """Вернуть статус: HEALTH_OK / HEALTH_WARN / HEALTH_ERR / UNKNOWN."""
    return str(health.get("status", "UNKNOWN")).upper()


def get_mds_dump() -> Optional[Dict[str, Any]]:
    """ceph mds dump — дополнительно уточняет состояние MDS (up/laggy/down)."""
    rc, out, err = ceph_raw(["mds", "dump"])
    if rc != 0 or not out.strip():
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return None


# ---------------------------------------------------------------------------
# Проверка доступности хранилища
# ---------------------------------------------------------------------------
def is_storage_accessible() -> bool:
    """
    Проверяет, что точка монтирования CephFS примонтирована и отвечает.
    ls на зависшем CephFS-клиенте может висеть надолго — оборачиваем в timeout.
    """
    mount = CEPH_MOUNT
    if not mount:
        logger.debug("CEPH_MOUNT не задан — проверка хранилища пропущена")
        return True
    if not os.path.exists(mount):
        logger.debug("Точка монтирования %s не существует", mount)
        return False
    if not os.path.ismount(mount):
        logger.debug("Путь %s не является активной точкой монтирования", mount)
        return False
    try:
        proc = subprocess.run(
            ["timeout", str(STORAGE_TIMEOUT), "ls", mount],
            capture_output=True, text=True, timeout=STORAGE_TIMEOUT + 5,
        )
        if proc.returncode != 0:
            logger.debug("ls %s завершён с кодом %s", mount, proc.returncode)
            return False
    except Exception as exc:
        logger.debug("Ошибка при проверке доступа к %s: %s", mount, exc)
        return False
    return True


# ---------------------------------------------------------------------------
# Определение проблемного MDS
# ---------------------------------------------------------------------------
MDS_NAME_RE = re.compile(r"mds\.([A-Za-z0-9_.\-]+)")


def _extract_mds_candidates(health: Dict[str, Any]) -> Set[str]:
    """Достаёт имена mds.<имя> из всего текста health detail."""
    candidates: Set[str] = set()
    text = json.dumps(health, ensure_ascii=False)
    for match in MDS_NAME_RE.finditer(text):
        candidates.add(match.group(1))
    return candidates


def _mds_state_from_dump(mds_dump: Optional[Dict[str, Any]]) -> Tuple[Set[str], Set[str], Set[str]]:
    """
    Из ceph mds dump возвращает (up, laggy, down) множества имён MDS без префикса mds.
    Структура ответа зависит от версии Ceph, поэтому 접근 보호щённый.
    """
    up: Set[str] = set()
    laggy: Set[str] = set()
    down: Set[str] = set()
    if not mds_dump:
        return up, laggy, down

    mdsmap = mds_dump.get("fsmap", {}).get("mdsmap", {}) or {}

    def _names(value: Any) -> Set[str]:
        result: Set[str] = set()
        if isinstance(value, dict):
            result.update(str(k) for k in value.keys())
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    result.update(str(k) for k in item.keys())
                else:
                    result.add(str(item))
        elif isinstance(value, str):
            result.add(value)
        return result

    up = _names(mdsmap.get("up"))
    laggy = _names(mdsmap.get("laggy"))
    down = _names(mdsmap.get("down"))
    # Некоторые версии кладут "вypadшие" MDS в 'failed' / 'standing'
    down |= _names(mdsmap.get("failed"))
    return up, laggy, down


def find_faulty_mds(health: Dict[str, Any]) -> Optional[str]:
    """
    Определяет экземпляр MDS, на котором произошла ошибка (slow request).

    Приоритет:
      1. Имя, встреченное в тексте ошибки health detail;
      2. Из тех, кто не в up или в laggy/down по mds dump;
      3. Иначе — первый попавшийся кандидат.
    """
    candidates = _extract_mds_candidates(health)
    if not candidates:
        logger.warning("Не найдено упоминаний mds.<имя> в выводе ceph health detail")
        return None

    mds_dump = get_mds_dump()
    up, laggy, down = _mds_state_from_dump(mds_dump)

    # Нормализуем имена: уберём префикс mds., если он есть в stati дампа.
    up_norm = {n[len("mds."):] if n.startswith("mds.") else n for n in up}
    laggy_norm = {n[len("mds."):] if n.startswith("mds.") else n for n in laggy}
    down_norm = {n[len("mds."):] if n.startswith("mds.") else n for n in down}

    def norm(name: str) -> str:
        return name[len("mds."):] if name.startswith("mds.") else name

    # 1. Кандидаты, которые явно проблемны по дампу.
    for name in sorted(candidates):
        short = norm(name)
        if short in laggy_norm or short in down_norm:
            logger.info("MDS %s помечен как laggy/down в ceph mds dump", name)
            return name

    # 2. Кандидаты, которых нет в up.
    for name in sorted(candidates):
        short = norm(name)
        if short not in up_norm:
            logger.info("MDS %s отсутствует в списке up (ceph mds dump)", name)
            return name

    # 3. Берём первого кандидата по алфавиту — это лучше, чем угадывать слепо.
    chosen = sorted(candidates)[0]
    logger.info("Явного проблемного MDS в дампе не найдено, берём кандидата из ошибки: %s", chosen)
    return chosen


# ---------------------------------------------------------------------------
# Перезапуск MDS
# ---------------------------------------------------------------------------
def restart_mds(instance: str) -> bool:
    """
    Перезапускает службу MDS на хосте.
    instance — часть имени после mds., например mst1.
    Команда по умолчанию: systemctl restart ceph-mds@<instance>
    """
    cmd_str = MDS_RESTART_CMD.replace("{daemon}", instance)
    cmd = shlex.split(cmd_str)
    logger.info("Выполняется перезапуск MDS: %s", cmd_str)
    rc, out, err = run_cmd(cmd, timeout=60)
    if rc == 0:
        logger.info("MDS %s успешно перезапущен (rc=0). Вывод: %s",
                    instance, out.strip()[:500] if out.strip() else "—")
        return True
    logger.error(
        "Не удалось перезапустить MDS %s (rc=%s). stderr: %s stdout: %s",
        instance, rc, err.strip()[:500], out.strip()[:500]
    )
    return False


# ---------------------------------------------------------------------------
# Машина состояний основного цикла
# ---------------------------------------------------------------------------
class MonitorState:
    def __init__(self) -> None:
        self.last_restart: Optional[float] = None   # метка времени последнего перезапуска MDS
        self.health_ok_since: Optional[float] = None  # когда 처음으로 увидели HEALTH_OK в текущей серии
        self.in_cooldown: bool = False              # пауза перезапусков (3 мин WARN → ждём HEALTH_OK 20 мин)


def evaluate(health: Dict[str, Any], state: MonitorState, now: float) -> None:
    """Одна итерация принятия решения."""
    status = get_overall_status(health)
    logger.info("Статус Ceph: %s", status)

    if status == "HEALTH_OK":
        if state.health_ok_since is None:
            state.health_ok_since = now
            logger.info("Зафиксирован HEALTH_OK — Started 20-минутный отсчёт для сброса паузы")
        if state.in_cooldown and (now - state.health_ok_since) >= HEALTH_OK_SEC:
            logger.info(
                "HEALTH_OK сохраняется уже %d мин — пауза перезапусков снята, "
                "мониторинг возвращён в обычный режим", HEALTH_OK_MIN
            )
            state.in_cooldown = False
            state.last_restart = None
        return

    # Любой статус, кроме OK, сбрасывает серию HEALTH_OK.
    state.health_ok_since = None

    if status == "HEALTH_ERR":
        logger.info("Статус HEALTH_ERR — игнорируется по заданию (реагируем только на WARN)")
        return

    if status != "HEALTH_WARN":
        logger.info("Статус %s — нет конкретных действий в инструкции", status)
        return

    # ---------------- HEALTH_WARN ----------------
    accessible = is_storage_accessible()

    if accessible:
        logger.info("WARN, но хранилище %s доступно — ждём самовосстановления Ceph", CEPH_MOUNT)
        return

    # WARN + хранилище недоступно — необходимо действовать.

    if state.in_cooldown:
        logger.info(
            "НА ПАУЗЕ: перезапуск MDS запрещён. Ожидание HEALTH_OK в течение %d мин.",
            HEALTH_OK_MIN
        )
        return

    if state.last_restart is not None:
        elapsed = now - state.last_restart
        if elapsed < COOLDOWN_WARN_SEC:
            logger.info(
                "WARN после перезапуска MDS: прошло %.0f сек из %d мин — наблюдаем, не перезапускаем",
                elapsed, COOLDOWN_WARN_MIN
            )
            return
        # 3 минуты прошли, статус всё ещё WARN → вводим паузу.
        logger.info(
            "WARN сохраняется уже %.0f мин (>= %d мин) после последнего перезапуска — "
            "вводим паузу до HEALTH_OK в течение %d мин",
            elapsed / 60, COOLDOWN_WARN_MIN, HEALTH_OK_MIN
        )
        state.in_cooldown = True
        return

    # Первое обнаружение: определяем проблемный MDS и перезапускаем его.
    instance = find_faulty_mds(health)
    if instance is None:
        logger.error(
            "WARN и хранилище недоступно, но проблемный MDS не определён — "
            "перезапуск невозможен, ждём следующей проверки"
        )
        return

    logger.info("Обнаружен проблемный MDS %s — перезапуск", instance)
    if restart_mds(instance):
        state.last_restart = now
        state.in_cooldown = False
        logger.info("MDS %s перезапущен. Отсчёт %d мин до возможного повторного действия",
                    instance, COOLDOWN_WARN_MIN)
    else:
        logger.error("Перезапуск MDS %s не удался", instance)


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------
running = True


def handle_signal(signum, frame):  # pragma: no cover - сигналы ОС
    global running
    logger.info("Получен сигнал %s — завершение работы", signum)
    running = False


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Мониторинг Ceph WARN с автоматическим перезапуском проблемного MDS"
    )
    parser.add_argument("--once", action="store_true",
                        help="Одна проверка и выход (для отладки)")
    parser.add_argument("--interval", type=int, default=CHECK_INTERVAL,
                        help="Интервал проверок в секундах")
    parser.add_argument("--mount", default=CEPH_MOUNT,
                        help="Точка монтирования CephFS")
    parser.add_argument("--container", default=CEPH_CONTAINER,
                        help="Имя Docker-контейнера с ceph CLI")
    parser.add_argument("--log-file", default=LOG_FILE,
                        help="Путь к файлу лога")
    parser.add_argument("--log-level", default=LOG_LEVEL,
                        help="Уровень логирования (DEBUG, INFO, …)")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    # Переопределяем настройки из аргументов.
    global CEPH_MOUNT, CEPH_CONTAINER, LOG_FILE, LOG_LEVEL, CHECK_INTERVAL
    CEPH_MOUNT = args.mount
    CEPH_CONTAINER = args.container
    LOG_FILE = args.log_file
    LOG_LEVEL = args.log_level
    CHECK_INTERVAL = args.interval

    setup_logging(LOG_FILE, LOG_LEVEL)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    state = MonitorState()
    logger.info(
        "ceph-mds-monitor запущен. interval=%ds, mount=%s, container=%s, "
        "cooldown_warn=%dmin, health_ok_required=%dmin",
        CHECK_INTERVAL, CEPH_MOUNT or "<не задан>", CEPH_CONTAINER or "<хост>",
        COOLDOWN_WARN_MIN, HEALTH_OK_MIN
    )

    while running:
        now = time.time()
        try:
            health = get_ceph_health()
        except Exception as exc:
            logger.error("Не удалось получить статус ceph: %s", exc)
            if args.once:
                return 2
            time.sleep(CHECK_INTERVAL)
            continue

        evaluate(health, state, now)

        if args.once:
            status = get_overall_status(health)
            logger.info("Одиночная проверка завершена, статус: %s", status)
            return 0

        time.sleep(CHECK_INTERVAL)

    logger.info("ceph-mds-monitor остановлен")
    return 0


if __name__ == "__main__":
    sys.exit(main())
