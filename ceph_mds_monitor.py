#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ceph_mds_monitor.py
===================

Обходное решение для Ceph: мониторит статус кластера и при необходимости
перезапускает проблемный MDS (Metadata Server).

Архитектура (по ТЗ):
  * 3 сервера с MDS: mst1, mst2, mst3.
  * Скрипт запускается на хосте mst1 (где примонтирован CephFS).
  * Статус Ceph снимается ТОЛЬКО на mst1 (статус кластера общий):
        docker exec ceph-mon-mst1 ceph health detail
  * Из текста ошибки извлекается проблемный MDS (например,
    mds.frm-mst1(mds.0) → хост frm-mst1) и перезапускается только он:
        systemctl restart ceph-mds@<хост>.service
    (на целевом хосте — локально, если это mst1, иначе через ssh).
  * Ceph примонтирован на mst1. В период аварии хранилище может быть
    доступно на ЧТЕНИЕ, но не на ЗАПИСЬ — поэтому мониторится именно
    возможность записи.

Сценарий:
  * Реагирует ТОЛЬКО на HEALTH_WARN. HEALTH_ERR игнорируется.
  * WARN + запись в CephFS доступна — ждём самовосстановления.
  * WARN + запись НЕДОСТУПНА — определить проблемный хост MDS из ошибки и
    перезапустить только его MDS.
  * После перезапуска: если в течение 3 мин статус всё ещё WARN —
    больше не перезапускать, пока HEALTH_OK не продержится 20 мин подряд.

Проверка записи:
  * Метод "file" (по умолчанию) — создание/запись/удаление тестового файла.
    Надёжный способ узнать, что запись действительно работает.
  * Метод "mds_dump" — анализ ceph mds dump без записи файла.
    Менее достоверен: MDS может быть в списке up, но клиенты всё равно не
    могут писать. Используйте, если категорически нельзя создавать файлы.

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
from typing import Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Конфигурация (по умолчанию — из переменных окружения)
# ---------------------------------------------------------------------------
def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)

# Хост, с которого снимаем статус Ceph (статус общий, достаточно одного)
CEPH_STATUS_HOST: str = _env("CEPH_STATUS_HOST", "mst1")

# Префикс имени контейнера с ceph CLI на хосте статуса: ceph-mon-mst1
CEPH_CONTAINER_PREFIX: str = _env("CEPH_CONTAINER_PREFIX", "ceph-mon-")

# Список всех хостов с MDS (для справки и возможного расширения)
CEPH_MON_HOSTS: List[str] = [h.strip() for h in _env("CEPH_MON_HOSTS", "mst1 mst2 mst3").split() if h.strip()]

# SSH для удалённых хостов (mst2, mst3). Локальный хост опрашивается напрямую.
SSH_USER: str = _env("SSH_USER", "root")
SSH_KEY: str = _env("SSH_KEY", "")
SSH_OPTS: List[str] = shlex.split(_env("SSH_OPTS",
    "-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"))

# Точка монтирования CephFS (только на mst1, где запускается скрипт)
CEPH_MOUNT: str = _env("CEPH_MOUNT", "/mnt/cephfs")
CEPH_TEST_FILE: str = _env("CEPH_TEST_FILE", ".ceph-mds-monitor-write-test")

# Метод проверки записи: "file" (реальная запись тестового файла) или
# "mds_dump" (анализ ceph mds dump без создания файлов — менее достоверно).
WRITE_CHECK_METHOD: str = _env("WRITE_CHECK_METHOD", "file")

CHECK_INTERVAL: int = int(_env("CHECK_INTERVAL", "30"))
LOG_FILE: str = _env("LOG_FILE", "/var/log/ceph-mds-monitor.log")
LOG_LEVEL: str = _env("LOG_LEVEL", "INFO")

# Шаблон команды перезапуска MDS на целевом хосте. Подставляется имя хоста.
MDS_RESTART_CMD_TEMPLATE: str = _env("MDS_RESTART_CMD", "systemctl restart ceph-mds@{host}.service")

COOLDOWN_WARN_MIN: int = int(_env("COOLDOWN_WARN_MIN", "3"))   # мин наблюдения после перезапуска
HEALTH_OK_MIN: int = int(_env("HEALTH_OK_MIN", "20"))          # мин HEALTH_OK для сброса паузы
WRITE_TIMEOUT: int = int(_env("WRITE_TIMEOUT", "15"))          # сек на проверку записи

COOLDOWN_WARN_SEC = COOLDOWN_WARN_MIN * 60
HEALTH_OK_SEC = HEALTH_OK_MIN * 60

# Имя локального хоста (тут запущен скрипт)
LOCAL_HOST: str = os.environ.get("HOSTNAME") or subprocess.run(
    ["hostname"], capture_output=True, text=True
).stdout.strip()

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
# Выполнение команд
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


def run_on_host(host: str, cmd: str, timeout: int = 60) -> Tuple[int, str, str]:
    """
    Выполнить команду на указанном хосте.
    Локальный хост — напрямую. Удалённые — через ssh.
    """
    if host == LOCAL_HOST:
        return run_cmd(shlex.split(cmd), timeout=timeout)

    ssh_args: List[str] = ["ssh"] + SSH_OPTS
    if SSH_KEY:
        ssh_args += ["-i", SSH_KEY]
    ssh_args += [f"{SSH_USER}@{host}", cmd]
    return run_cmd(ssh_args, timeout=timeout)


# ---------------------------------------------------------------------------
# Опрос Ceph (только на хосте статуса, по умолчанию mst1)
# ---------------------------------------------------------------------------
def ceph_health_on_host(host: str) -> Tuple[Optional[str], str]:
    """
    Вернуть (status, raw_output) для одного хоста.

    status: HEALTH_OK / HEALTH_WARN / HEALTH_ERR / None
    """
    container = f"{CEPH_CONTAINER_PREFIX}{host}"
    cmd = f"docker exec {container} ceph health detail"
    rc, out, err = run_on_host(host, cmd, timeout=60)
    if rc != 0:
        logger.debug("ceph health detail на %s не удался (rc=%s): %s",
                     host, rc, err.strip()[:200])
        return None, f"error: {err.strip()}"

    raw = out.strip()
    status: Optional[str] = None

    # Первая строка текстового вывода: HEALTH_WARN ..., HEALTH_OK ..., HEALTH_ERR ...
    first_line = raw.splitlines()[0] if raw.splitlines() else ""
    m = re.match(r"^(HEALTH_OK|HEALTH_WARN|HEALTH_ERR)\b", first_line)
    if m:
        status = m.group(1)

    return status, raw


def get_cluster_status() -> Tuple[Optional[str], str]:
    """
    Снимаем статус Ceph только с CEPH_STATUS_HOST (по умолчанию mst1).
    Возвращает (status, raw_output).
    """
    return ceph_health_on_host(CEPH_STATUS_HOST)


def extract_faulty_mds_hosts(raw: str) -> Set[str]:
    """
    Извлечь из текста ceph health detail имена хостов с проблемными MDS.

    Формат строки в ошибке: mds.frm-mst1(mds.0): ...
    Результат: {'frm-mst1'}
    """
    hosts: Set[str] = set()
    # mds.<имя_хоста>(mds.<инстанс>) — основной формат
    for match in re.finditer(r"mds\.([A-Za-z0-9_.\-]+)\s*\(mds\.[^)]+\)", raw):
        hosts.add(match.group(1))

    # Fallback: упоминания mds.<имя> без скобок (если формат вывода отличается)
    if not hosts:
        for match in re.finditer(r"\bmds\.([A-Za-z0-9_.\-]+)", raw):
            hosts.add(match.group(1))

    return hosts


# ---------------------------------------------------------------------------
# Проверка записи в CephFS (чтение может быть доступно, запись — нет)
# ---------------------------------------------------------------------------
def is_storage_writable_via_file() -> bool:
    """
    Проверяет возможность записи, создавая/пишая/удаляя тестовый файл.
    Надёжный метод, но требует права на создание файла в точке монтирования.
    """
    mount = CEPH_MOUNT
    if not mount:
        return True
    if not os.path.ismount(mount):
        logger.debug("Точка монтирования %s не является активной точкой монтирования", mount)
        return False

    test_file = os.path.join(mount, CEPH_TEST_FILE)
    quoted_file = shlex.quote(test_file)
    cmd = (
        f"timeout {WRITE_TIMEOUT} bash -c "
        f"'echo ceph-mds-monitor-test > {quoted_file} && "
        f"cat {quoted_file} && "
        f"rm -f {quoted_file}'"
    )
    try:
        rc, out, err = run_cmd(["bash", "-c", cmd], timeout=WRITE_TIMEOUT + 5)
        if rc == 0 and "ceph-mds-monitor-test" in out:
            return True
        logger.debug(
            "Проверка записи (file) не прошла: rc=%s out=%s err=%s",
            rc, out.strip()[:100], err.strip()[:100]
        )
        return False
    except Exception as exc:
        logger.debug("Исключение при проверке записи (file): %s", exc)
        return False


def _mds_names_from_dump(value) -> Set[str]:
    """Достаёт имена MDS из структуры ceph mds dump (без префикса mds.)."""
    result: Set[str] = set()
    if isinstance(value, dict):
        for k in value.keys():
            s = str(k)
            result.add(s[len("mds."):] if s.startswith("mds.") else s)
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                for k in item.keys():
                    s = str(k)
                    result.add(s[len("mds."):] if s.startswith("mds.") else s)
            else:
                s = str(item)
                result.add(s[len("mds."):] if s.startswith("mds.") else s)
    elif isinstance(value, str):
        s = value
        result.add(s[len("mds."):] if s.startswith("mds.") else s)
    return result


def is_storage_writable_via_mds_dump() -> bool:
    """
    Проверяет возможность записи через анализ ceph mds dump без создания файлов.

    Менее достоверно: MDS может быть в списке up, но клиенты всё равно не
    могут писать (capability release не отвечает). Используйте только если
    категорически нельзя создавать файлы в точке монтирования.
    """
    # Сначала берём статус на хосте статуса, чтобы понять общую картину.
    status, raw = get_cluster_status()
    if status is None:
        logger.debug("Не удалось получить статус ceph для mds_dump-проверки")
        return False

    # Получаем mds dump с того же хоста.
    container = f"{CEPH_CONTAINER_PREFIX}{CEPH_STATUS_HOST}"
    cmd = f"docker exec {container} ceph mds dump --format=json"
    rc, out, err = run_on_host(CEPH_STATUS_HOST, cmd, timeout=60)
    if rc != 0 or not out.strip():
        logger.debug("ceph mds dump не удался (rc=%s): %s", rc, err.strip()[:200])
        return False

    try:
        data = json.loads(out)
    except Exception as exc:
        logger.debug("Не удалось распарсить ceph mds dump JSON: %s", exc)
        return False

    mdsmap = (data.get("fsmap", {}).get("mdsmap", {})) or {}

    up = _mds_names_from_dump(mdsmap.get("up"))
    laggy = _mds_names_from_dump(mdsmap.get("laggy"))
    down = _mds_names_from_dump(mdsmap.get("down"))
    down |= _mds_names_from_dump(mdsmap.get("failed"))

    # Извлекаем проблемные хосты из текста ошибки.
    faulty = extract_faulty_mds_hosts(raw)
    if not faulty:
        # Если в ошибке нет явного mds.<host>, полагаемся на дамп:
        # если есть хосты в laggy/down — запись скорее всего недоступна.
        if laggy or down:
            logger.debug("mds_dump: есть проблемные MDS (laggy=%s, down=%s)",
                         laggy, down)
            return False
        return True

    # Если хост из ошибки не в up или в laggy/down — запись недоступна.
    for host in faulty:
        if host in down or host in laggy:
            logger.debug("mds_dump: хост %s в laggy/down — запись недоступна", host)
            return False
        if host not in up:
            logger.debug("mds_dump: хост %s отсутствует в up — запись недоступна", host)
            return False

    logger.debug("mds_dump: все проблемные хосты (%s) в up, нет laggy/down", faulty)
    return True


def is_storage_writable() -> bool:
    """
    Проверяет, что в CephFS можно записать данные.
    Метод выбирается переменной WRITE_CHECK_METHOD (file или mds_dump).
    """
    if WRITE_CHECK_METHOD == "mds_dump":
        logger.debug("Проверка записи через mds_dump (без создания файла)")
        return is_storage_writable_via_mds_dump()
    # По умолчанию — реальная запись тестового файла.
    return is_storage_writable_via_file()


# ---------------------------------------------------------------------------
# Перезапуск MDS на целевом хосте
# ---------------------------------------------------------------------------
def restart_mds_on_host(host: str) -> bool:
    """
    Перезапускает службу MDS на целевом хосте.
    Команда по умолчанию: systemctl restart ceph-mds@<host>.service
    """
    cmd = MDS_RESTART_CMD_TEMPLATE.replace("{host}", host)
    logger.info("Перезапуск MDS на хосте %s: %s", host, cmd)
    rc, out, err = run_on_host(host, cmd, timeout=60)
    if rc == 0:
        logger.info(
            "MDS на хосте %s перезапущен успешно (rc=0). %s",
            host, out.strip()[:300] if out.strip() else "—"
        )
        return True
    logger.error(
        "Не удалось перезапустить MDS на хосте %s (rc=%s). stderr: %s stdout: %s",
        host, rc, err.strip()[:300], out.strip()[:300]
    )
    return False


# ---------------------------------------------------------------------------
# Машина состояний основного цикла
# ---------------------------------------------------------------------------
class MonitorState:
    def __init__(self) -> None:
        self.last_restart: Optional[float] = None      # метка времени последнего перезапуска MDS
        self.health_ok_since: Optional[float] = None   # когда впервые увидели HEALTH_OK в текущей серии
        self.in_cooldown: bool = False                 # пауза перезапусков


def evaluate(status: Optional[str], raw: str, state: MonitorState, now: float) -> None:
    """Одна итерация принятия решения."""
    if status is None:
        logger.warning("Не удалось определить статус Ceph — пропускаем итерацию")
        return

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
    writable = is_storage_writable()

    if writable:
        logger.info("WARN, но запись в %s доступна — ждём самовосстановления Ceph", CEPH_MOUNT)
        return

    # WARN + запись недоступна — необходимо действовать.

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

    # Первый discovery: определяем проблемный хост MDS из ошибки и перезапускаем его.
    faulty = extract_faulty_mds_hosts(raw)
    if not faulty:
        logger.error(
            "WARN и запись в CephFS недоступна, но проблемный MDS хост не определён из ошибки — "
            "перезапуск невозможен, ждём следующей проверки"
        )
        return

    restarted_any = False
    for host in sorted(faulty):
        logger.info(
            "Обнаружен проблемный MDS на хосте %s — перезапуск ceph-mds@%s.service", host, host
        )
        if restart_mds_on_host(host):
            restarted_any = True

    if restarted_any:
        state.last_restart = now
        state.in_cooldown = False
        logger.info(
            "Перезапуск MDS выполнен. Отсчёт %d мин до возможного повторного действия",
            COOLDOWN_WARN_MIN
        )
    else:
        logger.error("Не удалось перезапустить ни один проблемный MDS")


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
    parser.add_argument("--log-file", default=LOG_FILE,
                        help="Путь к файлу лога")
    parser.add_argument("--log-level", default=LOG_LEVEL,
                        help="Уровень логирования (DEBUG, INFO, …)")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    # Переопределяем настройки из аргументов.
    global CEPH_MOUNT, LOG_FILE, LOG_LEVEL, CHECK_INTERVAL
    CEPH_MOUNT = args.mount
    LOG_FILE = args.log_file
    LOG_LEVEL = args.log_level
    CHECK_INTERVAL = args.interval

    setup_logging(LOG_FILE, LOG_LEVEL)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info(
        "ceph-mds-monitor запущен. interval=%ds, mount=%s, status_host=%s, "
        "local_host=%s, write_check=%s, cooldown_warn=%dmin, health_ok_required=%dmin",
        CHECK_INTERVAL, CEPH_MOUNT or "<не задан>", CEPH_STATUS_HOST,
        LOCAL_HOST, WRITE_CHECK_METHOD, COOLDOWN_WARN_MIN, HEALTH_OK_MIN
    )

    state = MonitorState()

    while running:
        now = time.time()
        try:
            status, raw = get_cluster_status()
        except Exception as exc:
            logger.error("Не удалось получить статус ceph: %s", exc)
            if args.once:
                return 2
            time.sleep(CHECK_INTERVAL)
            continue

        evaluate(status, raw, state, now)

        if args.once:
            logger.info("Одиночная проверка завершена, статус: %s", status)
            return 0

        time.sleep(CHECK_INTERVAL)

    logger.info("ceph-mds-monitor остановлен")
    return 0


if __name__ == "__main__":
    sys.exit(main())
