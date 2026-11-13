#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ceph_mds_monitor_test.py
=======================

ТЕСТОВЫЙ СКРИПТ для ручных запусков и отладки логики мониторинга Ceph MDS.

Особенности этой версии:
  * Все настройки жёстко заданы внутри скрипта (в секции CONFIG),
    никаких внешних .env файлов не требуется.
  * Лог пишется в ту же папку, из которой запущен скрипт.
  * Максимально подробные комментарии на русском — каждый блок, каждая
    функция и каждое решение расписано.
  * Есть режим MOCK_MODE: если True, скрипт не обращается к реальному
    Ceph, а использует встроенные тестовые сценарии (WARN с ошибкой,
    WARN без ошибки, HEALTH_OK, HEALTH_ERR). Это позволяет отлаживать
    логику без настоящего кластера.
  * Поддерживает --once (один запуск и выход) и бесконечный цикл.

Перед использованием на реальном кластере отредактируйте секцию CONFIG
под свои хосты, пути и учётные данные.
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
from typing import List, Optional, Set, Tuple

# ===========================================================================
# СЕКЦИЯ CONFIG — здесь правите все настройки под свою среду
# ===========================================================================

# Если True — скрипт не обращается к реальному Ceph/SSH/docker,
# а использует встроенные тестовые данные (см. MOCK_SCENARIOS ниже).
# Установите False, когда будете тестировать на настоящем кластере.
MOCK_MODE = True

# Хост, с которого снимаем статус Ceph (статус кластера общий, достаточно одного)
CEPH_STATUS_HOST = "mst1"

# Префикс имени контейнера с ceph CLI на хосте статуса: ceph-mon-mst1
CEPH_CONTAINER_PREFIX = "ceph-mon-"

# Список всех хостов с MDS (для справки и возможного расширения)
CEPH_MON_HOSTS = ["mst1", "mst2", "mst3"]

# Пользователь для ssh на удалённые хосты (mst2, mst3).
SSH_USER = "youruser"

# Путь к приватному ключу ssh (оставьте пустым, если ключ по умолчанию ~/.ssh/id_rsa)
SSH_KEY = ""

# Дополнительные опции ssh (отключаем интерактивные запросы, короткий таймаут)
SSH_OPTS = "-o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"

# Префикс команд, требующих root (docker, systemctl).
# Оставьте пустым, если запускаете от root; укажите "sudo" для обычного пользователя.
SUDO = "sudo"

# Точка монтирования CephFS (только на mst1, где запускается скрипт)
CEPH_MOUNT = "/mnt/cephfs"

# Имя тестового файла для проверки возможности записи
CEPH_TEST_FILE = ".ceph-mds-monitor-write-test"

# Целевые признаки ошибок в ceph health detail, при которых скрипт реагирует
# на HEALTH_WARN. Если ни одного признака не найдено — WARN игнорируется.
# Список задаётся через запятую. Можно расширять под свои ошибки.
WARN_ERROR_PATTERNS = [
    "MDS_SLOW_REQUEST",
    "MDS_CLIENT_LATE_RELEASE",
    "slow requests",
    "failing to respond to capability release",
]

# Интервал проверок в секундах (в цикле)
CHECK_INTERVAL = 30

# Файл лога — будет создан в той же папке, где лежит этот скрипт
LOG_FILE = None  # будет установлено автоматически в папку скрипта ниже

# Уровень логирования: DEBUG, INFO, WARNING, ERROR
LOG_LEVEL = "DEBUG"

# Шаблон команды перезапуска MDS на целевом хосте. Подставляется имя хоста.
# Локально: sudo systemctl restart ceph-mds@{host}.service
# Удалённо: ssh {user}@{host} sudo systemctl restart ceph-mds@{host}.service
MDS_RESTART_CMD_TEMPLATE = "sudo systemctl restart ceph-mds@{host}.service"

# Если после перезапуска MDS в течение N минут статус всё ещё WARN —
# больше не перезапускать до HEALTH_OK в течение M минут.
COOLDOWN_WARN_MIN = 3
HEALTH_OK_MIN = 20

# Таймаут проверки записи в CephFS (секунды)
WRITE_TIMEOUT = 15

# -------------------------------------------------------------------------
# МОК-СЦЕНАРИИ (используются только если MOCK_MODE = True)
# -------------------------------------------------------------------------
# Каждый сценарий — словарь с ключами:
#   status  — HEALTH_OK / HEALTH_WARN / HEALTH_ERR / None
#   raw     — текст вывода ceph health detail (строка)
#   delay   — задержка перед этим сценарием (секунды, только для цикла)
MOCK_SCENARIOS = [
    {
        "name": "Сценарий 1: HEALTH_OK (всё хорошо)",
        "status": "HEALTH_OK",
        "raw": "HEALTH_OK",
        "delay": 2,
    },
    {
        "name": "Сценарий 2: HEALTH_WARN с целевой ошибкой MDS_SLOW_REQUEST "
               "и mds.frm-mst1 — запись недоступна -> должен быть перезапуск",
        "status": "HEALTH_WARN",
        "raw": (
            "HEALTH_WARN 1 MDSs report slow requests\n"
            "[WRN] MDS_SLOW_REQUEST: 1 MDSs report slow requests\n"
            "   mds.frm-mst1(mds.0): 4 slow requests are blocked > 30 secs\n"
        ),
        "delay": 2,
    },
    {
        "name": "Сценарий 3: HEALTH_WARN без целевых ошибок -> игнорируем",
        "status": "HEALTH_WARN",
        "raw": (
            "HEALTH_WARN 1 OSDs down\n"
            "[WRN] OSD_DOWN: 1 osds down\n"
            "   osd.1 is down\n"
        ),
        "delay": 2,
    },
    {
        "name": "Сценарий 4: HEALTH_ERR -> игнорируем",
        "status": "HEALTH_ERR",
        "raw": "HEALTH_ERR 1 OSDs down",
        "delay": 2,
    },
    {
        "name": "Сценарий 5: HEALTH_WARN с целевой ошибкой, запись доступна -> ждём",
        "status": "HEALTH_WARN",
        "raw": (
            "HEALTH_WARN 1 MDSs report slow requests\n"
            "[WRN] MDS_SLOW_REQUEST: 1 MDSs report slow requests\n"
            "   mds.frm-mst2(mds.1): 2 slow requests are blocked > 30 secs\n"
        ),
        "delay": 2,
    },
]

# Глобальный счётчик для поочерёдного перебора сценариев в цикле
_mock_scenario_index = 0

# -------------------------------------------------------------------------
# Автоматически вычисляемые пути (не править вручную)
# -------------------------------------------------------------------------
# Папка, в которой лежит этот скрипт — туда же будем писать лог.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if LOG_FILE is None:
    LOG_FILE = os.path.join(SCRIPT_DIR, "ceph_mds_monitor_test.log")

# Локальный хост (машина, на которой запущен скрипт)
LOCAL_HOST = os.environ.get("HOSTNAME") or subprocess.run(
    ["hostname"], capture_output=True, text=True
).stdout.strip()

# ===========================================================================
# СЕКЦИЯ ЛОГИРОВАНИЯ
# ===========================================================================

logger = logging.getLogger("ceph-mds-monitor-test")


def setup_logging(log_file: str, level: str) -> None:
    """
    Настраивает логирование:
      - в stdout (консоль)
      - в файл log_file (с ротацией по 10 МБ, хранит 5 копий)
    """
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # Обработчик консоли
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    # Обработчик файла (в той же папке, что и скрипт)
    try:
        fh = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.info("Файл лога: %s", log_file)
    except OSError as exc:
        logger.error("Не удалось открыть файл лога %s: %s", log_file, exc)


# ===========================================================================
# СЕКЦИЯ ВЫПОЛНЕНИЯ КОМАНД
# ===========================================================================

def run_cmd(cmd: List[str], timeout: int = 30, use_sudo: bool = False) -> Tuple[int, str, str]:
    """
    Выполняет команду в локальной системе и возвращает (returncode, stdout, stderr).

    Аргументы:
      cmd        — список аргументов команды (например, ["docker", "exec", ...]).
      timeout    — таймаут ожидания в секундах.
      use_sudo   — если True и SUDO не пуст, добавляет sudo перед командой.

    Возвращает кортеж (rc, stdout, stderr).
    """
    final_cmd: List[str] = []
    if use_sudo and SUDO:
        final_cmd += shlex.split(SUDO)
    final_cmd += cmd

    try:
        proc = subprocess.run(
            final_cmd, capture_output=True, text=True, timeout=timeout
        )
        return proc.returncode, proc.stdout, proc.stderr
    except subprocess.TimeoutExpired:
        return -1, "", f"command timed out after {timeout}s: {' '.join(final_cmd)}"
    except FileNotFoundError:
        return -1, "", f"command not found: {final_cmd[0]}"
    except Exception as exc:  # защитная ветка на любой непредвиденный случай
        return -1, "", f"exception running {' '.join(final_cmd)}: {exc}"


def run_on_host(host: str, cmd: str, timeout: int = 60) -> Tuple[int, str, str]:
    """
    Выполняет команду на указанном хосте.

    Локальный хост — напрямую (с sudo если нужно).
    Удалённые хосты — через ssh (с sudo на удалённой стороне для systemctl).

    Аргументы:
      host    — имя хоста (mst1, mst2, mst3 и т.д.).
      cmd     — строка команды для выполнения на том хосте.
      timeout — таймаут в секундах.

    Возвращает (rc, stdout, stderr).
    """
    # Если команда требует sudo, добавляем его перед самой командой
    full_cmd = cmd

    if host == LOCAL_HOST:
        # Локальный запуск с sudo (если настроено)
        return run_cmd(shlex.split(full_cmd), timeout=timeout, use_sudo=True)

    # Удалённый запуск через ssh
    ssh_args: List[str] = ["ssh"] + shlex.split(SSH_OPTS)
    if SSH_KEY:
        ssh_args += ["-i", SSH_KEY]

    # На удалённом хосте тоже нужны права root для systemctl
    remote_cmd = f"sudo {full_cmd}" if not full_cmd.startswith("sudo ") else full_cmd
    ssh_args += [f"{SSH_USER}@{host}", remote_cmd]

    return run_cmd(ssh_args, timeout=timeout, use_sudo=False)


# ===========================================================================
# СЕКЦИЯ ОПРОСА CEPH
# ===========================================================================

def ceph_health_on_host(host: str) -> Tuple[Optional[str], str]:
    """
    Снимает статус Ceph с одного хоста через docker exec.

    Возвращает (status, raw_output):
      status   — HEALTH_OK / HEALTH_WARN / HEALTH_ERR / None
      raw_output — полный текст вывода ceph health detail
    """
    container = f"{CEPH_CONTAINER_PREFIX}{host}"
    cmd = f"docker exec {container} ceph health detail"
    rc, out, err = run_on_host(host, cmd, timeout=60)
    if rc != 0:
        logger.debug(
            "ceph health detail на %s не удался (rc=%s): %s",
            host, rc, err.strip()[:200]
        )
        return None, f"error: {err.strip()}"

    raw = out.strip()
    status: Optional[str] = None

    # Первая строка вывода ceph health detail имеет вид:
    #   HEALTH_WARN 1 MDSs report slow requests
    #   HEALTH_OK
    #   HEALTH_ERR ...
    first_line = raw.splitlines()[0] if raw.splitlines() else ""
    m = re.match(r"^(HEALTH_OK|HEALTH_WARN|HEALTH_ERR)\b", first_line)
    if m:
        status = m.group(1)

    return status, raw


def get_cluster_status() -> Tuple[Optional[str], str]:
    """
    Снимает статус Ceph только с одного хоста (CEPH_STATUS_HOST, по умолчанию mst1).
    Статус кластера общий, проверять все три хоста не нужно.

    Возвращает (status, raw_output).
    """
    return ceph_health_on_host(CEPH_STATUS_HOST)


def get_mock_status() -> Tuple[Optional[str], str]:
    """
    Возвращает следующий тестовый сценарий из MOCK_SCENARIOS.
    Используется только когда MOCK_MODE = True.

    Позволяет отлаживать логику без реального кластера Ceph.
    """
    global _mock_scenario_index
    scenario = MOCK_SCENARIOS[_mock_scenario_index % len(MOCK_SCENARIOS)]
    _mock_scenario_index += 1
    logger.info("МОК: используется сценарий '%s'", scenario["name"])
    return scenario["status"], scenario["raw"]


# ===========================================================================
# СЕКЦИЯ АНАЛИЗА ОШИБОК
# ===========================================================================

def has_target_error_patterns(raw: str) -> bool:
    """
    Проверяет, содержит ли вывод ceph health detail хотя бы один из
    целевых признаков ошибки (из списка WARN_ERROR_PATTERNS).

    Это позволяет не реагировать на каждый случайный WARN,
    а только на те, которые связаны с проблемами MDS (slow requests,
    capability release и т.д.).

    Возвращает True, если найден хотя бы один паттерн, иначе False.
    """
    for pattern in WARN_ERROR_PATTERNS:
        if re.search(re.escape(pattern), raw, flags=re.IGNORECASE):
            logger.debug("Найден целевой признак ошибки: %s", pattern)
            return True
    logger.debug("Целевые признаки ошибки не найдены в выводе ceph health detail")
    return False


def extract_faulty_mds_hosts(raw: str) -> Set[str]:
    """
    Извлекает из текста ceph health detail имена хостов с проблемными MDS.

    Ожидаемый формат строки в ошибке:
        mds.frm-mst1(mds.0): 4 slow requests are blocked > 30 secs
    Извлекается часть после mds. и до скобки: frm-mst1

    Возвращает множество имён хостов, например {'frm-mst1'} или {'mst1'}.
    """
    hosts: Set[str] = set()

    # Основной формат: mds.<имя_хоста>(mds.<инстанс>)
    for match in re.finditer(r"mds\.([A-Za-z0-9_.\-]+)\s*\(mds\.[^)]+\)", raw):
        hosts.add(match.group(1))

    # Fallback: упоминания mds.<имя> без скобок (если формат вывода отличается)
    if not hosts:
        for match in re.finditer(r"\bmds\.([A-Za-z0-9_.\-]+)", raw):
            hosts.add(match.group(1))

    return hosts


# ===========================================================================
# СЕКЦИЯ ПРОВЕРКИ ЗАПИСИ В CEPHFS
# ===========================================================================

def is_storage_writable() -> bool:
    """
    Проверяет, что в CephFS можно записать данные.

    Создаёт, пишет и удаляет тестовый файл в точке монтирования с таймаутом.
    Это единственный надёжный способ узнать, что запись действительно работает:
    чтение может быть доступно, а запись — нет (клиенты не могут писать,
    хотя монтирование живо).

    Возвращает True, если запись прошла успешно, иначе False.
    """
    mount = CEPH_MOUNT
    if not mount:
        return True
    if not os.path.ismount(mount):
        logger.debug(
            "Точка монтирования %s не является активной точкой монтирования", mount
        )
        return False

    test_file = os.path.join(mount, CEPH_TEST_FILE)
    quoted_file = shlex.quote(test_file)

    # Команда: записать строку в файл, прочитать её обратно, удалить файл.
    # Всё оборачиваем в timeout, чтобы не зависнуть на мёртвом клиенте CephFS.
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
            "Проверка записи не прошла: rc=%s out=%s err=%s",
            rc, out.strip()[:100], err.strip()[:100]
        )
        return False
    except Exception as exc:
        logger.debug("Исключение при проверке записи: %s", exc)
        return False


# ===========================================================================
# СЕКЦИЯ ПЕРЕЗАПУСКА MDS
# ===========================================================================

def restart_mds_on_host(host: str) -> bool:
    """
    Перезапускает службу MDS на целевомホсте.

    Использует шаблон MDS_RESTART_CMD_TEMPLATE, в который подставляется имя хоста.
    Локально (mst1) выполняется напрямую с sudo,
    на удалённых хостах — через ssh с sudo на удалённой стороне.

    Возвращает True при успешном перезапуске, иначе False.
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


# ===========================================================================
# СЕКЦИЯ ОСНОВНОЙ ЛОГИКИ (машина состояний)
# ===========================================================================

class MonitorState:
    """
    Хранит состояние монитора между итерациями цикла.

    Атрибуты:
      last_restart     — время последнего перезапуска MDS (timestamp) или None.
      health_ok_since  — время, когда впервые зафиксирован HEALTH_OK в текущей серии.
      in_cooldown      — флаг паузы перезапусков (True = перезапуски запрещены).
    """
    def __init__(self) -> None:
        self.last_restart: Optional[float] = None
        self.health_ok_since: Optional[float] = None
        self.in_cooldown: bool = False


def evaluate(
    status: Optional[str],
    raw: str,
    state: MonitorState,
    now: float,
) -> None:
    """
    Одна итерация принятия решения на основе текущего статуса Ceph.

    Аргументы:
      status — текущий статус кластера (HEALTH_OK / HEALTH_WARN / HEALTH_ERR / None).
      raw    — полный текст вывода ceph health detail.
      state  — объект MonitorState с текущим состоянием.
      now    — текущее время (timestamp, float).

    Логика:
      1. HEALTH_OK — сбрасываем паузу, если продержался 20 мин подряд.
      2. HEALTH_ERR — игнорируем.
      3. HEALTH_WARN + нет целевых ошибок — ждём самовосстановления.
      4. HEALTH_WARN + целевая ошибка + запись доступна — ждём самовосстановления.
      5. HEALTH_WARN + целевая ошибка + запись недоступна:
         - если в паузе — ничего не делаем.
         - если прошло меньше 3 мин после последнего перезапуска — наблюдаем.
         - если прошло 3 мин и более — вводим паузу до HEALTH_OK 20 мин.
         - иначе (первый раз) — ищем проблемный MDS и перезапускаем его.
    """
    if status is None:
        logger.warning("Не удалось определить статус Ceph — пропускаем итерацию")
        return

    logger.info("Статус Ceph: %s", status)

    # ---------- HEALTH_OK ----------
    if status == "HEALTH_OK":
        if state.health_ok_since is None:
            state.health_ok_since = now
            logger.info(
                "Зафиксирован HEALTH_OK — Started 20-минутный отсчёт для сброса паузы"
            )
        # Если в паузе и HEALTH_OK держится уже 20 мин — снимаем паузу.
        if state.in_cooldown and (now - state.health_ok_since) >= HEALTH_OK_SEC:
            logger.info(
                "HEALTH_OK сохраняется уже %d мин — пауза перезапусков снята, "
                "мониторинг возвращён в обычный режим",
                HEALTH_OK_MIN,
            )
            state.in_cooldown = False
            state.last_restart = None
        return

    # Любой статус, кроме OK, сбрасывает серию HEALTH_OK.
    state.health_ok_since = None

    # ---------- HEALTH_ERR ----------
    if status == "HEALTH_ERR":
        logger.info(
            "Статус HEALTH_ERR — игнорируется по заданию (реагируем только на WARN)"
        )
        return

    # ---------- другие статусы ----------
    if status != "HEALTH_WARN":
        logger.info("Статус %s — нет конкретных действий в инструкции", status)
        return

    # ---------- HEALTH_WARN ----------

    # Реагируем только если в выводе есть целевые признаки ошибки.
    if not has_target_error_patterns(raw):
        logger.info(
            "WARN, но целевые признаки ошибки (%s) не найдены — ждём самовосстановления",
            WARN_ERROR_PATTERNS,
        )
        return

    # Проверяем запись в CephFS.
    writable = is_storage_writable()

    if writable:
        logger.info(
            "WARN с целевой ошибкой, но запись в %s доступна — ждём самовосстановления Ceph",
            CEPH_MOUNT,
        )
        return

    # ---------- WARN + целевая ошибка + запись недоступна — нужно действовать ----------

    # Если в паузе — перезапуски запрещены.
    if state.in_cooldown:
        logger.info(
            "НА ПАУЗЕ: перезапуск MDS запрещён. Ожидание HEALTH_OK в течение %d мин.",
            HEALTH_OK_MIN,
        )
        return

    # Если был недавний перезапуск (меньше 3 мин назад) — наблюдаем.
    if state.last_restart is not None:
        elapsed = now - state.last_restart
        if elapsed < COOLDOWN_WARN_SEC:
            logger.info(
                "WARN после перезапуска MDS: прошло %.0f сек из %d мин — наблюдаем, не перезапускаем",
                elapsed,
                COOLDOWN_WARN_MIN,
            )
            return
        # 3 минуты прошли, статус всё ещё WARN — вводим паузу.
        logger.info(
            "WARN сохраняется уже %.0f мин (>= %d мин) после последнего перезапуска — "
            "вводим паузу до HEALTH_OK в течение %d мин",
            elapsed / 60,
            COOLDOWN_WARN_MIN,
            HEALTH_OK_MIN,
        )
        state.in_cooldown = True
        return

    # Первый обнаруженный случай: ищем проблемный MDS и перезапускаем его.
    faulty = extract_faulty_mds_hosts(raw)
    if not faulty:
        logger.error(
            "WARN с целевой ошибкой и запись в CephFS недоступна, но проблемный MDS хост "
            "не определён из ошибки — перезапуск невозможен, ждём следующей проверки"
        )
        return

    restarted_any = False
    for host in sorted(faulty):
        logger.info(
            "Обнаружен проблемный MDS на хосте %s — перезапуск ceph-mds@%s.service",
            host,
            host,
        )
        if restart_mds_on_host(host):
            restarted_any = True

    if restarted_any:
        state.last_restart = now
        state.in_cooldown = False
        logger.info(
            "Перезапуск MDS выполнен. Отсчёт %d мин до возможного повторного действия",
            COOLDOWN_WARN_MIN,
        )
    else:
        logger.error("Не удалось перезапустить ни один проблемный MDS")


# ===========================================================================
# СЕКЦИЯ ТОЧКИ ВХОДА
# ===========================================================================

running = True


def handle_signal(signum, frame):
    """Обработчик сигналов SIGTERM/SIGINT для корректной остановки скрипта."""
    global running
    logger.info("Получен сигнал %s — завершение работы", signum)
    running = False


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Парсит аргументы командной строки."""
    parser = argparse.ArgumentParser(
        description="Тестовый мониторинг Ceph WARN с автоматическим перезапуском проблемного MDS"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Одна проверка и выход (для отладки)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=CHECK_INTERVAL,
        help="Интервал проверок в секундах (по умолчанию 30)",
    )
    parser.add_argument(
        "--mount",
        default=CEPH_MOUNT,
        help="Точка монтирования CephFS",
    )
    parser.add_argument(
        "--log-file",
        default=LOG_FILE,
        help="Путь к файлу лога",
    )
    parser.add_argument(
        "--log-level",
        default=LOG_LEVEL,
        help="Уровень логирования (DEBUG, INFO, WARNING, ERROR)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    """
    Основная точка входа.

    Инициализирует логирование, обрабатывает сигналы и запускает цикл мониторинга.
    """
    args = parse_args(argv)

    # Переопределяем настройки из аргументов командной строки.
    global CEPH_MOUNT, LOG_FILE, LOG_LEVEL, CHECK_INTERVAL
    CEPH_MOUNT = args.mount
    LOG_FILE = args.log_file
    LOG_LEVEL = args.log_level
    CHECK_INTERVAL = args.interval

    setup_logging(LOG_FILE, LOG_LEVEL)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info(
        "ceph-mds-monitor-test запущен. interval=%ds, mount=%s, status_host=%s, "
        "local_host=%s, mock_mode=%s, sudo=%s, warn_patterns=%s, "
        "cooldown_warn=%dmin, health_ok_required=%dmin",
        CHECK_INTERVAL,
        CEPH_MOUNT or "<не задан>",
        CEPH_STATUS_HOST,
        LOCAL_HOST,
        MOCK_MODE,
        SUDO or "<нет>",
        ", ".join(WARN_ERROR_PATTERNS),
        COOLDOWN_WARN_MIN,
        HEALTH_OK_MIN,
    )

    state = MonitorState()

    while running:
        now = time.time()

        # Получаем статус: либо из реального Ceph, либо из мок-сценариев.
        try:
            if MOCK_MODE:
                status, raw = get_mock_status()
            else:
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

    logger.info("ceph-mds-monitor-test остановлен")
    return 0


if __name__ == "__main__":
    sys.exit(main())
