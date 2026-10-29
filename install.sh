#!/usr/bin/env bash
# Вспомогательный скрипт установки ceph-mds-monitor на целевой сервер (mst1).
# Требуются права root. Запускать на целевом хосте, а не в этой песочнице.
set -euo pipefail

SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="/usr/local/share/ceph-mds-monitor"
BIN_FILE="/usr/local/bin/ceph_mds_monitor.py"
ENV_FILE="/etc/default/ceph-mds-monitor"
SERVICE_FILE="/etc/systemd/system/ceph-mds-monitor.service"

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "Запустите от имени root (sudo)." >&2
  exit 1
fi

echo "[1/5] Копирование файлов..."
mkdir -p "$INSTALL_DIR"
cp "$SRC_DIR/ceph_mds_monitor.py" "$BIN_FILE"
chmod 0755 "$BIN_FILE"
cp "$SRC_DIR/ceph-mds-monitor.service" "$SERVICE_FILE"
if [[ ! -f "$ENV_FILE" ]]; then
  cp "$SRC_DIR/ceph-mds-monitor.env.example" "$ENV_FILE"
  chmod 0600 "$ENV_FILE"
fi

echo "[2/5] Проверка зависимостей..."
missing=0
for cmd in python3 docker systemctl ceph; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "  !! не найдено: $cmd (обратите внимание: docker/ceph нужны на целевом хосте)"
    missing=1
  fi
done
[[ "$missing" -eq 0 ]] || echo "  Продолжаем — некоторые зависимости могут быть только на целевом хосте."

echo "[3/5] Перезагрузка systemd и запуск службы..."
systemctl daemon-reload
systemctl enable --now ceph-mds-monitor

echo "[4/5] Статус службы..."
systemctl --no-pager status ceph-mds-monitor --lines=15 || true

echo "[5/5] Готово."
echo "Отредактируйте $ENV_FILE под свой кластер, затем: systemctl restart ceph-mds-monitor"
echo "Логи: journalctl -u ceph-mds-monitor -f   или   tail -f /var/log/ceph-mds-monitor.log"
