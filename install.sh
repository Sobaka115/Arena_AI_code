#!/usr/bin/env bash
# Вспомогательный скрипт установки ceph-mds-monitor.
# Разработан для обычного пользователя с правами sudo (без пароля root).
# Запускать на целевом хосте (mst1).
set -euo pipefail

SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
USER_HOME="${HOME}"
INSTALL_DIR="${USER_HOME}/ceph-mds-monitor"
BIN_FILE="${INSTALL_DIR}/ceph_mds_monitor.py"
ENV_FILE="${INSTALL_DIR}/ceph-mds-monitor.env"
SYSTEMD_USER_DIR="${USER_HOME}/.config/systemd/user"
SERVICE_FILE="${SYSTEMD_USER_DIR}/ceph-mds-monitor.service"

echo "[1/5] Создание директорий..."
mkdir -p "$INSTALL_DIR" "$SYSTEMD_USER_DIR"

cp "$SRC_DIR/ceph_mds_monitor.py" "$BIN_FILE"
chmod 0755 "$BIN_FILE"

if [[ ! -f "$ENV_FILE" ]]; then
  cp "$SRC_DIR/ceph-mds-monitor.env.example" "$ENV_FILE"
  chmod 0600 "$ENV_FILE"
fi

echo "[2/5] Проверка зависимостей..."
missing=0
for cmd in python3 docker ssh sudo systemctl; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "  !! не найдено: $cmd"
    missing=1
  fi
done
[[ "$missing" -eq 0 ]] || echo "  Продолжаем — некоторые команды могут быть доступны через sudo."

echo "[3/5] Настройка sudo (если требуется)..."
echo "Убедитесь, что ваш пользователь может без пароля выполнять через sudo:"
echo "  sudo docker exec ceph-mon-mst1 ceph health detail"
echo "  sudo systemctl restart ceph-mds@mst1.service"
echo ""
echo "Для этого добавьте в /etc/sudoers (через sudo visudo):"
echo "  %yourgroup ALL=(ALL) NOPASSWD: /usr/bin/docker exec ceph-mon-mst1 ceph health detail"
echo "  youruser ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart ceph-mds@*.service"
echo ""

echo "[4/5] Настройка SSH для удалённых хостов (mst2, mst3)..."
echo "Убедитесь, что с этого хоста (mst1) есть ssh-доступ без пароля на mst2 и mst3:"
echo "  ssh-copy-id $SSH_USER@mst2   (замените $SSH_USER на вашего пользователя)"
echo "  ssh-copy-id $SSH_USER@mst3"
echo "И проверьте: ssh $SSH_USER@mst2 hostname"
echo ""
echo "Также на удалённых хостах нужно настроить sudo без пароля для systemctl restart ceph-mds@*.service"

echo "[5/5] Создание systemd пользовательского сервиса..."
cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=Ceph MDS WARN monitor with auto-restart of faulty MDS
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=${ENV_FILE}
ExecStart=${BIN_FILE}
Restart=always
RestartSec=10
WorkingDirectory=${INSTALL_DIR}
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=default.target
EOF

echo "Готово."
echo ""
echo "=== Дальнейшие действия ==="
echo "1. Отредактируйте $ENV_FILE под свой кластер (в частности SSH_USER, CEPH_MOUNT, SUDO)."
echo "2. Настройте sudo без пароля для docker и systemctl (см. пункт 3 выше)."
echo "3. Настройте ssh без пароля на mst2 и mst3 (см. пункт 4 выше)."
echo "4. Запустите сервис:"
echo "   systemctl --user daemon-reload"
echo "   systemctl --user enable --now ceph-mds-monitor"
echo "5. Логи:"
echo "   journalctl --user -u ceph-mds-monitor -f"
echo "   или: tail -f $ENV_FILE  (замените на реальный LOG_FILE из env)"
echo ""
echo "Разовый запуск для отладки:"
echo "  $BIN_FILE --once"
