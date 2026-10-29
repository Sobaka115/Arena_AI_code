# Ceph MDS WARN Monitor

Обходное решение для проблемы с Ceph в Docker на серверах **mst1, mst2, mst3**.

## Проблема

Ceph иногда переходит в статус `WARN` с ошибками **slow request** / **clients failing to respond to capability release** (MDS):

* иногда сам выходит из `WARN` без влияния на работу хранилища (SFTP/ProFTPD);
* иногда при тех же ошибках хранилище становится недоступным для записи клиентами (чтение может остаться доступным);
* помогает только перезапуск MDS на сервере, указанном в ошибке.

Причины ошибки пока не установлены — нужен обходной манёвр: частая проверка статуса,
разделение «безопасного» WARN (запись в CephFS доступна) и «опасного» WARN (запись невозможна),
перезапуск только проблемного MDS с защитой от бесконечных перезапусков.

## Что делает скрипт (`ceph_mds_monitor.py`)

1. Каждые `CHECK_INTERVAL` секунд (по умолчанию 30 с) снимает статус Ceph **только на mst1**:

   ```bash
   docker exec ceph-mon-mst1 ceph health detail
   ```

   Статус кластера общий, проверять на всех трёх хостах не требуется.

2. Реагирует **только на `HEALTH_WARN`**. `HEALTH_ERR` игнорируется.

3. Если статус `WARN`, но **запись** в CephFS на `mst1` доступна
   (создание/запись/удаление тестового файла) — просто ждёт и пишет в лог,
   не делая перезапусков. Чтение может работать, а запись — нет, поэтому
   мониторится именно запись.

4. Если статус `WARN` **и** запись в CephFS недоступна — из текста ошибки
   извлекает имя проблемного MDS (`mds.<хост>(mds.<N>)` → хост `<хост>`)
   и перезапускает **только его**:

   ```bash
   systemctl restart ceph-mds@<хост>.service
   ```

   (на целевом хосте — локально, если это mst1, иначе через ssh).

5. После перезапуска отслеживает состояние:
   * если в течение **3 минут** статус всё ещё `WARN` — вводит паузу: больше не
     перезапускает MDS, пока статус `HEALTH_OK` не продержится **20 минут подряд**.
   * как только `HEALTH_OK` сохраняется 20 минут — пауза снимается, мониторинг
     возвращается в обычный режим.

Все действия подробно логируются в stdout и в файл (с ротацией).

## Пример ошибки, которую разбирает скрипт

```
HEALTH_WARN 2 clients failing to respond to capability release; 1 MDSs report slow requests

[WRN] MDS_CLIENT_LATE_RELEASE: 2 clients failing to respond to capability release
   mds.frm-mst1(mds.0): Client frm-adt-mst2 failing to respond to capability release client_id: 21960720
   mds.frm-mst1(mds.0): Client frm-data2 failing to respond to capability release client_id: 21960783

[WRN] MDS_SLOW_REQUEST: 1 MDSs report slow requests
   mds.frm-mst1(mds.0): 4 slow requests are blocked > 30 secs
```

Из таких строк скрипт выделяет хост `frm-mst1` (или `mst1` в вашей среде) и выполняет
`systemctl restart ceph-mds@frm-mst1.service` на этом хосте.

## Проверка записи: с файлом или без?

### Метод "file" (по умолчанию, рекомендуется)
Скрипт создаёт, пишет и удаляет тестовый файл `.ceph-mds-monitor-write-test` в точке
монтирования CephFS с таймаутом. Это единственный способ достоверно узнать, что запись
действительно работает — чтение может быть доступно, а запись — нет.

### Метод "mds_dump" (без создания файлов)
Если по каким-то причинам нельзя создавать файлы в CephFS, можно использовать
`WRITE_CHECK_METHOD="mds_dump"`. Тогда скрипт анализирует вывод `ceph mds dump`:
если проблемный хост из ошибки не в списке `up` или находится в `laggy`/`down`,
запись считается недоступной.

**Важно:** `mds_dump` менее достоверен — MDS может быть в `up`, но клиенты всё
равно не могут писать (не отвечают на capability release). Если возможно,
используйте метод `file`.

## Структура репозитория

```
ceph_mds_monitor.py           # основной скрипт мониторинга (Python 3)
ceph-mds-monitor.service      # systemd-юнит для запуска как службы на mst1
ceph-mds-monitor.env.example  # пример файла переменных окружения
install.sh                    # вспомогательный скрипт установки
README.md                     # эта инструкция
```

## Установка на сервере mst1

Требуются права root (доступ к Docker, ssh, systemd, точкам монтирования).

### 1. Скопировать файлы

```bash
sudo mkdir -p /usr/local/share/ceph-mds-monitor
sudo cp ceph_mds_monitor.py /usr/local/bin/ceph_mds_monitor.py
sudo chmod +x /usr/local/bin/ceph_mds_monitor.py
sudo cp ceph-mds-monitor.service /etc/systemd/system/ceph-mds-monitor.service
sudo cp ceph-mds-monitor.env.example /etc/default/ceph-mds-monitor
```

### 2. Отредактировать конфигурацию

```bash
sudo nano /etc/default/ceph-mds-monitor
```

Минимум что нужно проверить:

| Переменная | Что указать |
|---|---|
| `CEPH_STATUS_HOST` | Хост, с которого снимать статус (по умолчанию `mst1`) |
| `CEPH_CONTAINER_PREFIX` | Префикс контейнера с ceph CLI, по умолчанию `ceph-mon-` |
| `CEPH_MON_HOSTS` | Список всех хостов с MDS, например `mst1 mst2 mst3` |
| `SSH_USER` | Пользователь для ssh на `mst2`/`mst3` (по умолчанию `root`) |
| `SSH_KEY` | Путь к приватному ключу ssh (опционально) |
| `CEPH_MOUNT` | Точка монтирования CephFS на mst1, например `/mnt/cephfs` |
| `WRITE_CHECK_METHOD` | `file` (рекомендуется) или `mds_dump` |
| `MDS_RESTART_CMD` | Команда перезапуска, по умолчанию `systemctl restart ceph-mds@{host}.service` |
| `CHECK_INTERVAL` | Как часто проверять (сек), по умолчанию 30 |

### 3. SSH без пароля на удалённые хосты

Скрипт запускается на `mst1`, но может перезапускать MDS на `mst2` и `mst3` через ssh.
Настройте беспарольный доступ:

```bash
ssh-copy-id root@mst2
ssh-copy-id root@mst3
ssh root@mst2 hostname   # должно выдать mst2 без запроса пароля
ssh root@mst3 hostname   # должно выдать mst3 без запроса пароля
```

### 4. Запустить через systemd

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now ceph-mds-monitor
sudo systemctl status ceph-mds-monitor
```

### 5. Просмотр логов

```bash
sudo journalctl -u ceph-mds-monitor -f
# или файл лога, указанный в LOG_FILE:
tail -f /var/log/ceph-mds-monitor.log
```

## Примеры записей в логе

```
2026-10-29 12:00:00 [INFO] Статус Ceph: HEALTH_WARN
2026-10-29 12:00:00 [DEBUG] Проверка записи (file) не прошла: rc=124 ...
2026-10-29 12:00:00 [INFO] WARN, но запись в /mnt/cephfs недоступна — необходимо действовать
2026-10-29 12:00:00 [INFO] Обнаружен проблемный MDS на хосте mst1 — перезапуск ceph-mds@mst1.service
2026-10-29 12:00:00 [INFO] Перезапуск MDS на хосте mst1: systemctl restart ceph-mds@mst1.service
2026-10-29 12:00:00 [INFO] MDS на хосте mst1 перезапущен успешно (rc=0). —
2026-10-29 12:00:00 [INFO] Перезапуск MDS выполнен. Отсчёт 3 мин до возможного повторного действия
2026-10-29 12:03:00 [INFO] Статус Ceph: HEALTH_WARN
2026-10-29 12:03:00 [INFO] WARN после перезапуска MDS: прошло 180 сек из 3 мин — наблюдаем, не перезапускаем
2026-10-29 12:06:00 [INFO] WARN сохраняется уже 3.00 мин (>= 3 мин) после последнего перезапуска — вводим паузу до HEALTH_OK в течение 20 мин
...
2026-10-29 12:26:00 [INFO] HEALTH_OK сохраняется уже 20 мин — пауза перезапусков снята
```

## Безопасность

* Перезапуск выполняется только для MDS на хосте, названном в ошибке (`mds.<host>`).
* Нет перезапуска «на всякий случай» — только при `WARN` + недоступности записи в CephFS.
* Защита от зацикливания: после неудачного перезапуска пауза до `HEALTH_OK` (20 мин).
* Статус `HEALTH_ERR` не вызывает действий по умолчанию.

## Разовый запуск для отладки

```bash
python3 ceph_mds_monitor.py --once
```

## Лицензия

Внутреннее обходное решение, без гарантий. Перед вводом в эксплуатацию протестируйте на
stand-окружении.
