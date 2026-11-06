# Ceph MDS WARN Monitor

Обходное решение для проблемы с Ceph в Docker на серверах **mst1, mst2, mst3**.

Скрипт предназначен для запуска от **обычного пользователя с правами sudo** (без пароля root).

## Проблема

Ceph иногда переходит в статус `WARN` с ошибками **slow request** / **clients failing to respond to capability release** (MDS):

* иногда сам выходит из `WARN` без влияния на работу хранилища (SFTP/ProFTPD);
* иногда при тех же ошибках хранилище становится недоступным для записи клиентами (чтение может остаться доступным);
* помогает только перезапуск MDS на сервере, указанном в ошибке.

Причины ошибки пока не установлены — нужен обходной манёвр: частая проверка статуса,
фильтрация WARN по целевым признакам ошибки, разделение «безопасного» WARN (запись в CephFS доступна)
и «опасного» WARN (запись невозможна), перезапуск только проблемного MDS с защитой от бесконечных перезапусков.

## Что делает скрипт (`ceph_mds_monitor.py`)

1. Каждые `CHECK_INTERVAL` секунд (по умолчанию 30 с) снимает статус Ceph **только на mst1**:

   ```bash
   sudo docker exec ceph-mon-mst1 ceph health detail
   ```

   Статус кластера общий, проверять на всех трёх хостах не требуется.

2. Реагирует **только на `HEALTH_WARN`**. `HEALTH_ERR` игнорируется.

3. Если статус `WARN`, но в выводе `ceph health detail` **не найден ни один из
   целевых признаков ошибки** (см. `WARN_ERROR_PATTERNS`) — скрипт просто ждёт
   самовосстановления, не перезапуская MDS. Список признаков расширяется через
   переменную окружения, пример по умолчанию:

   ```
   MDS_SLOW_REQUEST,MDS_CLIENT_LATE_RELEASE,slow requests,failing to respond to capability release
   ```

4. Если статус `WARN` **и** найден целевой признак ошибки, но **запись**
   в CephFS на `mst1` доступна (создание/запись/удаление тестового файла) —
   просто ждёт и пишет в лог, не делая перезапусков. Чтение может работать,
   а запись — нет, поэтому мониторится именно запись.

5. Если статус `WARN` **и** найден целевой признак ошибки **и** запись в CephFS
   недоступна — из текста ошибки извлекает имя проблемного MDS
   (`mds.<хост>(mds.<N>)` → хост `<хост>`) и перезапускает **только его**:

   ```bash
   sudo systemctl restart ceph-mds@<хост>.service   (локально на mst1)
   ssh <user>@<хост> sudo systemctl restart ceph-mds@<хост>.service  (удалённо)
   ```

6. После перезапуска отслеживает состояние:
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
`sudo systemctl restart ceph-mds@frm-mst1.service` на этом хосте.

## Проверка записи

Скрипт создаёт, пишет и удаляет тестовый файл `.ceph-mds-monitor-write-test` в точке
монтирования CephFS с таймаутом. Это единственный способ достоверно узнать, что запись
действительно работает — чтение может быть доступно, а запись — нет.

## Структура репозитория

```
ceph_mds_monitor.py           # основной скрипт мониторинга (Python 3)
ceph-mds-monitor.service      # systemd пользовательский сервис (создаётся install.sh)
ceph-mds-monitor.env.example  # пример файла переменных окружения
install.sh                    # вспомогательный скрипт установки
README.md                     # эта инструкция
```

## Установка на сервере mst1

Скрипт запускается от обычного пользователя с правами `sudo`. Права root не требуются.

### 1. Скопировать файлы

```bash
mkdir -p ~/ceph-mds-monitor
cp ceph_mds_monitor.py ~/ceph-mds-monitor/
chmod +x ~/ceph-mds-monitor/ceph_mds_monitor.py
cp ceph-mds-monitor.env.example ~/ceph-mds-monitor/ceph-mds-monitor.env
```

### 2. Отредактировать конфигурацию

```bash
nano ~/ceph-mds-monitor/ceph-mds-monitor.env
```

Минимум что нужно проверить:

| Переменная | Что указать |
|---|---|
| `CEPH_STATUS_HOST` | Хост, с которого снимать статус (по умолчанию `mst1`) |
| `CEPH_CONTAINER_PREFIX` | Префикс контейнера с ceph CLI, по умолчанию `ceph-mon-` |
| `CEPH_MON_HOSTS` | Список всех хостов с MDS, например `mst1 mst2 mst3` |
| `SSH_USER` | Пользователь для ssh на `mst2`/`mst3` |
| `SSH_KEY` | Путь к приватному ключу ssh (опционально) |
| `SUDO` | Укажите `sudo`, если запускаете не от root |
| `CEPH_MOUNT` | Точка монтирования CephFS на mst1, например `/mnt/cephfs` |
| `WARN_ERROR_PATTERNS` | Признаки ошибок через запятую, на которые реагировать при WARN |
| `MDS_RESTART_CMD` | Команда перезапуска, по умолчанию `sudo systemctl restart ceph-mds@{host}.service` |
| `CHECK_INTERVAL` | Как часто проверять (сек), по умолчанию 30 |
| `LOG_FILE` | Путь к файлу лога (по умолчанию `~/ceph-mds-monitor.log`) |

### 3. Настройка sudo без пароля

Ваш пользователь должен иметь возможность без пароля выполнять через `sudo`:
* `docker exec ceph-mon-mst1 ceph health detail` (для опроса статуса)
* `systemctl restart ceph-mds@*.service` (для перезапуска MDS)

Добавьте в `/etc/sudoers` (через `sudo visudo`):

```
youruser ALL=(ALL) NOPASSWD: /usr/bin/docker exec ceph-mon-mst1 ceph health detail
youruser ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart ceph-mds@*.service
```

### 4. Настройка ssh без пароля на удалённые хосты

Скрипт запускается на `mst1`, но может перезапускать MDS на `mst2` и `mst3` через ssh.
Настройте беспарольный доступ:

```bash
ssh-copy-id youruser@mst2
ssh-copy-id youruser@mst3
ssh youruser@mst2 hostname   # должно выдать mst2 без запроса пароля
ssh youruser@mst3 hostname   # должно выдать mst3 без запроса пароля
```

Также на удалённыхホстах нужно настроить `sudo` без пароля для
`systemctl restart ceph-mds@*.service`.

### 5. Запуск через systemd (пользовательский сервис)

```bash
./install.sh
systemctl --user daemon-reload
systemctl --user enable --now ceph-mds-monitor
systemctl --user status ceph-mds-monitor
```

### 6. Просмотр логов

```bash
journalctl --user -u ceph-mds-monitor -f
# или файл лога, указанный в LOG_FILE:
tail -f ~/ceph-mds-monitor.log
```

## Примеры записей в логе

```
2026-10-29 12:00:00 [INFO] Статус Ceph: HEALTH_WARN
2026-10-29 12:00:00 [DEBUG] Найден целевой признак ошибки: MDS_SLOW_REQUEST
2026-10-29 12:00:00 [DEBUG] Проверка записи не прошла: rc=124 ...
2026-10-29 12:00:00 [INFO] WARN с целевой ошибкой, но запись в /mnt/cephfs недоступна — необходимо действовать
2026-10-29 12:00:00 [INFO] Обнаружен проблемный MDS на хосте mst1 — перезапуск ceph-mds@mst1.service
2026-10-29 12:00:00 [INFO] Перезапуск MDS на хосте mst1: sudo systemctl restart ceph-mds@mst1.service
2026-10-29 12:00:00 [INFO] MDS на хосте mst1 перезапущен успешно (rc=0). —
2026-10-29 12:00:00 [INFO] Перезапуск MDS выполнен. Отсчёт 3 мин до возможного повторного действия
2026-10-29 12:03:00 [INFO] Статус Ceph: HEALTH_WARN
2026-10-29 12:03:00 [INFO] WARN после перезапуска MDS: прошло 180 сек из 3 мин — наблюдаем, не перезапускаем
2026-10-29 12:06:00 [INFO] WARN сохраняется уже 3.00 мин (>= 3 мин) после последнего перезапуска — вводим паузу до HEALTH_OK в течение 20 мин
...
2026-10-29 12:26:00 [INFO] HEALTH_OK сохраняется уже 20 мин — пауза перезапусков снята
```

### Пример WARN без целевого признака (игнорируется)

```
2026-10-29 12:10:00 [INFO] Статус Ceph: HEALTH_WARN
2026-10-29 12:10:00 [INFO] WARN, но целевые признаки ошибки [...] не найдены — ждём самовосстановления
```

## Безопасность

* Перезапуск выполняется только для MDS на хосте, названном в ошибке (`mds.<host>`).
* Нет перезапуска «на всякий случай» — только при `WARN` + целевой ошибке + недоступности записи в CephFS.
* Защита от зацикливания: после неудачного перезапуска пауза до `HEALTH_OK` (20 мин).
* Статус `HEALTH_ERR` не вызывает действий по умолчанию.

## Разовый запуск для отладки

```bash
python3 ~/ceph-mds-monitor/ceph_mds_monitor.py --once
```

## Лицензия

Внутреннее обходное решение, без гарантий. Перед вводом в эксплуатацию протестируйте на
stand-окружении.
