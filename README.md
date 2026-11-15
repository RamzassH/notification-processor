# notification-processor

Go-сервис читает события из Kafka, сохраняет по одной задаче на канал в PostgreSQL и эмулирует доставку. В журнал успешной отправки попадают `event_id`, получатель, канал и текст.

| Событие | Каналы |
| --- | --- |
| `order_created` | email, push |
| `payment_received` | email |
| `order_shipped` | push, sms |

## Локальный запуск через PowerShell

Нужны Go версии из [go.mod](go.mod), Docker с Compose v2 и PowerShell 7 (`pwsh`). В Linux для проверок с `-race` нужен C-компилятор. Выполняйте команды из корня репозитория.

```powershell
pwsh -File scripts/run.ps1
```

Скрипт ждёт готовности PostgreSQL и Kafka, применяет SQL-файлы через tern v2.4.3, создаёт топик из `KAFKA_TOPIC` с тремя партициями и запускает `go run ./cmd/notification-processor`. При первом запуске Go и Docker скачивают зависимости. Готовность процесса видна по записи `ready`.

Значения для локального стенда находятся в [.env.example](.env.example). Если порты заняты, скопируйте файл в `.env` и измените `POSTGRES_PORT` или `KAFKA_PORT`. Compose подставляет их в адреса для Go-процесса. Переменные текущего окружения имеют приоритет над файлом. По умолчанию PostgreSQL доступен на `127.0.0.1:5433`, Kafka — на `localhost:9092`. Приложение получает `DATABASE_URL`, `KAFKA_BROKER`, `KAFKA_TOPIC` и `KAFKA_GROUP_ID` через окружение. По умолчанию топик — `user-actions`, группа — `notification-processor`. При смене группы Kafka использует её отдельные offsets.

Ctrl+C останавливает приложение. Контейнеры можно остановить командой `docker compose down`. Именованные тома сохраняют сообщения, offsets, уведомления и версию миграций. При следующем запуске tern читает `public.schema_version` и применяет только следующие версии. Первые три миграции также поддерживают локальную схему, ранее созданную этими SQL-файлами вручную.

### Демо

После `ready` во втором терминале:

```powershell
pwsh -File scripts/demo.ps1
```

В [demo/events.jsonl](demo/events.jsonl) девять строк: семь уникальных событий для четырёх пользователей, строка `{` с намеренно оборванным JSON и точный повтор первого события. Один ключ отправляет их в одну партицию. В журнале появится предупреждение для битой записи, а в результате SQL-запроса — одиннадцать строк по каналам из таблицы выше. Терминальные статусы — `sent` или `failed`, число попыток — от 1 до 3. У демособытий фиксированные UUID: повторная отправка сохраняет прежние строки, статусы и счётчики.

### Проверки

```powershell
pwsh -File scripts/check.ps1
```

Скрипт проверяет форматирование и `go vet ./...`, создаёт отдельный Compose-проект и запускает `go test -p 1 -count=1 -v ./...` с адресами тестовых Kafka и PostgreSQL. На Linux добавляется `-race`. Пакеты проверяются последовательно, поскольку работающий consumer и тест хранилища используют одну тестовую БД. После прогона тестовые контейнеры и тома удаляются. Порты тестового стенда — 15433 и 19094; их можно изменить через `TEST_POSTGRES_PORT` и `TEST_KAFKA_PORT`. Для быстрого прогона без инфраструктуры доступна команда `go test ./...`; интеграционные тесты без `TEST_*` пропускаются.

## Linux (Bash)

Нужны Bash, Go версии из [go.mod](go.mod), Docker с Compose v2 и C-компилятор для `go test -race`. Выполняйте блоки из корня репозитория.

### Запуск

При первом запуске создайте `.env` из примера. Если порты заняты, измените `POSTGRES_PORT` или `KAFKA_PORT` в этом файле перед запуском.

```bash
[[ -f .env ]] || cp .env.example .env
```

```bash
(
  set -e
  set -a
  source ./.env
  set +a

  docker compose up -d --wait --wait-timeout 120 postgres kafka
  go run github.com/jackc/tern/v2@v2.4.3 migrate \
    --migrations migrations --conn-string "$DATABASE_URL"
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 --create --if-not-exists \
    --topic "$KAFKA_TOPIC" --partitions 3 --replication-factor 1

  go run ./cmd/notification-processor
)
```

Дождитесь записи `ready` в журнале. Для остановки нажмите Ctrl+C, затем выполните `docker compose down`. Именованные тома сохранят данные для следующего запуска.

### Демо

Во втором терминале отправьте девять записей с одним ключом в одну партицию:

```bash
(
  set -eo pipefail
  set -a
  source ./.env
  set +a
  sed 's/^/demo:/' demo/events.jsonl |
    docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server kafka:19092 --topic "$KAFKA_TOPIC" \
      --property parse.key=true --property key.separator=:
)
```

Проверьте результат в PostgreSQL. Если ещё видны задачи `pending` или строк меньше одиннадцати, повторите запрос через несколько секунд.

```bash
docker compose exec -T postgres psql -X -U notifications -d notifications <<'SQL'
SELECT event_id, user_id, channel, status, attempts
FROM notifications
WHERE event_id BETWEEN '90000000-0000-4000-8000-000000000001'
                   AND '90000000-0000-4000-8000-000000000007'
ORDER BY event_id, channel;
SQL
```

Ожидаются 11 строк для 7 событий. Битая запись даст предупреждение в журнале, а повторная отправка тех же данных сохранит число строк.

### Проверки

Этот блок можно запускать отдельно, в том числе при работающем демо. Для каждого прогона создаётся свой Compose-проект. При выходе из блока удаляются его контейнеры и тома.

```bash
(
  set -e
  unformatted=$(gofmt -l cmd internal)
  if [[ -n "$unformatted" ]]; then
    printf 'Требуется gofmt:\n%s\n' "$unformatted" >&2
    exit 1
  fi
  go vet ./...

  export COMPOSE_PROJECT_NAME="notification-check-$(cat /proc/sys/kernel/random/uuid)"
  export POSTGRES_PORT="${TEST_POSTGRES_PORT:-15433}" KAFKA_PORT="${TEST_KAFKA_PORT:-19094}"
  export TEST_DATABASE_URL="postgres://notifications:notifications_dev@127.0.0.1:${POSTGRES_PORT}/notifications?sslmode=disable"
  export TEST_KAFKA_BROKER="localhost:${KAFKA_PORT}"
  export TEST_KAFKA_TOPIC=user-actions TEST_KAFKA_GROUP_ID=notification-processor
  compose() { docker compose --env-file .env.example "$@"; }
  trap 'compose down --volumes' EXIT

  compose up -d --wait --wait-timeout 120 postgres kafka
  go run github.com/jackc/tern/v2@v2.4.3 migrate \
    --migrations migrations --conn-string "$TEST_DATABASE_URL"
  compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:19092 --create --if-not-exists \
    --topic "$TEST_KAFKA_TOPIC" --partitions 3 --replication-factor 1
  go test -race -p 1 -count=1 -v ./...
)
```

## Доставка и восстановление

- Consumer читает через `FetchMessage`. Один SQL-запрос регистрирует `event_id` в `received_events` и создаёт задачи по каналам. Повтор уже принятого ID сохраняет первоначального получателя и набор каналов. После успешной записи consumer вручную подтверждает offset. При сбое БД сообщение остаётся доступно для повторной обработки.
- Битый JSON, пропущенные обязательные поля, неверный UUID и неизвестный тип события журналируются с топиком, партицией и offset. Consumer подтверждает такую запись и продолжает чтение. Содержимое `payload` хранится в `bytea` как исходный JSON, включая `null` и экранированные символы. Для текста уведомления воркер читает `order_id` в Go.
- Два воркера выбирают готовые строки через `FOR UPDATE SKIP LOCKED`. Эмулятор выдаёт временную ошибку в 10% вызовов, перманентную — в 1%. Для обоих видов ошибок предел по ТЗ - три попытки всего. После первой и второй неудачи время повтора сохраняется в БД с задержкой 1 и 2 секунды. Третья ошибка переводит задачу в `failed`.
- Штатная остановка прекращает чтение новых событий. Для уже принятого сообщения и начатой попытки доставки предусмотрено до пяти секунд. Если consumer не успел записать уведомления, offset останется неподтверждённым.
- Однократность создания задачи обеспечена в PostgreSQL. При падении после внешней отправки, но до фиксации `sent`, отправка может повториться. Для реального провайдера потребуется поддержка ключа идемпотентности на основе `event_id` и канала.
