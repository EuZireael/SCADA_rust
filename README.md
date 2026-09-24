# SCADA Gateway (Rust)

[![CI/CD](https://github.com/EuZireael/SCADA_rust/actions/workflows/ci.yml/badge.svg)](https://github.com/EuZireael/SCADA_rust/actions/workflows/ci.yml)

Шлюз сбора данных АСУ ТП на Rust — замена Java-шлюза из `scada-gateway`. Опрашивает
контроллеры по **OPC UA**, **Modbus TCP** и **PAC** (driver-master, Savushkin/ptusa),
публикует телеметрию, события и алармы в **Kafka** для монитора, принимает команды записи.

Внешние контракты — как у Java-шлюза, поэтому он встаёт на его место без правок у монитора:
тот же `controllers.yaml`, те же топики и формат сообщений Kafka, статусы команд,
env-переменные, `/actuator/health`, метрики Prometheus и схема БД (`event_log`, `tags`…).

| | Java-шлюз | Rust-шлюз |
|---|---|---|
| Память (стенд, 2517 тегов) | 823 МБ | 14–27 МБ |
| Образ | 646 МБ | 137 МБ |
| Старт до связи с контроллерами | 17,5 с | < 1 с |
| Остановка по SIGTERM | — | 0,3 с |
| Обнаружение обрыва OPC UA | 30 с | 1 с |

## Стек

tokio · [async-opcua](https://crates.io/crates/async-opcua) (OPC UA) ·
tokio-modbus · rdkafka (librdkafka, статически) · mlua (настоящий Lua 5.1, как в ptusa) ·
sqlx (PostgreSQL) · axum (HTTP) · prometheus.

## Запуск

Для сборки нужны Rust (stable), cmake, C/C++-компилятор и заголовки libcurl
(`libcurl4-openssl-dev` / `curl` в Arch) — librdkafka 2.12 включает `curl/curl.h` даже без curl.

```bash
cargo build --release
CONTROLLERS_YAML=../scada-gateway/SCADA-gateway/src/main/resources/controllers.yaml \
SIM_HOST=127.0.0.1 \
SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5433/scada_db \
SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9094 \
./target/release/scada-gateway
```

В стеке `scada-gateway` вместо Java-шлюза:

```bash
cd ../scada-gateway
docker compose -f docker-compose.yml -f ../SCADA_rust/docker-compose.gateway-rs.yml up -d --build gateway
```

В стеке монитора (`scada-editor-backend/docker-compose.gateway.yml`) — `build.context` сервиса
`scada-gateway` на эту папку и том с `controllers.yaml` в `/app/config/controllers.yaml`.

## Настройки (env)

| Переменная | По умолчанию | |
|---|---|---|
| `CONTROLLERS_YAML` | `config/controllers.yaml` | контроллеры и теги, `${VAR:default}` подставляются |
| `SIM_HOST` | `127.0.0.1` | хост контроллеров в endpoint'ах YAML |
| `SPRING_DATASOURCE_URL` / `DB_URL` | `jdbc:postgresql://localhost:5433/scada_db` | + `…_USERNAME`, `…_PASSWORD` |
| `DB_ENABLED` | `true` | `false` — без БД (журнал и история не пишутся) |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | |
| `KAFKA_TOPICS_TELEMETRY` … `_COMMAND_RESULTS` | `scada.tags` … `scada-command-results` | как у Java-шлюза |
| `GATEWAY_SEND_BAD_FRAMES` | **`true`** | кадр `value=null, quality=BAD` при обрыве |
| `GATEWAY_PERSIST_TELEMETRY` | **`false`** | история каждой точки в `telemetry` |
| `GATEWAY_TELEMETRY_RETENTION_HOURS` | `72` | срок хранения истории |
| `GATEWAY_ALARMS_ENABLED` | `false` | алармы по minValue/maxValue |
| `GATEWAY_OPCUA_OP_TIMEOUT_MS` / `_MODBUS_` / `_PAC_` | 5000 / 3000 / 3000 | таймауты операций |
| `GATEWAY_STALE_AFTER_MS` | `30000` | нет удачных чтений — пересоздать OPC UA-сессию |
| `SERVER_PORT` | `8888` | HTTP |
| `RUST_LOG` | `info,…` | уровни логов |

## Отличия от Java-шлюза (намеренные)

- **`pollingRate` учитывается** (Java опрашивала раз в секунду при 2000 мс).
- **БД не на горячем пути**: журнал и история пишутся через очереди отдельными задачами;
  зависшая база не останавливает телеметрию. Выборки REST всегда с `LIMIT`, добавлены индексы.
- **История `telemetry` выключена по умолчанию** (было ~15 ГБ/сут), при включении — пачками
  через `UNNEST` и с очисткой старше retention.
- **Кадры BAD включены по умолчанию** — монитор к ним готов; при обрыве OPC UA шлются на
  каждой попытке переподключения.
- **Смена качества — одно сводное событие на цикл** вместо события на каждый тег.
- **OPC UA без discovery** — подключение прямо к адресу из конфига; чтение пачками по 500 узлов.
- **Команды читаются с конца топика**: команда, пролежавшая, пока шлюз стоял, в ПЛК не уходит.
- **Lua от контроллера** — урезанный стейт (без io/os/package и чтения файлов). Тела ответов
  ptusa — C-строки с `\0` (настоящий Lua 5.1 на нём падает, текст режется).

## Не перенесено

- Режим `recordDevice`/`fields` (в конфиге не используется).
- JVM-метрики `/actuator/metrics/*`, которые читает `loadtest/run.py` (есть метрики процесса
  `process_*` в `/actuator/prometheus`) — нагрузочный стенд надо адаптировать.
- Политики безопасности OPC UA кроме None (у Java на деле тоже только None).

## Проверено

- 40 юнит-тестов (`cargo test`), clippy без замечаний.
- Стенд `scada-gateway` с Python-симулятором: 2517 тегов GOOD в `scada.tags`, формат байт-в-байт
  с Java (типизированные значения, timestamp в epoch-секундах); команды через Kafka — APPLIED
  (OPC UA, PAC), REJECTED_NOT_WRITABLE (датчик, Modbus), REJECTED_UNKNOWN_TAG,
  REJECTED_TYPE_MISMATCH, дубль отброшен; обрыв и восстановление всех трёх контроллеров.
- Эмулятор настоящего PAC (ptusa 2026.4.2.1, `scada-gateway/tools/ptusa_emulator.sh`):
  122/172 канала GOOD (50 — каналы, которых нет в реальном проекте ПЛК), запись применяется.

## CI/CD

`.github/workflows/ci.yml`: на каждый push и PR — `cargo fmt --check`, clippy (`-D warnings`),
`cargo test`; на push в `main` и теги `vX.Y.Z` — образ `ghcr.io/euzireael/scada_rust`
(`:latest`/`:vX.Y.Z` + `:<sha>`).

## Структура

```
src/
  main.rs        сборка, задачи, остановка по SIGTERM, подкоманда healthcheck
  config.rs      env и controllers.yaml
  model.rs       теги, контроллеры, значения, формат времени
  app.rs         общее состояние, связь с контроллерами (CONNECTED/DISCONNECTED)
  poller.rs      циклы опроса OPC UA / Modbus / PAC, heartbeat, сводка здоровья
  telemetry.rs   обработка значений, смена качества, алармы
  command.rs     команды записи: маршрутизация, права, типы, дубли
  opcua.rs       клиент OPC UA
  modbus.rs      батч-чтение FC03
  pac/           driver-master: протокол, Lua-снимок, соединение
  kafka.rs       продюсер и консьюмер команд
  events.rs      очереди журнала и истории
  db.rs          схема, синхронизация с YAML, журнал, история
  http.rs        /actuator/*, /api/*
migrations/      схема БД (совместима с Flyway-схемой Java-шлюза)
```
