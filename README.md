# SCADA Gateway (Rust)

[![CI/CD](https://github.com/EuZireael/SCADA_rust/actions/workflows/ci.yml/badge.svg)](https://github.com/EuZireael/SCADA_rust/actions/workflows/ci.yml)
[![Audit](https://github.com/EuZireael/SCADA_rust/actions/workflows/audit.yml/badge.svg)](https://github.com/EuZireael/SCADA_rust/actions/workflows/audit.yml)

Шлюз сбора данных АСУ ТП на Rust — замена Java-шлюза из `scada-gateway`. Опрашивает
контроллеры по **OPC UA**, **Modbus TCP** и **PAC** (driver-master, Savushkin/ptusa),
публикует телеметрию, события и алармы в **Kafka** для монитора, принимает команды записи.

Репозиторий самодостаточный: PLC-симулятор (`simulator/`), конфигурация станции
(`config/controllers.yaml`) и весь стенд (`docker-compose.yml`) — здесь же.

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

Весь стенд — postgres, kafka, PLC-симулятор и шлюз — одной командой:

```bash
docker compose up -d --build      # шлюз: http://localhost:8888/actuator/health
docker compose logs -f gateway
docker compose down               # остановить (данные БД сохранятся; -v — стереть)
```

Порты на хост: шлюз `:8888`, Kafka `:9094`, PostgreSQL `:5433`, симулятор `:4840` (OPC UA) /
`:5020` (Modbus) / `:10000` (PAC) — те же, что у стенда Java-шлюза, одновременно их не поднимать.

Шлюз локально (нужны Rust stable, cmake, C/C++-компилятор и заголовки libcurl — librdkafka 2.12
включает `curl/curl.h` даже без curl; в Debian/Ubuntu `libcurl4-openssl-dev`, в Arch — `curl`):

```bash
docker compose up -d postgres kafka simulator
cargo build --release
SIM_HOST=127.0.0.1 \
SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5433/scada_db \
SPRING_KAFKA_BOOTSTRAP_SERVERS=localhost:9094 \
./target/release/scada-gateway
```

В стеке монитора (`scada-editor-backend/docker-compose.gateway.yml`) — `build.context` сервиса
`scada-gateway` на эту папку и том `config/controllers.yaml` в `/app/config/controllers.yaml`.

## Симулятор и конфигурация станции

- `simulator/` — PLC-симулятор на Python: OPC UA, Modbus TCP и PAC (driver-master в формате
  ptusa) в одном процессе, проигрывает 5-суточный архив станции BN1_MCA1 (`data/`). Перенесён
  из `savushkin-dev/scada-gateway` (`plc-simulator`, коммит `76db32c`) и дальше живёт здесь.
  Тесты: `cd simulator && python -m pytest tests` (Python 3.11).
- `config/controllers.yaml` — 2517 каналов на трёх контроллерах; согласован с
  `simulator/config/replay_config.yaml` (одинаковые channelId, типы, адреса и право записи).
- `scripts/ptusa_emulator.sh <проект ПЛК>` — эмулятор настоящего PAC (прошивка ptusa под ПК)
  в Docker для сверки протокола.

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
- **Lua от контроллера — в песочнице**: без io/os/package и файлов, без `load`/`loadstring`/
  `string.dump` (байткод Lua 5.1 не проверяется — это выход из песочницы) и `pcall`; потолок
  64 МБ памяти и 1 с на скрипт — зависший или раздувшийся скрипт рвёт соединение, а не
  вешает опрос и команды. Тела ответов ptusa — C-строки с `\0` (настоящий Lua 5.1 на нём
  падает, текст режется).
- **Modbus подстраивает план чтения под карту регистров**: блоки читаются вместе с
  промежутками между тегами; если ПЛК отвечает на блок IllegalDataAddress (в промежутке
  регистра нет), блок делится пополам, пока части не прочитаются, — в том же цикле, без
  настройки. Исключение на регистре самого тега — BAD только у этого тега.

## Не перенесено

- Режим `recordDevice`/`fields` (в конфиге не используется).
- JVM-метрики `/actuator/metrics/*`, которые читает `loadtest/run.py` (есть метрики процесса
  `process_*` в `/actuator/prometheus`) — нагрузочный стенд надо адаптировать.
- Политики безопасности OPC UA кроме None (у Java на деле тоже только None).

## Проверено

- 46 юнит-тестов, сверка конфигураций шлюза и симулятора, 7 интеграционных
  (`cargo test -- --ignored`), 40 тестов симулятора (pytest); clippy без замечаний;
  `cargo audit` чистый (одно обоснованное исключение в `.cargo/audit.toml`).
- Стенд с PLC-симулятором: 2517 тегов GOOD в `scada.tags`, формат байт-в-байт
  с Java (типизированные значения, timestamp в epoch-секундах); команды через Kafka — APPLIED
  (OPC UA, PAC), REJECTED_NOT_WRITABLE (датчик, Modbus), REJECTED_UNKNOWN_TAG,
  REJECTED_TYPE_MISMATCH, дубль отброшен; обрыв и восстановление всех трёх контроллеров
  (автоматически — `tests/faults.rs`).
- Эмулятор настоящего PAC (ptusa 2026.4.2.1, `scripts/ptusa_emulator.sh`):
  122/172 канала GOOD (50 — каналы, которых нет в реальном проекте ПЛК), запись применяется.

## Тесты

```bash
cargo test                      # 46 юнит-тестов + сверка config ↔ simulator, без внешних систем
cargo test -- --ignored         # интеграционные: нужны симулятор и Kafka
```

Интеграционные тесты помечены `#[ignore]` и идут против PLC-симулятора и Kafka стенда
(`docker compose up -d`):

- `tests/simulator.rs` — клиенты протоколов: каждый тег `controllers.yaml` читается своим
  протоколом с верным типом (OPC UA, Modbus, PAC); запись по OPC UA и PAC применяется и
  откатывается; RO-узел и несуществующий прибор отклоняются.
- `tests/config_consistency.rs` (без `#[ignore]`) — `config/controllers.yaml` и конфиг симулятора
  описывают одни и те же каналы: протокол, тип, адрес, право записи, прибор/поле.
- `tests/e2e.rs` — собранный шлюз как процесс, проверка глазами монитора на топиках `it-<id>.*`:
  все теги GOOD и контракт тела телеметрии, команды через Kafka со всеми статусами и доходом
  записи до телеметрии, метрики, журнал в БД и REST (при `IT_DATABASE_URL`), остановка по SIGTERM.
- `tests/faults.rs` — обрыв связи: шлюз ходит к симулятору через TCP-прокси теста, прокси
  сначала рвёт соединения, потом «вешает» их (открыты, но без ответов); по всем трём
  контроллерам проверяются DOWN в `/actuator/health`, кадры BAD, события DISCONNECTED, затем
  восстановление — UP, GOOD и событие «link restored».

| Переменная | По умолчанию |
|---|---|
| `SIM_HOST` | `127.0.0.1` |
| `KAFKA_BOOTSTRAP` | `localhost:9094` (внешний listener стенда) |
| `CONTROLLERS_YAML` | `config/controllers.yaml` |
| `IT_DATABASE_URL` | не задан — шлюз в тесте без БД (пустая база: `createdb scada_it`) |

## CI/CD

`.github/workflows/ci.yml`: на каждый push и PR — `cargo fmt --check`, clippy (`-D warnings`),
`cargo test`, pytest симулятора, интеграционные тесты (свой симулятор, Kafka и Postgres
как service-контейнеры); на push в `main` и теги `vX.Y.Z` — образ `ghcr.io/euzireael/scada_rust`
(`:latest`/`:vX.Y.Z` + `:<sha>`).

`.github/workflows/audit.yml`: `cargo audit --deny warnings` по базе RustSec на каждый push/PR
и раз в неделю (новые уязвимости появляются без наших коммитов); исключения с обоснованием —
`.cargo/audit.toml`.

`.github/dependabot.yml`: раз в неделю PR на обновления — crates (патчи и миноры крейтов ≥ 1.0
одним PR, 0.x — по отдельности), pip симулятора, экшены, базовые образы и образы стенда
(Kafka закреплена версией). Исключены обновления, требующие ручной миграции: pymodbus ≥ 3.8,
asyncua 2, Python ≥ 3.12 у симулятора, мажор PostgreSQL (несовместим с томом данных).

### Релиз

1. Версия в `Cargo.toml` (+ `cargo check`, чтобы обновился `Cargo.lock`).
2. Раздел `## [X.Y.Z] — дата` в `CHANGELOG.md` (из `[Unreleased]`).
3. `git tag -a vX.Y.Z -m "…" && git push origin vX.Y.Z`.

CI проверит, что тег совпадает с `Cargo.toml`, прогонит тесты, опубликует
`ghcr.io/euzireael/scada_rust:vX.Y.Z` и создаст GitHub Release с текстом раздела.

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
tests/           интеграционные тесты (simulator.rs — протоколы, e2e.rs — шлюз целиком,
                 faults.rs — обрыв связи; common/ — запуск шлюза, TCP-прокси)
config/          controllers.yaml — каналы станции
simulator/       PLC-симулятор (Python) + его тесты
scripts/         эмулятор настоящего PAC (ptusa)
docker-compose.yml   стенд целиком
CHANGELOG.md         история версий (текст GitHub Release)
```
