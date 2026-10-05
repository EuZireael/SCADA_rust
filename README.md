# SCADA Gateway (Rust)

[![CI/CD](https://github.com/EuZireael/SCADA_rust/actions/workflows/ci.yml/badge.svg)](https://github.com/EuZireael/SCADA_rust/actions/workflows/ci.yml)
[![Audit](https://github.com/EuZireael/SCADA_rust/actions/workflows/audit.yml/badge.svg)](https://github.com/EuZireael/SCADA_rust/actions/workflows/audit.yml)

Шлюз сбора данных АСУ ТП на Rust — замена Java-шлюза из `scada-gateway`. Опрашивает
контроллеры по **OPC UA**, **Modbus TCP** и **PAC** (driver-master, Savushkin/ptusa),
публикует телеметрию, события и алармы в **Kafka** для монитора, принимает команды записи.

**Документация:** `docs/OPERATIONS.md` — настройки, здоровье, метрики, что делает шлюз при сбоях;
`docs/MONITOR_INTEGRATION.md` — подключение монитора и что нового; `docs/TELEMETRY_BY_EXCEPTION.md` — контракт публикации
«по исключению»; `CHANGELOG.md`.

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

- `simulator/` — PLC-симулятор на Rust: OPC UA (async-opcua), Modbus TCP и PAC (driver-master в
  формате ptusa) в одном процессе, проигрывает 5-суточный архив станции BN1_MCA1
  (`data/archive_replay.bin.gz`). Начинался как Python-симулятор `savushkin-dev/scada-gateway`
  (`plc-simulator`, коммит `76db32c`), переписан на Rust (файлы остались в истории git).
  Отдельный пакет со своим `Cargo.lock`: `cd simulator && cargo run --release -- config/replay_config.yaml`
  (`OPCUA_ENDPOINT`, `MODBUS_PORT`, `PAC_PORT`), тесты — `cargo test`, образ — `docker build ./simulator`.
  Условная логика настоящей прошивки (поля, которые программа ПЛК не отдаёт оператору, клапаны только в ручном режиме) снята
  с эмулятора мойки и повторена в симуляторе: `docs/FIRMWARE_WRITE_BEHAVIOR.md`; `simulator conformance` проверяет её у любого PAC.
  `simulator probe-pac [host] [порт]` печатает handshake, объектную модель и снимок любого PAC —
  симулятора или настоящей прошивки ptusa.
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
| `GATEWAY_PUBLISH_ENABLED` / `_DEADBAND` / `_DEADBAND_PERCENT` / `_MIN_INTERVAL_MS` / `_FULL_RESEND_MS` | `true` / 0 / 0 / 0 / `30000` | телеметрия «по исключению» — `docs/TELEMETRY_BY_EXCEPTION.md` |
| `GATEWAY_HISTORY_DEADBAND` / `_DEADBAND_PERCENT` / `_MIN_INTERVAL_MS` / `_MAX_INTERVAL_MS` | 0 / 0 / 0 / `600000` | фильтр истории (тег переопределяет блоком `history:` в YAML) |
| `GATEWAY_HA_ENABLED` | `false` | горячее резервирование (нужен Kafka) |
| `GATEWAY_HA_INSTANCE_ID` / `_GROUP_ID` / `_TOPIC` | `<HOSTNAME>-<pid>` / `scada-gateway-ha.<топик команд>` / `scada-gateway-ha` | имя экземпляра, группа и служебный топик выборов |
| `GATEWAY_HA_SESSION_TIMEOUT_MS` / `_HEARTBEAT_INTERVAL_MS` | `6000` / `1000` | время обнаружения отказа активного (брокеру нужен `group.min.session.timeout.ms` ≤ значения) |
| `GATEWAY_COMMANDS_MAX_AGE_MS` | `30000` | команда старше — `REJECTED_EXPIRED` |
| `GATEWAY_SCRIPTS_DIR` / `_TIMEOUT_MS` / `_RELOAD_INTERVAL_MS` | `scripts` / `50` / `5000` | пользовательские Lua-скрипты |
| `KAFKA_TOPICS_REPLICATION` | `1` | фактор репликации создаваемых топиков |
| `KAFKA_SECURITY_PROTOCOL` / `KAFKA_SASL_MECHANISM` / `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD` | — | TLS и SASL (PLAIN, SCRAM-SHA-256/512): `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`; OpenSSL вшит в бинарник |
| `KAFKA_SSL_CA_LOCATION` / `_CERTIFICATE_LOCATION` / `_KEY_LOCATION` / `_KEY_PASSWORD` | — | сертификаты (в том числе клиентский для mTLS) |
| `KAFKA_CLIENT_<СВОЙСТВО>` | — | любое свойство librdkafka: `KAFKA_CLIENT_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM=none` → `ssl.endpoint.identification.algorithm`; перекрывает именованные |
| `SPRING_KAFKA_PROPERTIES_SECURITY_PROTOCOL` / `_SASL_MECHANISM` / `_SASL_JAAS_CONFIG` | — | как у Java-шлюза (из JAAS берутся `username` и `password`) |
| `GATEWAY_OPCUA_PKI_DIR` / `GATEWAY_OPCUA_TRUST_SERVER_CERTS` | tmp / `false` | сертификаты защищённого OPC UA (`security:`, `username:`, `password:` в `controllers.yaml`); см. `docs/OPERATIONS.md` |
| `GATEWAY_API_TOKEN` / `GATEWAY_API_TOKEN_FILE` | — | токен REST: `/api/*` требует `Authorization: Bearer <токен>` (без токена — открыт, как раньше; `/actuator/*` открыт всегда) |
| `GATEWAY_HTTP_BIND` | `0.0.0.0` | адрес HTTP (`127.0.0.1` — только локально) |
| `GATEWAY_HA_YIELD_AFTER_MS` | `30000` | «слепой» активный (связь потеряна со всеми контроллерами) отдаёт лидерство партнёру; `0` — выключено |
| `CONTROLLERS_CONFIG` | — | то же, что `CONTROLLERS_YAML` (форма `file:/путь` из Java) |
| `SERVER_PORT` | `8888` | HTTP |
| `RUST_LOG` | `info,…` | уровни логов |

## Телеметрия «по исключению»

Тег уходит в Kafka при первом значении, смене качества, изменении (с зоной нечувствительности)
и раз в `GATEWAY_PUBLISH_FULL_RESEND_MS` — полная отправка. Контракт, настройки и замеры
(54 800 тегов, 18 280 сообщений/с, ≈25 % одного ядра, 125 МБ) — `docs/TELEMETRY_BY_EXCEPTION.md`,
стенд — `loadtest/`.

## Горячее резервирование

`GATEWAY_HA_ENABLED=true` на двух экземплярах с общим Kafka: выборы активного — группа
потребителей Kafka на служебном топике из одной партиции (`src/ha.rs`). Активный публикует
телеметрию и события и принимает команды; резерв опрашивает контроллеры (горячий) и молчит. Отказ
активного — переключение за `session-timeout` (≈1,9 с при 2 с; при штатной остановке ≈0,6 с), новый
активный сразу шлёт все теги. Состояние — `GET /api/ha`, метрика `scada_ha_active`, события
`HotStandby` в журнале. Активный, потерявший связь со всеми контроллерами (`GATEWAY_HA_YIELD_AFTER_MS`, считается от
момента, когда связь признана потерянной, — для OPC UA это `GATEWAY_STALE_AFTER_MS`), выходит из
группы и передаёт лидерство партнёру, если тот в группе, и возвращается резервным; без партнёра
остаётся активным и шлёт BAD. Команды: ручное назначение партиций и коммит позиции до исполнения
(at-most-once; команда старше `GATEWAY_COMMANDS_MAX_AGE_MS` не исполняется).

## Пользовательские скрипты (Lua 5.1)

`config/scripts/scripts.yaml` привязывает Lua-скрипты к тегам по маскам (`*`, `?`):
`process(value, quality, ctx)` — обработка значения до публикации (масштаб, фильтры, проверка
обрыва датчика), `write(value, ctx)` — преобразование значения команды перед записью в ПЛК,
`ctx.state` — состояние на канал. Песочница: без io/os/require/load/pcall/coroutine, лимиты
памяти и времени; ошибка в скрипте — BAD по тегу, а не падение. Правки папки подхватываются
без перезапуска (ошибка в новой версии — остаётся старая). `GET /api/scripts`.

## Станция мойки целиком по OPC UA

`config/stations/BN1_MCA1.yaml` — 1834 канала станции BN1-МСА1 на одном OPC UA-контроллере; данные
даёт эмулятор мойки (прошивка ptusa) через фасад `ptusa-opcua/` (Rust: driver-master → OPC UA, запись —
`set_cmd`). Запуск — `MOIKA_PROJECT=<проект ПЛК> ./up-moika.sh`, подключение монитора —
`docs/MONITOR_INTEGRATION.md`, генерация конфигурации — `tools/station_config.sh`.

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
- JVM-метрики `/actuator/metrics/*` Java-шлюза; вместо них — метрики Prometheus (`process_*`,
  `scada_poll_seconds`, `scada_process_seconds`…), нагрузочный стенд — `loadtest/`.
- Kerberos (SASL GSSAPI) для Kafka: нужна сборка librdkafka с libsasl2.

## Проверено

Каждый pull request проходит в CI (`.github/workflows/ci.yml`), релиз по тегу — только если зелёные все пункты:

| Проверка | Что доказывает |
|---|---|
| 120 юнит-тестов шлюза | фильтр «по исключению» (правила, зоны, разброс полной отправки), HA, скрипты и песочница, команды, конфигурация и её проверка, Kafka-безопасность, надзиратель задач |
| 20 интеграционных тестов (`cargo test -- --ignored`) | протоколы на симуляторе (OPC UA / Modbus / PAC, запись, `localhost` с IPv6), шлюз целиком глазами монитора, обрыв и возврат связи, горячий резерв (kill -9, штатная остановка, «слепой» активный), REST под токеном, защищённый OPC UA (политики, пароль, доверие к сертификату) |
| `stack` — `scripts/stack_smoke.sh` | `docker compose` из исходников и сбои: Kafka пропал на 45 с, БД на 30 с, ПЛК пропал — шлюз жив, опрос не отстаёт, после возврата данные восстановлены сами |
| `load` — `scripts/load_check.sh` | 27 400 тегов: опрос не отстаёт, нет ошибок доставки и перезапусков задач, память < 500 МБ |
| симулятор, фасад `ptusa-opcua` | свои fmt/clippy/тесты (53 и 19), сборка образов |
| `cargo audit`, Dependabot | уязвимости и устаревшие зависимости |

Руками проверено на настоящей прошивке ptusa (эмулятор мойки, `docker-compose.moika.yml`): 1834 канала GOOD,
команда `APPLIED` за 27 мс, переключение резерва, и на нагрузке контракта «по исключению» (54 800 тегов,
≈ 18 000 изменений/с): ≈ 25 % одного ядра, ≈ 125 МБ, потерь нет.

## Тесты

```bash
cargo test                      # 120 юнит-тестов + сверка config ↔ simulator, без внешних систем
cargo test -- --ignored         # интеграционные: нужны симулятор и Kafka
scripts/stack_smoke.sh          # стенд целиком + сбои (нужен docker compose)
scripts/load_check.sh           # нагрузка на 27 400 тегов (нужен docker для Kafka)
```

Интеграционные тесты помечены `#[ignore]` и идут против PLC-симулятора и Kafka стенда
(`docker compose up -d`; для защищённых тестов симулятор запускают с `SIM_OPCUA_USER=operator
SIM_OPCUA_PASSWORD=operator-pass`):

- `tests/simulator.rs` — клиенты протоколов: каждый тег `controllers.yaml` читается своим
  протоколом с верным типом; запись по OPC UA и PAC применяется и откатывается; RO-узел и
  несуществующий прибор отклоняются; `localhost`; защищённый канал, пароль, цепочка доверия.
- `tests/config_consistency.rs` (без `#[ignore]`) — `config/controllers.yaml` и конфиг симулятора
  описывают одни и те же каналы.
- `tests/e2e.rs` — собранный шлюз как процесс, проверка глазами монитора на топиках `it-<id>.*`:
  все теги GOOD и контракт тела телеметрии, команды через Kafka со всеми статусами, метрики,
  здоровье (ни одна задача не перезапускалась, доставка идёт), журнал в БД и REST, остановка по SIGTERM.
- `tests/faults.rs` — обрыв связи через TCP-прокси (рвёт и «вешает» соединения) по всем трём
  контроллерам, затем восстановление.
- `tests/ha.rs` — два процесса шлюза: переключение при `kill -9` и штатной остановке, полная
  отправка после переключения, «слепой» активный передаёт роль партнёру.
- `tests/api_auth.rs`, `tests/secure.rs` — REST под токеном; шлюз с защитой OPC UA из конфигурации.

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
simulator/       PLC-симулятор (Rust, отдельный пакет) + конфигурация и архив replay
scripts/         эмулятор настоящего PAC (ptusa)
docker-compose.yml   стенд целиком
CHANGELOG.md         история версий (текст GitHub Release)
```
