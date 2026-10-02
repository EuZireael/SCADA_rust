# Changelog

Формат — [Keep a Changelog](https://keepachangelog.com/ru/1.1.0/), версии — [SemVer](https://semver.org/lang/ru/).

Релиз: версия в `Cargo.toml` → раздел здесь → тег `vX.Y.Z`. CI проверяет, что тег совпадает
с `Cargo.toml`, публикует образ `ghcr.io/euzireael/scada_rust:vX.Y.Z` и GitHub Release с
текстом раздела.

## [Unreleased]

## [0.2.0] — 2026-10-02

Перенос наработок Java-шлюза: телеметрия «по исключению» (контракт монитора), горячее
резервирование, пользовательские Lua-скрипты и станция мойки целиком по OPC UA.

### Добавлено

- **Телеметрия «по исключению»** (`GATEWAY_PUBLISH_*`): тег уходит в Kafka при первом значении,
  смене качества, изменении за зону нечувствительности и раз в `full-resend-ms` (полная
  отправка). Фильтр локальной истории (`GATEWAY_HISTORY_*`, блок `history:` тега, колонки
  `tags.history_*`). Контракт — `docs/TELEMETRY_BY_EXCEPTION.md`. На стенде 54 800 тегов
  (20 × 2740, 18 280 изменений/с): ≈25 % одного ядра, 125 МБ, без потерь.
- **Горячее резервирование** (`GATEWAY_HA_*`): выборы через группу потребителей Kafka, резерв
  опрашивает, но молчит; переключение при отказе ≈1,9 с (штатная остановка ≈0,6 с), новый
  активный сразу шлёт все теги; `GET /api/ha`, метрика `scada_ha_active`, события `HotStandby`.
  Команды: ручное назначение партиций, коммит позиции до исполнения, возраст команды
  (`GATEWAY_COMMANDS_MAX_AGE_MS`, `REJECTED_EXPIRED`).
- **Пользовательские скрипты Lua 5.1** (`config/scripts/`, `GATEWAY_SCRIPTS_*`): `process` для
  входящих значений, `write` для команд, состояние на канал, цепочки, горячая перезагрузка;
  песочница с лимитами памяти и времени; `GET /api/scripts`.
- **Станция мойки по OPC UA**: `config/stations/BN1_MCA1.yaml` (1834 канала), фасад
  `ptusa-opcua/` (прошивка ptusa → OPC UA, запись через `set_cmd`), `docker-compose.moika.yml`,
  `up-moika.sh`, `tools/station-config/`, `docs/MONITOR_INTEGRATION.md`.
- **Нагрузочный стенд** `loadtest/loadsim`; метрики `scada_telemetry_suppressed_total`,
  `scada_process_seconds`, `scada_poll_seconds`, `scada_poll_overruns_total`,
  `scada_script_errors_total`, `scada_scripts_bound_tags`.
- `CONTROLLERS_CONFIG` (форма `file:/путь`), `KAFKA_TOPICS_REPLICATION`.

### Изменено

- Все узлы OPC UA вернули BAD ⇒ связь с контроллером считается потерянной.
- `${NAME}` в `controllers.yaml` без значения и без умолчания — ошибка старта (`${NAME:}` — пусто).
- Миграция `0002`: колонки `history_*` в `tags` (совместима с Flyway V2 Java-шлюза).

### Безопасность

- Песочница Lua: убраны `coroutine` (в сопрограмме не работает хук времени), `getfenv`/`setfenv`,
  `newproxy`, `collectgarbage`; лимит распаковки ответа PAC — 8 МБ (zlib-бомба).

## [0.1.0] — 2026-09-24

Первый релиз: шлюз АСУ ТП на Rust — замена Java-шлюза `savushkin-dev/scada-gateway` с теми же
внешними контрактами (`controllers.yaml`, топики и формат Kafka, статусы команд, env,
`/actuator/health`, метрики, схема БД), поэтому встаёт на его место без правок у монитора.

### Добавлено

- **Опрос контроллеров**, по задаче на контроллер с периодом по `pollingRate`:
  - OPC UA — подключение прямо к адресу из конфига (без discovery), чтение пачками по 500 узлов,
    пересоздание сессии при зависании;
  - Modbus TCP — FC03 блоками до 120 регистров; при исключении IllegalDataAddress блок делится
    пополам, пока не прочитается (план подстраивается под карту регистров ПЛК);
  - PAC driver-master в формате ptusa (баннер `PAC accept`, zlib, снимок `t` исполняется
    в Lua 5.1), запись через `set_cmd` с кодом результата.
- **Kafka**: телеметрия `scada.tags` (ключ — путь канала, тело `{value, quality, timestamp}`,
  timestamp — epoch-секунды с наносекундами), события, алармы, команды записи и результаты со
  статусами Java-шлюза; идемпотентный продюсер, который не блокирует опрос; команды читаются
  с конца топика, повторы отбрасываются.
- **PostgreSQL**: схема, совместимая с Flyway-схемой Java-шлюза, и индексы для выборок;
  синхронизация тегов с `controllers.yaml`; журнал событий и (по флагу) история телеметрии
  пачками через очереди — база не на горячем пути.
- **HTTP**: `/actuator/health`, `/actuator/prometheus`, REST журнала `/api/*` (всегда с `LIMIT`).
- Кадры BAD при обрыве, события связи (одно сводное событие смены качества на цикл),
  heartbeat, остановка по SIGTERM, подкоманда `healthcheck` для контейнера.
- **Стенд в репозитории**: PLC-симулятор (`simulator/`: OPC UA, Modbus, PAC, реплей архива
  BN1_MCA1), конфигурация станции (2517 каналов на трёх контроллерах), `docker-compose.yml`,
  эмулятор настоящего PAC (`scripts/ptusa_emulator.sh`).
- **Тесты**: юнит (протоколы на фейковых серверах PAC и Modbus), сверка конфигов шлюза и
  симулятора, интеграционные против симулятора (каждый тег своим протоколом, запись),
  сквозной через Kafka и БД, обрыв и восстановление связи через TCP-прокси.
- **CI/CD**: fmt, clippy, тесты, pytest симулятора, интеграционные (Kafka и Postgres —
  service-контейнеры), образ в GHCR, GitHub Release по тегу; `cargo audit` на каждый push и
  раз в неделю; Dependabot.

### Безопасность

- Lua-скрипт в ответе PAC исполняется в песочнице: без io/os/package и файлов, без загрузки
  байткода (`load`, `loadstring`, `string.dump`) и `pcall`; не больше 64 МБ памяти и 1 с на
  скрипт.
- RUSTSEC-2023-0071 (`rsa` из async-opcua, исправленной версии нет) — в исключениях
  `cargo audit`: при политике OPC UA None RSA-расшифровки у клиента нет.

### Отличия от Java-шлюза

- Учитывается `pollingRate` (Java опрашивала раз в секунду при 2000 мс).
- История `telemetry` по умолчанию выключена (было ~15 ГБ/сут), кадры BAD — включены.
- Обрыв OPC UA обнаруживается за ~1 с (было 30 с); память 14–27 МБ (было 823 МБ),
  образ 137 МБ (было 646 МБ), старт до связи с контроллерами < 1 с (было 17,5 с).
- Не перенесено: режим `recordDevice`/`fields`, JVM-метрики `/actuator/metrics/*`,
  политики безопасности OPC UA кроме None.

[Unreleased]: https://github.com/EuZireael/SCADA_rust/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/EuZireael/SCADA_rust/releases/tag/v0.1.0
