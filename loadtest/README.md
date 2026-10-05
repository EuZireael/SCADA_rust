# Нагрузочный стенд

`loadsim` — N OPC UA-серверов («ПЛК») по M узлов; часть узлов меняется каждую секунду. Масштаб по
умолчанию — как в контракте «по исключению»: 20 контроллеров × 2740 тегов, треть меняется каждую
секунду (≈18 000 изменений/с). Отдельный crate (не входит в сборку шлюза).

```sh
cd loadtest/loadsim && cargo build --release
./target/release/loadsim config > /tmp/load.yaml     # controllers.yaml: теги LOAD-<i>.T<j>.V
./target/release/loadsim run &                       # серверы на портах 4900…4919

# шлюз против стенда (Kafka и PostgreSQL — любые, например docker compose up -d kafka postgres)
CONTROLLERS_YAML=/tmp/load.yaml GATEWAY_PERSIST_TELEMETRY=true ../../target/release/scada-gateway
curl -s localhost:8888/actuator/prometheus | grep -E 'scada_(telemetry|poll|process)'
```

Параметры: `--servers`, `--tags`, `--base-port`, `--fraction` (доля узлов, меняющихся за период),
`--period-ms`, `--poll-ms` (только для `config`). Смотреть: `scada_telemetry_sent_total` /
`scada_telemetry_suppressed_total` (скорость), `scada_poll_seconds` и `scada_poll_overruns_total`
(успевает ли шлюз за периодом), `scada_process_seconds`, `scada_telemetry_rows_dropped_total`.
Результаты — в `docs/TELEMETRY_BY_EXCEPTION.md`.

Автоматическая проверка (`scripts/load_check.sh`, job `load` в CI): 10 серверов × 2740 тегов, замер 30 с,
падает при пропущенных циклах опроса, ошибках доставки в Kafka, перезапусках задач или памяти больше 500 МБ.
На машине разработчика: 9 140 сообщений/с, ≈ 10 % одного ядра, 75 МБ, обработка цикла 14 мс.
