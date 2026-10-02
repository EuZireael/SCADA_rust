# ptusa-opcua — OPC UA-фасад эмулятора мойки

Отдаёт по OPC UA **все** каналы станции BN1-МСА1 из настоящей прошивки ptusa. Собственный
OPC UA-сервер прошивки (`--opc r|rw`) публикует только `<прибор>.state/.value` — 22 % каналов и
запись, которая на прибор не действует, — поэтому фасад берёт у прошивки весь снимок её родным
протоколом (driver-master) и превращает в узлы OPC UA. Устройство, соглашения и проверка — в
`docs/SPECIFICATION.md` §5; код — `bridge.py`.

```
прошивка ptusa  ←driver-master→  bridge.py  ←OPC UA :4840→  шлюз
```

- Узел: `ns=2;s=<прибор>.<поле>` (`LINE1V0.ST`, `OBJECT1.RT_PAR_F[12]`, `SYSTEM.UP_TIME`).
- Запись = `__<прибор>:set_cmd('<поле>', <индекс>, v)`; код результата прошивки → статус записи.
- Нет связи с прошивкой → все узлы `BadCommunicationError`, фасад переподключается сам.

Стенд целиком — `docker-compose.moika.yml` в корне репозитория. Тесты (фейковый PAC, без
прошивки): `pip install -r requirements.txt pytest && python -m pytest -q tests` (Python 3.11).

Настройки: `PAC_HOST`/`PAC_PORT` (прошивка), `STATION_CONFIG` (`config/stations/*.yaml`),
`OPCUA_ENDPOINT` (адрес, который сервер анонсирует клиентам: `localhost` — шлюз на хосте,
имя сервиса — шлюз в сети compose; клиент после discovery идёт именно по нему),
`OPCUA_BIND` (на чём слушать, `0.0.0.0:4840`), `POLL_MS` (период снимка, 500).
