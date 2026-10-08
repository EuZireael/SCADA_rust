# Поля прошивки без канала в базе станции

Снимок настоящей прошивки ptusa (BN1-МСА1, `GET_DEVICES_STATES`, снято `simulator probe-pac` 2026-10-08) против
`config/stations/BN1_MCA1.yaml`. Прошивка отдаёт **1113** пар «прибор.поле» (поля-массивы развёрнуты в `ПОЛЕ[ i ]`),
в базе каналов монитора — 2647 тегов станции. **138 пар прошивки в базе отсутствуют**: шлюз их не опрашивает и
в Kafka не публикует. Ниже полный список. Чтобы начать собирать, нужны каналы (`channelId`, имя тега) в базе монитора —
после этого запись добавляется в `BN1_MCA1.yaml` (`tools/station-config`).

Обратное — в базе есть 1672 тега, которых нет в этом снимке (прибор не поднят в данной сборке прошивки; шлюз по ним
получит BAD, пока прибор не появится).

| Прибор | Поля без канала |
|---|---|
| `CAB1HLA1` | `L_BLUE`, `L_GREEN`, `L_YELLOW`, `V` |
| `CAB2G1` | `LOAD_CURRENT_CH[ 5 ]`, `LOAD_CURRENT_CH[ 6 ]`, `LOAD_CURRENT_CH[ 7 ]`, `LOAD_CURRENT_CH[ 8 ]`, `NOMINAL_CURRENT_CH[ 5 ]`, `NOMINAL_CURRENT_CH[ 6 ]`, `NOMINAL_CURRENT_CH[ 7 ]`, `NOMINAL_CURRENT_CH[ 8 ]`, `ST_CH[ 5 ]`, `ST_CH[ 6 ]`, `ST_CH[ 7 ]`, `ST_CH[ 8 ]` |
| `CAB3G1` | `LOAD_CURRENT_CH[ 5 ]`, `LOAD_CURRENT_CH[ 6 ]`, `LOAD_CURRENT_CH[ 7 ]`, `LOAD_CURRENT_CH[ 8 ]`, `NOMINAL_CURRENT_CH[ 5 ]`, `NOMINAL_CURRENT_CH[ 6 ]`, `NOMINAL_CURRENT_CH[ 7 ]`, `NOMINAL_CURRENT_CH[ 8 ]`, `ST_CH[ 5 ]`, `ST_CH[ 6 ]`, `ST_CH[ 7 ]`, `ST_CH[ 8 ]` |
| `FQT1` | `F`, `P_CZ`, `P_DT` |
| `LINE1DI3` | `M`, `ST` |
| `LINE1LS1` | `E`, `M_EXP`, `S_DEV` |
| `LINE1LS2` | `E`, `M_EXP`, `S_DEV` |
| `LINE1LS3` | `E`, `M_EXP`, `S_DEV` |
| `LINE1M1` | `AMP`, `MAX_FRQ` |
| `LINE1PT1` | `E`, `M_EXP`, `ST`, `S_DEV` |
| `LINE1TE1` | `E`, `M_EXP`, `P_ERR`, `S_DEV` |
| `LINE1TE2` | `E`, `M_EXP`, `P_ERR`, `S_DEV` |
| `LINE1VC14` | `P_FB` |
| `LINE1WATCHDOG101` | `V` |
| `LINE1WATCHDOG11` | `V` |
| `LINE2DI3` | `M`, `ST` |
| `LINE2LS1` | `E`, `M_EXP`, `S_DEV` |
| `LINE2LS2` | `E`, `M_EXP`, `S_DEV` |
| `LINE2LS3` | `E`, `M_EXP`, `S_DEV` |
| `LINE2M1` | `AMP`, `MAX_FRQ` |
| `LINE2PT1` | `E`, `M_EXP`, `ST`, `S_DEV` |
| `LINE2TE1` | `E`, `M_EXP`, `P_ERR`, `S_DEV` |
| `LINE2TE2` | `E`, `M_EXP`, `P_ERR`, `S_DEV` |
| `LINE2VC14` | `P_FB` |
| `LINE2WATCHDOG101` | `V` |
| `LINE2WATCHDOG12` | `V` |
| `LINE2WATCHDOG201` | `V` |
| `LINE2WATCHDOG501` | `V` |
| `LINE3LS1` | `E`, `M_EXP`, `S_DEV` |
| `LINE3LS2` | `E`, `M_EXP`, `S_DEV` |
| `LINE3LS3` | `E`, `M_EXP`, `S_DEV` |
| `LINE3M1` | `AMP`, `MAX_FRQ` |
| `LINE3PT1` | `E`, `M_EXP`, `ST`, `S_DEV` |
| `LINE3TE1` | `E`, `M_EXP`, `P_ERR`, `S_DEV` |
| `LINE3TE2` | `E`, `M_EXP`, `P_ERR`, `S_DEV` |
| `LINE3VC14` | `P_FB` |
| `LINE3WATCHDOG1` | `V` |
| `LINE3WATCHDOG101` | `V` |
| `LS4` | `E`, `M_EXP`, `S_DEV` |
| `LS5` | `E`, `M_EXP`, `S_DEV` |
| `LS6` | `E`, `M_EXP`, `S_DEV` |
| `LS7` | `E`, `M_EXP`, `S_DEV` |
| `LS8` | `E`, `M_EXP`, `S_DEV` |
| `LS9` | `E`, `M_EXP`, `S_DEV` |
| `LT1` | `ST` |
| `LT2` | `ST` |
| `LT3` | `ST` |
| `M3` | `ERRT`, `R` |
