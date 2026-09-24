#!/usr/bin/env python3
"""
Подготовка архива тегов BN1_MCA1 для воспроизведения (replay) симулятором.

Парсит SQL-дамп таблицы taglog(cid, dtdatetime, dvalue) и формирует:

  1. data/archive_replay.pkl.gz   — компактные ряды значений по каждому тегу
                                     (numpy-массивы смещений во времени и значений);
  2. config/replay_config.yaml    — готовый конфиг симулятора со всеми тегами
                                     (generator: replay, строковые OPC UA nodeId);
  3. config/gateway_opcua_tags.yaml — сниппет opcua.tags для application.yaml шлюза.

Запуск (из каталога simulator):
    python tools/build_archive.py "../BN1_MCA1 Архив тегов/BN1_MCA1.sql"
"""

import sys
import re
import gzip
import pickle
import datetime as dt
from pathlib import Path

import numpy as np
import yaml

# --- Пути -------------------------------------------------------------------
HERE = Path(__file__).resolve().parent
SIM_ROOT = HERE.parent
DATA_DIR = SIM_ROOT / "data"
CONFIG_DIR = SIM_ROOT / "config"

ARCHIVE_PKL = DATA_DIR / "archive_replay.pkl.gz"
REPLAY_CONFIG = CONFIG_DIR / "replay_config.yaml"
GATEWAY_TAGS = CONFIG_DIR / "gateway_opcua_tags.yaml"

# Регэксп строки INSERT
ROW_RE = re.compile(
    r"VALUES\s*\(\s*'([0-9A-Fa-f]+)'\s*,\s*'([^']+)'\s*,\s*'([^']*)'\s*\)"
)

# --- Справочник типов сигнала по второму байту cid --------------------------
# Второй байт (cid[2:4]) кодирует класс сигнала в схеме адресации МСА.
# unit — best-effort: где диапазон явно указывает на величину.
SIGNAL_HINTS = {
    "01": ("DI",   ""),      # дискретный вход
    "05": ("AI",   "%"),     # аналоговый, 0..100
    "0A": ("AI",   ""),      # ratio / счётчики
    "0D": ("STAT", ""),      # статус / код
    "13": ("AI",   "%"),     # 0..100
    "15": ("DI",   ""),      # дискретный статус
    "1D": ("AI",   ""),      # аналог (разное)
    "1F": ("DI",   ""),      # дискретный
    "22": ("STAT", ""),      # код/статус (… , 0..555)
    "31": ("STAT", ""),
    "32": ("STAT", ""),
    "3B": ("AI",   "Hz"),    # частота 0..68
    "37": ("AI",   ""),
    "36": ("AI",   ""),
    "4D": ("AI",   ""),      # аналог большой (1000..7950)
    "51": ("AI",   ""),      # 0..1000
    "58": ("DI",   ""),      # дискретный
    "5B": ("DI",   ""),      # дискретный
    "63": ("AI",   ""),      # аналог
    "75": ("STAT", ""),      # -1..1
}


def parse_sql(sql_path: Path):
    """Один проход по SQL: cid -> список (epoch_ts, value)."""
    series = {}            # cid -> list[(epoch, value)]
    min_ts = None
    max_ts = None
    skipped = 0
    total = 0

    def to_epoch(s: str) -> float:
        # формат "2026-06-11 00:44:44.77" (дробная часть 0..6 знаков, может отсутствовать)
        if "." in s:
            base, frac = s.split(".", 1)
            frac = (frac + "000000")[:6]
            s = f"{base}.{frac}"
            fmt = "%Y-%m-%d %H:%M:%S.%f"
        else:
            fmt = "%Y-%m-%d %H:%M:%S"
        return dt.datetime.strptime(s, fmt).timestamp()

    with open(sql_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            m = ROW_RE.search(line)
            if not m:
                continue
            total += 1
            cid, ts_str, val_str = m.group(1).upper(), m.group(2), m.group(3)
            try:
                value = float(val_str)
            except ValueError:
                skipped += 1
                continue
            try:
                epoch = to_epoch(ts_str)
            except ValueError:
                skipped += 1
                continue

            series.setdefault(cid, []).append((epoch, value))
            if min_ts is None or epoch < min_ts:
                min_ts = epoch
            if max_ts is None or epoch > max_ts:
                max_ts = epoch

    print(f"  строк обработано: {total}, пропущено: {skipped}, тегов: {len(series)}")
    return series, min_ts, max_ts


def classify(values: np.ndarray):
    """Определить тип данных по фактическому множеству значений."""
    vmin = float(values.min())
    vmax = float(values.max())
    uniq = np.unique(values)
    if np.all(np.isin(uniq, (0.0, 1.0))):
        return "bool", vmin, vmax
    # все ли целочисленные?
    if np.all(values == np.floor(values)) and abs(vmax) < 2**31 and abs(vmin) < 2**31:
        return "int", vmin, vmax
    return "float", round(vmin, 4), round(vmax, 4)


def build(sql_path: Path):
    """Главный конвейер: SQL → ряды numpy по тегам → .pkl.gz архив + два YAML-конфига."""
    print(f"Читаю {sql_path} ...")
    series, min_ts, max_ts = parse_sql(sql_path)
    duration = max_ts - min_ts
    print(f"  период архива: {duration/86400:.2f} сут "
          f"({dt.datetime.fromtimestamp(min_ts)} .. {dt.datetime.fromtimestamp(max_ts)})")

    replay = {}      # cid -> {"t": float32[], "v": float32[]}
    tags_meta = []   # для конфигов

    for cid in sorted(series):
        rows = sorted(series[cid])  # по времени
        t = np.fromiter((r[0] - min_ts for r in rows), dtype=np.float64)
        v = np.fromiter((r[1] for r in rows), dtype=np.float64)
        # на всякий случай убираем дубли по времени (оставляем последний)
        t = t.astype(np.float32)
        replay[cid] = {"t": t, "v": v.astype(np.float32)}

        data_type, vmin, vmax = classify(v)
        sclass, unit = SIGNAL_HINTS.get(cid[2:4], ("AI", ""))
        tags_meta.append({
            "cid": cid,
            "type": data_type,
            "min": vmin,
            "max": vmax,
            "unit": unit,
            "signal_class": sclass,
            "count": len(rows),
        })

    # --- 1) бинарь для replay ---
    DATA_DIR.mkdir(exist_ok=True)
    with gzip.open(ARCHIVE_PKL, "wb") as f:
        pickle.dump({
            "start_epoch": min_ts,
            "duration": duration,
            "series": replay,
        }, f, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"  -> {ARCHIVE_PKL} ({ARCHIVE_PKL.stat().st_size/1e6:.1f} МБ)")

    # --- 2) конфиг симулятора ---
    write_replay_config(tags_meta, duration)
    # --- 3) сниппет тегов для шлюза ---
    write_gateway_tags(tags_meta)

    print("Готово.")


def write_replay_config(tags_meta, duration):
    """Пишет config/replay_config.yaml — конфиг симулятора (все теги как generator=replay)."""
    sim_tags = []
    for m in tags_meta:
        tag = {
            "name": m["cid"],
            "address": m["cid"],            # -> ns=2;s=<cid>
            "type": m["type"],
            "protocol": "opcua",
            "access": "RO",
            "generator": "replay",
            "noise_enabled": False,
            "drift_enabled": False,
        }
        if m["unit"]:
            tag["unit"] = m["unit"]
        if m["type"] != "bool":
            tag["min"] = m["min"]
            tag["max"] = m["max"]
        sim_tags.append(tag)

    config = {
        "plc": {
            "id": "BN1-MCA1-REPLAY",
            "name": "BN1_MCA1 Archive Replay",
            "endpoint": "opc.tcp://0.0.0.0:4840",
            "update_rate": 0.5,
            "data_blocks": [
                {"db_number": 1, "name": "BN1_MCA1", "tags": sim_tags}
            ],
        },
        # Параметры воспроизведения архива
        "replay": {
            "enabled": True,
            "data_path": "data/archive_replay.pkl.gz",
            # ускорение времени: 1.0 = реальное время (5 суток),
            # 60.0 = 1 минута архива за секунду (~2 часа на весь архив),
            # 720.0 = весь архив примерно за 10 минут.
            "speed": 720.0,
            "loop": True,
        },
    }
    CONFIG_DIR.mkdir(exist_ok=True)
    with open(REPLAY_CONFIG, "w", encoding="utf-8") as f:
        yaml.safe_dump(config, f, allow_unicode=True, sort_keys=False, default_flow_style=False)
    print(f"  -> {REPLAY_CONFIG} ({len(sim_tags)} тегов)")


def write_gateway_tags(tags_meta):
    """Пишет config/gateway_opcua_tags.yaml — сниппет opcua.tags для application.yaml шлюза."""
    type_map = {"bool": "BOOLEAN", "int": "INT", "float": "FLOAT"}
    gw_tags = []
    for m in tags_meta:
        t = {
            "name": m["cid"],
            "nodeId": f"ns=2;s={m['cid']}",
            "dataType": type_map[m["type"]],
            "pollingRate": 1000,
            "enabled": True,
        }
        if m["unit"]:
            t["unit"] = m["unit"]
        if m["type"] != "bool":
            # операционные пределы чуть уже физического диапазона — для алармов
            span = m["max"] - m["min"]
            t["minValue"] = round(m["min"] + 0.05 * span, 3)
            t["maxValue"] = round(m["max"] - 0.05 * span, 3)
        gw_tags.append(t)

    snippet = {
        "opcua": {
            "servers": [{
                "id": "bn1-mca1-001",
                "name": "BN1_MCA1 Archive Replay",
                "endpoint": "opc.tcp://127.0.0.1:4840",
                "security": "NONE",
                "enabled": True,
                "tags": gw_tags,
            }]
        }
    }
    with open(GATEWAY_TAGS, "w", encoding="utf-8") as f:
        yaml.safe_dump(snippet, f, allow_unicode=True, sort_keys=False, default_flow_style=False)
    print(f"  -> {GATEWAY_TAGS} ({len(gw_tags)} тегов для application.yaml)")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        # путь по умолчанию
        default = SIM_ROOT.parent / "BN1_MCA1 Архив тегов" / "BN1_MCA1.sql"
        sql = default
    else:
        sql = Path(sys.argv[1])
    if not sql.exists():
        print(f"Не найден SQL-файл: {sql}")
        sys.exit(1)
    build(sql)
