#!/usr/bin/env bash
# ============================================================================
# Стенд на эмуляторе мойки одной командой: прошивка ptusa (проект станции) + OPC UA-фасад +
# шлюз (Rust) + Kafka + Postgres. Подробности — docker-compose.moika.yml, docs/SPECIFICATION.md §5.
#
#   MOIKA_PROJECT=<папка проекта ПЛК> ./up-moika.sh          # весь стенд
#   MOIKA_PROJECT=… ./up-moika.sh plc                        # только прошивка + фасад (шлюз и Kafka — свои)
#   ./up-moika.sh down [-v]                                  # остановить (-v — стереть БД)
#
# MOIKA_PROJECT — папка проекта станции (BN1-CIP1): main.plua, main.io.lua, main.objects.lua…
# и подмодуль bin/ (git submodule update --init). Проект проприетарный, в репозиторий не входит;
# по умолчанию ищется ../Moika/BN1-CIP1. Windows: те же команды docker compose напрямую (см.
# docs/MONITOR_INTEGRATION.md).
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")"
COMPOSE=(docker compose -f docker-compose.moika.yml)

if [ "${1:-}" = "down" ]; then
  MOIKA_PROJECT="${MOIKA_PROJECT:-.}" "${COMPOSE[@]}" down "${@:2}"
  exit 0
fi

export MOIKA_PROJECT="${MOIKA_PROJECT:-../Moika/BN1-CIP1}"
[ -f "$MOIKA_PROJECT/main.plua" ] || { echo "Нет проекта ПЛК в $MOIKA_PROJECT (нужен main.plua). Задайте MOIKA_PROJECT." >&2; exit 1; }
[ -f "$MOIKA_PROJECT/bin/linux-default/Release/ptusa_main" ] || {
  echo "В проекте нет прошивки bin/linux-default/Release/ptusa_main — подмодуль bin не загружен:" >&2
  echo "  git -C \"$MOIKA_PROJECT\" submodule update --init" >&2; exit 1; }

if [ "${1:-}" = "plc" ]; then
  "${COMPOSE[@]}" up -d --build ptusa ptusa-opcua
  echo "▶ OPC UA станции: opc.tcp://localhost:4840 (driver-master прошивки: localhost:10100)"
  echo "  Шлюз: CONTROLLERS_CONFIG=file:$PWD/config/stations/BN1_MCA1.yaml PLC_HOST=localhost"
  exit 0
fi

"${COMPOSE[@]}" up -d --build
echo "▶ Шлюз: http://localhost:8888/actuator/health · Kafka для монитора: localhost:9094 · OPC UA: localhost:4840"
