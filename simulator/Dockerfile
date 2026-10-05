# ============================================================================
# Образ PLC-симулятора (Rust). Поднимает OPC UA (:4840), Modbus TCP (:5020) и PAC driver-master
# (:10000), проигрывает архив BN1_MCA1 в реальном времени. Двухэтапная сборка: компиляция в
# rust:bookworm, рантайм — debian:bookworm-slim (бинарнику нужна только glibc).
#
#   docker build -t scada-simulator ./simulator
#   docker run -e OPCUA_ENDPOINT=opc.tcp://simulator:4840 -p 4840:4840 -p 5020:5020 -p 10000:10000 scada-simulator
# ============================================================================
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Кэш реестра crates и target между сборками (BuildKit).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked \
 && cp target/release/simulator /usr/local/bin/simulator

FROM debian:bookworm-slim
RUN groupadd -r sim && useradd -r -g sim -u 1001 sim
WORKDIR /app
COPY --from=build /usr/local/bin/simulator /app/simulator
# Конфигурация станции и архив для replay (data/archive_replay.bin.gz).
COPY config ./config
COPY data ./data
EXPOSE 4840 5020 10000
USER sim
# OPCUA_ENDPOINT задаётся в docker-compose.yml (opc.tcp://simulator:4840): сервер привязывается к
# этому адресу и анонсирует его клиентам.
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=6 CMD ["/app/simulator", "healthcheck"]
ENTRYPOINT ["/app/simulator"]
CMD ["config/replay_config.yaml"]
