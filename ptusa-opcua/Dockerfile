# ============================================================================
# Образ OPC UA-фасада эмулятора мойки (Rust). Сама прошивка ptusa — отдельный контейнер
# (debian:stable: glibc ≥ 2.38), фасад — бинарник, поэтому два сервиса. Двухэтапная сборка:
# компиляция в rust:bookworm (Lua собирается из исходников — нужен C-компилятор, он в образе),
# рантайм — debian:bookworm-slim.
#
#   docker build -t ptusa-opcua ./ptusa-opcua
#   docker run -v $PWD/config/stations/BN1_MCA1.yaml:/config/station.yaml:ro \
#     -e PAC_HOST=ptusa -p 4840:4840 ptusa-opcua
# ============================================================================
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# Кэш реестра crates и target между сборками (BuildKit).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked \
 && cp target/release/ptusa-opcua /usr/local/bin/ptusa-opcua

FROM debian:bookworm-slim
RUN groupadd -r opcua && useradd -r -g opcua -u 1001 opcua
WORKDIR /app
COPY --from=build /usr/local/bin/ptusa-opcua /app/ptusa-opcua
EXPOSE 4840
USER opcua
HEALTHCHECK --interval=10s --timeout=5s --retries=6 --start-period=10s CMD ["/app/ptusa-opcua", "healthcheck"]
ENTRYPOINT ["/app/ptusa-opcua"]
