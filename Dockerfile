# ============================================================================
# Образ SCADA-шлюза на Rust. Двухэтапная сборка: компиляция в rust:bookworm (нужен
# cmake — librdkafka собирается статически), рантайм — debian:bookworm-slim (бинарнику
# нужны только glibc и zlib). controllers.yaml монтируется в /app/config/.
#
#   docker build -t scada-gateway-rs .
#   docker run -v $PWD/controllers.yaml:/app/config/controllers.yaml:ro -p 8888:8888 \
#     -e SIM_HOST=… -e SPRING_DATASOURCE_URL=… -e SPRING_KAFKA_BOOTSTRAP_SERVERS=… scada-gateway-rs
# ============================================================================
FROM rust:1-bookworm AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends cmake \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock rustfmt.toml ./
COPY src ./src
COPY migrations ./migrations
# Кэш реестра crates и target между сборками (BuildKit).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked \
 && cp target/release/scada-gateway /usr/local/bin/scada-gateway

FROM debian:bookworm-slim
# Непривилегированный пользователь, как у Java-образа.
RUN groupadd -r scada && useradd -r -g scada -u 1001 scada
WORKDIR /app
COPY --from=build /usr/local/bin/scada-gateway /app/scada-gateway
ENV CONTROLLERS_YAML=/app/config/controllers.yaml
EXPOSE 8888
USER scada
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=6 CMD ["/app/scada-gateway", "healthcheck"]
ENTRYPOINT ["/app/scada-gateway"]
