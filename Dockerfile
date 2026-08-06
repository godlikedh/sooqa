FROM rust:1.97-bookworm AS builder

WORKDIR /usr/src/sooqa
COPY . .
RUN cargo build --release --workspace

FROM debian:bookworm-slim

RUN groupadd --system sooqa && useradd --system --gid sooqa sooqa \
    && apt-get update \
    && apt-get install --no-install-recommends --yes ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/sooqa/target/release/sooqa-server /usr/local/bin/sooqa-server
COPY --from=builder /usr/src/sooqa/target/release/sooqa-worker /usr/local/bin/sooqa-worker
COPY --from=builder /usr/src/sooqa/target/release/sooqa-companion /usr/local/bin/sooqa-companion

USER sooqa
ENTRYPOINT ["sooqa-server"]
