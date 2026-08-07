# sooqa

sooqa is a self-hosted Telegram media inbox, catalogue, deduplication engine,
and publishing queue. It is intended for a single administrator first, with a
durable always-on backend and an optional Windows capture companion.

The product and engineering source of truth is [docs/PROJECT_SPEC.md](docs/PROJECT_SPEC.md).
The repository is being built through focused vertical increments. PostgreSQL
backed ingest, library, media primitives, and the first Telegram adapter are
already present; URL ingest through the bot, media storage, and publication are
still later slices.

## Development

The pinned toolchain is Rust 1.97.1. Run the local quality gate with:

```bash
just check
```

The equivalent Cargo commands are:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```

To inspect the workspace applications:

```bash
cargo run -p sooqa-server
cargo run -p sooqa-worker
cargo run -p sooqa-companion
```

These binaries are intentionally minimal until their respective roadmap slices
are implemented.

## Repository shape

- `apps/` contains executable applications.
- `crates/` contains the modular-monolith library boundaries.
- `docs/` contains the specification, architecture notes, and ADRs.
- `userscripts/` will contain browser capture helpers.

The project uses Apache-2.0. See [LICENSE](LICENSE) and
[ADR 0006](docs/adr/0006-license.md).

## Configuration check

The server, worker, and companion accept a TOML configuration file and
environment-variable overrides. Start from config.example.toml.

    cargo run -p sooqa-server -- --config config.toml --check-config
    cargo run -p sooqa-worker -- --config config.toml --check-config
    cargo run -p sooqa-companion -- --config config.toml --check-config

Use SOOQA_CONFIG_FILE when you do not want to repeat --config. Environment
variables take precedence over TOML values. Configuration summaries redact
secret values. To enable the current Telegram adapter, configure
`SOOQA_TELEGRAM_BOT_TOKEN` and `SOOQA_TELEGRAM_ADMIN_USER_IDS`; see
[operations.md](docs/operations.md) for the startup sequence.

## PostgreSQL

On macOS, Docker Desktop is not required. Colima provides the Docker-compatible
engine used by the commands below:

    brew install colima
    colima start --runtime docker --cpu 2 --memory 4 --disk 30
    docker context use colima

If the `colima` context does not exist, register its socket once:

    docker context create colima --docker host=unix://$HOME/.colima/default/docker.sock
    docker context use colima

Confirm the active runtime with `docker context show`; it should print
`colima`. Stop it when finished with `colima stop`.

Start the development database with Docker Compose:

    docker compose up -d postgres

Apply the forward-only migrations:

    DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa cargo run -p sooqa-server -- migrate

Run the PostgreSQL integration test after the database is healthy:

    DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa cargo test -p sooqa-persistence --test postgres -- --ignored

The development password is intentionally simple and must not be reused in
production.
