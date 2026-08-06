# sooqa

sooqa is a self-hosted Telegram media inbox, catalogue, deduplication engine,
and publishing queue. It is intended for a single administrator first, with a
durable always-on backend and an optional Windows capture companion.

The product and engineering source of truth is [docs/PROJECT_SPEC.md](docs/PROJECT_SPEC.md).
The repository is currently at the bootstrap stage: the Rust workspace and
quality gates exist, while database, Telegram, media, and business workflows
will be introduced in focused increments.

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
secret values.
