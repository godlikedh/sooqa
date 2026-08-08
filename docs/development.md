# Development

Use the pinned Rust toolchain (1.97.1). The normal quality gate is:

```bash
just check
```

It runs formatting, Clippy with warnings denied, and non-ignored workspace
tests. Inspect the changed files with `git diff --check` before opening a PR.

## OpenAPI

The versioned HTTP contract is [openapi.yaml](openapi.yaml). Validate it with:

```bash
just openapi-validate
```

The generator preview, when a JDK is installed, is intentionally written under
`target/` and is not committed. Handwritten API code remains responsible for
authentication, persistence, and orchestration.

## PostgreSQL integration tests

Start PostgreSQL with the local Docker-compatible runtime, apply migrations,
then run the complete database-backed set:

```bash
just db-up
just db-migrate
just test-integration
```

The `-- --ignored` part is intentional: Cargo uses the first `--` to stop
parsing Cargo test options and passes `--ignored` to the test harness, which
then runs tests marked `#[ignore]`. The integration recipe covers persistence,
API ingest and library routes, worker behavior and source inspection.

The current `teloxide` dependency graph also emits a known Rust
future-incompatibility warning for the transitive `proc-macro-error2` crate
through `aquamarine`. There is no compatible local upgrade in this slice;
revisit it during the next Telegram dependency refresh.

Media integration tests that need locally installed binaries are separate:

```bash
just test-media
```

CI installs ffmpeg for those tests. Normal unit tests use fake command runners
and do not contact Telegram or third-party media sites.

## Telegram adapter

The server starts Telegram long polling only when a bot token and administrator
IDs are configured. The adapter claims update IDs in PostgreSQL before
handling them, completes a claim only after the response succeeds, and releases
failed claims for retry. It accepts private admin `/start`, `/help`, `/add`,
`/status`, bare URLs, and supported media messages.

The application download ceiling is `[telegram].max_download_bytes` or
`SOOQA_TELEGRAM_MAX_DOWNLOAD_BYTES`. The standard cloud Bot API smaller limit
still applies when the configured API host is `api.telegram.org`.

## Storage reconciliation CLI

These commands run through `sooqa-server` and require `DATABASE_URL`:

```bash
cargo run -p sooqa-server -- storage-intents list
cargo run -p sooqa-server -- storage-intents mark-unknown <intent-id>
cargo run -p sooqa-server -- storage-intents reset <intent-id> --confirm
cargo run -p sooqa-server -- storage-intents attach <intent-id> <asset-id> <chat-id> <message-id> <media-kind> <file-id> <file-unique-id>
```

`reset` is deliberately explicit because an unknown intent may represent a
Telegram message that was created successfully. Prefer `attach` when the
external object can be identified.

## Repository conventions

Keep state-machine transitions and idempotency in Inbox/Persistence, keep
external calls outside database transactions, and pass subprocess arguments as
arrays. Add a focused test and update the active documentation whenever
behavior changes. The historical specification under `docs/reference/` is
roadmap context; code, tests, README, active docs, and ADRs describe the
current system.
