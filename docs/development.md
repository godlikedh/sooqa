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
API ingest and library routes, worker behavior, source inspection, source
download handoff, probe-to-normalize handoff, fake-runner normalization,
canonical library finalization, video fingerprint extraction, similarity
candidate generation, and the storage-upload job handoff.

The current `teloxide` dependency graph also emits a known Rust
future-incompatibility warning for the transitive `proc-macro-error2` crate
through `aquamarine`. There is no compatible local upgrade in this slice;
revisit it during the next Telegram dependency refresh. CI runs
`cargo check --workspace --all-targets --future-incompat-report`, and the
current dependency path is:

```text
proc-macro-error2 v2.0.1
└── aquamarine v0.6.0
    └── teloxide v0.17.0
```

The production worker uses direct HTTP for URL inspection, so it does not need
an external binary for that path. The composed probe, normalization, and video
fingerprint handlers require `ffprobe` and `ffmpeg`. The yt-dlp adapter is
tested separately and is not enabled in the worker until its subprocess egress
is isolated. Media integration tests that need locally installed binaries are
separate:

```bash
just test-media
```

CI installs ffmpeg for those tests. Normal unit tests use fake command runners
and do not contact Telegram or third-party media sites.

The current composed normalizer accepts video and static JPEG/PNG images.
For images, the probed format is authoritative; MIME type and safe filename
metadata are only fallbacks when probing cannot identify the format. Image
normalization reuses existing outputs only when their encoded bytes match the
current input and profile. Image finalization records both canonical and
thumbnail assets. Telegram documents with missing or generic metadata are
downloaded for probing; explicitly unsupported document metadata is rejected
at the adapter boundary. Finalization uses the existing ingest JSON metadata
to queue the versioned `frame_dhash_v1` video fingerprint job without a schema
migration. Videos persist their fingerprint after frame extraction, compare it
with completed Library videos, and persist candidate evidence in
`duplicate_candidates`; images skip the video-only stage and complete normally.
The worker currently composes `SimilarityConfig::default()` (`0.90` likely,
`0.75` possible) until similarity thresholds receive their own configuration
slice. Audio, animation, and unknown media are intentionally recorded by
probing and then fail terminally with `unsupported_media_kind` until their
normalization paths are added.

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
cargo run -p sooqa-server -- storage-intents mark-unknown <intent-id> --force --confirm
cargo run -p sooqa-server -- storage-intents reset <intent-id> --confirm
cargo run -p sooqa-server -- storage-intents attach <intent-id> <chat-id> <message-id> <file-id> <file-unique-id>
```

`mark-unknown` rejects active, unexpired reservations unless the operator
supplies both `--force` and `--confirm`. `reset` is deliberately explicit
because an unknown intent may represent a Telegram message that was created
successfully. It keeps the old job for history and creates a new upload
generation. Prefer `attach` when the external object can be identified; it
derives the asset, provider, and media kind from the locked intent.

## Repository conventions

Keep state-machine transitions and idempotency in Inbox/Persistence, keep
external calls outside database transactions, and pass subprocess arguments as
arrays. Unix media commands run in an owned process group so timeout and
cancellation cleanup reaches yt-dlp descendants; non-Unix builds retain the
direct-child fallback until a platform Job Object implementation is added. Add a focused test and update the active documentation whenever
behavior changes. The historical specification under `docs/reference/` is
roadmap context; code, tests, README, active docs, and ADRs describe the
current system.
