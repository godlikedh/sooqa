# sooqa

sooqa is a self-hosted Telegram media inbox and durable media-processing
pipeline for a single administrator. It is a modular monolith: PostgreSQL is
the source of truth, and workers advance durable jobs outside database
transactions.

## Current implementation

The repository currently provides:

- an OpenAPI-described HTTP Inbox API for creating and reading ingest requests;
    - one configured bearer secret for the single-admin API;
    - PostgreSQL-backed `ingests`, one-row `media`, `channels`, `posts`, and
      durable `queue.jobs` with leases and stale-lease recovery;
- direct HTTP and yt-dlp media adapters, ffprobe inspection, ffmpeg
  normalization, image normalization, exact hashing, and the bounded
  pre-storage video identity gate;
    - a Telegram long-polling adapter for admin URL/media submission and a
      media-row storage state machine;
- an explicit storage-intent reconciliation CLI for ambiguous Telegram
    uploads.
    - Publisher channel/post persistence with cadence slots and fenced sends.
- a minimal loopback companion at `POST /v1/submit` and a reviewed 2ch
  Tampermonkey script for direct MP4/WebM attachments;
    - Windows PR artifacts and version-tagged GitHub Releases provide a
      standalone x86_64 companion executable plus SHA-256 checksum.

The composed worker registers `inspect_source` with the SSRF-hardened direct
HTTP/allowlisted yt-dlp adapters, `download_source` into the shared media
workspace, `probe_asset`,
`normalize_asset`, `compute_fingerprint`, `finalize_ingest`, and, when
configured, `upload_storage_asset`.
The production worker is direct-only when `media.ytdlp_allowed_hosts` is empty;
when hosts are configured, page-like URLs use the pinned yt-dlp/Deno runtime
only for an exact host or dot-delimited subdomain match. Direct MP4/WebM
responses remain on the direct adapter regardless of the page allowlist. Video
normalization records a canonical artifact and
queues sequence fingerprinting; the worker then performs exact-SHA reuse or
the bounded `video_sequence_v1` identity decision before creating a
`pending_storage` media row. Strong perceptual matches become durable
`duplicate_pending` ingests and can be overridden through the authorized
force-save route. Images, animations, and audio use exact SHA only. Storage
upload and publication remain separate durable stages; Telegram publication
itself is not enabled yet.

Active documentation is the authority for current behavior:

- [Architecture](docs/architecture.md)
- [Development and testing](docs/development.md)
- [Operations](docs/operations.md)
- [Security](docs/security.md)
- [OpenAPI contract](docs/openapi.yaml)
- [ADRs](docs/adr/)

The original roadmap specification is retained as
[historical reference](docs/reference/PROJECT_SPEC.md); its future phases are
not an implementation status report.

## Development

The pinned toolchain is Rust 1.97.1. Run the local gate with:

```bash
just check
```

Useful focused commands:

```bash
just openapi-validate
just test-integration       # requires PostgreSQL
just test-media             # requires ffmpeg and ffprobe
```

The applications are:

```bash
cargo run -p sooqa-server
cargo run -p sooqa-worker
cargo run -p sooqa-companion
```

The companion requires its own local token plus the backend API token. Configure
`SOOQA_COMPANION_LOCAL_TOKEN`, `SOOQA_COMPANION_BACKEND_TOKEN`, and
`SOOQA_COMPANION_BACKEND_URL`; it never exposes or logs the backend token. The
userscript stores only the local token in Tampermonkey storage. Windows users
can download the standalone companion executable from the GitHub Releases page;
the setup and checksum verification steps are in
[operations.md](docs/operations.md).

`apps/` contains executable processes. `crates/` contains the modular-monolith
boundaries: configuration/runtime, Inbox, Library, Publisher, Jobs, Media,
Telegram, Persistence, API, and test support.

## PostgreSQL

Docker Desktop is not required on macOS. Colima supplies a Docker-compatible
engine:

```bash
brew install colima
colima start --runtime docker --cpu 2 --memory 4 --disk 30
docker context use colima
docker compose up -d postgres
```

The root Compose file is development-only. The separate self-hosted home
topology, including the pinned official local Telegram Bot API server, is
documented in [operations.md](docs/operations.md) and configured under
`deploy/home`.

PostgreSQL integration tests use SQLx-managed isolated databases. The database
named by `DATABASE_URL` is a test-control database: SQLx writes its
`_sqlx_test` bookkeeping state there before creating per-test databases. A
successful test is cleaned up automatically; a failed or panicking test may
leave its database behind for diagnosis, after which a later run of the same
test path can reclaim it or it must be dropped explicitly. The `sooqa` database
used by the test command is intentionally disposable local/CI test-control
state, not the runtime/home database. Use a separate test-control database if
the runtime also uses `sooqa`.

The `DATABASE_URL` role used for `just test-integration` must own or be allowed
to write to that control database and have `CREATEDB` privilege so SQLx can
create test databases. Use a dedicated development or CI account, never the
runtime/production account. Tests remain parallel and do not require
`--test-threads=1`.

Apply forward-only migrations:

```bash
DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa \
  cargo run -p sooqa-server -- migrate
```

The development password must not be reused in production. See
[operations.md](docs/operations.md) for worker, Telegram, and reconciliation
startup details.

## Configuration

Start from [config.example.toml](config.example.toml). TOML values can be
overridden by environment variables; secrets belong in the environment.
Check the effective redacted configuration with:

```bash
cargo run -p sooqa-server -- --config config.toml --check-config
cargo run -p sooqa-worker -- --config config.toml --check-config
```

The server and worker must share `media.work_root`. The current production
worker preflights only binaries required by its registered handlers; the
composed probe and normalization handlers require `ffprobe` and `ffmpeg`. When
the yt-dlp allowlist is non-empty, startup also requires the pinned `yt-dlp`
and Deno binaries and verifies the supported Deno version. The image in
`Dockerfile` contains the pinned official distributions.

The project is licensed under Apache-2.0. See [LICENSE](LICENSE) and
[ADR 0006](docs/adr/0006-license.md).
