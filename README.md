# sooqa

sooqa is a self-hosted Telegram media inbox and durable media-processing
pipeline for a single administrator. It is a modular monolith: PostgreSQL is
the source of truth, and workers advance durable jobs outside database
transactions.

## Current implementation

The repository currently provides:

- an OpenAPI-described HTTP Inbox API for creating and reading ingest requests;
- scoped bearer-token authentication with SHA-256 token hashes;
- PostgreSQL-backed idempotency, ingest state, job attempts, leases, and
  stale-lease recovery;
- direct HTTP and yt-dlp media adapters, ffprobe inspection, ffmpeg
  normalization, image normalization, hashing, and duplicate primitives;
- a Telegram long-polling adapter for admin URL/media submission and a durable
  Telegram storage-upload intent flow;
- an explicit storage-intent reconciliation CLI for ambiguous Telegram
  uploads.

The composed worker registers `inspect_source` with the SSRF-hardened direct
HTTP adapter, `download_source` into the shared media workspace, `probe_asset`,
`normalize_asset`, `compute_fingerprint`, and, when configured,
`upload_storage_asset`.
The yt-dlp adapter remains available behind the media boundary but is not yet
enabled in the production worker because its subprocess egress needs an
equivalent SSRF boundary. Normalization records a canonical video or static
image artifact (plus an image thumbnail) and queues `finalize_ingest`;
finalization creates or reuses the canonical library rows, fingerprints videos
using the existing ingest JSON metadata, and leaves storage upload/publishing
as separate boundaries.

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
composed probe and normalization handlers require `ffprobe` and `ffmpeg`. The
image in `Dockerfile` also contains `yt-dlp` for handlers added later.

The project is licensed under Apache-2.0. See [LICENSE](LICENSE) and
[ADR 0006](docs/adr/0006-license.md).
