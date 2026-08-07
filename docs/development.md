# Development

Use the pinned Rust toolchain and run `just check` before submitting changes.
Keep implementation work aligned with one roadmap slice from
[PROJECT_SPEC.md](PROJECT_SPEC.md), and keep each slice independently
compilable and testable.

## HTTP API contract

The versioned HTTP contract lives in [openapi.yaml](openapi.yaml). Validate it
with:

    just openapi-validate

The repository also pins the OpenAPI Generator CLI version in
`openapitools.json`. When a JDK is installed, generate a models-only Rust
preview with:

    just openapi-generate

Generated output is written to `target/openapi-generated/` and is intentionally
not committed. The API crate remains the integration boundary: generated
models can be adopted there when the contract and generator output are stable,
while authentication, persistence, and request orchestration stay in the
handwritten server layer.

## Source inspection

The C3 handler is tested with a deterministic fake downloader. Run its
PostgreSQL-backed integration test with:

    DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa cargo test -p sooqa-worker --test inspection -- --ignored

Durable jobs are represented in application code by `JobCommand` variants and
typed payload structs. JSONB decoding is limited to the persistence row
mapper, where the database `job_type` discriminator is checked against the
payload before a `Job` enters the worker. New enqueue sites should use a typed
`NewJob` constructor rather than constructing JSON directly.

The D1 direct HTTP adapter has focused unit tests with a local fake server:

    cargo test -p sooqa-media

It performs DNS/IP policy checks before every request, disables automatic
redirect handling, validates each redirect target, and streams downloads with
byte and timeout limits. The worker binary still uses explicit handler
composition; D1 does not yet register this adapter in production.

yt-dlp integration, ffprobe, and isolated media workspaces remain later slices.
