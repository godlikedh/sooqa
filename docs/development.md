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

The D2 workspace and hashing tests are included in the same command. A
workspace is created under `<work-root>/jobs/<job-id>/`, exposes only fixed
output areas, writes a diagnostic manifest, and cleans up only its own
directory. `sha256_file` reads fixed-size chunks and returns both byte count and
lowercase SHA-256.

The D3 ffprobe adapter is also covered by `cargo test -p sooqa-media`. Its
parser maps container, duration, size, bitrate, stream, codec, video geometry,
frame-rate, rotation, and audio metadata into `MediaProbe`. The process runner
passes arguments directly, captures output with a fixed limit, and terminates
timed-out commands. The ignored `ffprobe` test generates a tiny WAV fixture in
the system temporary directory and requires a locally installed `ffprobe`:

    cargo test -p sooqa-media ffprobe::tests::probes_generated_wav_fixture_with_real_ffprobe -- --ignored

The worker reads `media.ffmpeg_path`, `media.ffprobe_path`, and
`media.ytdlp_path` from TOML, with `SOOQA_MEDIA_*_PATH` environment overrides.
Normal worker startup reports each binary version and exits if one is missing;
`--check-config` only validates and prints configuration, so it remains usable
on machines without media binaries.

The D4 yt-dlp adapter uses `--dump-single-json --skip-download --no-playlist`
for inspection and a controlled output path for downloads. It validates
HTTP(S) URLs, passes the configured format as one argument, bounds subprocess
output, classifies transient process failures, and checks the downloaded file
size. Its Unix fake-executable test verifies that URLs, format strings, and
paths are not shell-interpolated. Normal CI does not contact live third-party
sites.

Normalization and production media-job wiring remain later slices.
