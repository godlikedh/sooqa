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

## Telegram adapter

H1 adds the first Telegram vertical boundary. The server enables long polling
only when `SOOQA_TELEGRAM_BOT_TOKEN` is configured. It also requires at least
one positive administrator ID when the token is configured:

    SOOQA_TELEGRAM_BOT_TOKEN=123456:secret
    SOOQA_TELEGRAM_ADMIN_USER_IDS=123456789
    DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa
    cargo run -p sooqa-server -- migrate
    cargo run -p sooqa-server

The equivalent TOML settings are `[telegram].api_base_url`,
`[telegram].admin_user_ids`, `[telegram].poll_timeout_seconds`, and optional
`[telegram].storage_chat_id`. The API
base URL accepts only an HTTP(S) URL without credentials, which also supports
a self-hosted Local Bot API Server. Telegram update IDs are claimed in
`telegram_update_receipts` before command handling, so redelivered updates do
not send a second response. Claims have lease tokens and are completed only
after a response succeeds; a failed response releases the claim for retry.
The polling loop advances Telegram's offset only after the update handler
succeeds, so a transient response or persistence failure is retried in the
same process. Each handler gets five bounded attempts; a still-failing update
stops the runtime without advancing the offset, allowing a supervisor restart
to reclaim it. Transient `getUpdates` failures use the same offset and a short
backoff.
After five consecutive polling failures, the runtime returns an error so a
supervisor can surface or restart it instead of spinning forever.
The initial authorized commands are `/start`, `/help`, `/add`, and `/status`;
only private messages from configured administrator IDs are handled. Send a
URL as `/add https://example.test/video.webm` or as a bare message containing
one URL to create a durable Inbox request. The bot replies with the request ID
and current status. A failed Inbox call releases the Telegram receipt and
clears its response rate-limit entry so the update can be retried safely. If
the response limiter is active, the request is still created and only the
acknowledgement is suppressed.
Callback data uses the versioned `v1:ingest_status:<request-id>` convention,
but callback buttons and status refreshes are not implemented yet. Responses
are rate-limited per user/chat, and unauthorized attempts produce structured
warnings without message contents or secrets.

H4 also accepts photo, video, animation, audio, and recognizable document
messages from the same administrators. The adapter downloads them through the
configured Bot API, preserves Telegram message/file metadata and captions in
the Inbox request, and enqueues a typed `probe_asset` job. Telegram's cloud
Bot API limit of 20 MiB is rejected before download; a Local Bot API Server is
used when configured through `api_base_url` and is not subject to that
cloud-only check. Unsupported document types receive a warning and do not
create an ingest request.

The adapter tests use a mocked API and receipt store, so they do not contact
Telegram:

    cargo test -p sooqa-telegram

H3 storage tests use a mocked Telegram API and a fake upload store:

    cargo test -p sooqa-telegram storage

H4 direct media tests use the mocked Telegram API and exercise metadata
preservation, document type detection, idempotent update handling, and
unsupported-document rejection:

    cargo test -p sooqa-telegram authorized_media

The PostgreSQL-backed H4 persistence test verifies that a Telegram submission
creates one `telegram_message` request and one idempotent `probe_asset` job:

    DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa cargo test -p sooqa-persistence --test ingest creates_telegram_ingest_and_probe_job_atomically -- --ignored

The storage provider requires a negative Telegram chat ID, loads the canonical
asset hash from PostgreSQL, hashes the local file before upload, and persists
references only after Telegram returns a message. Definitive API rejections
can release a pending intent; ambiguous failures remain unknown for
reconciliation; pending intents are not automatically reclaimed. The server and worker verify that the bot is an administrator
with posting rights in the configured private channel. The worker registers
`upload_storage_asset` when the Telegram token and storage chat are configured;
canonical-asset recording enqueues that job with a deterministic idempotency key.

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

## Normalization execution

F2 adds `FfmpegExecutor` for running a planner result. It adds
`-progress pipe:1`, bounds captured output, reports process failures and
cancellation, requires a final `progress=end` record, validates the generated
MP4 with ffprobe, and hashes it incrementally. The executor should be called
before opening the short persistence transaction; database transactions must
not span ffmpeg, ffprobe, or hashing.

Run its focused tests with:

    cargo test -p sooqa-media execute::tests

The generated-media test is intentionally ignored in normal runs because it
requires local ffmpeg and ffprobe binaries. Run it explicitly with:

    cargo test -p sooqa-media execute::tests::executes_generated_mp4_with_real_ffmpeg_and_ffprobe -- --ignored

After successful execution, convert the digest to the library's 32-byte SHA
representation and call `LibraryRepository::record_canonical_asset`. That
method is idempotent for a replay of the same content and digest and rejects a
canonical digest already attached to another content item.

## Image normalization

F3 adds `ImageNormalizer` for the MVP JPEG/PNG path. It uses the configured
maximum dimensions without upscaling, strips incidental metadata by decoding
and re-encoding, keeps meaningful alpha as PNG, converts opaque images to JPEG,
and emits a same-format thumbnail. Decoding is bounded by configurable input
byte, pixel, decoder-allocation, and estimated working-set limits; EXIF
orientation is applied before metadata is discarded; APNG is rejected for the
static path; and output files use same-directory atomic no-clobber publication.
The worker revalidates the fixed workspace directories immediately before I/O,
and input parents are checked for symlinks. The working-set setting is a
conservative preflight budget for decoder, conversion, resize, and output
buffers; the image crate does not expose a strict process-wide allocator cap.
Callers should obtain paths from `MediaWorkspace` so the workspace boundary is
established before normalization.
Decoding runs on Tokio's blocking pool because image codecs are CPU-bound.

Run its focused tests with:

    cargo test -p sooqa-media image_normalize::tests

## Frame fingerprinting

G1 adds `FrameExtractor` for the versioned `frame_dhash_v1` fingerprint. It
uses ffprobe's validated duration, extracts stable relative timestamps through
the shell-free ffmpeg runner, keeps frame files in the workspace `frames`
area, and hashes decoded images on Tokio's blocking pool. The 64-bit dHash is
computed from a grayscale 9×8 image by comparing adjacent horizontal pixels;
very short inputs collapse duplicate timestamp positions. Frame decoding has
byte, pixel, and conservative working-set limits, and valid frame outputs are
reused on retry so partial extraction can resume. G2 will add scoring,
candidate persistence, and similarity thresholds.

Run its focused tests with:

    cargo test -p sooqa-media fingerprint::tests

## Similarity scoring and duplicate candidates

G2 adds pure `compare_videos` scoring for G1 fingerprints. Duration and aspect
ratio prefilters avoid expensive comparisons for clearly unrelated inputs;
remaining frames are matched by relative timestamp and scored with median
64-bit Hamming distance. The default score combines visual, duration, and
structure signals, with 0.90 likely-duplicate and 0.75 possible-duplicate
thresholds. Evidence is versioned and serializable for later review.

The library and persistence boundaries add the ordered `duplicate_candidates`
record. Upserts preserve a candidate's review status while refreshing its
score/evidence, and the unique algorithm-versioned pair makes rescans
idempotent. G3 will add review actions and the read API.

Run focused media tests with:

    cargo test -p sooqa-media similarity::tests

Run the PostgreSQL candidate integration test with:

    DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa cargo test -p sooqa-persistence --test library duplicate_candidates_upsert_ordered_pairs_and_evidence -- --ignored

## Duplicate resolution API

G3 exposes duplicate candidates through the authenticated API. Library readers
can list candidates by status or inspect one with its audit events. Library
writers can confirm a variant, keep the pair separate, or dismiss it. Each
action is a single PostgreSQL transaction that locks the candidate, allows only
the pending state, and records the acting device token and `Idempotency-Key`.
Retrying the same action with the same key replays the decision; a different
key returns a stable conflict. The API does not automatically merge or delete
content.

Run its PostgreSQL API integration test with:

    DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa cargo test -p sooqa-api --test library authenticated_duplicate_candidate_api_supports_review_actions -- --ignored

## Normalization planner

F1 adds a pure planner for the canonical video profile. It selects a remux for
already-compatible MP4/H.264/`yuv420p`/AAC inputs within the configured
dimensions and frame-rate cap; all other video probes receive a deterministic
transcode command with aspect-preserving scaling. The planner returns the
shell-free `ExternalCommand` but does not run ffmpeg.

Run its tests with:

    cargo test -p sooqa-media normalize::tests

The profile defaults to MP4, H.264, `yuv420p`, AAC, 1920×1080 maximum
dimensions, 60 fps maximum, x264 medium, CRF 23, 128 kbps audio, fast start,
and stripped incidental metadata. See [ADR 0008](adr/0008-canonical-media-profile-v1.md)
for the decision boundary and the F2 follow-up.

## Library persistence

The E1 library migration is `migrations/0004_library.sql`. It creates
`content_items`, `media_assets`, `source_records`, `tags`,
`content_item_tags`, and `storage_objects`. The repository round-trip test is
ignored unless PostgreSQL is available:

    DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa cargo test -p sooqa-persistence --test library -- --ignored

The E3 API layer now exposes this repository through authenticated search and
detail routes; its PostgreSQL-backed test is described below.

## Exact duplicate resolution

The E2 repository flow accepts typed content, canonical-asset, and source
drafts. It checks normalized URLs and platform identities first, then uses the
asset SHA-256. Exact matches reuse the existing content and canonical asset;
new source metadata is attached when its identity is new. The insert paths use
PostgreSQL conflict handling inside one transaction, so concurrent requests do
not create a second canonical asset.

Run the focused PostgreSQL tests with:

    DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa cargo test -p sooqa-persistence --test library -- --ignored

The concurrency test submits two different source URLs for the same SHA-256
and verifies one content item, one canonical asset, and two source records.

## Library API

The E3 HTTP slice exposes authenticated Library search and item operations. It
uses `library:read` for reads and `library:write` for edits, tag mutations, and
archive. Search returns an opaque cursor ordered by `(updated_at, id)` and
defaults to active items; pass `status=archived` when reviewing archived
content.

Run the PostgreSQL-backed API test with:

    DATABASE_URL=postgres://sooqa:sooqa_dev_only@127.0.0.1:5432/sooqa cargo test -p sooqa-api --test library -- --ignored
