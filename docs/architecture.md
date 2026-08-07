# Architecture

The intended architecture is a modular monolith with separate server, worker,
and optional Windows companion processes. The module boundaries and durable
workflow rules are defined in [PROJECT_SPEC.md](PROJECT_SPEC.md).

This document will grow with the implementation. Shared configuration and
process lifecycle plumbing lives in sooqa-config and sooqa-runtime. The server
exposes the HTTP API through sooqa-api, and sooqa-persistence provides
PostgreSQL migrations plus durable repositories. sooqa-worker provides the
bounded job loop, handler registry, leases, and graceful shutdown. The H1
Telegram boundary now adds a project-owned adapter around Teloxide: the server
can run long polling, authenticate private messages by configured Telegram
user ID, answer the initial command set, and persist Telegram update receipts
for restart-safe deduplication. URL ingest, media uploads, and publication
remain later Telegram slices.

The first Inbox vertical slice now lives in `sooqa-inbox` and
`sooqa-persistence`. It validates and conservatively normalizes URL
submissions, models the user-visible ingest state machine, stores
`ingest_requests`, and atomically creates the first `inspect_source` job.
Idempotency records bind a request key and payload hash to the original ingest
request, so a repeated request returns the existing resource while a changed
payload is rejected.

The C3 source-inspection boundary now lives in `sooqa-media`. A worker handler
loads the durable ingest request, invokes an injected `SourceDownloader`
outside any database transaction, then atomically moves the request to
`downloading` and enqueues the durable `download_source` job. Inspection
results travel in that job's typed `DownloadSource` command until a source
record is introduced. The current implementation uses a deterministic fake in
integration tests; the D1 `DirectHttpDownloader` now provides the separate
direct-HTTP adapter, while worker composition remains intentionally deferred.

The direct HTTP adapter validates only `http` and `https` URLs, rejects
credentials and private/special IP ranges, resolves domains before connecting,
pins the selected validated address in the HTTP client, and manually follows
bounded redirects so every target is checked again. It streams downloads to a
caller-provided path, enforces byte and timeout limits, and performs bounded
content sniffing. The D3 ffprobe adapter now runs external commands through a
shell-free Tokio process boundary with separate arguments, bounded stdout and
stderr capture, and a timeout. It converts ffprobe JSON into the project-owned
`MediaProbe` model instead of leaking tool-specific JSON into business logic.
The D4 `YtDlpDownloader` uses the same runner for supported page metadata and
downloads, with single-item mode, configured format selection, bounded output,
and a final destination-size check. Its parsed metadata is reduced to the
project-owned `YtDlpMetadata` summary; raw yt-dlp JSON does not cross the
adapter boundary. Production media-job wiring remains a later slice.

The F1/F2 normalization slices live in `sooqa-media`. A validated
`CanonicalVideoProfile` describes the default MP4, H.264, `yuv420p`, AAC,
1080p-capped representation. Compatible inputs receive a shell-free remux
command; other video probes receive an aspect-preserving transcode command with
explicit codec, bitrate, frame-rate, fast-start, and metadata arguments.
`FfmpegExecutor` runs that plan with bounded machine-readable progress output,
supports cancellation by dropping the child future, validates the generated
file with ffprobe, and computes its SHA-256 digest. The executor performs no
database work. A separate `LibraryRepository::record_canonical_asset` call
locks the content row briefly, idempotently records the canonical asset, and
updates `content_items.canonical_asset_id` after external work has completed.
Production media-job wiring remains a later slice.

The F3 image-normalization slice adds `ImageNormalizer` to `sooqa-media`.
JPEG and PNG inputs are decoded with an allocation limit, metadata is stripped
by re-encoding, opaque images become bounded JPEGs, and images with meaningful
transparency remain PNG. Canonical images and same-format thumbnails preserve
aspect ratio and never upscale smaller inputs. The result exposes dimensions,
format, output paths, and independent SHA-256 digests. Image plans are built
from `MediaWorkspace` paths, the workspace boundary is revalidated immediately
before I/O, input paths must be regular files without symlinked parents, output
files are published with same-directory atomic no-clobber links, and decoder
and conservative working-set budgets are explicit. EXIF orientation is applied
before metadata is discarded, and animated PNGs are rejected by the static
image path.
Persistence and production job composition remain outside the media crate.

The MVP assumes the media work root is process-owned and mode 0700. Workspace
validation is deliberately path-based and is not a defense against a separate
same-user process racing directory replacement between validation and I/O; a
shared hostile work root would need descriptor-relative no-follow operations.

The G1 fingerprinting slice adds a typed `FrameExtractor` to `sooqa-media`.
Callers pass the duration from the project-owned `MediaProbe` (or a validated
duration directly); ffmpeg extracts one PNG at each stable 5%, 15%, 30%, 50%,
70%, 85%, and 95% position. Very short videos collapse duplicate timestamps.
Frames are decoded on Tokio's blocking pool and converted to the versioned
`frame_dhash_v1` 64-bit horizontal difference hash. The result stores the
algorithm version, duration, relative timestamp basis points, and hashes;
extracted frame decoding has byte, pixel, and conservative working-set caps;
valid frames are reused on retry so partial extraction can resume. Similarity
scoring and durable candidate persistence remain G2.

The G2 similarity boundary keeps comparison pure in `sooqa-media`. It
prefilters by duration and aspect ratio, compares relative frame positions with
Hamming distance, aggregates the median visual distance, and combines visual,
duration, and structure signals into configurable candidate thresholds. The
evidence includes the algorithm version, per-frame distances, component
scores, and final score. `sooqa-library` owns the typed duplicate-candidate
record, while `sooqa-persistence` upserts ordered content-item pairs in
`duplicate_candidates`; G3 owns review actions and API exposure.

The G3 duplicate-resolution API adds authenticated candidate list/detail routes
and explicit confirm-variant, keep-separate, and dismiss transitions. A
PostgreSQL row lock allows only a pending candidate to transition, and each
decision inserts an immutable `duplicate_candidate_events` audit row with the
authenticated device-token ID and request idempotency key. Retrying the same
action with the same key replays the resolved candidate; a different key still
returns a stable conflict. Decisions do not merge or delete content; Telegram
presentation remains in Stack H.

The D2 media primitives now provide an isolated workspace at
`<work-root>/jobs/<job-id>/` with fixed source, normalized, frames, previews,
and logs directories plus a diagnostic `manifest.json`. Output names are
single validated components, symlinked directories and files are rejected,
cleanup is restricted to the workspace's expected jobs root, and
`sha256_file` hashes files incrementally without loading them into memory.
The manifest is diagnostic convenience only; PostgreSQL remains the source of
truth for durable workflow state.

The E1 Library slice adds relational PostgreSQL records for content items, media
assets, source records, tags, and provider storage objects. The `sooqa-library`
crate owns typed enums and records, while `sooqa-persistence::LibraryRepository`
converts database discriminators and signed PostgreSQL integer fields at the
adapter boundary. Tags are normalized and attached through an explicit join
table; perceptual duplicate candidates and search remain later slices.

The E2 exact-duplicate boundary is implemented by a transaction-level method
on `LibraryRepository`. It checks normalized source URLs and platform IDs before
looking up the downloaded SHA-256, then inserts a canonical asset and source
with conflict-safe re-reads. Concurrent requests therefore converge on one
content item and canonical asset; a new source can still attach to that item.
Migration `0005_exact_duplicates.sql` keeps a general SHA lookup index while
making canonical SHA-256 values and one canonical asset per content item unique.

Worker startup now checks the configured `ffmpeg`, `ffprobe`, and `yt-dlp`
executables and logs their detected versions. A worker exits before connecting
to PostgreSQL when a required media binary is missing or cannot report a
version; the HTTP server does not perform these checks.

Jobs have a typed command boundary. `Job` contains one `JobCommand` variant,
such as `InspectSource` or `DownloadSource`, with a payload struct specific to
that command. PostgreSQL still stores the durable queue using its
`job_type` discriminator and `payload_json` JSONB columns, but those are
storage details: the private persistence `JobRow` validates and decodes them
before returning a domain `Job`. Enqueuers use typed `NewJob` constructors, so
business logic does not inspect arbitrary JSON.

The server now connects to PostgreSQL for the authenticated ingest API. Device
tokens are stored as SHA-256 hashes with scopes and revocation timestamps; the
API requires `ingest:create` for submission and `ingest:read` for status reads.
`POST /api/v1/ingest-requests` accepts a generic URL and returns a durable
request ID, while `GET /api/v1/ingest-requests/{id}` exposes its current
user-visible state. The request and response shapes are declared in
[`openapi.yaml`](openapi.yaml); CI validates the contract, and a pinned
OpenAPI Generator recipe can emit Rust model previews without replacing the
handwritten authentication and orchestration boundary. Token provisioning and
revocation commands remain a later administration slice.

The H1 Telegram adapter is intentionally narrow. `sooqa-telegram` owns the
normalized message and command boundary, authorization decision, response
text, and polling lifecycle. Teloxide types and the Bot API client stay at that
edge. `sooqa-persistence` owns the `telegram_update_receipts` table and exposes
claim, complete, and release operations; the server supplies that repository
to the adapter. Claims have a lease token, completed receipts are durable, and
failed sends release the claim for retry. A configured bot token enables
polling in the server, while the configured administrator IDs decide which
private users may receive command responses. Responses are bounded by a
per-user/chat in-process rate limiter, unauthorized attempts are structured
warnings, and group messages never enter Inbox.
The polling loop advances its Telegram offset only after a handler succeeds;
failed handlers retain the offset and retry up to five times with a short
backoff. A still-failing handler returns an error so the server supervisor can
restart it without acknowledging the update; transient polling errors are
also retried up to five times, while an invalid bot token is reported as
terminal.

H2 adds the first Telegram-to-Inbox path. The adapter accepts either `/add
<url>` or a private message containing exactly one HTTP(S) URL, then calls a
small `IngestService` port. The server adapter turns that command into the
same typed `IngestSubmission` used by the HTTP API and delegates the durable
request/job transaction to `InboxRepository`. The Telegram update ID becomes
the idempotency key (`telegram:update:<id>:v1`), so handler retries cannot
create a second request. The Telegram user ID remains in the adapter command;
the current administrator configuration has no user repository to map it to
the existing UUID foreign key, so persistence of that mapping is deferred to
the administration slice. Callback data is versioned as
`v1:ingest_status:<request-id>`; callback handling is a later UI increment.

The E3 Library API adds authenticated read and editorial-write routes over the
typed Library repository. Search defaults to active items, supports text,
kind, status, all-tag filtering, and opaque `(updated_at, id)` cursor pages.
Detail responses include the canonical asset, tags, and source records. Title,
description, and notes edits use optional optimistic timestamp checks; tags can
be attached or detached; archive is a reversible state transition for the MVP.
The API boundary owns bearer-token scope checks and JSON serialization, while
the repository remains responsible for PostgreSQL mapping and transactions.
