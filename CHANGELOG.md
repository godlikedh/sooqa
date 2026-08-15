# Changelog

All notable changes to sooqa will be documented here.

## [Unreleased]

No changes yet.

## [0.2.0] - 2026-08-15

### Added

- Five-table PostgreSQL persistence for durable ingests, media, channels,
  posts, and leased jobs, with forward migrations and fenced recovery.
- Direct HTTP, allowlisted yt-dlp, and Telegram media capture with durable
  download, probe, normalization, fingerprint, deduplication, storage, and
  cleanup stages.
- Canonical video/image normalization, exact SHA reuse, bounded aligned video
  fingerprints, duplicate decisions, previews, editable metadata, and fenced
  Telegram storage-caption synchronization.
- Durable publication intent materialization, cadence and exact scheduling,
  14-day repeat decisions, revision-fenced post editing, and fenced Telegram
  copy/send behavior.
- Bounded bearer-authenticated admin APIs and an embedded dark web admin for
  dashboard decisions, ingest status, media metadata, channel settings, and
  unpublished schedule management.
- A loopback Windows companion and 2ch userscript with six capture actions,
  retry-safe action IDs, cross-mirror accepted history, and versioned release
  artifacts with SHA-256 checksums.
- A pinned home Compose topology with PostgreSQL, the official local Telegram
  Bot API server, large-media streaming, yt-dlp, Deno, and shared workspaces.

### Security

- Added SSRF-resistant direct downloads, an explicit yt-dlp host allowlist,
  bounded subprocesses and workspaces, separate local/backend companion
  secrets, and a same-origin admin CSP.
- Telegram long polling uses a process-local duplicate-delivery gate; durable
  business effects are idempotent through ingest/job/media/post keys rather
  than a Telegram receipt table.

### Known limitations

- Public YouTube/Shorts extraction is best-effort while issue #102 tracks
  resolved-download URL handling and bounded recovery from HTTP 403 responses.
