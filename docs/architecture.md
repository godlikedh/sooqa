# Architecture

The intended architecture is a modular monolith with separate server, worker,
and optional Windows companion processes. The module boundaries and durable
workflow rules are defined in [PROJECT_SPEC.md](PROJECT_SPEC.md).

This document will grow with the implementation. The bootstrap stage contains
no Telegram or media integrations yet. Shared configuration and process
lifecycle plumbing lives in sooqa-config and sooqa-runtime. The server exposes
the initial liveness API through sooqa-api, and sooqa-persistence now provides
PostgreSQL migrations plus a durable job repository. sooqa-worker now provides
the bounded polling loop, handler registry, leases, and graceful shutdown; real
media and Telegram handlers remain future slices.

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
content sniffing. yt-dlp, ffprobe, workspaces, and production worker wiring
remain later slices.

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
