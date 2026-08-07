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
payload is rejected. The HTTP ingest routes and real source-inspection handler
are still future slices.
