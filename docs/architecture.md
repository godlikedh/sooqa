# Architecture

The intended architecture is a modular monolith with separate server, worker,
and optional Windows companion processes. The module boundaries and durable
workflow rules are defined in [PROJECT_SPEC.md](PROJECT_SPEC.md).

This document will grow with the implementation. The bootstrap stage contains
no database, Telegram, or media integrations yet. Shared configuration and
process lifecycle plumbing lives in sooqa-config and sooqa-runtime. The server
now exposes the initial liveness API through sooqa-api.
