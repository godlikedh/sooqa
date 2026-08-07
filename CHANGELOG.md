# Changelog

All notable changes to sooqa will be documented here.

## [Unreleased]

### Added

- Bootstrapped the Rust 2024 workspace and application/crate layout.
- Added formatting, linting, testing, and GitHub CI foundations.
- Added typed TOML/environment configuration, redacted summaries, structured
  tracing, and graceful shutdown scaffolding.
- Added the server liveness endpoint, request IDs, limits, build metadata, and
  a Dockerfile skeleton.
- Added the PostgreSQL Compose service, SQLx connection layer, initial schema
  migration, and migration integration test harness.
- Added typed durable job values and a PostgreSQL repository with atomic
  claiming, leases, retries, attempt history, and stale-lease recovery.
- Added the bounded worker loop, worker identity, handler registry, graceful
  shutdown, structured job logs, and in-process worker metrics.
- Added the first Telegram adapter with configurable long polling, private
  administrator authorization, `/start`, `/help`, and `/status`, plus durable
  update-id deduplication receipts.
- Added private Telegram URL ingest through `/add` or a single bare URL,
  durable Inbox request acknowledgements, and versioned status callback data.
