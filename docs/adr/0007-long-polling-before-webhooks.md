# ADR 0007: Use long polling before webhooks

- Status: Accepted
- Date: 2026-08-07

## Context

The first Telegram adapter needs a reliable update delivery mechanism for a
single-admin self-hosted installation. Webhooks require a publicly reachable
HTTPS endpoint, certificate and reverse-proxy configuration, and an explicit
secret-token deployment path. Those concerns do not help the first private bot
command flow and would make local operation harder to verify.

## Decision

Use Telegram long polling for the initial adapter. The server constructs the
polling client with a configurable Bot API base URL and timeout, deletes an
existing webhook before polling, and persists each Telegram update ID in
PostgreSQL before handling it. Teloxide remains behind the project-owned
`sooqa-telegram` boundary.

The adapter does not expose a public webhook route in H1. A future webhook
implementation must preserve the same normalized update boundary and durable
deduplication semantics, and must add deployment-specific authentication and
replay behavior before replacing polling.

## Consequences

- Local and private-network deployments need only outbound access to the Bot
  API.
- Restarted polling does not answer the same update twice after its receipt is
  committed.
- Receipt rows are durable operational data and need retention/cleanup policy
  in a later operations slice.
- Long polling is not the final answer for every deployment; webhooks remain a
  future option when public ingress and deployment diagnostics are ready.
