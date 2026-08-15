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
existing webhook before polling, and claims each Telegram update ID in a
process-local lease-aware store before handling it. The store suppresses
concurrent delivery only for the lifetime of that server process; it is not a
PostgreSQL receipt ledger. The polling loop advances its Telegram offset only
after the handler succeeds; failed handlers retain the offset and retry with a
short backoff. Durable business effects use their owning aggregate's keys,
especially the Telegram-derived `ingests.input_key`, so a replay after restart
does not create a second ingest. Teloxide remains behind the project-owned
`sooqa-telegram` boundary.

The runtime makes five handler attempts. If an update still fails, it returns
an error without advancing the offset so a process supervisor can restart the
server and reclaim the update. Transient `getUpdates` errors are retried with
backoff for five attempts; an invalid bot token is terminal and is surfaced
immediately. Other persistent polling failures also return an error after the
bounded attempts so a supervisor can restart or alert on the condition.

The adapter does not expose a public webhook route in H1. A future webhook
implementation must preserve the same normalized update boundary and durable
business-effect idempotency, and must add deployment-specific authentication
and replay behavior before replacing polling.

## Consequences

- Local and private-network deployments need only outbound access to the Bot
  API.
- One running server suppresses duplicate concurrent handling, while abandoned
  in-process claims can be reclaimed after five minutes.
- A restart may redeliver an update; durable ingest keys prevent duplicate
  ingest work, but purely conversational replies are not stored as receipts.
- A failed response can be retried, with the usual Telegram ambiguity if the
  network fails after Telegram accepted the request.
- The adapter owns a small polling loop instead of delegating handler errors to
  a dispatcher that would log them and continue past the update.
- There is no Telegram receipt table or receipt-retention task.
- Long polling is not the final answer for every deployment; webhooks remain a
  future option when public ingress and deployment diagnostics are ready.
