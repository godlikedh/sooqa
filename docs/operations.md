# Operations

## Database

The database is the durable source of truth. Apply forward-only migrations
before starting the server or worker:

```bash
DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/sooqa \
  cargo run -p sooqa-server -- migrate
```

Migration 0009 adds storage reservation ownership/expiry, media digest checks,
canonical-asset triggers, and job lease/attempt invariants. Migration 0010
binds storage intents to their asset, job, provider, chat, and upload
generation. Deploy both migrations before deploying code that relies on those
columns.

## Worker

Start the worker with the same `DATABASE_URL` and `media.work_root` used by the
server:

```bash
DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/sooqa \
  cargo run -p sooqa-worker
```

The worker discovers its registered job capabilities and preflights only the
external binaries those handlers need. The composed source-inspection handler
needs `yt-dlp`, and the probe handler needs `ffprobe`; ffmpeg is not required
until a normalization handler uses it.
The worker also creates and writes a small check file in the work root before
polling. The supplied container image installs ffmpeg, ffprobe, and yt-dlp and
creates a writable `/var/lib/sooqa/work` for the non-root `sooqa` user.

A running job has a bounded lease and heartbeat. On startup and periodically,
the worker returns expired running jobs to the queue. A graceful shutdown
requeues the active job immediately. If a worker loses its lease, it stops
before acknowledging the job; inspect the job and attempt rows when
diagnosing a crash or database outage.

## Telegram

Configure the token, positive administrator IDs, API endpoint, and optional
storage channel:

```bash
SOOQA_TELEGRAM_BOT_TOKEN=123456:secret
SOOQA_TELEGRAM_ADMIN_USER_IDS=123456789
SOOQA_TELEGRAM_API_BASE_URL=https://api.telegram.org
SOOQA_TELEGRAM_MAX_DOWNLOAD_BYTES=2147483648
SOOQA_TELEGRAM_STORAGE_CHAT_ID=-1001234567890
```

The server starts polling only when the token is configured. The Bot API cloud
endpoint has a 20 MiB upstream limit; a Local Bot API Server can be selected by
changing `SOOQA_TELEGRAM_API_BASE_URL`, but the application byte ceiling still
applies. Telegram downloads are staged in the shared work root and cleaned up
on errors.

## Storage intent recovery

An upload intent is `pending` while the worker owns a renewable reservation.
The job and intent leases are coordinated: when another live owner holds the
intent, the job is deferred without consuming an attempt. If the external
Telegram result is uncertain, the intent becomes `unknown` and is kept
durable. An expired pending reservation is also converted to `unknown` before
another attempt can observe it. The normal upload path will not guess whether
Telegram created a message.

Inspect and reconcile intents with:

```bash
cargo run -p sooqa-server -- storage-intents list
cargo run -p sooqa-server -- storage-intents mark-unknown <intent-id>
cargo run -p sooqa-server -- storage-intents mark-unknown <intent-id> --force --confirm
cargo run -p sooqa-server -- storage-intents reset <intent-id> --confirm
cargo run -p sooqa-server -- storage-intents attach <intent-id> <chat-id> <message-id> <file-id> <file-unique-id>
```

Use `attach` when the Telegram message exists. It derives the asset, provider,
and media kind from the intent and rejects chat or digest mismatches. It also
requires a positive Telegram message ID and non-empty file identifiers,
trimming surrounding whitespace before storage. Use `reset --confirm` only
when the operator has verified that the external upload
did not create an object; reset retains the old job for history and creates a
new upload generation with a fresh idempotency key.

## Logs and secrets

Configuration summaries redact secrets. Do not put bot tokens, database URLs,
or bearer tokens in logs or committed TOML. The default development database
password is for local use only.
