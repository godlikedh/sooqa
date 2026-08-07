# Operations

The development Compose file provides PostgreSQL. Production deployment and
database backup remain deployment-specific, but the first Telegram runtime is
now available behind explicit configuration.

## Local container runtime

Docker Desktop is optional on macOS. Colima supplies the Docker-compatible
engine without a Docker account:

    brew install colima
    colima start --runtime docker
    docker context use colima

Verify the selected engine with `docker context show`, then use the normal
Compose commands. Stop the VM with `colima stop` when it is not needed.

The B1 migration command is:

    DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/sooqa cargo run -p sooqa-server -- migrate

The migration set now includes the initial Library tables for content items,
media assets, source records, tags, and storage objects. Apply migrations with
the same command after upgrading the application; migrations are forward-only.
The E2 migration also adds general SHA-256 lookup and canonical-asset
uniqueness indexes.

After migrations are applied, the worker can be started with:

    DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/sooqa cargo run -p sooqa-worker

The worker's non-secret media executable paths can be configured in TOML:

    [media]
    ffmpeg_path = "ffmpeg"
    ffprobe_path = "ffprobe"
    ytdlp_path = "yt-dlp"
    ytdlp_format = "bestvideo*+bestaudio/best"

The equivalent environment overrides are `SOOQA_MEDIA_FFMPEG_PATH`,
`SOOQA_MEDIA_FFPROBE_PATH`, `SOOQA_MEDIA_YTDLP_PATH`, and
`SOOQA_MEDIA_YTDLP_FORMAT`. On normal startup,
the worker logs the detected version of each required binary and exits before
database connection if a binary is unavailable. The server does not require
these media tools. The worker polls durably stored jobs, executes registered
handlers including the configured Telegram storage upload handler, records
outcomes, and stops gracefully on SIGTERM or Ctrl-C.

## Telegram bot

Apply migrations before starting a configured bot; migration `0008` creates
the durable `telegram_update_receipts` table:

    DATABASE_URL=postgres://USER:PASSWORD@HOST:5432/sooqa cargo run -p sooqa-server -- migrate

Configure the bot token as a secret and provide the administrator's Telegram
user ID:

    SOOQA_TELEGRAM_BOT_TOKEN=123456:secret
    SOOQA_TELEGRAM_ADMIN_USER_IDS=123456789
    SOOQA_TELEGRAM_API_BASE_URL=https://api.telegram.org
    SOOQA_TELEGRAM_POLL_TIMEOUT_SECONDS=30
    SOOQA_TELEGRAM_STORAGE_CHAT_ID=-1001234567890

The server starts polling alongside the HTTP API only when the bot token is
configured. It ignores group messages, rejects non-admin private users with a
generic response, and supports `/start`, `/help`, `/add`, and `/status` for
configured admins. `/add <url>` and a bare single-URL message create the same
durable Inbox request as the HTTP API; the response includes its request ID
and status. Update receipts are retained as durable deduplication records. A
five-minute claim lease allows an abandoned in-progress update to be reclaimed;
failed API or Inbox calls release their claim immediately. URL source
inspection, downloading, media processing, and channel publication remain
later Telegram slices.

For H3 storage, make the bot an administrator of a private storage channel and
set `SOOQA_TELEGRAM_STORAGE_CHAT_ID` to its negative chat ID. The server checks
that chat during Telegram startup. Upload intents are durable in
`idempotency_records`; pending or ambiguous intents are retained and must be
reconciled before retrying. They are not automatically reclaimed because a
long-running Telegram request could still be in flight. The worker enables the
upload job only when the Telegram token and storage chat are configured;
canonical-asset recording creates the durable upload job.
