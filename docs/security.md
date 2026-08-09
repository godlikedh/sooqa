# Security

sooqa is a single-admin self-hosted service. Keep the server, database, media
work root, API secret, and Telegram credentials on a trusted host.

HTTP routes require the configured bearer secret. Authorization is configuration
based; PostgreSQL stores no administrator or device-token records. Request
bodies and timeouts are bounded, and stable errors expose a request ID without
secrets.

Telegram accepts private messages from configured positive administrator IDs.
Media is staged below UUID-derived workspaces. Direct HTTP rejects credentials,
private/special destinations, and unsafe redirects. ffmpeg, ffprobe, and
yt-dlp receive argument arrays, bounded output, timeouts, and no shell.

Workspaces reject separators and parent-directory components in file names.
Temporary files are published only after validation, and cleanup is limited to
known workspace paths. PostgreSQL constraints enforce digest length, media
storage readiness, post send state, and queue lease invariants.

No transaction remains open across a network call or subprocess. Retryable
external effects use local keys/generations; an ambiguous Telegram upload or
post send is retained as `storage_unknown` or `unknown` instead of guessed away.
