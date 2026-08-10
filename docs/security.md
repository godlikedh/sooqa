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

The local Telegram Bot API service is a private Compose-network dependency. Its
`api_id`, `api_hash`, and bot token are deployment secrets; the service has a
dedicated persistent volume and no host-published HTTP port. Configuration
rejects credential-bearing URLs and public HTTP Bot API hosts. The configured
normalized-output ceiling is below Telegram's documented local upload maximum,
while source budgets remain independent so a large input can still normalize
successfully. The worker and storage provider both enforce the canonical-output
ceiling before an upload effect.

Workspaces reject separators and parent-directory components in file names.
Temporary files are published only after validation, and cleanup is limited to
known workspace paths. PostgreSQL constraints enforce digest length, media
storage readiness, post send state, and queue lease invariants.

No transaction remains open across a network call or subprocess. Retryable
external effects use local keys/generations; an ambiguous Telegram upload or
post send is retained as `storage_unknown` or `unknown` instead of guessed away.
