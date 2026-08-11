# Security

sooqa is a single-admin self-hosted service. Keep the server, database, media
work root, API secret, and Telegram credentials on a trusted host.

HTTP routes require the configured bearer secret. Authorization is configuration
based; PostgreSQL stores no administrator or device-token records. Request
bodies and timeouts are bounded, and stable errors expose a request ID without
secrets.

The Windows companion is a loopback-only fixed proxy. It accepts only
`POST /v1/submit`, requires a separate local bearer token, caps JSON bodies, and
forwards only the backend ingest route with a bounded timeout and a stable
`Idempotency-Key`. Its backend URL and backend bearer token are companion-only
configuration; neither is returned to the browser or written to logs. The
Tampermonkey script stores only the local token and never contains the backend
or Telegram token.

Telegram accepts private messages from configured positive administrator IDs.
Media is staged below UUID-derived workspaces. Direct HTTP rejects credentials,
private/special destinations, and unsafe redirects. ffmpeg, ffprobe, and
yt-dlp receive argument arrays, bounded output, timeouts, and no shell. yt-dlp
is used for page-like URLs only when the submitted URL's normalized initial
hostname is in `media.ytdlp_allowed_hosts`; exact hosts and dot-delimited
subdomains are matched, while credentials, IP literals, and non-default ports
are rejected. Its child environment is cleared and rebuilt with only a fixed
`PATH`; yt-dlp ignores configuration and plugins, and remote components are
disabled. The home image pins the official yt-dlp and Deno distributions by
version and SHA-256 checksum. yt-dlp may follow provider redirects and fetch
provider/CDN URLs after an allowlisted page is accepted, which is part of the
single-admin deployment's explicit trust boundary.

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
