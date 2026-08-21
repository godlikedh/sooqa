# Security

sooqa is a single-admin self-hosted service. Keep the server, database, media
work root, API secret, and Telegram credentials on a trusted host.

Every `/api/v1` route requires the configured bearer secret. `/health/live`
and the static `/admin` HTML, JavaScript, and CSS are intentionally public to
the listening network; the admin page cannot read or mutate application data
until it supplies the bearer token to the API. Authorization is configuration
based; PostgreSQL stores no administrator or device-token records. Request
bodies and timeouts are bounded, and stable errors expose a request ID without
secrets.

The bounded admin routes use the same bearer authorization. Dashboard and list
responses contain only aggregate counts and capped operational cards; they do
not return queue payloads, lease tokens, bearer values, Telegram credentials,
or unbounded error logs. Media lookup accepts only UUIDs, credential-free HTTP(S)
source URLs, the allowlisted 2ch mirror identity, or HTTPS private Telegram
storage links. The API canonicalizes lookup input without changing stored
source provenance, and generated storage links remain behind authentication.

The `/admin` page is embedded in the server binary and uses only same-origin
local assets. Its restrictive CSP permits scripts, styles, and API connections
only from the server origin; it disables objects, framing, base-URI changes,
and form actions. The browser keeps the bearer token only in `sessionStorage`
and the lock action clears that value and the in-memory copy. Backend titles,
captions, source URLs, tags, and errors are rendered as text or validated
HTTP(S) links rather than injected HTML. This is a trusted-home-LAN MVP
boundary, not a claim that the bearer-token UI is suitable for internet
exposure.

The home deployment serves plain HTTP. A bearer token sent over a network is
not encrypted, so bind the port only to the trusted host/LAN described in
the [operations guide](operations.md), or put an authenticated TLS reverse
proxy in front of it. Do not expose the server port directly to the internet.

Media previews are requested with the bearer header from same-origin API paths;
the token is never placed in a preview URL or `<img>` request. The browser
uses object URLs for returned bytes and revokes them when the catalogue is
re-rendered or the page is left. Publication dialogs submit public post text
and exact times as separate fields from internal catalogue metadata.

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
is used for page-like URLs only when the submitted URL belongs to the closed
provider-family policy and is enabled by `media.ytdlp_allowed_hosts`. The
policy has explicit submitted/canonical hosts, bounded extractor identities,
and URL shapes for YouTube, TikTok, Instagram, and X/Twitter; `t.co` is accepted
only when canonical inspection produces an X/Twitter status. Credentials, IP
literals, unsupported hosts, and non-default ports are rejected. The inspected
canonical URL is checked against the same family and allowlist before it reaches
the child, so metadata cannot redirect execution to a new host or weaken the
initial SSRF boundary. A `t.co` submission is first resolved with a bounded
redirect-disabled, no-proxy preflight using the shared DNS resolver; every hop
rejects private or special addresses and pins the HTTP connection to the
validated address. Every redirect target must remain an X/Twitter status host
before the final URL is passed to yt-dlp, and non-status X pages are rejected
before the child starts. Its child environment is cleared and
rebuilt with only a fixed `PATH`; `--ignore-config`, `--no-cookies`, and
`--no-cookies-from-browser` are explicit, and no netrc or browser state is
available. Remote components are disabled, and plugin discovery is reset to
the exact `/usr/local/share/sooqa/yt-dlp-plugins` directory containing the
pinned bgutil plugin. The home image pins the official yt-dlp and Deno
distributions by version and SHA-256 checksum, plus the plugin release ZIP by
its release checksum. yt-dlp may follow provider/CDN redirects after an
allowlisted page is accepted, which is part of the single-admin deployment's
explicit trust boundary; the separate `t.co` preflight prevents an open
redirect from reaching yt-dlp first. The home Compose PO-token
provider is private-network-only, has no host-published port, and is checked
for the pinned `1.3.1` version at worker startup. Each yt-dlp attempt gets a
unique job-owned directory with a relative output and explicit home/temp paths;
yt-dlp keeps its normal supported-client selection; the pinned PO-token
provider is supplied only to YouTube selection without globally forcing an mweb
client. TikTok, Instagram, and X/Twitter do not use the YouTube PO-token or
media-byte recovery path.
the worker monitors the aggregate directory while the child is running, not
only the final file, and removes the complete directory on success, failure,
timeout, or cancellation. The aggregate budget is three times the final source
limit to leave room for split-stream merging. A successful attempt must leave
exactly one regular final media file. A specific YouTube
`unable to download video data: HTTP Error 403` failure receives one fresh
high-quality attempt and then one clean combined-progressive fallback; the bounded
the versioned `input_json.download.selected_format` marker records which format won. Other
extractor failures remain terminal, and every failed attempt directory is
removed. The worker also refuses to enable the adapter if the offline
plugin/EJS/Deno probes fail, or if the YouTube PO-token health/version check
fails when YouTube is enabled.

The local Telegram Bot API service is a private Compose-network dependency. Its
`api_id`, `api_hash`, and bot token are deployment secrets; the service has a
dedicated persistent volume and no host-published HTTP port. Configuration
rejects credential-bearing URLs and public HTTP Bot API hosts. The configured
normalized-output ceiling is below Telegram's documented local upload maximum,
while source budgets remain independent so a large input can still normalize
successfully. Only the worker mounts the Bot API volume, and it does so
read-only. An absolute `getFile` path is canonicalized against the configured
root before opening; traversal, outside-root paths, symlink escapes, directories,
and other non-regular files are rejected. Local copies stay size-limited and
atomic, while relative/cloud paths retain bounded HTTP downloads. Internal Bot
API paths are not persisted, returned by the API, or emitted in normal logs.
The worker and storage provider both enforce the canonical-output ceiling before
an upload effect.

Media previews are private derived bytes on the existing media row. The API
requires the normal bearer authorization, serves only validated JPEG/PNG data
at a fixed 320-by-320 and 128 KiB maximum, uses `Cache-Control: private`, and
does not expose the source or workspace path. Preview generation never retains
the original input or downloads older media for backfill. Storage captions are
bounded to internal description, normalized tags, and source URL; public post
text, identifiers, hashes, workflow data, and schedules are excluded.

The Schedule page renders post text through DOM text/value properties and keeps
its bearer token in the existing session-only storage. It exposes only bounded
unpublished rows, fetches advertised still previews through the authenticated
same-origin media route, and never adds media playback or a source/Telegram
proxy. Preview object URLs are revoked on replacement, navigation, lock, and
unload; sending and unknown rows have no retry or removal controls because
their Telegram outcome is ambiguous. Refreshes may preserve a local form
temporarily, but the persisted post revision remains authoritative.

Workspaces reject separators and parent-directory components in file names.
Temporary files are published only after validation, and cleanup is limited to
known workspace paths. PostgreSQL constraints enforce digest length, media
storage readiness, post send state, and queue lease invariants.

No transaction remains open across a network call or subprocess. Retryable
external effects use local keys/generations; an ambiguous Telegram upload or
post send is retained as `storage_unknown` or `unknown` instead of guessed away.
Publisher sends reuse only the persisted Telegram storage chat/message or its
stored file ID; the publication worker has no local-media upload path. It sends
the public caption explicitly, including an empty caption when absent, so
private storage descriptions and tags are not copied into the target channel.
