# sooqa 2ch userscript

Install [`sooqa-2ch-save.user.js`](https://raw.githubusercontent.com/godlikedh/sooqa/main/userscripts/sooqa-2ch-save.user.js)
in Tampermonkey on these supported HTTPS domains:

- `https://2ch.su/*`
- `https://2ch.org/*`
- `https://2ch.life/*`

The script adds one vertically attached action row per direct `.mp4` or `.webm`
attachment, including attachments added after the page initially loads. Each
row has `Post now`, `Post now…`, `Queue`, `Queue…`, `Save`, and `Save…` in a
3x2 action grid. Existing previews and native link/play/download behavior stay
in place; the script does not resize or replace media and does not scrape other
domains or page-like links.

The script sends only to `http://127.0.0.1:47831/v1/submit` with the local
companion token read from Tampermonkey storage. It contains no backend API or
Telegram token. Configure the companion first, then enter its local token when
the script prompts on first use.

Plain actions send no metadata. `Save…` asks for tags and an internal
description; `Post now…` adds public post text; and `Queue…` additionally asks
for a required browser-local date/time, which is sent as an RFC3339 instant.
`Queue` uses the normal cadence. The request is synchronous from the browser's
perspective: `Accepted request` means the companion/backend accepted an ingest
request, not that the media has finished downloading or appeared in Telegram
storage. There is no status polling.

Buttons remain available after acceptance. Only a request already in flight
suppresses another click; a timeout or no-response retry reuses its action ID,
while a later deliberate action gets a new ID. Accepted requests are shown as
informational history for the canonical thread/media identity, persisted in
Tampermonkey storage, bounded to the latest 200 entries, and cleared with the
`Clear accepted history` button. History never disables actions or claims that
publication completed.

Tampermonkey can detect updates through the script's `@updateURL` and
`@downloadURL`, both pointing at the stable `main` branch path. The script
stores only the local companion token and never contains a backend or Telegram
token.
