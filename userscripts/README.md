# sooqa 2ch userscript

Install `sooqa-2ch-save.user.js` in Tampermonkey on the supported
`https://2ch.org/*` pages. The script adds `Save` and `Save...` controls beside
direct `.mp4` and `.webm` attachments, including attachments added after the
page initially loads.

The script sends only to `http://127.0.0.1:47831/v1/submit` with the local
companion token read from Tampermonkey storage. It contains no backend API or
Telegram token. Configure the companion first, then enter its local token when
the script prompts on first use.

`Save...` asks for comma-separated tags and an optional internal description.
The request is synchronous from the browser's perspective: `Accepted` means
the backend accepted an ingest request, not that the media has finished
downloading or appeared in Telegram storage. There is no status polling.
