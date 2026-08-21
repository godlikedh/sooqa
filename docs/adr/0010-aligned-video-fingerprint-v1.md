# ADR 0010: Aligned video fingerprint v1

## Status

Accepted and shipped as issue #44's video identity gate.

## Context

The previous video comparison sampled seven relative frames and emitted a
warning after media finalization. That cannot identify a video before Telegram
storage, tolerate a short prefix or suffix, or prevent two equivalent videos
from reserving separate storage objects. Issue #44 requires a bounded,
versioned fingerprint whose ordered data is verified in Rust after PostgreSQL
has narrowed the candidate set.

## Decision

The algorithm identifier is `video_sequence_v1`. The normalized, canonical
video is the input. Orientation is therefore already resolved by canonical
normalization; each decoded frame is resized exactly to 32x32 with the
project's Triangle filter before feature extraction. Aspect ratio remains
available as media metadata but is not a hard candidate filter or silently
baked into a second frame transform.

### Sampling

Timestamps are `0, interval, 2 * interval, ... < duration_ms`. The interval is
`max(500, ceil(duration_ms / 2048))` milliseconds. This gives regular 500 ms
samples for ordinary videos, expands deterministically above the 2,048-sample
bound, and uses every distinct timestamp produced by the same generator for
short videos. The actual interval and duration are stored in the fingerprint.
For each timestamp, the selected frame is the first decoded input frame whose
normalized presentation timestamp is at or after that grid point. The grid is
not rounded to the nearest decoded frame; this distinction is part of the v1
fingerprint contract.

### Sample features

Each fixed-width sample contains:

- a 64-bit low-frequency DCT pHash;
- a 64-bit horizontal dHash;
- mean luma and mean U/V chroma summaries;
- a 0..10,000 information score combining normalized variance and 16-bin
  luminance entropy;
- a 0..10,000 transition score, the mean absolute luminance difference from
  the preceding normalized frame. The first sample has transition zero.

The pHash uses the 8x8 low-frequency coefficients of a 32x32 grayscale image
and compares all 64 coefficients with the deterministic median.
The dHash uses rows `floor(y * 31 / 7)` for `y = 0..7`; within each row it
compares horizontal positions `floor(x * 31 / 8)` and
`floor((x + 1) * 31 / 8)` for `x = 0..7`. Low-information samples are
retained in the authoritative blob but are not selected as search anchors and
receive little alignment weight.

### Binary representation

The persisted blob is manually encoded, little-endian, and independent of Rust
memory layout:

```text
header  = 4-byte magic `SQVS`
          u16 codec version = 1
          u16 sample count
          u64 duration_ms
          u32 interval_ms
          u32 reserved = 0
sample  = u64 phash
          u64 dhash
          u8 mean_luma
          i8 mean_chroma_u
          i8 mean_chroma_v
          u16 information_bps
          u16 transition_bps
```

The header is 24 bytes and each sample is 23 bytes, so a maximum sequence is
bounded to 47,128 bytes. Decoding rejects bad magic/version, zero or excessive
sample counts, invalid timestamp grids, truncation, non-zero reserved data,
trailing bytes, and blobs larger than the maximum. Golden bytes and
byte-identical round trips are committed in media tests.

### Search tokens and PostgreSQL shortlist

Anchors require information of at least 1,000 basis points and are ranked by
`3 * information + transition`, with stable sample-index ties. At most 128
anchors are retained. Each anchor contributes four 16-bit bands from its pHash
and four from its dHash. A token packs algorithm ID 1, hash kind (pHash 1 or
dHash 2), band position, and the 16-bit band value into a positive `BIGINT`.
Tokens are sorted, deduplicated, and capped at 1,024.

`media.fingerprint_data BYTEA` is authoritative and
`media.fingerprint_search_tokens BIGINT[]` is the retrieval projection. A
partial native GIN index covers video rows in `pending_storage` or `ready`
state with non-null tokens. The shortlist query requires the same algorithm
version, video kind, an identity-visible storage state, at least 8 shared
tokens, and at least 10% overlap relative to the smaller token set. It orders
by shared-token count, overlap, and media ID, and applies `LIMIT 20` as a
maximum rather than a quota. No process-wide fingerprint catalog, vector
extension, or per-frame/candidate table is used.

### Alignment contract

The Rust verifier uses bounded local dynamic programming over the capped
sequences. A full pair of 2,048-sample inputs uses at most 4,200,000 DP cells.
It can leave prefixes/suffixes unmatched and represent short gaps,
which provides constant-offset tolerance without a duration hard filter. Pair
distance combines pHash and dHash Hamming distance with luma/chroma distance.
Low-information pairs below the 1,000-bps information threshold receive a
non-positive local-match score. At or above the threshold, the match weight is
`4,600 + (information - 1,000) * 5,400 / 9,000` bps before the distance
penalty, so real extracted features contribute positive evidence while black
frames cannot create a path. Low-information pairs do not count as informative
matches. The default strong-duplicate thresholds are:

- at least 70% coverage in both directions;
- at least 8 informative matched samples;
- median visual distance at most 2,200 bps;
- 95th-percentile distance at most 3,500 bps;
- a temporally consecutive run of at least 6 samples;
- no more than 8 gap runs and a final score of at least 6,500 bps.

Evidence reports aligned offset, bidirectional coverage, informative count,
median and high-percentile distance, longest consistent run, unmatched
prefix/suffix counts, gap count, and score. A contained short clip therefore
cannot qualify as a full duplicate merely because its local frames align.

### Identity transaction and force-save

Video fingerprint extraction and all ffmpeg/file work happen before identity
coordination. The worker may perform a read-only exact-SHA preflight to skip
extraction, but it does not merge metadata or reserve media there. The
finalizer acquires one session-level advisory lock on a dedicated PostgreSQL
connection, then commits a short preparation transaction that revalidates the
ingest and current `JobLease`, rechecks canonical SHA, and reads the bounded
shortlist. The transaction is closed before the worker decodes candidate blobs
and runs alignment on Tokio's blocking/CPU pool. The session lock stays held
while that CPU work runs, preserving one globally serialized identity
decision without retaining a transaction or row locks.

After alignment, a short final transaction on the same locked connection
revalidates the ingest/job lease and canonical SHA, then either reuses the
existing media row, persists bounded `duplicate_pending` evidence, or inserts
one `pending_storage` reservation with the fingerprint blob and tokens.
Media/evidence/storage-queue mutations and the winning job success remain one
transaction; an expired or recovered finalizer therefore commits none of
them. The session lock is released only after that transaction commits, and
the dedicated connection is closed if the worker is cancelled before release.
The lock does not cover download, ffmpeg, filesystem, HTTP, or Telegram work.
The worker/library boundary validates the v1 binary envelope and the bounded,
sorted, data-derived token projection before persistence stores it. Persistence
only queries and persists that representation; it does not decode candidates
or execute the alignment algorithm.
A single stable lock key is intentional: equivalent re-encodes with different
SHAs must observe the first in-progress reservation before either can enqueue
a storage upload. The canonical SHA unique constraint remains the final
byte-identity barrier.

### Extraction resource behavior

The active extractor preserves the timestamp grid above with one FFmpeg process
per canonical video. It normalizes input PTS with `setpts=PTS-STARTPTS`, pads
the terminal decoded frame with `tpad=stop_mode=clone` for one sample interval,
and then uses
`select='isnan(prev_selected_pts)+gte(t,selected_n*interval/1000)'` to choose
the first decoded frame at or after each grid timestamp. The padding makes a
final grid point available when container or audio duration extends just beyond
the video stream; the exact sample-count validation remains in force. An
explicit `-frames:v` cap equals the exact expected sample count, never above
2,048, and `-fps_mode vfr` prevents the output writer from duplicating or
rounding the selected frames. FFmpeg writes a numbered PNG sequence into a fresh
extraction-scoped directory under the workspace. A bounded consumer validates
the numbered sequence, waits for each file to stabilize, decodes it under the
existing byte/pixel/working-set limits, and deletes it before consuming the
next frame. The production process runner also monitors the aggregate regular
file size while FFmpeg is running and aborts the process group over the
4,294,967,296-byte sequence limit; the consumer and final validation enforce
the same limit for non-production runners and producer races. The sequence
directory is removed on every normal exit path and by a synchronous drop guard
when the extraction is cancelled.

This changes only execution and resource lifetime. The Rust resize, feature,
transition, binary codec, shortlist-token, and alignment contracts remain the
accepted v1 semantics. A future streaming raw-frame implementation would need
the same framing, backpressure, and cleanup guarantees; it is not required by
this decision.

The authorized `POST /api/v1/ingests/{id}/force-save` route is idempotent. It
is accepted only from `duplicate_pending`, persists `force_save = true`, clears
derived pipeline artifacts, and restarts a durable source-to-normalization
chain. URL ingests re-run source inspection and direct download from the
persisted URL; Telegram ingests re-download from the persisted bot-specific
`telegram_file_id`. This makes force-save safe after the workspace scavenger
has removed every local artifact. Force-save stage keys are generation-scoped
so historical completed jobs cannot suppress the new chain, while repeated or
concurrent requests still create at most one active stage job. The resumed
identity transaction still checks exact SHA but skips only the perceptual
decision.

## Test fixtures and current results

This slice includes deterministic unit fixtures for 500 ms sampling, cap
expansion, stable features, codec golden bytes, malformed/oversized blobs,
sorted/deduplicated tokens, a one-second blank prefix, a contained clip, and
low-information footage. PostgreSQL tests cover version/state/token bounds,
pending and ready candidates, unknown-state exclusion, stable shortlist
ordering, the 20-row contract, exact reuse, strong duplicate-pending, force-
save bypass, concurrent equivalent-video reservation, and a recovered stale
finalizer. Composed worker tests cover URL/Telegram source reconstruction
after complete workspace cleanup, repeated force-save dedupe, exact/strong/no-
match storage effects, and the non-video handoff. The ignored media acceptance
matrix runs the active ffmpeg extractor and Rust alignment over generated
fixtures. Its calibrated outcomes are:

| Generated variant | Observed identity outcome |
| --- | --- |
| ordinary re-encode with bitrate/resolution change | `strong_duplicate` |
| one-second black prefix | `strong_duplicate` |
| 500 ms prefix and suffix trim | `strong_duplicate` |
| unrelated `testsrc` at the same shape and duration | `not_duplicate` |
| black, static blue, or repetitive SMPTE bars | `not_duplicate` |
| two-second contained clip | `partial_match` |
| 750 ms very-short clip | `partial_match` |
| same video without audio | `strong_duplicate` |

The worker storage test uses a counted fake Telegram API; it verifies zero
calls for exact and strong duplicate paths and exactly one call for a new
reservation, including safe reuse on the repeated upload attempt.

## Consequences

The authoritative media fingerprint is compact, deterministic, and safe to
decode without trusting Rust layout. PostgreSQL does retrieval work while Rust
retains control of the final visual decision. Algorithm changes that alter
sampling, feature meaning, token packing, alignment, or thresholds require a
new fingerprint version instead of reinterpretation of stored v1 bytes.

The active worker consumes this representation before storage. Images,
animations, and audio deliberately skip sequence extraction and use exact SHA
identity only. Telegram duplicate-card presentation is outside this backend
ADR.
