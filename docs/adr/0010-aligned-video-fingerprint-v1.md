# ADR 0010: Aligned video fingerprint v1

## Status

Proposed as the first slice of issue #44. The media and shortlist primitives
land here; the ingest identity gate and force-save workflow land in the
dependent slice before issue #44 closes.

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
metadata used by the later identity decision and is not silently baked into a
second frame transform.

### Sampling

Timestamps are `0, interval, 2 * interval, ... < duration_ms`. The interval is
`max(500, ceil(duration_ms / 2048))` milliseconds. This gives regular 500 ms
samples for ordinary videos, expands deterministically above the 2,048-sample
bound, and uses every distinct timestamp produced by the same generator for
short videos. The actual interval and duration are stored in the fingerprint.

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
The dHash uses nine evenly spaced horizontal samples across each of eight
rows. Low-information samples are retained in the authoritative blob but are
not selected as search anchors and receive little alignment weight.

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
Low-information pairs are down-weighted and do not count as informative
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

### Deferred identity boundary

The dependent issue #44 slice will acquire one transaction-scoped advisory lock
for video identity finalization, recheck canonical SHA, run the shortlist and
bounded alignment, then insert one `pending_storage` reservation or persist
`duplicate_pending` evidence before commit. The lock will not cover download,
ffmpeg, filesystem, HTTP, or Telegram work. Exact SHA uniqueness remains the
final byte-identity barrier. That slice will add the durable authorized
`force_save` transition from `duplicate_pending`; force-save will skip only the
perceptual decision and will still perform exact SHA checking.

## Test fixtures and current results

This slice includes deterministic unit fixtures for 500 ms sampling, cap
expansion, stable features, codec golden bytes, malformed/oversized blobs,
sorted/deduplicated tokens, a one-second blank prefix, a contained clip, and
low-information footage. PostgreSQL tests cover version/state/token bounds,
pending and ready candidates, unknown-state exclusion, stable shortlist
ordering, and the 20-row contract. The required codec re-encode, real blank
prefix/trimmed-prefix media fixtures, identity transaction, force-save, and
Telegram-call race tests are owned by the dependent workflow slice.

## Consequences

The authoritative media fingerprint is compact, deterministic, and safe to
decode without trusting Rust layout. PostgreSQL does retrieval work while Rust
retains control of the final visual decision. Algorithm changes that alter
sampling, feature meaning, token packing, alignment, or thresholds require a
new fingerprint version instead of reinterpretation of stored v1 bytes.

The current worker still consumes the legacy seven-frame path until the
dependent workflow slice switches it to this representation; this PR does not
pretend that the end-to-end duplicate gate is already active.
