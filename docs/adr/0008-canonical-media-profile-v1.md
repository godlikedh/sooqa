# ADR 0008: Canonical media profile v2

## Status

Accepted

## Context

Library storage and publication need one predictable video representation, but
the input may already be Telegram-compatible or may require re-encoding. The
normalization decision must be explainable and testable without invoking
`ffmpeg`, and command arguments must remain separate from shell syntax.

## Decision

F1 defines a project-owned `CanonicalVideoProfile` in `sooqa-media` with an MP4
container, H.264 video, `yuv420p`, AAC audio, 1080p maximum dimensions without
upscaling, a configurable frame-rate cap, medium x264 preset, CRF 23, 128 kbps
audio, fast start, and stripped incidental metadata by default.

The current profile version is `canonical_video_v2`. It adds a bounded Telegram
inline-playback policy: a 14 MiB target, preferred CRF 23, maximum CRF 27, and a
480-pixel minimum short edge. CRF is tried before resolution reduction. Every
candidate is checked by its actual output bytes, and candidates above the
configured normalized-storage ceiling are discarded. If no candidate fits the
inline target before both quality floors, the earliest highest-quality
candidate within that storage ceiling is retained; if none is storable,
normalization fails clearly.

`NormalizationPlanner` selects a remux plan only when the probe proves the
input is MP4-compatible, within profile limits, unrotated, and already uses
the target codecs. Other valid video inputs receive a transcode plan with an
aspect-preserving scale filter. Missing video streams and invalid profile
values are rejected before command construction.

An MP4/H.264 remux additionally requires a valid `avc1`/`avc3` tag, matching
`mime_codec_string` (the container `avcC` declaration) and SPS `level`, and
enough H.264 level capacity for the probed dimensions and frame rate. Missing
or contradictory declarations are transcoded.

The planner returns an `ExternalCommand` containing an argument vector. It
does not execute the command or persist assets; F2 owns execution, progress,
output validation, hashing, and durable finalization. The worker executes the
bounded adaptation candidates outside database transactions and publishes only
the selected canonical artifact. Video normalization metadata records the
`canonical_video_v2` profile version in the durable ingest payload; older
payloads without the optional field remain readable.

## Consequences

- Remuxes avoid unnecessary quality loss and CPU work when both the byte target
  and H.264 compatibility evidence are satisfied.
- Portrait and landscape inputs share one aspect-preserving scale expression.
- Profile and command decisions can be unit-tested with synthetic probes.
- The profile is intentionally video-only; image normalization is a separate
  JPEG/PNG profile and does not alter this video contract.
- Changing the profile or algorithm requires explicit versioning/documentation
  before existing canonical assets are reprocessed; existing Telegram storage
  messages are not backfilled.

## Alternatives considered

- Always transcode: simpler but wastes resources and can reduce quality for
  already-compatible media.
- Copy the illustrative specification command directly: rejected because the
  planner needs typed policy decisions and shell-free argument construction.
- Let worker handlers build arguments: rejected because it would spread media
  policy across orchestration code.

## Implementation status

F2 executes the plan, parses progress, validates the output against this
profile with ffprobe, hashes it, and hands the typed result to the durable
video identity boundary. That boundary records or reuses the canonical library
asset and its SHA-256 without holding a database transaction across ffmpeg or
ffprobe. The composed worker dispatches static JPEG/PNG inputs to the separate
image normalizer and records its thumbnail asset. Audio and animation inputs
retain their downloaded source artifact and use exact SHA storage behavior;
none of these non-video paths perform video fingerprinting.

Telegram storage upload carries the probed duration, width, and height together
with `supports_streaming=true`. The already-persisted bounded JPEG preview is
staged only for the multipart request, validated against Telegram's thumbnail
limits, and removed after the request.
