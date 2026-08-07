# Security

The security model is specified in [PROJECT_SPEC.md](PROJECT_SPEC.md),
including loopback-only companion access, SSRF controls, subprocess
constraints, token handling, and responsible-use boundaries.

The direct HTTP downloader resolves domain names before connecting and rejects
loopback, private, link-local, multicast, metadata-service, carrier-grade NAT,
and other special address ranges. Redirects are disabled in the HTTP client
and followed manually so every destination is validated and pinned separately.
