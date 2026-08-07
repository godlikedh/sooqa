# Security

The security model is specified in [PROJECT_SPEC.md](PROJECT_SPEC.md),
including loopback-only companion access, SSRF controls, subprocess
constraints, token handling, and responsible-use boundaries.

The direct HTTP downloader resolves domain names before connecting and rejects
loopback, private, link-local, multicast, metadata-service, carrier-grade NAT,
and other special address ranges. Redirects are disabled in the HTTP client
and followed manually so every destination is validated and pinned separately.

Media workspaces use job IDs rather than user-controlled path components. File
names cannot contain separators or parent-directory components, symlinked
workspace paths are rejected, and cleanup verifies the workspace is the
expected direct child of the configured jobs directory before removing it.
