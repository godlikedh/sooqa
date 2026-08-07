# Development

Use the pinned Rust toolchain and run `just check` before submitting changes.
Keep implementation work aligned with one roadmap slice from
[PROJECT_SPEC.md](PROJECT_SPEC.md), and keep each slice independently
compilable and testable.

## HTTP API contract

The versioned HTTP contract lives in [openapi.yaml](openapi.yaml). Validate it
with:

    just openapi-validate

The repository also pins the OpenAPI Generator CLI version in
`openapitools.json`. When a JDK is installed, generate a models-only Rust
preview with:

    just openapi-generate

Generated output is written to `target/openapi-generated/` and is intentionally
not committed. The API crate remains the integration boundary: generated
models can be adopted there when the contract and generator output are stable,
while authentication, persistence, and request orchestration stay in the
handwritten server layer.
