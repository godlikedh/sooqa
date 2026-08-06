# ADR 0006: Apache-2.0 license

## Status

Accepted

## Context

sooqa is intended to be an open-source, self-hosted project. The repository
needs an explicit license before implementation grows beyond the bootstrap
stage.

## Decision

Use Apache-2.0 for the project. It provides a permissive copyright license and
an explicit patent grant while allowing self-hosted and commercial use.

## Consequences

- Source and derived distributions must retain the license notices required by
  Apache-2.0.
- Third-party dependencies and notices must be reviewed as the dependency set
  grows.
- The repository includes the complete license text in `LICENSE`.

## Alternatives considered

- MIT: similarly permissive, but with a less explicit patent license.
- AGPL-3.0: not chosen because the initial goal does not require network-use
  copyleft obligations.
