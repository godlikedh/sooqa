#!/bin/sh

set -eu

if ! command -v npx >/dev/null 2>&1; then
    echo "openapi validation requires npm/npx" >&2
    exit 1
fi

npx --yes @redocly/cli@2.46.0 lint docs/openapi.yaml
