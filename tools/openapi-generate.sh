#!/bin/sh

set -eu

if ! command -v npx >/dev/null 2>&1; then
    echo "OpenAPI generation requires npm/npx" >&2
    exit 1
fi

if ! java -version >/dev/null 2>&1; then
    echo "OpenAPI generation requires a working JDK; install one and retry" >&2
    exit 1
fi

rm -rf target/openapi-generated

# Generate only portable Rust models. The server crate keeps auth, state, and
# orchestration handwritten at the API boundary.
npx --yes @openapitools/openapi-generator-cli generate \
    -i docs/openapi.yaml \
    -g rust \
    -o target/openapi-generated \
    --global-property models,modelDocs=false,modelTests=false \
    --additional-properties packageName=sooqa_api_contract,packageVersion=0.1.0
