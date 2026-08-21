#!/bin/sh

set -eu

case "${1:-all}" in
  all)
    node --test userscripts/test/*.test.cjs
    node --test apps/server/assets/admin/test/*.test.cjs
    ;;
  userscript)
    node --test userscripts/test/*.test.cjs
    ;;
  admin-assets)
    node --test apps/server/assets/admin/test/*.test.cjs
    ;;
  *)
    echo "usage: sh tools/test-javascript.sh [all|userscript|admin-assets]" >&2
    exit 2
    ;;
esac
