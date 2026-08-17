#!/usr/bin/env sh
set -eu

if [ "$#" -eq 0 ]; then
  echo "Usage: $0 <docker> <compose> [compose args...]" >&2
  exit 2
fi

asset_file="$(mktemp)"

cleanup() {
  rm -f "$asset_file"
}

trap cleanup EXIT INT TERM

if ! "$@" exec -T dashboard sh -lc '
set -eu

emit_assets() {
  kind="$1"
  pattern="$2"
  set -- $pattern
  if [ "$1" = "$pattern" ] || [ ! -f "$1" ]; then
    echo "missing dashboard $kind asset matching $pattern" >&2
    exit 1
  fi
  for asset in "$@"; do
    [ -f "$asset" ] || continue
    printf "%s\n" "$asset"
  done
}

emit_assets JS "dist/assets/index-*.js"
emit_assets CSS "dist/assets/index-*.css"
' >"$asset_file"; then
  echo "Dashboard container did not publish built JS/CSS assets" >&2
  exit 1
fi

if ! grep -Eq '\.js$' "$asset_file" || ! grep -Eq '\.css$' "$asset_file"; then
  echo "Dashboard container did not publish both built JS and CSS assets" >&2
  exit 1
fi

tr '\n' ' ' <"$asset_file"
printf '\n'
