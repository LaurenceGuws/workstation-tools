#!/usr/bin/env bash
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)
cd "$root"
sources=(build.zig src/*.zig)
fail() { printf 'source audit: %s\n' "$*" >&2; exit 1; }
[[ $(<.zigversion) == "$(sed -n 's/.*\.minimum_zig_version = "\([^"]*\)".*/\1/p' build.zig.zon)" ]] || fail 'Zig pins disagree'
for file in "${sources[@]}"; do [[ $(head -n 1 "$file") == '//!'* ]] || fail "$file lacks owner documentation"; done
awk 'length($0)>135 {printf "source audit: %s:%d exceeds 135 columns\n",FILENAME,FNR>"/dev/stderr";bad=1} END{exit bad}' AGENTS.md "${sources[@]}"
if rg -n '\b(Remoter|REMOTER_AGENT|FLEET_OP|QAgent)\b' src; then fail 'consumer-specific name leaked into shared source'; fi
printf 'SOURCE AUDIT: PASS\n'
