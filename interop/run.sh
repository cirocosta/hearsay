#!/usr/bin/env bash
set -euo pipefail

project_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd -- "$project_dir"
upstream_dir="$project_dir/.agi.local/repositories/rumors"
upstream_commit=bf2ba264e2360eab1ba5f06148ea3ef3050825c8

if [[ ! -e "$upstream_dir" ]]; then
    mkdir -p -- "$(dirname -- "$upstream_dir")"
    git clone --no-checkout https://github.com/oxidecomputer/rumors.git "$upstream_dir"
    git -C "$upstream_dir" checkout --detach "$upstream_commit"
fi
actual_commit=$(git -C "$upstream_dir" rev-parse HEAD)
if [[ "$actual_commit" != "$upstream_commit" ]]; then
    printf 'upstream clone must be at %s; found %s\n' "$upstream_commit" "$actual_commit" >&2
    exit 1
fi
if [[ -n $(git -C "$upstream_dir" status --porcelain --untracked-files=no) ]]; then
    printf 'upstream clone has tracked modifications; verify against a clean pinned clone\n' >&2
    exit 1
fi

mkdir -p -- "$project_dir/.agi.local/interop-artifacts"
(
    cd -- "$project_dir/interop/rust"
    cargo build --locked --target-dir "$project_dir/.agi.local/interop-target"
)
export HEARSAY_INTEROP_RUST_BIN="$project_dir/.agi.local/interop-target/debug/hearsay-interop"
if [[ ${HEARSAY_INTEROP_RACE:-0} == 1 ]]; then
    exec go run -race ./interop/runner
fi
exec go run ./interop/runner
