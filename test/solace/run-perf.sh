#!/usr/bin/env bash
# run-perf.sh — pre-warm Docker cache from GHCR, then launch the perf benchmark.
#
# mzbuild computes a content-addressed fingerprint hash for every changed file and
# tries to pull that tag from ghcr.io/materializeinc. For fork images that hash
# doesn't exist there, so mzbuild falls back to a local Rust recompile — before any
# workflow Python code runs. _ensure_images_from_ghcr inside workflow_perf is
# therefore too late.
#
# This script runs the cache pre-warm step via bin/pyactivate (bypassing mzbuild
# entirely), then hands off to bin/mzcompose. mzbuild finds the retagged image in
# the local Docker cache and skips the build.
#
# Usage:
#   test/solace/run-perf.sh --goal throughput [--rate 500] [--duration 600] \
#                            [--probe-interval 200ms] [--ghcr-tag fix-solace-probe-frontier]
#
# All flags except --ghcr-tag are forwarded verbatim to `mzcompose run perf`.
set -euo pipefail

# Default tag mirrors ci/solace/build.py: main -> "latest", other branches ->
# sanitized branch name. Override with --ghcr-tag if needed.
_current_branch="$(git branch --show-current 2>/dev/null || echo '')"
if [[ -z "$_current_branch" || "$_current_branch" == "main" ]]; then
    GHCR_TAG="latest"
else
    GHCR_TAG="${_current_branch//[^a-zA-Z0-9._-]/-}"
    GHCR_TAG="${GHCR_TAG#-}"
fi

PERF_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ghcr-tag)
            GHCR_TAG="$2"; shift 2 ;;
        *)
            PERF_ARGS+=("$1"); shift ;;
    esac
done

# Resolve repo root regardless of where the script is called from.
REPO_ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
cd "$REPO_ROOT"

echo "==> Pre-warming Docker cache from ghcr.io/jessemenning/materialize-solace (tag: $GHCR_TAG)"
MZ_ROOT="$REPO_ROOT" bin/pyactivate -c "
import subprocess, sys
from pathlib import Path
from materialize import mzbuild as _mzbuild

GHCR_FORK = 'ghcr.io/jessemenning/materialize-solace'
TARGET_IMAGES = {'materialized', 'testdrive'}
BRANCH_TAG = '$GHCR_TAG'

repo = _mzbuild.Repository(Path('.'), profile=_mzbuild.Profile.OPTIMIZED)
deps = repo.resolve_dependencies(image for image in repo if image.name in TARGET_IMAGES)
for dep in [d for d in deps if d.name in TARGET_IMAGES]:
    alias = f'{GHCR_FORK}/{dep.name}:{BRANCH_TAG}'
    fingerprint_tag = dep.spec()
    print(f'  pull  {alias}')
    subprocess.run(['docker', 'pull', alias], check=True)
    print(f'  retag -> {fingerprint_tag}')
    subprocess.run(['docker', 'tag', alias, fingerprint_tag], check=True)
    print(f'  ok: {dep.name} -> {fingerprint_tag}')
"

echo "==> Launching perf benchmark (goal: ${PERF_ARGS[*]:-<no args>})"
exec bin/mzcompose --find solace run perf "${PERF_ARGS[@]}"
