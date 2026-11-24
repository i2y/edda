#!/usr/bin/env bash
# Bump the durax-io/schema submodule pin across all local repos.
# Usage: scripts/bump-schema.sh [commit-ish]  (default: origin/main)
#
# Removes the "submodule pin dance" from schema updates: fetch, checkout
# the target commit in each repo's schema/ submodule, and commit the pin.
set -euo pipefail

REV="${1:-origin/main}"
REPOS="${REPOS:-$HOME/romancy $HOME/edda $HOME/shikibu $HOME/kairo}"

for repo in $REPOS; do
  sub="$repo/schema"
  [ -d "$sub/.git" ] || [ -f "$sub/.git" ] || { echo "skip $repo (no schema submodule)"; continue; }
  git -C "$sub" fetch -q origin
  target=$(git -C "$sub" rev-parse "$REV" 2>/dev/null || true)
  [ -n "$target" ] || { echo "skip $repo ($REV not found)"; continue; }
  cur=$(git -C "$sub" rev-parse HEAD)
  if [ "$cur" = "$target" ]; then
    echo "$repo: already at ${target:0:7}"
    continue
  fi
  git -C "$sub" checkout -q "$target"
  git -C "$repo" add schema
  git -C "$repo" commit -q -m "chore: bump schema submodule ($(git -C "$sub" log -1 --format=%s "$target" | head -c 60))"
  echo "$repo: ${cur:0:7} -> ${target:0:7}"
done
