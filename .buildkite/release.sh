#!/usr/bin/env bash
set -euo pipefail

tag="${RELEASE_TAG:?RELEASE_TAG must name an existing tag}"

# Checkouts don't always fetch tags, and goreleaser refuses a tag that isn't on HEAD.
git fetch --force origin "refs/tags/${tag}:refs/tags/${tag}"

token="$(buildkite-agent secret get QUACKTEL_GITHUB_TOKEN 2>/dev/null)" || {
  echo "failed to read the QUACKTEL_GITHUB_TOKEN cluster secret" >&2
  exit 1
}
GITHUB_TOKEN="${token}" GORELEASER_CURRENT_TAG="${tag}" goreleaser release --clean
