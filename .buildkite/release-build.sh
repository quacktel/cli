#!/usr/bin/env bash
set -euo pipefail

tag="${BUILDKITE_TAG:?run this from a tag build of the release pipeline}"

# Checkouts don't always fetch tags, and goreleaser refuses a tag that isn't on HEAD.
git fetch --force origin "refs/tags/${tag}:refs/tags/${tag}"

# Nothing is published here: release-publish.sh creates the release from every runner's artifacts at once.
GORELEASER_CURRENT_TAG="${tag}" goreleaser release --clean --skip=publish

buildkite-agent artifact upload "dist/quacktel_*.tar.gz*"
# Every agent generates a formula for its own archive; only macOS's is the real one, and artifacts share one path.
if [[ "$(uname -s)" == "Darwin" ]]; then
  buildkite-agent artifact upload dist/homebrew/Formula/quacktel.rb
fi
