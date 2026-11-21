#!/usr/bin/env bash
set -euo pipefail

tag="${RELEASE_TAG:?RELEASE_TAG must name an existing tag}"

# Checkouts don't always fetch tags, and goreleaser refuses a tag that isn't on HEAD.
git fetch --force origin "refs/tags/${tag}:refs/tags/${tag}"

# Nothing is published here: release-publish.sh creates the release from every runner's artifacts at once.
GORELEASER_CURRENT_TAG="${tag}" goreleaser release --clean --skip=publish

buildkite-agent artifact upload "dist/quacktel_*.tar.gz*"
if [[ -f dist/homebrew/Formula/quacktel.rb ]]; then
  buildkite-agent artifact upload dist/homebrew/Formula/quacktel.rb
fi
