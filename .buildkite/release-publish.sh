#!/usr/bin/env bash
set -euo pipefail

tag="${RELEASE_TAG:?RELEASE_TAG must name an existing tag}"
repo="quacktel/cli"
tap="quacktel/homebrew-tap"
formula="Formula/quacktel.rb"
targets=(darwin_arm64 linux_amd64 linux_arm64)

token="$(buildkite-agent secret get QUACKTEL_GITHUB_TOKEN 2>/dev/null)" || {
  echo "failed to read the QUACKTEL_GITHUB_TOKEN cluster secret" >&2
  exit 1
}

# Immutable releases can't be changed once published, so a re-run must cut a new tag instead.
if GH_TOKEN="${token}" gh release view "${tag}" --repo "${repo}" >/dev/null 2>&1; then
  echo "release ${tag} already exists; tag a new version instead" >&2
  exit 1
fi

buildkite-agent artifact download "dist/*" .

assets=()
for target in "${targets[@]}"; do
  archive="dist/quacktel_${target}.tar.gz"
  for f in "${archive}" "${archive}.sha256" "${archive}.sbom.json" "${archive}.sbom.json.sha256"; do
    [[ -f "${f}" ]] || { echo "missing release asset ${f}" >&2; exit 1; }
    assets+=("${f}")
  done
  for f in "${archive}" "${archive}.sbom.json"; do
    if [[ "$(sha256sum "${f}" | cut -d' ' -f1)" != "$(cat "${f}.sha256")" ]]; then
      echo "checksum mismatch for ${f}" >&2
      exit 1
    fi
  done
done
[[ -f "dist/homebrew/${formula}" ]] || { echo "missing dist/homebrew/${formula}" >&2; exit 1; }

flags=(--repo "${repo}" --verify-tag --title "${tag}" --notes "")
if [[ "${tag}" == *-* ]]; then
  flags+=(--prerelease)
fi

# gh uploads the assets to a draft and publishes it last, so the release is complete when it becomes immutable.
GH_TOKEN="${token}" gh release create "${tag}" "${flags[@]}" "${assets[@]}"

sha="$(GH_TOKEN="${token}" gh api "repos/${tap}/contents/${formula}" --jq .sha)"
GH_TOKEN="${token}" gh api --method PUT "repos/${tap}/contents/${formula}" --silent \
  -f message="Brew formula update for quacktel version ${tag}" \
  -f content="$(base64 <"dist/homebrew/${formula}" | tr -d '\n')" \
  -f sha="${sha}"
