#!/usr/bin/env bash
set -euo pipefail

tag="${RELEASE_TAG:?RELEASE_TAG must name an existing tag}"
repo="quacktel/cli"
tap="quacktel/homebrew-tap"
formula="Formula/quacktel.rb"
targets=(darwin_arm64 linux_amd64 linux_arm64)

# The tag is interpolated into the provenance JSON below, so it must be a plain semver tag.
if [[ ! "${tag}" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$ ]]; then
  echo "RELEASE_TAG ${tag} is not a vX.Y.Z[-pre] tag" >&2
  exit 1
fi
git fetch --force origin "refs/tags/${tag}:refs/tags/${tag}"
commit="$(git rev-parse "${tag}^{commit}")"

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

# SLSA v1 provenance, signed keyless with this job's Buildkite OIDC identity (Fulcio cert, Rekor entry).
# It attests that this build of ${commit} published these digests; the archives were compiled by the release_build jobs.
cat >dist/provenance.json <<JSON
{
  "buildDefinition": {
    "buildType": "https://github.com/${repo}/blob/${commit}/.buildkite/pipeline.yml",
    "externalParameters": {
      "repository": "https://github.com/${repo}",
      "ref": "refs/tags/${tag}"
    },
    "internalParameters": {
      "buildkite_organization": "${BUILDKITE_ORGANIZATION_SLUG}",
      "buildkite_pipeline": "${BUILDKITE_PIPELINE_SLUG}"
    },
    "resolvedDependencies": [
      {"uri": "git+https://github.com/${repo}@refs/tags/${tag}", "digest": {"gitCommit": "${commit}"}}
    ]
  },
  "runDetails": {
    "builder": {"id": "https://buildkite.com/${BUILDKITE_ORGANIZATION_SLUG}/${BUILDKITE_PIPELINE_SLUG}"},
    "metadata": {"invocationId": "${BUILDKITE_BUILD_URL}"}
  }
}
JSON

for target in "${targets[@]}"; do
  archive="quacktel_${target}.tar.gz"
  # Run from dist so the attestation subject is the bare asset name.
  (cd dist && cosign attest-blob --yes --oidc-provider buildkite-agent --type slsaprovenance1 \
    --predicate provenance.json --bundle "${archive}.sigstore.json" "${archive}")
  assets+=("dist/${archive}.sigstore.json")
done

flags=(--repo "${repo}" --verify-tag --title "${tag}" --notes "")
if [[ "${tag}" == *-* ]]; then
  flags+=(--prerelease)
fi

# gh uploads the assets to a draft and publishes it last, so the release is complete when it becomes immutable.
GH_TOKEN="${token}" gh release create "${tag}" "${flags[@]}" "${assets[@]}"

# Spike: also store the bundles in GitHub's attestation API, so gh attestation verify can find them. Not fatal yet.
for target in "${targets[@]}"; do
  bundle="dist/quacktel_${target}.tar.gz.sigstore.json"
  if ! { printf '{"bundle":'; cat "${bundle}"; printf '}'; } |
    GH_TOKEN="${token}" gh api --method POST "repos/${repo}/attestations" --input - --silent; then
    echo "warning: failed to upload ${bundle} to the GitHub attestation API" >&2
  fi
done

sha="$(GH_TOKEN="${token}" gh api "repos/${tap}/contents/${formula}" --jq .sha)"
GH_TOKEN="${token}" gh api --method PUT "repos/${tap}/contents/${formula}" --silent \
  -f message="Brew formula update for quacktel version ${tag}" \
  -f content="$(base64 <"dist/homebrew/${formula}" | tr -d '\n')" \
  -f sha="${sha}"
