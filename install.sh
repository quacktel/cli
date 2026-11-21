#!/bin/sh
# Installs the quacktel release binary for this OS and architecture.
# Usage: install.sh [--version vX.Y.Z] [--prefix DIR]
set -eu

repo="quacktel/cli"
version="latest"
prefix="${HOME}/.local"

while [ $# -gt 0 ]; do
  case "$1" in
    --version) version="$2"; shift 2 ;;
    --prefix) prefix="$2"; shift 2 ;;
    -h | --help) echo "usage: install.sh [--version vX.Y.Z] [--prefix DIR]"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

case "$(uname -s)-$(uname -m)" in
  Darwin-arm64) target="darwin_arm64" ;;
  Linux-x86_64) target="linux_amd64" ;;
  Linux-aarch64 | Linux-arm64) target="linux_arm64" ;;
  *) echo "unsupported platform: $(uname -s)-$(uname -m)" >&2; exit 1 ;;
esac

asset="quacktel_${target}.tar.gz"
tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT

# gh works while the repository is private; curl needs it public.
if command -v gh >/dev/null 2>&1; then
  tag=""
  [ "${version}" = "latest" ] || tag="${version}"
  # shellcheck disable=SC2086 # An empty tag means latest.
  gh release download ${tag} --repo "${repo}" --dir "${tmp}" --pattern "${asset}" --pattern "${asset}.sha256"
else
  if [ "${version}" = "latest" ]; then
    base="https://github.com/${repo}/releases/latest/download"
  else
    base="https://github.com/${repo}/releases/download/${version}"
  fi
  curl -fsSL -o "${tmp}/${asset}" "${base}/${asset}"
  curl -fsSL -o "${tmp}/${asset}.sha256" "${base}/${asset}.sha256"
fi

expected="$(cut -d' ' -f1 <"${tmp}/${asset}.sha256")"
if command -v sha256sum >/dev/null 2>&1; then
  actual="$(sha256sum "${tmp}/${asset}" | cut -d' ' -f1)"
else
  actual="$(shasum -a 256 "${tmp}/${asset}" | cut -d' ' -f1)"
fi
if [ "${expected}" != "${actual}" ]; then
  echo "checksum mismatch for ${asset}: expected ${expected}, got ${actual}" >&2
  exit 1
fi
echo "checksum verified; build provenance was not verified" >&2

tar -xzf "${tmp}/${asset}" -C "${tmp}" quacktel
mkdir -p "${prefix}/bin"
install -m 0755 "${tmp}/quacktel" "${prefix}/bin/quacktel"
echo "installed $("${prefix}/bin/quacktel" version) to ${prefix}/bin/quacktel"

case ":${PATH}:" in
  *":${prefix}/bin:"*) ;;
  *) echo "add ${prefix}/bin to your PATH" >&2 ;;
esac
