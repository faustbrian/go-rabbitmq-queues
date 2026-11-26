#!/usr/bin/env bash
# Maintainer-run: mirror one reviewed php-ubuntu tarball to a dedicated,
# non-semver release of this repository so CI never depends on upstream's
# rolling "builds" assets, which are rebuilt and replaced in place.
#
# Usage: scripts/mirror-php-toolchain.sh <php-version> <reviewed-sha256> [asset-name]
# Example:
#   scripts/mirror-php-toolchain.sh 8.5.11 \
#       02cfc1197d7fc40528c901ea775bc361e08895456a6fa8e5ddc519529a4832b5
#
# The reviewed digest is required input: the script refuses any upstream bytes
# whose GitHub-recorded digest or locally computed SHA-256 differs from it.

set -euo pipefail
umask 077

upstream_repo='shivammathur/php-ubuntu'
upstream_tag='builds'
mirror_repo='faustbrian/go-rabbitmq-queues'

if [[ $# -lt 2 || $# -gt 3 ]]; then
    printf 'usage: %s <php-version> <reviewed-sha256> [asset-name]\n' "$0" >&2
    exit 2
fi
version="$1"
expected_sha256="$2"
asset_name="${3:-php_${version}-nts+ubuntu24.04.tar.zst}"
[[ "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { printf 'invalid PHP version\n' >&2; exit 2; }
[[ "${expected_sha256}" =~ ^[0-9a-f]{64}$ ]] || { printf 'invalid SHA-256\n' >&2; exit 2; }
mirror_tag="ci-toolchain-php-${version}"

if gh release view "${mirror_tag}" --repo "${mirror_repo}" >/dev/null 2>&1; then
    printf 'release %s already exists; mirror releases are never replaced\n' "${mirror_tag}" >&2
    exit 1
fi

task_root="$(mktemp -d "${TMPDIR:-/tmp}/php-toolchain-mirror.XXXXXX")"
cleanup() { find "${task_root}" -depth -delete; }
trap cleanup EXIT

# Exactly one upstream asset with this name, and its recorded digest must be
# the reviewed one before any bytes are fetched.
gh api "repos/${upstream_repo}/releases/tags/${upstream_tag}" \
    --jq ".assets[] | select(.name == \"${asset_name}\") | [.id, .digest, .size, .updated_at] | @tsv" \
    >"${task_root}/asset.tsv"
[[ "$(wc -l <"${task_root}/asset.tsv")" -eq 1 ]] || {
    printf 'expected exactly one upstream asset named %s\n' "${asset_name}" >&2
    exit 1
}
IFS=$'\t' read -r upstream_id upstream_digest upstream_size upstream_updated <"${task_root}/asset.tsv"
if [[ "${upstream_digest}" != "sha256:${expected_sha256}" ]]; then
    printf 'upstream digest %s does not match reviewed sha256:%s\n' "${upstream_digest}" "${expected_sha256}" >&2
    exit 1
fi

mkdir "${task_root}/upload"
tarball="${task_root}/upload/${asset_name}"
gh api -H 'Accept: application/octet-stream' \
    "repos/${upstream_repo}/releases/assets/${upstream_id}" >"${tarball}"
if command -v sha256sum >/dev/null; then
    actual_sha256="$(sha256sum "${tarball}" | cut -d' ' -f1)"
else
    actual_sha256="$(shasum -a 256 "${tarball}" | cut -d' ' -f1)"
fi
if [[ "${actual_sha256}" != "${expected_sha256}" ]]; then
    printf 'downloaded bytes have sha256 %s, expected %s\n' "${actual_sha256}" "${expected_sha256}" >&2
    exit 1
fi

notes="Immutable CI toolchain mirror; not a library release.

Source: https://github.com/${upstream_repo}/releases/tag/${upstream_tag}
Upstream asset: ${asset_name} (ID ${upstream_id}, ${upstream_size} bytes, updated ${upstream_updated})
SHA-256: ${expected_sha256}

Consumed by scripts/ci-php-toolchain.sh, which verifies this digest before use."

# --latest=false keeps the library's real releases as "latest". The tag is not
# semver, so the Go module proxy ignores it.
gh release create "${mirror_tag}" "${tarball}" \
    --repo "${mirror_repo}" \
    --title "CI toolchain: PHP ${version} (Ubuntu 24.04)" \
    --notes "${notes}" \
    --latest=false

# GitHub may rename assets containing characters such as '+', so read the
# mirrored asset back by digest rather than by name.
mirror_asset="$(gh api "repos/${mirror_repo}/releases/tags/${mirror_tag}" \
    --jq ".assets[] | select(.digest == \"sha256:${expected_sha256}\") | [.id, .name] | @tsv")"
[[ -n "${mirror_asset}" && "$(printf '%s\n' "${mirror_asset}" | wc -l)" -eq 1 ]] || {
    printf 'mirrored asset with sha256:%s not found on %s\n' "${expected_sha256}" "${mirror_tag}" >&2
    exit 1
}
IFS=$'\t' read -r mirror_id mirror_name <<<"${mirror_asset}"
printf 'Mirrored %s as asset %s (%s) on %s\n' "${asset_name}" "${mirror_id}" "${mirror_name}" "${mirror_tag}"
printf 'Pin in scripts/ci-php-toolchain.sh:\n'
printf '  https://api.github.com/repos/%s/releases/assets/%s\n' "${mirror_repo}" "${mirror_id}"
printf "  php_sha256='%s'\n" "${expected_sha256}"
