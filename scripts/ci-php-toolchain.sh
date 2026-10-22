#!/usr/bin/env bash

set -euo pipefail
umask 077

[[ "${RUNNER_TEMP:?}" == /* && -d "${RUNNER_TEMP}" ]]
project_root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
task_root="$(mktemp -d "${RUNNER_TEMP}/go-rabbitmq-queues-php.XXXXXX")"
cleanup() {
    result=$?
    trap - EXIT HUP INT TERM
    if ! find "${task_root}" -depth -delete; then
        printf '%s\n' 'Private PHP toolchain cleanup failed' >&2
        result=1
    fi
    exit "${result}"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# Official php-ubuntu asset 613079354, reviewed PHP 8.5.11 / Ubuntu 24.04.
# The numeric asset and digest must advance together after profile review.
php_sha256='8fe4b28e1e5c3228d45416fed28152cc0ab8325ae7f6e0d5bb531f8501671bf9'
composer_sha256='345b9c6a98da5c30dcbd4b0d99fc8710bf0ae98a3898eea18f7b2ad9dec93f06'
curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
    --connect-timeout 10 --max-time 120 --max-filesize 67108864 \
    --header 'Accept: application/octet-stream' \
    --header "Authorization: Bearer ${GH_TOKEN:?}" \
    --output "${task_root}/php.tar.zst" \
    https://api.github.com/repos/shivammathur/php-ubuntu/releases/assets/613079354
printf '%s  %s\n' "${php_sha256}" "${task_root}/php.tar.zst" | sha256sum --check --strict -
curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
    --connect-timeout 10 --max-time 120 --max-filesize 8388608 \
    --output "${task_root}/composer.phar" \
    https://getcomposer.org/download/2.10.1/composer.phar
printf '%s  %s\n' "${composer_sha256}" "${task_root}/composer.phar" | sha256sum --check --strict -
unset GH_TOKEN GITHUB_TOKEN

# Never install the package's system configuration, services, dpkg or apt data.
# Fixed destinations, regular members and bounded sizes avoid archive paths
# choosing the extraction surface; both acquired inputs were verified above.
python3 - "${task_root}" <<'PY'
import pathlib
import subprocess
import tarfile
import sys

root = pathlib.Path(sys.argv[1])
(root / "lib").mkdir()
(root / "extensions").mkdir()
members = {"./usr/bin/php8.5": root / "lib" / "php8.5"}
for extension in ("mbstring", "sockets", "phar", "iconv", "ctype"):
    members[f"./usr/lib/php/20250925/{extension}.so"] = root / "extensions" / f"{extension}.so"
seen = set()
with subprocess.Popen(["zstd", "--decompress", "--stdout", str(root / "php.tar.zst")], stdout=subprocess.PIPE) as decoder:
    try:
        with tarfile.open(fileobj=decoder.stdout, mode="r|") as archive:
            for member in archive:
                if member.name not in members:
                    continue
                if member.name in seen or not member.isfile() or not 0 < member.size <= 32 * 1024 * 1024:
                    raise ValueError("Invalid private PHP archive member")
                with archive.extractfile(member) as source:
                    body = source.read(32 * 1024 * 1024 + 1)
                if len(body) != member.size:
                    raise ValueError("Incomplete private PHP archive member")
                members[member.name].write_bytes(body)
                seen.add(member.name)
    finally:
        decoder.stdout.close()
    if decoder.wait() != 0 or seen != members.keys():
        raise ValueError("Incomplete private PHP archive")
(root / "lib" / "php8.5").chmod(0o700)
PY

mkdir "${task_root}/bin"
cat >"${task_root}/bin/php" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
prefix="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
exec "${prefix}/lib/php8.5" -n -d "extension_dir=${prefix}/extensions" \
    -d extension=mbstring.so -d extension=sockets.so -d extension=phar.so \
    -d extension=iconv.so -d extension=ctype.so "$@"
SH
cat >"${task_root}/bin/composer" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
prefix="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
exec "${prefix}/bin/php" "${prefix}/composer.phar" "$@"
SH
chmod 0700 "${task_root}/bin/php" "${task_root}/bin/composer"
export PATH="${task_root}/bin:${PATH}"

# Host libraries and builtin OpenSSL are deliberately not installed or replaced.
# Missing libraries, wrong runtime or unloaded extensions fail before the broker.
php <<'PHP'
<?php
if (PHP_VERSION !== "8.5.11") { fwrite(STDERR, "Unexpected PHP runtime\n"); exit(1); }
foreach (["mbstring", "openssl", "sockets", "phar", "iconv", "ctype"] as $extension) {
    if (!extension_loaded($extension)) { fwrite(STDERR, "Missing required PHP extension\n"); exit(1); }
}
echo PHP_VERSION, "\n";
PHP
composer --version --no-ansi | grep -Eq '^Composer version 2\.10\.1 '
"${project_root}/scripts/ci-live-broker.sh" php
