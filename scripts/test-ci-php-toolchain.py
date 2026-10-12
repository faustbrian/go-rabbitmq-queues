#!/usr/bin/env python3
"""Exercise acquisition with real checksums; never execute acquired Linux code."""

import hashlib
import io
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tempfile
import unittest


class PrivatePHPToolchainAcquisition(unittest.TestCase):
    def test_extraction_selects_only_regular_profile_files(self):
        source = Path(__file__).with_name("ci-php-toolchain.sh").read_text()
        extraction = source.split("<<'PY'\n", 1)[1].split("\nPY\n", 1)[0]
        names = ["./usr/bin/php8.5"] + [
            f"./usr/lib/php/20250925/{name}.so"
            for name in ("mbstring", "sockets", "phar", "iconv", "ctype")
        ]
        for profile in ("regular", "symlink", "missing", "duplicate"):
            with self.subTest(profile=profile), tempfile.TemporaryDirectory() as owned:
                root = Path(owned) / "private"
                root.mkdir(mode=0o700)
                data = io.BytesIO()
                with tarfile.open(fileobj=data, mode="w") as archive:
                    for name in names + ["../../outside", "./usr/sbin/merge_status"]:
                        if profile == "missing" and name == names[-1]:
                            continue
                        member = tarfile.TarInfo(name)
                        body = b"profile fixture; not executable"
                        member.size = len(body)
                        if profile == "symlink" and name == names[-1]:
                            member.type, member.linkname, member.size = tarfile.SYMTYPE, names[0], 0
                            body = b""
                        archive.addfile(member, io.BytesIO(body))
                        if profile == "duplicate" and name == names[0]:
                            archive.addfile(member, io.BytesIO(body))
                compressed = subprocess.run(["zstd", "--quiet", "--stdout"], input=data.getvalue(),
                                            capture_output=True, check=True, timeout=20)
                (root / "php.tar.zst").write_bytes(compressed.stdout)
                result = subprocess.run([sys.executable, "-c", extraction, str(root)],
                                        capture_output=True, timeout=20, umask=0o077)
                self.assertEqual(result.returncode == 0, profile == "regular")
                self.assertFalse((root.parent / "outside").exists())
                self.assertFalse((root / "usr").exists())
                if profile == "regular":
                    self.assertEqual(len(list((root / "extensions").iterdir())), 5)
                    self.assertEqual((root / "lib" / "php8.5").stat().st_mode & 0o777, 0o700)

    def run_acquisition(self, failure, valid_php=False, valid_composer=False):
        source = Path(__file__).with_name("ci-php-toolchain.sh").read_text()
        # Fixture identity substitutions are confined to a disposable copy.
        # This admits earlier inputs without acquiring/executing real PHP.
        for name, payload, valid in (
            ("php", b"PHP acquisition fixture", valid_php),
            ("composer", b"Composer acquisition fixture", valid_composer),
        ):
            if valid:
                source, count = re.subn(
                    rf"{name}_sha256='[0-9a-f]{{64}}'",
                    f"{name}_sha256='{hashlib.sha256(payload).hexdigest()}'",
                    source,
                )
                self.assertEqual(count, 1)
        with tempfile.TemporaryDirectory(prefix="rabbit-php-acquisition-test-") as owned:
            task = Path(owned)
            scripts, bin_dir, runner = task / "scripts", task / "bin", task / "runner"
            for directory in (scripts, bin_dir, runner):
                directory.mkdir(mode=0o700)
            installer = scripts / "ci-php-toolchain.sh"
            installer.write_text(source)
            log = task / "commands.log"
            stubs = {
                "curl": """#!/usr/bin/env bash
set -euo pipefail
role=php
[[ "${*: -1}" != https://getcomposer.org/* ]] || role=composer
printf '%s\n' "${role}" >>"${COMMAND_LOG}"
[[ "${FAILURE}" != "${role}-download" ]] || exit 42
while [[ $# -gt 0 ]]; do
    if [[ "$1" == --output ]]; then
        shift
        if [[ "${role}" == php ]]; then
            printf 'PHP acquisition fixture' >"$1"
        else
            printf 'Composer acquisition fixture' >"$1"
        fi
    fi
    shift
done
""",
                "python3": """#!/usr/bin/env bash
printf 'extraction\n' >>"${COMMAND_LOG}"
exit 41
""",
            }
            for name, body in stubs.items():
                stub = bin_dir / name
                stub.write_text(body)
                stub.chmod(0o700)
            environment = dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}",
                               RUNNER_TEMP=str(runner), GH_TOKEN="fixture-only",
                               COMMAND_LOG=str(log), FAILURE=failure)
            result = subprocess.run(["bash", str(installer)], env=environment,
                                    capture_output=True, text=True, timeout=20)
            commands = log.read_text().splitlines()
            self.assertEqual(list(runner.iterdir()), [], "owned acquisition prefix leaked")
            return result, commands

    def test_failed_or_corrupt_inputs_stop_before_extraction(self):
        for role in ("php", "composer"):
            for failure in ("download", "digest"):
                with self.subTest(role=role, failure=failure):
                    result, commands = self.run_acquisition(
                        f"{role}-{failure}", valid_php=role == "composer")
                    self.assertEqual(result.returncode, 42 if failure == "download" else 1)
                    self.assertEqual(commands, ["php"] if role == "php" else ["php", "composer"])
                    if failure == "digest":
                        self.assertIn("FAILED", result.stdout + result.stderr)

    def test_verified_inputs_reach_extraction_and_cleanup_its_failure(self):
        result, commands = self.run_acquisition("none", valid_php=True, valid_composer=True)
        self.assertEqual(result.returncode, 41)
        self.assertEqual(commands, ["php", "composer", "extraction"])


if __name__ == "__main__":
    unittest.main()
