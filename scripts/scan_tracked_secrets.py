"""Fail closed on common concrete credentials without printing secret values."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path


_PATTERNS = {
    "aws_access_key": re.compile(rb"AKIA[0-9A-Z]{16}"),
    "github_token": re.compile(rb"gh[ps]_[A-Za-z0-9]{30,}"),
    "slack_token": re.compile(rb"xox[baprs]-[A-Za-z0-9-]{20,}"),
    "stripe_live_key": re.compile(rb"sk_live_[A-Za-z0-9]{16,}"),
    "private_key": re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "credentialed_database_url": re.compile(
        rb"postgres(?:ql)?(?:\+[a-z0-9]+)?://[^\s:'\"]+:[^\s@'\"]+@",
        re.IGNORECASE,
    ),
}

_BINARY_SUFFIXES = {
    ".docx", ".pdf", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".xlsx",
}

_KNOWN_FAKE_VALUES = (
    b"-----BEGIN " + b"PRIVATE KEY-----\nexample\n-----END PRIVATE KEY-----",
)


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    tracked = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
    ).stdout.split(b"\0")
    findings: list[tuple[str, str, int]] = []
    for raw_path in tracked:
        if not raw_path:
            continue
        relative = raw_path.decode("utf-8", errors="surrogateescape")
        path = root / relative
        if path.suffix.lower() in _BINARY_SUFFIXES or not path.is_file():
            continue
        content = path.read_bytes().replace(b"\r\n", b"\n")
        for fake_value in _KNOWN_FAKE_VALUES:
            content = content.replace(fake_value, b"known-test-fixture")
        for pattern_name, pattern in _PATTERNS.items():
            for match in pattern.finditer(content):
                line = content.count(b"\n", 0, match.start()) + 1
                findings.append((relative, pattern_name, line))
    if findings:
        for relative, pattern_name, line in findings:
            print(f"{relative}:{line}: possible {pattern_name} (value redacted)")
        return 1
    print(f"Tracked-secret scan passed: {len(tracked) - 1} files checked; 0 findings.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
