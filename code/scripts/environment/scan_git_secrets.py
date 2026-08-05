#!/usr/bin/env python3
"""High-precision secret scanner for git-tracked content.

Blocks the common leak shapes from entering commits:

* private key material (``-----BEGIN ... PRIVATE KEY-----``);
* known API token formats (HuggingFace ``hf_``, OpenAI ``sk-``, GitHub
  ``ghp_``/``github_pat_``, Slack ``xox*``, Google ``AIza``);
* ``sshpass -p <pw>`` with an embedded password;
* credential URLs ``scheme://user:password@host`` where the host is external
  (not localhost) OR the localhost pair is not the grandfathered dev default.

Grandfathered (not external credentials, kept verbatim in the repo):

* ``postgres:postgres@localhost[:port]`` -- the public PostgreSQL local dev
  default (only binds to localhost; not an external credential);
* template hosts carrying a ``<...>`` placeholder, e.g.
  ``connect.<region>.seetacloud.com``.

This is a HIGH-PRECISION guard against accidental commits, not a full
secret-discovery tool; for deep scans run ``gitleaks`` or ``trufflehog``.
Real secrets must also live only in gitignored runtime env files
(``.gitignore`` covers ``*.env`` / ``*.env.local`` with ``!*.env.example``).

Usage:
  python code/scripts/environment/scan_git_secrets.py            # staged files
  python code/scripts/environment/scan_git_secrets.py --all      # all tracked files
  python code/scripts/environment/scan_git_secrets.py path/a path/b
Exit code 1 if any violation is found. Intended as a pre-commit hook:
  .githooks/pre-commit -> git config core.hooksPath .githooks
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path

DEFAULT_BASELINE = Path("code/scripts/environment/secret_scan_baseline.txt")

PRIVATE_KEY = re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA |PGP )?PRIVATE KEY-----")

API_TOKENS = re.compile(
    r"(?:"
    r"hf_[A-Za-z0-9]{20,}"
    r"|sk-(?:[A-Za-z0-9]{20,}|proj-[A-Za-z0-9_-]{20,})"
    r"|ghp_[A-Za-z0-9]{36}"
    r"|github_pat_[A-Za-z0-9_]{20,}"
    r"|xox[baprs]-[A-Za-z0-9-]{10,}"
    r"|AIza[1-9A-Za-z_-]{35}"
    r")"
)

# sshpass with a literal password. Exclude template ``<...>`` and ``$VAR``
# references so the rule's own documentation does not self-trigger.
SSHPASS = re.compile(r"sshpass\s+-p\s+[^\s<$][^\s]*", re.I)

# scheme://user:password@host  (password may not contain '@' or '/')
CRED_URL = re.compile(r"[a-z][a-z0-9+.\-]*://([^@\s:/]+):([^@\s/]+)@([^@\s:/]+)", re.I)

LOCAL_HOSTS = {"localhost", "127.0.0.1", "0.0.0.0", "::1"}
TEMPLATE_HOST = re.compile(r"<[^>]+>")
# Only treat a host as a REAL external target if it is an IPv4 address or ends
# in a real TLD. This excludes documentation/test fixtures like ``host``,
# ``h``, ``db.host`` (no real TLD) while still catching
# ``user:pw@connect.bjb1.seetacloud.com`` / ``user:pw@10.0.0.5``.
IPV4 = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")
REAL_TLD = re.compile(
    r"\.(?:com|org|net|io|cloud|dev|ai|cn|jp|uk|us|de|fr|co|app|xyz|info|biz|tv|cc|me)$"
)
EXAMPLE_DOMAINS = {"example.com", "example.org", "example.net", "your-host.com"}


def _credential_url_violation(line: str) -> str | None:
    for match in CRED_URL.finditer(line):
        user, _pw, host = match.group(1), match.group(2), match.group(3)
        host_lc = host.lower()
        if TEMPLATE_HOST.search(host) or host_lc in LOCAL_HOSTS:
            continue  # localhost binds locally; not an external credential
        if host_lc in EXAMPLE_DOMAINS:
            continue
        if IPV4.match(host) or REAL_TLD.search(host_lc):
            return f"external credential url {user}:***@{host}"
    return None


def scan_line(line: str) -> str | None:
    """Return a violation reason for ``line`` or ``None`` if clean."""

    if PRIVATE_KEY.search(line):
        return "private key material"
    token = API_TOKENS.search(line)
    if token:
        return f"api token ({token.group(0)[:7]}...)"
    if SSHPASS.search(line):
        return "sshpass -p with embedded password"
    return _credential_url_violation(line)


def _staged_files() -> list[str]:
    out = subprocess.run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACM"],
        capture_output=True, text=True, check=True,
    ).stdout
    return [f for f in out.splitlines() if f]


def _all_files() -> list[str]:
    out = subprocess.run(
        ["git", "ls-files"], capture_output=True, text=True, check=True,
    ).stdout
    return [f for f in out.splitlines() if f]


def _load_baseline(path: Path | None) -> list[re.Pattern]:
    if path is None:
        return []
    if not path.is_file():
        return []
    patterns: list[re.Pattern] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        patterns.append(re.compile(line))
    return patterns


def _suppressed(rel: str, snippet: str, baseline: list[re.Pattern]) -> bool:
    for pat in baseline:
        if pat.search(rel) or pat.search(snippet):
            return True
    return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--all", action="store_true",
        help="scan all tracked files (default: only staged)",
    )
    parser.add_argument(
        "paths", nargs="*",
        help="explicit files to scan (overrides --all / staged)",
    )
    parser.add_argument(
        "--baseline", default=str(DEFAULT_BASELINE),
        help="file of regex allowlist entries (one per line; '#'-comments); "
        "a violation is suppressed if its path or snippet matches. "
        f"default: {DEFAULT_BASELINE} if present",
    )
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)

    if args.paths:
        files = args.paths
    elif args.all:
        files = _all_files()
    else:
        files = _staged_files()

    baseline = _load_baseline(Path(args.baseline))

    violations: list[tuple[str, int, str, str]] = []
    suppressed = 0
    for rel in files:
        path = Path(rel)
        if not path.is_file():
            continue
        data = path.read_bytes()
        if b"\x00" in data[:4096]:
            continue  # skip binary
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            continue
        for lineno, line in enumerate(text.splitlines(), 1):
            reason = scan_line(line)
            if not reason:
                continue
            snippet = line.strip()[:140]
            if _suppressed(rel, snippet, baseline):
                suppressed += 1
                continue
            violations.append((rel, lineno, reason, snippet))

    if not violations:
        print(
            f"scan_git_secrets: no violations across {len(files)} file(s)"
            + (f" ({suppressed} baseline-suppressed)" if suppressed else "")
        )
        return 0
    print(f"scan_git_secrets: {len(violations)} violation(s) -- BLOCKING")
    for rel, lineno, reason, snippet in violations:
        print(f"  {rel}:{lineno}: {reason}")
        print(f"    {snippet}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
