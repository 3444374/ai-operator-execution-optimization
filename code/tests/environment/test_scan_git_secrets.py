from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

_SCANNER = CODE_ROOT / "scripts" / "environment" / "scan_git_secrets.py"
_spec = importlib.util.spec_from_file_location("scan_git_secrets", _SCANNER)
scan = importlib.util.module_from_spec(_spec)
sys.modules["scan_git_secrets"] = scan
_spec.loader.exec_module(scan)

# Fixtures below are SHAPE-MATCHING FAKES (not real secrets). Detection keys
# on shape, so a fake matching the shape validates the rule without putting a
# real secret in the repo.

_FAKE_HF = "hf_fakefakefakefakefakefake"  # hf_ + >=20 alnum
_FAKE_SK = "sk-proj-fakefakefakefakefake2025"  # sk-proj- + >=20 [alnum_-]
_FAKE_SK_CLASSIC = "sk-fakefakefakefakefake12"  # sk- + >=20 alnum
_FAKE_GHP = "ghp_" + "a" * 36
_FAKE_PW = "FakePass12345"
_FAKE_HOST = "my-real-db-host.example.cloud"  # real TLD (.cloud), clearly fake


class ScanLineTests(unittest.TestCase):
    def test_private_key_blocked(self) -> None:
        self.assertIsNotNone(scan.scan_line('KEY="-----BEGIN RSA PRIVATE KEY-----"'))

    def test_hf_and_openai_tokens_blocked(self) -> None:
        self.assertIsNotNone(scan.scan_line(f"a {_FAKE_HF} b"))
        self.assertIsNotNone(scan.scan_line(_FAKE_SK))
        self.assertIsNotNone(scan.scan_line(_FAKE_SK_CLASSIC))

    def test_github_pat_blocked(self) -> None:
        self.assertIsNotNone(scan.scan_line(_FAKE_GHP))

    def test_sshpass_literal_blocked(self) -> None:
        self.assertIsNotNone(scan.scan_line(f"sshpass -p {_FAKE_PW} ssh root@h"))

    def test_sshpass_template_and_envvar_allowed(self) -> None:
        # Rule docs / env-var refs must NOT self-trigger.
        self.assertIsNone(scan.scan_line("sshpass -p <pw> ssh root@h"))
        self.assertIsNone(scan.scan_line("sshpass -p $PW ssh root@h"))

    def test_external_real_host_blocked(self) -> None:
        line = f"DATABASE_URL=postgresql://admin:s3cr3t@{_FAKE_HOST}:5432/db"
        self.assertIsNotNone(scan.scan_line(line))

    def test_external_ipv4_blocked(self) -> None:
        self.assertIsNotNone(scan.scan_line("url=postgres://u:pw@10.0.0.5:5432/db"))

    def test_localhost_any_credential_allowed(self) -> None:
        # localhost binds locally; not an external credential.
        self.assertIsNone(scan.scan_line("postgresql://postgres:postgres@localhost:5432/db"))
        self.assertIsNone(scan.scan_line("postgresql://alice:s3cret@127.0.0.1:5432/db"))

    def test_template_host_allowed(self) -> None:
        self.assertIsNone(scan.scan_line("ssh root@connect.<region>.seetacloud.com"))

    def test_example_and_fake_hosts_allowed(self) -> None:
        # example.com / db.host / 'host' are docs/test fixtures, not real targets.
        self.assertIsNone(scan.scan_line("postgresql://alice:s3cret@example.com:5432/db"))
        self.assertIsNone(scan.scan_line("postgresql://alice:s3cret@db.host:5432/x"))
        self.assertIsNone(scan.scan_line("scheme://user:password@host"))

    def test_short_fake_token_not_flagged(self) -> None:
        self.assertIsNone(scan.scan_line("key = hf_real_token"))
        self.assertIsNone(scan.scan_line("key = sk-fake1234"))


if __name__ == "__main__":
    unittest.main()
