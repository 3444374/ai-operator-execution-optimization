from __future__ import annotations

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

from src.baselines.common.redact import (  # noqa: E402
    redact_argument_list,
    redact_database_url,
    redact_text,
)


class RedactDatabaseUrlTests(unittest.TestCase):
    def test_strips_password_keeps_host_port(self) -> None:
        out = redact_database_url("postgresql://postgres:postgres@localhost:5432/ai_operator")
        self.assertNotIn("postgres:postgres@", out)
        self.assertIn("postgres:***@localhost:5432", out)
        self.assertIn("/ai_operator", out)

    def test_no_password_returned_as_is(self) -> None:
        url = "postgresql://localhost:5432/ai_operator"
        self.assertEqual(redact_database_url(url), url)

    def test_keeps_username_when_password_present(self) -> None:
        out = redact_database_url("postgresql://alice:s3cret@db.host:5432/x")
        self.assertIn("alice:***@db.host:5432", out)
        self.assertNotIn("s3cret", out)


class RedactArgumentListTests(unittest.TestCase):
    def test_database_url_split_form_redacts_next_value(self) -> None:
        out = redact_argument_list(
            ["python", "gate.py", "--database-url", "postgresql://u:p@h:5432/db", "--mode", "full"]
        )
        idx = out.index("--database-url")
        self.assertEqual(out[idx + 1], "postgresql://u:***@h:5432/db")

    def test_database_url_equals_form(self) -> None:
        out = redact_argument_list(["--database-url=postgresql://u:p@h:5432/db"])
        self.assertEqual(out[0], "--database-url=postgresql://u:***@h:5432/db")

    def test_api_key_split_and_equals(self) -> None:
        self.assertEqual(
            redact_argument_list(["--api-key", "hf_real_token"]),
            ["--api-key", "***"],
        )
        self.assertEqual(
            redact_argument_list(["--api-key=hf_real_token"]),
            ["--api-key=***"],
        )

    def test_password_and_secret_markers(self) -> None:
        for flag in ("--db-password", "--auth-token", "--client-secret"):
            self.assertEqual(
                redact_argument_list([flag, "v"]),
                [flag, "***"],
                msg=flag,
            )

    def test_non_secret_args_preserved(self) -> None:
        out = redact_argument_list(["--mode", "sampled", "--sample-count", "256"])
        self.assertEqual(out, ["--mode", "sampled", "--sample-count", "256"])

    def test_endpoint_and_metrics_url_flags_redacted(self) -> None:
        out = redact_argument_list(
            ["--endpoint-url", "http://u:p@host:8000/v1/chat/completions",
             "--metrics-url=http://u:p@host:8000/metrics"]
        )
        self.assertNotIn(":p@", " ".join(out))
        self.assertIn("u:***@", out[1])
        self.assertIn("u:***@", out[2])

    def test_bare_url_value_with_password_redacted(self) -> None:
        out = redact_argument_list(["positional", "postgresql://u:secret@h:5432/db"])
        self.assertIn("u:***@", out[1])
        self.assertNotIn("secret", out[1])


class RedactTextTests(unittest.TestCase):
    def test_scrubs_embedded_dsn_in_exception_text(self) -> None:
        msg = "connection failed: postgresql://postgres:postgres@localhost:5432/ai_operator"
        scrubbed = redact_text(msg)
        self.assertNotIn("postgres:postgres@", scrubbed)
        self.assertIn("postgres:***@localhost:5432", scrubbed)

    def test_no_credentials_returned_as_is(self) -> None:
        self.assertEqual(redact_text("some benign error"), "some benign error")

    def test_empty(self) -> None:
        self.assertEqual(redact_text(""), "")


if __name__ == "__main__":
    unittest.main()
