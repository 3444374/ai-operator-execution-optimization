"""Command-line and connection-string secret redaction.

Centralizes the argument-list, database-URL, and free-text redaction used
wherever a raw command line, connection string, or exception message would
otherwise be persisted to evidence JSON (capability gates, runners,
provenance sidecars, failure reports).

* Secret-bearing flags (``--api-key``, ``--auth-token``, ``*secret*``,
  ``*password*``) are replaced with ``***``.
* URL-bearing flags (``--database-url``, ``--endpoint-url``, ``--metrics-url``,
  ...) and any value that parses as a URL with a password keep scheme /
  username / host / port / path but drop the password
  (``postgres:postgres@host`` -> ``postgres:***@host``).
* Free text (exception messages, tracebacks) is scrubbed of embedded
  ``scheme://user:password@host`` credentials and named secret assignments
  such as ``api_key=...`` via :func:`redact_text`.

Both ``--flag value`` and ``--flag=value`` forms are handled. Flag matching is
intentionally substring-based on the lower-cased flag stem so newly added
secret-shaped flags are caught without enumeration.
"""

from __future__ import annotations

import re
from urllib import parse

DEFAULT_SECRET_MARKERS: tuple[str, ...] = (
    "api-key",
    "auth-token",
    "secret",
    "password",
)

URL_FLAGS: frozenset[str] = frozenset(
    {
        "--database-url",
        "--endpoint-url",
        "--metrics-url",
        "--uploader-url",
        "--url",
    }
)

# Matches "scheme://user:password@" embedded anywhere in free text (exception
# messages, tracebacks) so a DSN echoed by psycopg/DuckDB cannot leak.
_CREDENTIAL_IN_URL = re.compile(r"(://[^\s:/@]+):[^\s/@]+@")
_QUOTED_NAMED_SECRET = re.compile(
    r"(?i)([\"']?(?:api[_-]?key|auth[_-]?token|access[_-]?token|"
    r"client[_-]?secret|secret|password)[\"']?\s*[:=]\s*)"
    r"([\"'])[^\"']*\2"
)
_UNQUOTED_NAMED_SECRET = re.compile(
    r"(?i)([\"']?(?:api[_-]?key|auth[_-]?token|access[_-]?token|"
    r"client[_-]?secret|secret|password)[\"']?\s*[:=]\s*)"
    r"(?![\"'])[^\s,;}]+"
)
_BEARER_SECRET = re.compile(
    r"(?i)([\"']?authorization[\"']?\s*[:=]\s*[\"']?bearer\s+)"
    r"[^\s,;}\]\"']+"
)
_CLI_SECRET = re.compile(
    r"(?i)(--(?:api-key|auth-token|access-token|client-secret|secret|password)"
    r"(?:=|\s+|[\"']\s*,\s*[\"']))[^\s,;}\]\"']+"
)
_KNOWN_TOKEN = re.compile(
    r"\b(?:hf_[A-Za-z0-9]{8,}|sk-[A-Za-z0-9_-]{8,}|"
    r"ghp_[A-Za-z0-9]{8,}|github_pat_[A-Za-z0-9_]{8,}|"
    r"xox[baprs]-[A-Za-z0-9-]{8,}|AIza[A-Za-z0-9_-]{8,})\b"
)


def redact_database_url(value: str) -> str:
    """Drop the password from a database URL, preserving every other component."""

    parsed = parse.urlsplit(value)
    if parsed.password is None:
        return value
    username = parse.quote(parsed.username or "", safe="")
    hostname = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port is not None else ""
    netloc = f"{username}:***@{hostname}{port}"
    return parse.urlunsplit(
        (parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment)
    )


def redact_text(value: str) -> str:
    """Scrub embedded ``scheme://user:password@host`` credentials from free text.

    Use on exception messages and tracebacks before persisting them to a
    failure report, so a DSN echoed by a database driver cannot leak the
    password. Returns the input unchanged when it is empty or has no match.
    """

    if not value:
        return value
    without_url_passwords = _CREDENTIAL_IN_URL.sub(r"\1:***@", value)
    without_cli = _CLI_SECRET.sub(r"\1***", without_url_passwords)
    without_quoted = _QUOTED_NAMED_SECRET.sub(
        r"\1\2***\2", without_cli
    )
    without_named = _UNQUOTED_NAMED_SECRET.sub(r"\1***", without_quoted)
    without_bearer = _BEARER_SECRET.sub(r"\1***", without_named)
    return _KNOWN_TOKEN.sub("***", without_bearer)


def _is_url_with_password(value: str) -> bool:
    try:
        parsed = parse.urlsplit(value)
    except ValueError:
        return False
    return bool(parsed.scheme) and parsed.password is not None


def redact_argument_list(
    values: list[str],
    *,
    secret_markers: tuple[str, ...] = DEFAULT_SECRET_MARKERS,
    url_flags: frozenset[str] = URL_FLAGS,
) -> list[str]:
    """Return a copy of ``values`` with secrets and URL passwords removed."""

    redacted: list[str] = []
    redact_next = False
    url_next = False
    for value in values:
        if redact_next:
            redacted.append("***")
            redact_next = False
            continue
        if url_next:
            redacted.append(redact_database_url(value))
            url_next = False
            continue
        is_flag = value.startswith("-")
        if is_flag:
            normalized = value.lower()
            flag = normalized.split("=", 1)[0]
            is_url_flag = flag in url_flags
            is_secret_flag = any(marker in flag for marker in secret_markers)
            if "=" in value and (is_url_flag or is_secret_flag):
                name, raw = value.split("=", 1)
                if is_secret_flag:
                    redacted.append(f"{name}=***")
                else:
                    redacted.append(f"{name}={redact_database_url(raw)}")
                continue
            if is_secret_flag:
                redacted.append(value)
                redact_next = True
                continue
            if is_url_flag:
                redacted.append(value)
                url_next = True
                continue
        # A positional value (or unhandled flag value) that is itself a URL
        # carrying a password.
        if _is_url_with_password(value):
            redacted.append(redact_database_url(value))
            continue
        redacted.append(value)
    return redacted
