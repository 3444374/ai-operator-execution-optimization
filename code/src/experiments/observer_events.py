"""Read bounded observer logs and their close receipts without loading runtimes."""
import hashlib
import json
from pathlib import Path


def read_observer_events(directory, *, max_events=1_000_000, event_names=None):
    """Read one private observer events directory; retain file/line evidence."""
    directory = Path(directory).resolve(strict=True)
    paths = sorted(directory.glob("events-*.jsonl"))
    events, issues, sources = [], [], []
    total = 0
    if not paths:
        issues.append("no_event_files")
    for path in paths:
        digest = hashlib.sha256()
        sequences, scopes, line_count = [], set(), 0
        with path.open("rb") as stream:
            for line_count, line in enumerate(stream, 1):
                total += 1
                if total > max_events:
                    raise ValueError("event count exceeds analysis limit")
                digest.update(line)
                try:
                    value = json.loads(line)
                    if not isinstance(value, dict):
                        raise ValueError("event is not an object")
                    scopes.add((value.get("run_id"), value.get("observer_session_id"), value.get("pid")))
                    sequences.append(value.get("sequence"))
                    if event_names is None or value.get("event") in event_names:
                        events.append(dict(value, _evidence=dict(file=path.name, line=line_count)))
                except (ValueError, TypeError):
                    issues.append(path.name + ":invalid_event")
        if len(scopes) != 1 or sequences != list(range(1, line_count + 1)):
            issues.append(path.name + ":identity_or_sequence_incomplete")
        status_path = path.with_suffix(".status.json")
        try:
            status_bytes = status_path.read_bytes()
            status = json.loads(status_bytes)
            buffer = status.get("feedback_buffer", {})
            valid = (status.get("closed") is True and status.get("flush_confirmed") is True
                     and status.get("events_written") == line_count
                     and status.get("dropped") == 0 and status.get("errors") == 0
                     and buffer.get("drain_complete") is True and buffer.get("closed") is True
                     and buffer.get("remaining") == 0 and buffer.get("dropped") == 0 and buffer.get("errors") == 0)
            if not valid:
                issues.append(path.name + ":sink_loss_or_unconfirmed_close")
            status_sha = hashlib.sha256(status_bytes).hexdigest()
        except (OSError, json.JSONDecodeError):
            issues.append(path.name + ":status_unavailable")
            status_sha = None
        sources.append(dict(file=path.name, sha256=digest.hexdigest(), lines=line_count, status_sha256=status_sha))
    for footer in directory.glob("events-*.status.json"):
        if not footer.with_name(footer.name.replace(".status.json", ".jsonl")).is_file():
            issues.append(footer.name + ":event_file_missing")
    return events, issues, sources
