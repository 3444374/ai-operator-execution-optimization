"""Best-effort cell evidence collection; failed queries never enter successful evaluation."""

from contextlib import contextmanager
import json
import time

from .map_query_recording import _error_details


class CellErrors:
    def __init__(self):
        self.first = None
        self.details = {}

    def record(self, phase, failure):
        if self.first is None:
            self.first = failure
        self.details.setdefault(phase, dict(_error_details(failure), observed_ns=time.monotonic_ns()))

    @contextmanager
    def capture(self, phase):
        try:
            yield
        except BaseException as failure:
            self.record(phase, failure)
            raise

    def attempt(self, phase, action):
        try:
            return action()
        except BaseException as failure:
            self.record(phase, failure)
            return None

    def raise_if_failed(self):
        if self.first is not None:
            raise self.first


def collect_cell_evidence(root, config):
    """Inventory existing artifacts even after partial execution or failed preparation."""
    report = {"queries": [], "observer": None, "resource_state": "unknown", "read_errors": {}}
    def read_json(path):
        try:
            return json.loads(path.read_text())
        except (OSError, ValueError) as failure:
            report["read_errors"][str(path.relative_to(root))] = _error_details(failure)
            return None
    for index in range(config.queries):
        path = root / f"q{index}" / "execution.json"
        fallback = root / f"q{index}" / "recording-failure.json"
        if fallback.exists():
            path = fallback
        if path.exists():
            record = read_json(path)
            report["queries"].append({"index": index, "execution": record,
                "producer_log_exists": (root / f"q{index}-producer.log").exists()})
    observer = root / "observer.json"
    if observer.exists():
        report["observer"] = read_json(observer)
    events = root / "events.jsonl"
    if events.exists():
        count, last_usage, malformed = 0, None, 0
        with events.open() as stream:
            for line in stream:
                try:
                    event = json.loads(line)
                except ValueError:
                    malformed += 1
                    continue
                count += event.get("event") == "request"
                if event.get("event") == "core_job_drained":
                    last_usage = event.get("usage")
        report.update(observed_request_events=count, malformed_event_lines=malformed,
                      last_drained_usage=last_usage)
        if last_usage is not None and not any(last_usage.values()):
            report["resource_state"] = "last_observed_job_drained"
    return report
