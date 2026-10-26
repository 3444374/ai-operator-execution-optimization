"""Session lifetime for the existing IMLane private model transport.

This is compatibility code, not an upstream IMLane API. Each call still uses
the existing synchronous finite-batch engine; the author schedules every UDF.
"""

from pathlib import Path
import time

from integrations.imlane_records.model_process import IMLaneEngineProcess


class ResidentEngineProcess(IMLaneEngineProcess):
    def _reap_idle(self):
        while not self._reaper_stop.wait(.02):
            # Read the binding while holding the same lock as run_query. An
            # expired previous query must not close a newly rebound engine.
            with self._lock:
                if (self.control / "stop").exists() or time.monotonic() >= self.deadline:
                    self._broken = True
                    self.close()
                    return

    def run_query(self, rows, *, control, deadline):
        with self._lock:
            original = self.control, self.deadline
            self.control, self.deadline = Path(control), min(deadline, self.deadline)
            try:
                return super().run_prepared(rows)
            finally:
                self.control, self.deadline = original


class QueryModel:
    """Borrow a session model without granting a query ownership of close."""

    def __init__(self, model, control, deadline):
        self.model, self.control, self.deadline = model, control, deadline
        self.ready_report = model.ready_report
        self.process = getattr(model, "process", None)

    def run_prepared(self, rows):
        return self.model.run_query(rows, control=self.control, deadline=self.deadline)

    def close(self):
        raise RuntimeError("the resident session owns model close")
