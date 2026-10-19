"""Driver-owned stop and registered-process cleanup for one local Ray stage."""

import json
import os
import signal
import threading
import time
import uuid


class StageEngineCleanup:
    """Own only this stage's private directory and recorded PID/create-time pairs."""

    def __init__(self, control):
        self.control = control
        self._lock = threading.Lock()
        self.reports = []
        self.errors = []
        self._driver_reports = {}

    def request_stop(self):
        try:
            (self.control / "stop").touch(mode=0o600)
            return True
        except Exception as error:
            self.errors.append("stop_request: " + type(error).__name__)
            return False

    def _read(self):
        reports = []
        for path in self.control.glob("engine-*.json"):
            if path.stat().st_size > 8 * 1024**2:
                raise ValueError("cascade private engine report is oversized")
            value = json.loads(path.read_text())
            if type(value) is not dict:
                raise ValueError("cascade private engine report must be an object")
            key = (value.get("model_pid"), value.get("process_created"))
            reports.append(value if value.get("process_exit_confirmed") else self._driver_reports.get(key, value))
        return reports

    def stop(self, *, grace_s=5.5, started=True):
        self.request_stop()
        if not self._lock.acquire(timeout=grace_s + 2):
            return dict(reports=[], errors=["stage_engine_cleanup_still_running"], process_exit_confirmed=False)
        try:
            deadline = time.monotonic() + grace_s
            reports = self._read()
            # The stop file is visible during EngineProcess construction too.
            # Give its own bounded cleanup time to register and close its child
            # before the Ray actor can be terminated by the graph watchdog.
            while started and (not reports or any(not r.get("process_exit_confirmed") for r in reports)):
                if time.monotonic() >= deadline:
                    break
                time.sleep(min(0.02, max(0, deadline - time.monotonic())))
                reports = self._read()
            for index, report in enumerate(reports):
                if not report.get("process_exit_confirmed"):
                    reports[index] = self._stop_registered(report)
                    key = (report.get("model_pid"), report.get("process_created"))
                    self._driver_reports[key] = reports[index]
            self.reports = reports
            if started and not reports:
                self.errors.append("stage_model_process_registration_unconfirmed")
            for report in reports:
                self.errors.extend(report.get("cleanup_errors", []))
            self.errors = list(dict.fromkeys(self.errors))
            from src.baselines.common.private_artifacts import write_private_json
            temporary = self.control / ("driver-cleanup-" + uuid.uuid4().hex + ".tmp")
            write_private_json(temporary, dict(reports=reports, errors=self.errors))
            os.replace(temporary, self.control / "driver-cleanup.json")
            return dict(reports=reports, errors=list(self.errors), process_exit_confirmed=(
                (bool(reports) or not started) and all(r.get("process_exit_confirmed") is True for r in reports)))
        except BaseException as exc:
            self.errors.append("stage_engine_cleanup: " + type(exc).__name__)
            return dict(reports=list(self.reports), errors=list(self.errors), process_exit_confirmed=False)
        finally:
            self._lock.release()

    @staticmethod
    def _stop_registered(report):
        import psutil
        from src.experiments.lotus_records_supervisor import _live, _observe, _signal, _wait

        pid, created = report.get("model_pid"), report.get("process_created")
        if type(pid) is not int or pid <= 1 or type(created) not in (int, float) or created <= 0:
            raise ValueError("cascade engine report lacks a valid process identity")
        owned = {tuple(item) for item in report.get("owned_processes", [(pid, created)])}
        owned.add((pid, created))
        errors = ["engine_API_cleanup_unconfirmed_driver_stopped_registered_processes"]
        # A process-table restriction need not prevent stopping the already
        # registered session. PID reuse is still checked before any signal.
        try:
            leader = psutil.Process(pid)
            if leader.create_time() != created:
                raise RuntimeError("registered cascade process identity was reused")
        except psutil.NoSuchProcess:
            pass
        try:
            _observe(owned, pid, created)
        except (PermissionError, psutil.AccessDenied) as error:
            errors.append("process_tree_observation: " + type(error).__name__)
        for signum in (signal.SIGTERM, signal.SIGKILL):
            if not _live(owned):
                break
            _signal(owned, pid, created, signum)
            deadline = time.monotonic() + 0.5
            try:
                _wait(owned, pid, created, deadline)
            except (PermissionError, psutil.AccessDenied) as error:
                errors.append("process_tree_wait: " + type(error).__name__)
                while _live(owned) and time.monotonic() < deadline:
                    time.sleep(min(0.02, max(0, deadline - time.monotonic())))
        remaining = _live(owned)
        return dict(report, status="unknown", process_exit_confirmed=not remaining,
                    live_observed_processes=len(remaining), gpu_exit_confirmed=None,
                    process_exit_scope="observed_registered_processes",
                    unobserved_descendants_confirmed=False,
                    cleanup_errors=list(dict.fromkeys(errors)))
