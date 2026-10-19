"""Private CPU worker for close timing; no model or external service is used."""

import os
import signal
import sys
import threading
import time

from src.baselines.text.frameworks.daft_engine_process import EngineProcess


class DelayedCloseEngineProcess(EngineProcess):
    mode = "delayed-close"

    def _command(self, engine, result_fd):
        return [sys.executable, "-m", __name__, str(result_fd), self.mode]


class HungCloseEngineProcess(DelayedCloseEngineProcess):
    mode = "hung-close"


class FailedCloseEngineProcess(DelayedCloseEngineProcess):
    mode = "failed-close"


class ExitedEngineProcess(DelayedCloseEngineProcess):
    mode = "exit-after-run"


class BrokenExitedEngineProcess(DelayedCloseEngineProcess):
    mode = "exit-after-failed-run"


class TruthyCloseEngineProcess(DelayedCloseEngineProcess):
    mode = "truthy-close"


class AcknowledgedIdleEngineProcess(DelayedCloseEngineProcess):
    mode = "acknowledged-idle"


if __name__ == "__main__":
    from src.baselines.text.frameworks.daft_engine_process import serve
    from src.serving.backends import vllm_engine

    mode = sys.argv[2]

    class FixedEngine:
        def __init__(self, *_):
            self.engine_uid = "cpu-close-fixture-" + str(os.getpid())

        def ready(self):
            return dict(engine_uid=self.engine_uid, pid=os.getpid(), fixture=True)

        def run_prepared(self, rows, **_):
            failed = mode in ("failed-close", "exit-after-failed-run")
            if mode in ("exit-after-run", "exit-after-failed-run"):
                def exit_without_shutdown():
                    time.sleep(0.15)
                    os._exit(0)
                threading.Thread(target=exit_without_shutdown, daemon=True).start()
            return [dict(row, status="failed" if failed else "succeeded",
                         text=None if failed else "True",
                         error_type="FixtureInferenceError" if failed else None,
                         prompt_tokens=7, completion_tokens=2, total_tokens=9,
                         engine_uid=self.engine_uid, submission_confirmed=True)
                    for row in rows]

        def close(self):
            started = time.monotonic()
            if mode == "delayed-close":
                time.sleep(3.2)
            elif mode == "hung-close":
                signal.signal(signal.SIGTERM, signal.SIG_IGN)
                time.sleep(30)
            failed = mode == "failed-close"
            return dict(status="unknown" if failed else "closed",
                        engine_api_shutdown_confirmed=1 if mode == "truthy-close" else not failed,
                        close_delay_s=time.monotonic() - started,
                        error_type="FixtureCloseError" if failed else None)

    vllm_engine.VllmEngine = FixedEngine
    serve(int(sys.argv[1]))
    if mode == "acknowledged-idle":
        time.sleep(30)
