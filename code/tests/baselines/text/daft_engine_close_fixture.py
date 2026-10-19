"""Private CPU worker for close timing; no model or external service is used."""

import os
import signal
import sys
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
            return [dict(row, status="failed" if mode == "failed-close" else "succeeded",
                         text=None if mode == "failed-close" else "True",
                         error_type="FixtureInferenceError" if mode == "failed-close" else None,
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
                        engine_api_shutdown_confirmed=not failed,
                        close_delay_s=time.monotonic() - started,
                        error_type="FixtureCloseError" if failed else None)

    vllm_engine.VllmEngine = FixedEngine
    serve(int(sys.argv[1]))
