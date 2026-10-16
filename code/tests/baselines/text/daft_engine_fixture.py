"""Explicit CPU response subprocess; never imports or constructs a real model."""

import os
import sys
import time

from src.baselines.text.frameworks.daft_engine_process import EngineProcess


class FixedEngineProcess(EngineProcess):
    mode = "normal"

    def _command(self, engine, result_fd):
        return [sys.executable, "-m", __name__, str(result_fd), self.mode]


class StallEngineProcess(FixedEngineProcess):
    mode = "stall"


class FailCloseEngineProcess(FixedEngineProcess):
    mode = "fail-close"


class CrashEngineProcess(FixedEngineProcess):
    mode = "crash"


class StartupStallEngineProcess(FixedEngineProcess):
    mode = "startup-stall"


if __name__ == "__main__":
    from src.baselines.text.frameworks.daft_engine_process import serve
    from src.serving.backends import vllm_engine
    from tests.experiments.test_lotus_records_libraries import FixedResponses

    mode = sys.argv[2]

    class FixedEngine:
        def __init__(self, *_):
            if mode == "startup-stall":
                time.sleep(30)

        def ready(self):
            return dict(engine_uid="cpu-fixture-" + str(os.getpid()), pid=os.getpid(),
                        fixture=True, inference_calls=0)

        def run_prepared(self, rows, **_):
            import pyarrow as pa

            if mode == "stall":
                time.sleep(30)
            if mode == "crash":
                os._exit(17)
            result = FixedResponses()(pa.Table.from_pylist(rows)).to_pylist()
            for row in result:
                row["engine_uid"] = "cpu-fixture-" + str(os.getpid())
                row["model_fixture_pid"] = os.getpid()
            return result

        def close(self):
            if mode == "fail-close":
                return dict(status="unknown", engine_api_shutdown_confirmed=False,
                            error_type="FixtureCloseError")
            return dict(status="closed", engine_api_shutdown_confirmed=True)

    vllm_engine.VllmEngine = FixedEngine
    serve(int(sys.argv[1]))
