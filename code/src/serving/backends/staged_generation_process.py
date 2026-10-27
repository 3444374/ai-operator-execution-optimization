"""Per-call generation over the existing private engine process transport.

The caller owns this process. Pipe serialization, startup and shutdown remain
part of that caller's cost; this does not introduce a shared model pool.
"""

from __future__ import annotations

from dataclasses import asdict
import json
import os
from pathlib import Path
import sys
import time

from .vllm_sync_process import EngineProcess, MAX_MESSAGE_BYTES, _encode
from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, _prepared


class ChainEngineProcess(EngineProcess):
    """Reuse finite IPC and process-tree ownership, with stage-local generation."""

    @staticmethod
    def _command(engine, result_fd):
        return [engine.model_python, "-m", __name__, "--serve", str(result_fd)]

    def _check(self, deadline, *, stopping=True):
        if stopping and (Path(self.control) / "stop").exists():
            raise InterruptedError("Ray LOTUS chain cancelled")
        if time.monotonic() >= deadline:
            raise TimeoutError("Ray LOTUS chain engine deadline expired")

    def run_prepared(self, rows, *, generation):
        checked = GenerationSpec.from_dict(asdict(generation))
        identities, _ = _prepared(rows)
        with self._lock:
            try:
                if self._broken or self._closed:
                    raise RuntimeError("private engine cannot be reused after stop or close")
                response = self._exchange(dict(op="run", rows=rows, generation=asdict(checked),
                                               deadline=self.deadline), self.deadline)
                if response.get("kind") != "results" or type(response.get("rows")) is not list:
                    raise RuntimeError("private engine call failed: " + str(response.get("error_type")))
                # The method owns association. Preserve every raw response,
                # including valid usage beside malformed/duplicate identities.
                return response["rows"]
            except Exception as exc:
                self._broken = True
                try:
                    self.close()
                except BaseException as cleanup:
                    exc.add_note("private engine cleanup also failed: " + type(cleanup).__name__)
                return [dict(item, status="unknown", submission_confirmed=False,
                             error_type=type(exc).__name__, prompt_tokens=None,
                             completion_tokens=None, total_tokens=None,
                             engine_uid=(self.ready_report or {}).get("engine_uid"))
                        for item in identities]


def serve(result_fd):
    """Execute in model_python, without importing LOTUS or Ray."""
    from src.serving.backends.vllm_engine import VllmEngine

    engine = None
    with os.fdopen(result_fd, "wb", buffering=0) as output:
        def send(value):
            output.write(_encode(value))

        try:
            for raw in iter(lambda: sys.stdin.buffer.readline(MAX_MESSAGE_BYTES + 1), b""):
                if len(raw) > MAX_MESSAGE_BYTES or not raw.endswith(b"\n"):
                    raise ValueError("engine IPC request exceeds its byte limit")
                value = json.loads(raw)
                if engine is None and value["op"] == "open":
                    engine = VllmEngine(EngineSpec.from_dict(value["engine"]),
                                        GenerationSpec.from_dict(value["generation"]))
                    send(dict(kind="ready", **engine.ready()))
                elif engine is not None and value["op"] == "run":
                    send(dict(kind="results", rows=engine.run_prepared(
                        value["rows"],
                        generation=GenerationSpec.from_dict(value["generation"]),
                        deadline_monotonic=value["deadline"])))
                elif engine is not None and value["op"] == "close":
                    report = engine.close()
                    send(dict(kind="closed", report=report))
                    engine = None
                    return
                else:
                    raise ValueError("unexpected engine IPC operation")
        except Exception as exc:
            send(dict(kind="error", error_type=type(exc).__name__))
        finally:
            if engine is not None:
                engine.close()


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "--serve":
        raise SystemExit("private engine worker requires an inherited result descriptor")
    serve(int(sys.argv[2]))
