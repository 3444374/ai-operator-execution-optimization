"""Declared CPU responses over the original three-socket IPC; no model/GPU."""
import asyncio
import json
from pathlib import Path
import os
import sys
import time

from src.serving.backends.async_engine_process import serve
from src.serving.backends.vllm_async_engine import result_record
from src.serving.backends.vllm_engine import _prepared


class FixtureEngine:
    def __init__(self, spec, generation, *, limits):
        self.config = json.loads(Path(spec.model_path).read_text())
        self.log = Path(spec.model_path + ".events")
        self.engine_uid = "sections-cpu-" + str(os.getpid())
        self.gates = {}
        self.closed = False
        self.event("construct")

    def event(self, name, **values):
        with self.log.open("a") as stream:
            stream.write(json.dumps(dict(event=name, at=time.monotonic(), **values)) + "\n")

    def ready(self):
        return dict(engine_uid=self.engine_uid, physical_gpu=False)

    async def generate_prepared(self, row, *, request_id, generation, deadline_monotonic):
        identity = _prepared([row])[0][0]
        token = row["algorithm_stage"] + "|" + row["row_id"]
        response = self.config["responses"][token]
        started = time.monotonic()
        self.event("start", token=token, request_id=request_id)
        if response.get("wait_file"):
            gate = self.gates[request_id] = asyncio.Event()
            while not Path(response["wait_file"]).exists() and not gate.is_set():
                await asyncio.sleep(0.002)
        if token in self.config.get("release_on", []):
            for gate in self.gates.values():
                gate.set()
        if token in self.config.get("hold", []):
            gate = self.gates[request_id] = asyncio.Event()
            await gate.wait()
        if response.get("delay_s"):
            await asyncio.sleep(response["delay_s"])  # Explicit CPU mechanism fixture only.
        status = response.get("status", "succeeded")
        result = result_record(identity, engine_uid=self.engine_uid, start=started,
            end=time.monotonic(), status=status, submitted=True,
            error_type="CPUFixtureOutcome" if status != "succeeded" else None)
        result.update(text=response["text"], prompt_tokens=11, completion_tokens=3, total_tokens=14,
            finish_reason="abort" if status == "unknown" else "stop", engine_request_id=request_id,
            engine_terminal_confirmed=status != "unknown", logprobs_json="null",
            fixture_generation=generation.kwargs(), fixture_messages=row["messages_json"])
        self.gates.pop(request_id, None)
        self.event("end", token=token, status=status)
        return result

    async def abort(self, request_id):
        if request_id in self.gates:
            self.gates[request_id].set()
        self.event("abort", request_id=request_id)
        return dict(requested=True, engine_terminal_confirmed=False)

    async def close(self, timeout=10):
        for gate in self.gates.values():
            gate.set()
        if not self.closed:
            self.event("close")
        self.closed = True
        return dict(status="closed", engine_api_shutdown_confirmed=True, gpu_exit_confirmed=None)


if __name__ == "__main__":
    asyncio.run(serve(*map(int, sys.argv[1:]), engine_factory=FixtureEngine))
