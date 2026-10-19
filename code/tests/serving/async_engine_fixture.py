"""CPU child only: explicit fixed results, no Ray, vLLM, weights or GPU."""

import asyncio
import json
import os
import sys
import time

from src.serving.backends.async_engine_process import serve
from src.serving.backends.vllm_async_engine import result_record
from src.serving.backends.vllm_engine import _prepared


class FixtureEngine:
    def __init__(self, spec, generation, *, limits):
        self.engine_uid = "cpu-fixture-" + str(os.getpid())
        self.gates = {}
        self.stalled_aborts = set()
        self.closed = False

    def ready(self):
        return dict(engine_uid=self.engine_uid, physical_gpu=False, inference_calls=0)

    async def generate_prepared(self, row, *, request_id, generation, deadline_monotonic):
        text = json.loads(row["messages_json"])[0]["content"]
        identity = _prepared([row])[0][0]
        if text == "die":
            os._exit(7)
        start = time.monotonic()
        if text.startswith("hold"):
            gate = self.gates[request_id] = asyncio.Event()
            if text == "hold-stall-abort":
                self.stalled_aborts.add(request_id)
            await gate.wait()
        if text == "slow":
            await asyncio.sleep(0.05)
        unknown = text in ("unknown", "hold-unknown")
        result = result_record(identity, engine_uid=self.engine_uid, start=start,
            end=time.monotonic(), status="unknown" if unknown else "succeeded",
            submitted=True)
        result.update(text="x" * 8192 if text == "large" else "fixed fixture text",
            prompt_tokens=11, completion_tokens=1, total_tokens=12,
            prompt_token_ids_json=json.dumps(list(range(11))), output_token_ids_json="[3]",
            finish_reason="abort" if unknown else "stop", engine_request_id=request_id,
            engine_terminal_confirmed=not unknown)
        if text == "wrong-call":
            result["call_id"] = "not-the-accepted-call"
        if text == "bad-shape":
            result["fixture_nonserializable"] = object()
        self.gates.pop(request_id, None)
        return result

    async def abort(self, request_id):
        if request_id in self.stalled_aborts:
            await asyncio.Event().wait()
        gate = self.gates.get(request_id)
        if gate is not None:
            gate.set()
        return dict(requested=True, engine_terminal_confirmed=False)

    async def close(self, timeout=10):
        self.closed = True
        for gate in self.gates.values():
            gate.set()
        return dict(status="closed", engine_api_shutdown_confirmed=True, gpu_exit_confirmed=None)


if __name__ == "__main__":
    asyncio.run(serve(*map(int, sys.argv[1:]), engine_factory=FixtureEngine))
