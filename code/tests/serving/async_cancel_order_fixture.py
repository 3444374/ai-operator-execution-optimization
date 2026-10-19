"""CPU-only abort/shutdown witness for the real three-socket child protocol."""

import asyncio
import json
from pathlib import Path
import sys

from src.serving.backends.async_engine_process import serve
from tests.serving.async_engine_fixture import FixtureEngine


class OrderWitnessEngine(FixtureEngine):
    def __init__(self, spec, generation, *, limits):
        super().__init__(spec, generation, limits=limits)
        self.events_path = Path(spec.model_path) / "events.jsonl"

    def record(self, event, request_id=None):
        with self.events_path.open("a") as stream:
            stream.write(json.dumps(dict(event=event, request_id=request_id)) + "\n")

    async def generate_prepared(self, row, **kwargs):
        self.record("fixture_native_admitted", kwargs["request_id"])
        return await super().generate_prepared(row, **kwargs)

    async def abort(self, request_id):
        self.record("fixture_native_abort_requested", request_id)
        return await super().abort(request_id)

    async def close(self, timeout=10):
        self.record("fixture_shutdown_entered")
        return await super().close(timeout)


if __name__ == "__main__":
    asyncio.run(serve(*map(int, sys.argv[1:]), engine_factory=OrderWitnessEngine))
