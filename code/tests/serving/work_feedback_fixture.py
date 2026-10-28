"""Explicit CPU work-counter fixture over the existing real IPC child."""
import asyncio
import sys

from src.serving.backends.async_engine_process import serve
from tests.serving.resource_feedback_fixture import FixtureEngine as BaseEngine


class FixtureEngine(BaseEngine):
    async def generate_prepared(self, row, **options):
        value = await super().generate_prepared(row, **options)
        value.update(prompt_cached_tokens=6, prompt_computed_tokens=5,
                     input_work_source="declared_cpu_fixture")
        return value


if __name__ == "__main__":
    asyncio.run(serve(*map(int, sys.argv[1:]), engine_factory=FixtureEngine))
