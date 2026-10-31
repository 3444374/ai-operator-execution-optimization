"""Explicit CPU model replacement with known work counters, never hardware work."""
import asyncio
import sys

from src.serving.backends.async_engine_process import serve
from tests.serving.resource_feedback_fixture import FixtureEngine as BaseEngine


class FixtureEngine(BaseEngine):
    async def generate_prepared(self, row, **options):
        value = await super().generate_prepared(row, **options)
        # The CPU replacement does no prefix caching. These fixture counters
        # exercise computed-work plumbing; they are not measured GPU costs.
        value.update(prompt_cached_tokens=0, prompt_computed_tokens=value['prompt_tokens'])
        return value


if __name__ == '__main__':
    asyncio.run(serve(*map(int, sys.argv[1:]), engine_factory=FixtureEngine))
