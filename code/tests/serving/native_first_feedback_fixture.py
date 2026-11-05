"""CPU source publishes a real fixture stat while its first generation is held."""
import asyncio
import sys

from src.serving.backends.async_engine_process import serve
from src.serving.backends.vllm_async_engine import NativeStats
from tests.serving.async_engine_fixture import FixtureEngine as BaseEngine
from tests.serving.resource_feedback_fixture import FixtureEngine as FeedbackEngine


class FirstFeedbackEngine(FeedbackEngine):
    def __init__(self, spec, generation, *, limits):
        BaseEngine.__init__(self, spec, generation, limits=limits)
        self.stats = NativeStats(self.engine_uid)
        self.running = 0
        self.pressure = False
        self.first_feedback = asyncio.Event()
        if spec.model_path.endswith('/initial'):
            self.emit()

    def emit(self, **options):
        super().emit(**options)
        self.first_feedback.set()

    def feedback_snapshot(self):
        value = self.stats.snapshot()
        return None if value is None else dict(value, gated_requests=len(self.gates))

    async def wait_for_first_feedback(self):
        if self.spec.model_path.endswith('/notify-error'):
            raise OSError('fixture optional source notification unavailable')
        await self.first_feedback.wait()

    async def generate_prepared(self, row, **options):
        task = asyncio.create_task(BaseEngine.generate_prepared(self, row, **options))
        # Establish the fixture's hold/abort gate before announcing execution.
        await asyncio.sleep(0)
        self.running += 1
        self.emit()
        try:
            return await task
        finally:
            self.running -= 1
            self.emit(finished=True)


if __name__ == '__main__':
    asyncio.run(serve(*map(int, sys.argv[1:]), engine_factory=FirstFeedbackEngine))
