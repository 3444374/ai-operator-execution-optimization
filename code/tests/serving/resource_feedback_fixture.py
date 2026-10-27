"""Native-stat-shaped CPU child; explicit deterministic model replacement."""
import asyncio
from types import SimpleNamespace
import sys
from src.serving.backends.async_engine_process import serve
from src.serving.backends.vllm_async_engine import NativeStats
from tests.serving.async_engine_fixture import FixtureEngine as BaseEngine


class FixtureEngine(BaseEngine):
    def __init__(self, spec, generation, *, limits):
        super().__init__(spec, generation, limits=limits)
        self.stats = NativeStats(self.engine_uid)
        self.running = 0
        self.pressure = False
        self.emit()

    def emit(self, *, finished=False):
        self.stats.record(SimpleNamespace(num_running_reqs=self.running,
            num_waiting_reqs=0, kv_cache_usage=.96 if self.pressure else .1),
            SimpleNamespace(num_preempted_reqs=0, num_generation_tokens=int(finished),
                finished_requests=[SimpleNamespace(finish_reason='stop')] if finished else []))

    def feedback_snapshot(self):
        return dict(self.stats.snapshot(), gated_requests=len(self.gates))

    async def generate_prepared(self, row, **options):
        self.running += 1
        self.emit()
        try:
            await asyncio.sleep(.025)
            inner = asyncio.create_task(super().generate_prepared(row, **options))
            await asyncio.sleep(0)
            self.emit()
            value = await inner
            value['text'] = 'True' if row['source_position'] % 2 == 0 else 'False'
            return value
        finally:
            self.running -= 1
            self.emit(finished=True)


if __name__ == '__main__':
    asyncio.run(serve(*map(int, sys.argv[1:]), engine_factory=FixtureEngine))
