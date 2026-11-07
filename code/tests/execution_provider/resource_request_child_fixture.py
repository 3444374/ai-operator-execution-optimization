"""CPU IPC model replacement emulating the selected backend's ready/source API."""
import asyncio
import sys
from types import SimpleNamespace

from src.serving.backends.async_engine_worker import serve
from src.serving.backends.vllm_async_engine import NativeStats
from tests.execution_provider.section_child_fixture import FixtureEngine as BaseEngine


class FixtureEngine(BaseEngine):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stats = NativeStats(self.engine_uid)
        self.running = 0
        self.emit()

    def ready(self):
        # Explicit mock capability, never an assertion that this child is vLLM.
        result = super().ready()
        result.update(batch_mode='independent_final_results', closed=self.closed,
            resource_owner=dict(schema='semloom.model_resource_owner.v1', engine_uid=self.engine_uid,
                request_memory_owner='vllm', native_feedback_clock='private_child_and_model_actor.monotonic'))
        return result

    def emit(self, *, finished=False):
        self.stats.record(SimpleNamespace(num_running_reqs=self.running,
            num_waiting_reqs=0, kv_cache_usage=.96),
            SimpleNamespace(num_preempted_reqs=0, num_generation_tokens=int(finished),
                finished_requests=[SimpleNamespace(finish_reason='stop')] if finished else []))

    def feedback_snapshot(self):
        return self.stats.snapshot()

    async def generate_prepared(self, row, **options):
        self.running += 1
        self.emit()
        try:
            value = await super().generate_prepared(row, **options)
            value.update(prompt_computed_tokens=value['prompt_tokens'], prompt_cached_tokens=0)
            return value
        finally:
            self.running -= 1
            self.emit(finished=True)


if __name__ == '__main__':
    asyncio.run(serve(*map(int, sys.argv[1:]), engine_factory=FixtureEngine))
