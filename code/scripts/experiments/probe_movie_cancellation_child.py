"""Same async model child, with the existing native API sidecar for qualification."""
from functools import wraps
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def install(directory):
    from src.observability.native_model_api_trace import install as install_api, writer
    from src.serving.backends import vllm_engine as common
    from src.serving.backends.vllm_async_engine import VllmAsyncEngine
    from vllm.v1.engine.async_llm import AsyncLLM
    emit = writer(directory)
    install_api(AsyncLLM, common, emit)
    original = VllmAsyncEngine.generate_prepared
    @wraps(original)
    async def generate(self, row, *args, **kwargs):
        try:
            emit('qualification_request', request_id=kwargs['request_id'],
                 **{name: row[name] for name in ('call_id', 'query_id', 'row_id', 'source_position', 'call_index')})
        except Exception:
            pass  # Missing evidence rejects qualification, never changes model execution.
        return await original(self, row, *args, **kwargs)
    VllmAsyncEngine.generate_prepared = generate


if __name__ == '__main__':
    import asyncio
    from src.serving.backends.async_engine_worker import serve
    install(sys.argv[1])
    asyncio.run(serve(*map(int, sys.argv[2:])))
