"""Bounded direct HTTP diagnostic: same Map values/transport, no execution core."""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict
import json
from types import SimpleNamespace

from src.execution_provider.adapters.async_fixed_model import AsyncFixedModelTransport
from src.execution_provider.adapters.completion_response import decode_backend_completion
from src.execution_provider.adapters.model_config import MAX_MODEL_RESPONSE_BYTES
from src.execution_provider.semantic_map import canonical_messages, completion_status, MapCompletionStatus
from src.scheduling.core.session_contract import TaskKey
from src.experiments.request_identity import request_identity
from src.baselines.common.private_artifacts import content_digest


def request_body(plan, text):
    return dict(model=plan.model_id, messages=json.loads(canonical_messages(plan.instruction, text)),
                **plan.generation_constraints())


class DirectMap:
    """Own one reusable HTTP client; task set and completed-result set never exceed C."""
    def __init__(self, config, concurrency, plan, observer):
        if type(concurrency) is not int or concurrency < 1:
            raise ValueError('positive direct concurrency required')
        self.concurrency, self.plan, self.observer = concurrency, plan, observer
        self.transport = AsyncFixedModelTransport(config, concurrency, observer)

    async def close(self):
        await self.transport.close()

    async def _one(self, sequence, row, session):
        body = request_body(self.plan, row['input_text'])
        key = TaskKey(session, sequence)
        request = SimpleNamespace(key=key, task=SimpleNamespace(
            payload=json.dumps(body, ensure_ascii=False, separators=(',', ':')).encode(),
            max_result_bytes=MAX_MODEL_RESPONSE_BYTES))
        self.observer(dict(event='direct_input', key=asdict(key), row_id=row['source_example_id'],
                           request_values_sha256=content_digest(body)))
        token = request_identity.set(asdict(key))
        try:
            completion = decode_backend_completion(await self.transport.execute(request, 'model'))
        finally:
            request_identity.reset(token)
        if completion_status(self.plan, completion) != MapCompletionStatus.VALID:
            raise ValueError('direct completion violates Map policy')
        if row.get('input_tokens') is not None and completion.prompt_tokens != row['input_tokens']:
            raise ValueError('direct complete-message token usage differs')
        self.observer(dict(event='direct_completion', key=asdict(key), **asdict(completion)))
        return row['source_example_id'], completion.raw_output

    @asynccontextmanager
    async def rows(self, inputs, session):
        pending = set()
        iterator = iter(enumerate(inputs))

        async def results():
            exhausted = False
            while pending or not exhausted:
                while not exhausted and len(pending) < self.concurrency:
                    item = next(iterator, None)
                    if item is None:
                        exhausted = True
                    else:
                        pending.add(asyncio.create_task(self._one(*item, session)))
                if not pending:
                    break
                done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    result = task.result()
                    pending.remove(task)
                    yield result
        stream = results()
        try:
            yield stream
        finally:
            await stream.aclose()
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
