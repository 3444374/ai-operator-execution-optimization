"""Actual CPU child and three sockets; deterministic fake inference only."""
import asyncio
import json
import os
import sys
import time

from src.serving.backends.async_engine_process import serve
from src.serving.backends.vllm_async_engine import result_record
from src.serving.backends.vllm_engine import _prepared
from src.modalities.text.tokenization import chat_token_count
from chain_tokenizer_fixture import tokenizer

class FixtureEngine:
    def __init__(self, spec, generation, *, limits):
        self.engine_uid = 'cpu-chain-' + str(os.getpid())
        self.tokenizer = tokenizer()
        self.gates = {}

    def ready(self):
        return dict(engine_uid=self.engine_uid, physical_gpu=False, inference_calls=0)

    async def generate_prepared(self, row, *, request_id, generation, deadline_monotonic):
        identities, messages = _prepared([row])
        content = row['messages_json']
        start = time.monotonic()
        if 'WAIT_' in content and generation.max_tokens == 8:
            gate = self.gates[request_id] = asyncio.Event()
            await gate.wait()
        unknown = 'OUTCOME_UNKNOWN' in content
        failed = 'CONFIRMED_FAILURE' in content
        now = time.monotonic()
        result = result_record(identities[0], engine_uid=self.engine_uid, start=start, end=now,
            status='unknown' if unknown else ('failed' if failed else 'succeeded'), submitted=True,
            error_type='CPUFixtureUnknown' if unknown else ('CPUFixtureFailure' if failed else None))
        n = chat_token_count(self.tokenizer, messages[0])
        # This generation split belongs only to the fixture, never production.
        text = ('False' if 'STOP_ME' in content else 'True') if generation.max_tokens == 8 else 'rewritten'
        result.update(text=text, prompt_tokens=n, completion_tokens=1, total_tokens=n+1,
            prompt_token_ids_json=json.dumps([0]*n), output_token_ids_json='[0]',
            engine_request_id=request_id, engine_terminal_confirmed=not unknown,
            finish_reason='stop', fixture_generation=generation.kwargs())
        self.gates.pop(request_id,None)
        return result

    async def abort(self, request_id):
        if request_id in self.gates:
            self.gates[request_id].set()
        return dict(requested=True, engine_terminal_confirmed=False)

    async def close(self, timeout=10):
        for gate in self.gates.values():
            gate.set()
        return dict(status='closed', engine_api_shutdown_confirmed=True, gpu_exit_confirmed=None)

if __name__ == '__main__':
    asyncio.run(serve(*map(int,sys.argv[1:]),engine_factory=FixtureEngine))
