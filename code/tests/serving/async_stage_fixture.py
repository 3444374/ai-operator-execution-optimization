"""Two real IPC children with scripted CPU phase durations; no model runtime."""
import asyncio
import json
import os
import sys
import time

from src.serving.backends.async_engine_worker import serve
from src.serving.backends.model_context import model_context_record
from src.serving.backends.vllm_protocol import _prepared, result_record


class Engine:
    def __init__(self, spec, generation, *, limits):
        self.spec = spec
        self.engine_uid = 'cpu-phase-' + str(os.getpid())
        self.gates = {}

    def ready(self):
        return dict(engine_uid=self.engine_uid,
                    model_context=model_context_record(self.spec, self.engine_uid, 2048),
                    physical_gpu=False, inference_calls=0)

    async def generate_prepared(self, row, *, request_id, generation, deadline_monotonic, kv_transfer_params):
        producer = kv_transfer_params.get('do_remote_decode') is True
        mode = json.loads(row['messages_json'])[0]['content']
        started = time.monotonic()
        if mode == 'hold':
            gate = self.gates[request_id] = asyncio.Event()
            await gate.wait()
        else:
            await asyncio.sleep(.15 if producer and mode == 'slow-prefill' else .005 if producer else .04)
        unknown = mode == 'hold' or producer and mode == 'unknown-prefill'
        identity = _prepared([row])[0][0]
        result = result_record(identity, engine_uid=self.engine_uid, start=started,
            end=time.monotonic(), status='unknown' if unknown else 'succeeded', submitted=True,
            error_type='FixtureUnknown' if unknown else None)
        result.update(text='discarded' if producer else 'answer-' + str(row['source_position']),
            engine_request_id=request_id, engine_terminal_confirmed=not unknown,
            prompt_tokens=2, completion_tokens=1 if producer else 3, total_tokens=3 if producer else 5,
            prompt_token_ids_json='[7, 8]', output_token_ids_json='[1]' if producer else '[1, 2, 3]',
            finish_reason='stop', observed_max_tokens=generation.max_tokens)
        if producer:
            result['kv_transfer_params'] = dict(do_remote_prefill=True, remote_engine_id=self.engine_uid,
                remote_request_id=request_id, remote_block_ids=list(range(10000)) if mode == 'large-kv' else [[1]],
                fixture_call_id=row['call_id'])
        else:
            if kv_transfer_params['fixture_call_id'] != row['call_id']:
                raise ValueError('fixture transfer belongs to a different logical call')
            result['kv_transfer_params'] = None
        self.gates.pop(request_id, None)
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
    asyncio.run(serve(*map(int, sys.argv[1:]), engine_factory=Engine))
