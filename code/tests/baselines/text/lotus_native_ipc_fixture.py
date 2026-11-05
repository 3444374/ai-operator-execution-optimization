"""CPU-only model child for the real serving IPC; no model/Ray/GPU imports."""
import asyncio
import json
import os
from pathlib import Path
import sys
import time

from src.serving.backends.async_engine_worker import serve
from src.serving.backends.vllm_protocol import _prepared, result_record


class FixtureEngine:
    def __init__(self, spec, generation, *, limits):
        self.spec = spec
        self.engine_uid = 'native-CPU-' + str(os.getpid())
        self.pending = {}

    def ready(self):
        from src.serving.backends.model_context import model_context_record
        return dict(engine_uid=self.engine_uid, physical_gpu=False, inference_calls=0,
            model_context=model_context_record(self.spec, self.engine_uid, self.spec.max_model_len or 32768))

    async def generate_prepared(self, row, *, request_id, generation, deadline_monotonic):
        identity = _prepared([row])[0][0]
        content = json.dumps(json.loads(row['messages_json']), ensure_ascii=False)
        started = time.monotonic()
        if 'slow' in content:
            await asyncio.sleep(.05)
        if 'hold' in content:
            gate = self.pending[request_id] = asyncio.Event()
            for message in json.loads(row['messages_json']):
                if isinstance(message['content'],str):
                    for line in message['content'].splitlines():
                        if line.startswith('fixture_start_path='):
                            Path(line.split('=',1)[1]).write_text(request_id)
            await gate.wait()
        unknown = 'unknown' in content
        failed = 'fail-model' in content
        positive = 'good' in content
        if 'duplicate' in content:
            positive = row['source_position'] % 2 == 0
        status = 'unknown' if unknown else 'failed' if failed else 'succeeded'
        result = result_record(identity, engine_uid=self.engine_uid, start=started,
            end=time.monotonic(), status=status, submitted=True,
            error_type='CPUUnknown' if unknown else 'CPUFailure' if failed else None)
        result.update(engine_request_id=request_id, engine_terminal_confirmed=not unknown,
            text='Answer: True' if positive else 'Answer: False', prompt_tokens=11,
            completion_tokens=2, total_tokens=13, finish_reason='abort' if unknown else 'stop',
            prompt_token_ids_json=json.dumps(list(range(11))), output_token_ids_json='[2,3]',
            logprobs_json='null', fixture_generation=generation.kwargs())
        if 'protocol-failure' in content:
            result['engine_protocol_error']='CPUProtocolFailure'
        self.pending.pop(request_id, None)
        return result

    async def abort(self, request_id):
        if request_id in self.pending:
            self.pending[request_id].set()
        return dict(requested=True, engine_terminal_confirmed=False)

    async def close(self, timeout=10):
        for gate in self.pending.values():
            gate.set()
        return dict(status='closed', engine_api_shutdown_confirmed=True, gpu_exit_confirmed=None)


if __name__ == '__main__':
    asyncio.run(serve(*map(int, sys.argv[1:]), engine_factory=FixtureEngine))
