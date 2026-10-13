"""Fixed LOTUS single-Map work callback with real SDK/Core, memory replies only."""

from dataclasses import asdict
from importlib.util import find_spec
from importlib.metadata import version
import hashlib
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from src.baselines.common.redact import redact_json_values
from src.execution_provider.adapters.full_response import FullModelResponse, encode_full_response
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.adapters.native_tasks import build_native_execution
from src.planning.work import StageWork, WorkDescriptor
from src.scheduling.core.session_contract import Usage
from src.semantic_methods.lotus.batch import LotusBatchExecutor, lotus_executor


PINNED = all(find_spec(name) is not None for name in ('lotus', 'litellm', 'openai', 'httpx', 'pandas'))


@unittest.skipUnless(PINNED, 'fixed LOTUS SDK is unavailable; no install or network performed')
class ActualWorkSDKTests(unittest.TestCase):
    def test_default_and_declared_text_work_keep_complete_single_map_and_release(self):
        report = dict(scope='actual SDK + local Core, one Map, in-memory backend replies',
                      network_posts=0, model_calls=0, gpu_calls=0, runs=[])
        try:
            with patch.dict(os.environ, dict(LITELLM_LOCAL_MODEL_COST_MAP='True', HF_HUB_OFFLINE='1',
                                            TRANSFORMERS_OFFLINE='1', CUDA_VISIBLE_DEVICES='')), \
                 patch('socket.socket.connect', side_effect=AssertionError('SDK fixture attempted network')):
                import lotus
                import pandas as pd
                from lotus.cache import InMemoryCache
                from lotus.models import LM
                from src.semantic_methods.lotus.sdk import validate_source, SOURCE_HASHES, LITELLM_SOURCE_HASHES
                validate_source()
                report.update(versions={p: version(p) for p in ('lotus-ai', 'litellm', 'openai', 'httpx', 'pandas')},
                              lotus_source_sha256=SOURCE_HASHES, litellm_source_sha256=LITELLM_SOURCE_HASHES)
                for mode in ('default', 'declared_text_work'):
                    calls, descriptions = [], []
                    def describe(index, call):
                        body = json.loads(call.payload)
                        cap = body.get('max_completion_tokens', body.get('max_tokens'))
                        if type(cap) is not int or cap != 16:
                            raise ValueError('fixture requires its explicit effective output cap')
                        content = [m['content'] for m in body['messages']]
                        if any(type(value) is not str for value in content):
                            raise ValueError('this fixture describes text only')
                        byte_count = sum(len(value.encode('utf-8')) for value in content)
                        lower = (byte_count + 127) // 128
                        config = {key: value for key, value in body.items() if key != 'messages'}
                        signature = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
                        work = WorkDescriptor((StageWork('model', lower + cap, 'work_units'),), 'model',
                            'fixture-byte128-output-cap:' + signature,
                            lower_primary_units=lower, upper_primary_units=lower + cap)
                        descriptions.append(dict(index=index, utf8_bytes=byte_count,
                            effective_output_cap=cap, work=asdict(work),
                            payload_sha256=hashlib.sha256(call.payload).hexdigest()))
                        return work
                    async def execute(request, endpoint):
                        calls.append(dict(sequence=request.key.sequence, payload=request.task.payload.decode(),
                                          estimated_work=request.task.estimated_work,
                                          info=asdict(request.task.info)))
                        response = dict(id='fixture', object='chat.completion', created=1, model='lotus-fixture',
                            choices=[dict(index=0, message=dict(role='assistant', content='reply' + str(request.key.sequence)),
                                          finish_reason='stop')],
                            usage=dict(prompt_tokens=8, completion_tokens=2, total_tokens=10))
                        return encode_full_response(FullModelResponse(200, (), json.dumps(response).encode()))
                    config = FixedModelConfig('http://localhost/v1/chat/completions', 'lotus-fixture', 5000, 'fixture')
                    execution = build_native_execution(config, physical=None, execute=execute,
                        max_tasks=2, max_active_requests=2, active_work=128)
                    lm = LM('openai/lotus-fixture', api_base='http://localhost/v1', api_key='fixture',
                        max_batch_size=2, temperature=0, top_p=1, max_tokens=16, n=1,
                        num_retries=0, max_retries=0, timeout=5, cache=InMemoryCache(max_size=8))
                    executor = LotusBatchExecutor(execution, config, query_id='fixture-map', operator_id='map',
                        max_batch_rows=4, prevalidate_batch=True, retain_full_responses=False,
                        timeout_s=20, describe_work=describe if mode == 'declared_text_work' else None)
                    frame = pd.DataFrame(dict(row_id=[0, 1, 2, 3], text=['short', 'x'*1280, '中'*200, 'tail']))
                    primary = None
                    try:
                        with lotus.settings.context(lm=lm, enable_cache=False), lotus_executor(lm, executor):
                            result = frame.sem_map('Return a fixture reply for {text}.', suffix='_reply')
                        self.assertEqual(result['row_id'].tolist(), [0, 1, 2, 3])
                        self.assertEqual(result['_reply'].tolist(), ['reply0', 'reply1', 'reply2', 'reply3'])
                        self.assertEqual(execution.engine.capacity.usage(), Usage())
                        self.assertFalse(execution.engine.jobs.jobs)
                        self.assertEqual(executor.last_cleanup_errors, ())
                        self.assertEqual(sorted(c['sequence'] for c in calls), [0, 1, 2, 3])
                        if mode == 'default':
                            self.assertEqual([c['estimated_work'] for c in calls], [1]*4)
                        else:
                            self.assertEqual([d['index'] for d in descriptions], [0, 1, 2, 3])
                            self.assertGreater(len({d['work']['stages'][0]['units'] for d in descriptions}), 1)
                            for call in calls:
                                description = descriptions[call['sequence']]
                                self.assertEqual(call['info']['work'], description['work'])
                                self.assertEqual(call['estimated_work'], description['work']['stages'][0]['units'])
                                self.assertEqual(hashlib.sha256(call['payload'].encode()).hexdigest(), description['payload_sha256'])
                        report['runs'].append(dict(mode=mode, calls=calls, descriptions=descriptions,
                            rows=result[['row_id', '_reply']].to_dict('records'),
                            final_usage=asdict(execution.engine.capacity.usage()), jobs=0))
                    except BaseException as error:
                        primary = error
                        raise
                    finally:
                        closed = execution.close()
                        if not closed:
                            if primary is None:
                                self.fail('fixture execution cleanup unconfirmed')
                            primary.add_note('fixture execution cleanup also unconfirmed')
                self.assertEqual([c['payload'] for c in sorted(report['runs'][0]['calls'], key=lambda c:c['sequence'])],
                                 [c['payload'] for c in sorted(report['runs'][1]['calls'], key=lambda c:c['sequence'])])
                self.assertEqual(report['runs'][0]['rows'], report['runs'][1]['rows'])
                report['status'] = 'passed'
        except BaseException as error:
            report.update(status='failed', error_type=type(error).__name__, error=str(error))
            raise
        finally:
            output = os.environ.get('SEMLOOM_WORK_SDK_OUTPUT')
            if output:
                with Path(output).open('x', encoding='utf-8') as stream:
                    json.dump(redact_json_values(report), stream, ensure_ascii=False, indent=2)


if __name__ == '__main__':
    unittest.main()
