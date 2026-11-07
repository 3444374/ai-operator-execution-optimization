"""Execute selected pinned upstream methods with a CPU renderer and engine seam."""
import ast
from collections.abc import Sequence
from copy import deepcopy
from dataclasses import asdict
import hashlib
import itertools
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from src.baselines.text.frameworks.sync_reference_engine import ReferenceVllmEngine
from src.serving.backends import vllm_engine as sync
from tests.serving.test_sync_model_context import native_api

SOURCE_HASHES = {
    'vllm/entrypoints/llm.py': '3a7b7cf9c0e5f0920d1c7f06475809280848d247c5044933a75dbde2fd4cff55',
    'vllm/entrypoints/offline_utils.py': '688fbad0af9c2180b83aa77dcd0dbda85ca076a6c72bffa61840896d950cf458',
}


def check_native_pipeline(source_root):
    folder = Path(source_root)
    sources = {
        'vllm/entrypoints/offline_utils.py': ('OfflineInferenceMixin', (
            '_preprocess_chat', '_preprocess_chat_one', '_params_to_seq', '_lora_request_to_seq',
            '_priority_to_seq', '_run_chat', '_add_chat_requests', '_render_and_add_requests', '_add_request')),
        'vllm/entrypoints/llm.py': ('LLM', ('chat', 'wait_for_completion')),
    }

    class Sampling(SimpleNamespace):
        pass

    class RequestOutput(SimpleNamespace):
        pass

    class PoolingOutput(SimpleNamespace):
        pass

    scope = dict(Sequence=Sequence, ChatParams=SimpleNamespace, SamplingParams=Sampling,
        PoolingParams=type('PoolingParams', (), {}), RequestOutput=RequestOutput,
        PoolingRequestOutput=PoolingOutput, RequestOutputKind=SimpleNamespace(FINAL_ONLY='final_only'),
        is_mistral_tokenizer=lambda value: False,
        merge_kwargs=lambda first, second: dict(first or {}, **second),
        conversation_to_seq=lambda messages: messages,
        maybe_tqdm=lambda values, **options: values)
    methods, identities = {}, {}
    for filename, (class_name, names) in sources.items():
        raw = (folder / filename).read_bytes()
        if hashlib.sha256(raw).hexdigest() != SOURCE_HASHES[filename]:
            raise ValueError('fixed vLLM source differs: ' + filename)
        source = raw.decode('utf-8')
        root = ast.parse(source)
        cls = next(item for item in root.body if isinstance(item, ast.ClassDef) and item.name == class_name)
        records = {}
        for name in names:
            candidates = [item for item in cls.body if isinstance(item, ast.FunctionDef) and item.name == name]
            method = deepcopy(max(candidates, key=lambda item: item.end_lineno - item.lineno))
            records[name] = dict(line=method.lineno, end_line=method.end_lineno,
                ast_sha256=hashlib.sha256(ast.dump(method, include_attributes=False).encode()).hexdigest())
            method.decorator_list = []
            module = ast.Module(body=[ast.ImportFrom(module='__future__',
                names=[ast.alias(name='annotations')], level=0), method], type_ignores=[])
            ast.fix_missing_locations(module)
            exec(compile(module, filename, 'exec'), scope)
            methods[name] = scope[name]
        identities[filename] = dict(sha256=hashlib.sha256(source.encode()).hexdigest(), methods=records)

    def render_token_ids(conversation):
        return [ord(char) for char in json.dumps(conversation, ensure_ascii=False, sort_keys=True)]

    class Renderer:
        tokenizer = None
        default_chat_tok_params = SimpleNamespace(with_kwargs=lambda **values: dict(native_tokenization='CPU', **values))

        def __init__(self):
            self.calls = []

        def render_chat(self, conversations, chat_params, tok_params, **extras):
            self.calls.extend((deepcopy(conversation), asdict_options(chat_params), deepcopy(tok_params), deepcopy(extras))
                for conversation in conversations)
            return None, [dict(type='token', prompt_token_ids=render_token_ids(value)) for value in conversations]

    def asdict_options(value):
        return deepcopy(vars(value))

    class FakeNative:
        def __init__(self, **options):
            self.options = options
            self.model_config = SimpleNamespace(max_model_len=options.get('max_model_len', 32768),
                runner_type='generate', enable_prompt_embeds=False)
            self.request_counter = itertools.count()
            self.renderer = Renderer()
            self.submissions = []
            self.llm_engine = SimpleNamespace(add_request=self.add_request,
                engine_core=SimpleNamespace(shutdown=lambda **values: None),
                abort_request=lambda *args, **kwargs: None)

        def _resolve_mm_lora(self, prompt, requested):
            assert requested is None and 'multi_modal_data' not in prompt
            return None

        def add_request(self, request_id, prompt, params, **options):
            self.submissions.append((request_id, deepcopy(prompt), asdict_options(params), deepcopy(options)))
            return request_id

        def _run_engine(self, output_type, *, use_tqdm):
            assert use_tqdm is False
            values = [RequestOutput(request_id=request_id, finished=True,
                prompt_token_ids=prompt['prompt_token_ids'],
                outputs=[SimpleNamespace(index=0, text='CPU response', token_ids=[99],
                    finish_reason='stop', stop_reason=None, logprobs=None, cumulative_logprob=None)])
                for request_id, prompt, params, options in self.submissions]
            assert all(isinstance(value, output_type) for value in values)
            return values

    for name, value in methods.items():
        setattr(FakeNative, name, value)

    rows = []
    for index, text in enumerate(('CPU text', '中文 CPU 原文')):
        rows.append(dict(query_id='source-method-check', model_role='main', algorithm_stage='map',
            row_id='row-'+str(index), source_position=index, call_index=0, call_id='call-'+str(index),
            messages_json=json.dumps([dict(role='user', content=text)], ensure_ascii=False)))
    spec = sync.EngineSpec('/tmp/CPU-source-method-model', '/tmp/model-python', '/tmp/driver-python',
        max_model_len=1024, max_num_seqs=3, max_num_batched_tokens=2048)
    generation = sync.GenerationSpec(max_tokens=8)
    outcomes = []
    for owner_type in (ReferenceVllmEngine, sync.VllmEngine):
        with native_api(actual=1024), patch('vllm.LLM', FakeNative), patch('vllm.SamplingParams', Sampling):
            owner = owner_type(spec, generation)
            try:
                results = owner.run_prepared(rows)
                assert [row['status'] for row in results] == ['succeeded', 'succeeded']
                assert all(json.loads(row['prompt_token_ids_json']) == render_token_ids(
                    json.loads(request['messages_json'])) for row, request in zip(results, rows, strict=True))
                outcomes.append(dict(rendered=owner._llm.renderer.calls,
                    submissions=owner._llm.submissions,
                    results=[{key:value for key,value in row.items()
                        if key not in ('engine_uid', 'model_start_monotonic', 'model_end_monotonic', 'model_elapsed_s')}
                        for row in results]))
            finally:
                owner.close(timeout=1)
    assert outcomes[0]['rendered'] == outcomes[1]['rendered']
    assert outcomes[0]['submissions'] == outcomes[1]['submissions']
    assert outcomes[0]['results'] == outcomes[1]['results'], (outcomes[0]['results'], outcomes[1]['results'])
    report = dict(schema='semloom.native_pipeline_source_check.v2', version='0.25.1', upstream='https://github.com/vllm-project/vllm/tree/v0.25.1',
        source_kind='pinned public upstream function bodies; CPU renderer and queue stubs',
        source_files=identities, requested=asdict(spec), generation=asdict(generation), requests=len(rows),
        render_options_equal=True, prepared_engine_inputs_equal=True, sampling_and_queue_parameters_equal=True,
        normalized_results_equal=True, actual_native_runtime_verified=False,
        normalized_results_excluded=['engine_uid','model_start_monotonic','model_end_monotonic','model_elapsed_s'],
        model_calls=0, real_Ray_runs=0, GPU_runs=0)
    return report
