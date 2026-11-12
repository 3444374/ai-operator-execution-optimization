def native_observer_setup():
    from pathlib import Path
    import os
    S = Path(os.environ['Q7_SOURCE'])
    root = Path(os.environ['Q7_OUTPUT'])
    budget_id = os.environ['Q7_BUDGET_ID']
    import os, sys, time, json, hashlib, functools, inspect, importlib.metadata
    from pathlib import Path
    sys.path.insert(0, str(S / 'code'))
    from src.experiments.attempt_ledger import AttemptBudget
    from src.experiments.shared_request_budget import SharedClaimedUnit
    from src.infrastructure.private_artifacts import publish_private_json
    from .binding import rows_per_pass
    bound = json.loads((root / 'RUN-BOUND.json').read_text())
    cap = bound['physical_attempt_limit']
    rows_per_pass(cap)
    if bound['budget_id'] != budget_id:
        raise ValueError('Q7 actor budget identity differs from the frozen owner binding')
    ledger = SharedClaimedUnit(root / 'logical-unit.sqlite', AttemptBudget(budget_id, cap), budget_id)

    def event(kind, **data):
        entry = dict(event=kind, monotonic_ns=time.monotonic_ns(), epoch_ns=time.time_ns(), pid=os.getpid(), **data)
        fd = os.open(str(root / ('native-events-' + str(os.getpid()) + '.jsonl')), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 384)
        try:
            os.write(fd, (json.dumps(entry, separators=(',', ':'), default=str) + '\n').encode())
        finally:
            os.close(fd)
    from vllm.v1.engine.async_llm import AsyncLLM
    old_init = AsyncLLM.__init__
    old_generate = AsyncLLM.generate

    @functools.wraps(old_init)
    def observed_init(self, *args, **kwargs):
        event('native_constructor_started')
        old_init(self, *args, **kwargs)
        cfg = getattr(self, 'vllm_config', None)
        mc = getattr(cfg, 'model_config', None)
        sc = getattr(cfg, 'scheduler_config', None)
        cc = getattr(cfg, 'cache_config', None)
        event('native_constructor_ready', python=sys.executable, prefix=sys.prefix, versions={k: importlib.metadata.version(k) for k in ['ray', 'vllm', 'torch', 'transformers']}, native_capacity=dict(max_num_seqs=getattr(sc, 'max_num_seqs', None), max_num_batched_tokens=getattr(sc, 'max_num_batched_tokens', None), max_model_len=getattr(mc, 'max_model_len', None), dtype=str(getattr(mc, 'dtype', None)), gpu_memory_utilization=getattr(cc, 'gpu_memory_utilization', None), num_gpu_blocks=getattr(cc, 'num_gpu_blocks', None), enable_prefix_caching=getattr(cc, 'enable_prefix_caching', None)), meaning='AsyncLLM constructor returned; not proof of GPU kernel execution')

    @functools.wraps(old_generate)
    async def observed_generate(self, *args, **kwargs):
        bound = inspect.signature(old_generate).bind_partial(self, *args, **kwargs)
        rid = bound.arguments.get('request_id')
        attempt = ledger.reserve(hashlib.sha256(str(rid).encode()).hexdigest())
        params = bound.arguments.get('sampling_params')
        prompt = bound.arguments.get('prompt', bound.arguments.get('inputs'))
        mm = prompt.get('multi_modal_data', {}) if isinstance(prompt, dict) else {}
        images = mm.get('image', []) if isinstance(mm, dict) else []
        event('native_generate_started', attempt=attempt, request_id=rid, sampling={key: getattr(params, key, 'unavailable') for key in ['max_tokens', 'min_tokens', 'temperature', 'top_p', 'top_k', 'min_p', 'seed', 'stop', 'stop_token_ids', 'ignore_eos', 'include_stop_str_in_output', 'skip_special_tokens', 'spaces_between_special_tokens', 'truncate_prompt_tokens', 'presence_penalty', 'frequency_penalty', 'repetition_penalty', 'n']}, multimodal_keys=list(mm) if isinstance(mm, dict) else None, images_per_request=len(images) if isinstance(images, list) else 1, prompt_token_ids_before_MM_expansion=prompt.get('prompt_token_ids') if isinstance(prompt, dict) else None)
        terminal = False
        try:
            async for out in old_generate(self, *args, **kwargs):
                if getattr(out, 'finished', False):
                    terminal = True
                    actual = getattr(out, 'request_id', rid)
                    outputs = [dict(index=getattr(o, 'index', None), text=getattr(o, 'text', None), token_ids=[int(x) for x in getattr(o, 'token_ids', [])], finish_reason=getattr(o, 'finish_reason', None), stop_reason=getattr(o, 'stop_reason', None)) for o in getattr(out, 'outputs', [])]
                    publish_private_json(root / ('native-output-' + str(attempt) + '.json'), dict(attempt=attempt, request_id=actual, prompt_token_count=len(getattr(out, 'prompt_token_ids', []) or []), prompt_token_ids=[int(t) for t in getattr(out, 'prompt_token_ids', []) or []], outputs=outputs))
                    event('native_generate_finished', attempt=attempt, request_id=actual, prompt_tokens=len(getattr(out, 'prompt_token_ids', []) or []), output_tokens=sum((len(o['token_ids']) for o in outputs)), finish_reasons=[o['finish_reason'] for o in outputs])
                yield out
        except BaseException as error:
            if isinstance(error, GeneratorExit) and terminal:
                event('native_generate_consumer_closed', attempt=attempt, request_id=rid, terminal_confirmed=True)
            else:
                event('native_generate_error', attempt=attempt, request_id=rid, error_type=type(error).__name__)
            raise
        finally:
            if not terminal:
                event('native_generate_unconfirmed', attempt=attempt, request_id=rid)
    AsyncLLM.__init__ = observed_init
    AsyncLLM.generate = observed_generate
    event('native_observer_installed', physical_attempt_limit=cap, unseen_attempts_not_refunded=True, requested_config=json.loads(Path(os.environ['Q7_CONFIG']).read_text()) if 'Q7_CONFIG' in os.environ else 'unavailable')
