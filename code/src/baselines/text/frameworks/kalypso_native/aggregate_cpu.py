"""Native SemAgg plan and initial chunks; future prompts require actual summaries."""
import json

from .map_observation import digest
from .movie_map_inputs import load_inputs
from .native_server import physical_call_caps


def check(cfg, output, executor, manager, planner_class, data, max_model_len):
    from vllm.kalypso.sem_ops.reducers import SemAgg
    unit = cfg['units'][0]
    if len(cfg['units']) != 1 or {q['task'] for q in unit['queries']} != {'sembench-aggregate'}:
        raise ValueError('CPU preflight requires one dedicated native Aggregate unit')
    caps = physical_call_caps(unit)
    manifest, inputs = load_inputs(cfg)
    plans = []
    for item in inputs:
        spec, rows = item['spec'], item['table']['rows']
        manager.reset_stages()
        contexts = list(data._data_source(None, item['paths']['csv'], executor))
        if [ctx.state.idx for ctx in contexts] != list(range(len(rows))):
            raise ValueError('Native aggregate source changed occurrence order')
        planner = planner_class(executor, virtual_pinning=True, blocking=False)
        ops = planner.parse_ops([dict(op='sem_agg', args=dict(instruction=item['table']['native_instruction']))])
        plan = planner.build(contexts, ops)
        op = ops[0]
        if (len(ops) != 1 or type(op) is not SemAgg or plan != list(ops)
                or op.max_tokens != 8192 or op.concurrency != 8 or op.pin or op.unpin):
            raise ValueError('Original native blocking aggregation defaults changed')
        chunks = op._chunk_by_tokens(contexts) if len(contexts) > 1 else []
        reducible = [chunk for chunk in chunks if len(chunk) > 1]
        first_batches = (reducible or [contexts]) if len(contexts) > 1 else []
        previews = []
        for chunk in first_batches:
            prompt = op._build_prompt(chunk)
            chat = executor._build_chat_request(prompt, op.max_tokens, False, 0)
            ids = manager.tokenizer.encode(manager.tokenizer.apply_chat_template(chat.messages,
                tokenize=False, add_generation_prompt=chat.add_generation_prompt),
                add_special_tokens=chat.add_special_tokens)
            if len(ids) + chat.max_tokens > max_model_len:
                raise ValueError('Complete initial aggregate prompt and native output cap exceed model context')
            previews.append(dict(native_context_indices=[ctx.state.idx for ctx in chunk], messages=prompt,
                messages_sha256=digest(prompt), token_ids=ids, prompt_tokens=len(ids), max_tokens=chat.max_tokens))
        (output/(spec['query_id']+'-native-initial-reductions.json')).write_text(json.dumps(previews, indent=2)+'\n')
        plans.append(dict(query_id=spec['query_id'], logical_occurrences=len(rows), physical_call_cap=caps[spec['query_id']],
            expected_model_calls=None, plan_segments=len(plan), initial_reductions=len(previews),
            max_prompt_tokens=max((row['prompt_tokens'] for row in previews), default=None),
            previews_are_model_requests=False, future_prompts='unknown; depend on actual native summaries'))
    return dict(status='passed_real_native_CPU_Aggregate_initial_planning_no_model_execution', plans=plans,
        frozen_input_binding=manifest['binding'], frozen_input_manifest_sha256=cfg['native_cpu_inputs']['sha256'],
        model_calls=0, CPU_numeric_KV_budget_GiB=6, performance_claim=False,
        complete_generated_prompt_qualification=False)
