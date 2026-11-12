"""Original native full-ranking plan and complete possible-comparison token gate."""
import json

from .movie_map_inputs import load_inputs
from .native_server import physical_call_caps
from .map_observation import digest


def check(cfg,output,executor,manager,planner_class,data,max_model_len):
    from vllm.kalypso.sem_ops.reducers import SemTopK
    unit=cfg['units'][0]
    if len(cfg['units'])!=1 or {q['task'] for q in unit['queries']}!={'sembench-topk'}:
        raise ValueError('CPU preflight requires one dedicated native TopK unit')
    caps=physical_call_caps(unit);manifest,inputs=load_inputs(cfg);plans=[]
    for item in inputs:
        spec=item['spec'];rows=item['table']['rows'];manager.reset_stages()
        if not rows:
            plans.append(dict(query_id=spec['query_id'],logical_occurrences=0,physical_call_cap=0,
                previewed_orientations=0,max_prompt_tokens=0,plan_segments=0,
                algorithm='original empty query returns before accessor',previews_are_model_requests=False))
            continue
        contexts=list(data._data_source(None,item['paths']['csv'],executor))
        if [ctx.state.idx for ctx in contexts]!=list(range(len(rows))):
            raise ValueError('Native TopK source changed occurrence order')
        planner=planner_class(executor,virtual_pinning=True,blocking=False)
        ops=planner.parse_ops([dict(op='sem_topk',args=dict(
            instruction=item['table']['native_instruction'],k=len(rows)))])
        plan=planner.build(contexts,ops)
        if (len(ops)!=1 or type(ops[0]) is not SemTopK or plan!=list(ops)
                or ops[0].k!=len(rows) or ops[0].max_tokens!=5 or ops[0].pin or ops[0].unpin):
            raise ValueError('Native blocking full-ranking plan or output cap changed')
        count=0;maximum=0
        # Both orientations can occur; the native algorithm chooses its subset.
        with (output/(spec['query_id']+'-native-comparisons.jsonl')).open('x') as out:
            for first in contexts:
                for second in contexts:
                    if first is second:continue
                    prompt=ops[0]._build_compare_prompt(first,second)
                    chat=executor._build_chat_request(prompt,5,False,0)
                    ids=manager.tokenizer.encode(manager.tokenizer.apply_chat_template(chat.messages,
                        tokenize=False,add_generation_prompt=chat.add_generation_prompt),
                        add_special_tokens=chat.add_special_tokens)
                    if len(ids)+chat.max_tokens>max_model_len:
                        raise ValueError('Complete TopK comparison and original output cap exceed model context')
                    out.write(json.dumps(dict(pair=[first.state.idx,second.state.idx],messages=prompt,
                        messages_sha256=digest(prompt),token_ids=ids,prompt_tokens=len(ids),max_tokens=5))+'\n')
                    maximum=max(maximum,len(ids));count+=1
        plans.append(dict(query_id=spec['query_id'],logical_occurrences=len(rows),
            physical_call_cap=caps[spec['query_id']],previewed_orientations=count,
            max_prompt_tokens=maximum,plan_segments=len(plan),algorithm='original insertion/merge full ranking',
            previews_are_model_requests=False))
    return dict(status='passed_real_native_CPU_TopK_planning_no_model_execution',plans=plans,
        frozen_input_binding=manifest['binding'],frozen_input_manifest_sha256=cfg['native_cpu_inputs']['sha256'],
        model_calls=0,CPU_numeric_KV_budget_GiB=6,performance_claim=False)
