"""Full native CartesianProduct/Filter planning and pair token checks, no model."""
import json
from types import SimpleNamespace

from . import native_server
from .join_binding import install
from .join_inputs import load_inputs
from .map_observation import digest


def check(cfg,output,executor,manager,planner_class,data,max_model_len):
    from vllm.kalypso.sem_ops.joins import CartesianProduct
    from vllm.kalypso.sem_ops.filters import SemFilter
    manifest,inputs=load_inputs(cfg);unit=cfg['units'][0]
    roots={item['paths']['prompt'].parent for item in inputs}
    if len(roots)!=1:raise ValueError('Join CPU requires one frozen field table directory')
    install(dict(cfg,map_prompt_root=str(next(iter(roots)))),unit,native_server,CartesianProduct,SemFilter)
    plans=[]
    for item in inputs:
        spec=item['spec'];table=item['table'];manager.reset_stages()
        contexts=list(data._data_source(None,item['paths']['left'],executor))
        for ctx in contexts:ctx.state.raw_request=SimpleNamespace(headers={'X-Kalypso-Observation':spec['query_id']})
        planner=planner_class(executor,virtual_pinning=True,blocking=False)
        ops=planner.parse_ops([dict(op='join',args=dict(right_table=str(item['paths']['right']),instruction=table['instruction']))])
        if (len(ops)!=2 or type(ops[0]) is not CartesianProduct or type(ops[1]) is not SemFilter
                or ops[1].max_tokens!=8 or any(op.pin or op.unpin for op in ops)):
            raise ValueError('Original CartesianProduct plus unpinned default Filter changed')
        plan=planner.build(contexts,ops)
        pairs=[child for ctx in contexts for child in ops[0](ctx)]
        if [ctx.state.idx for ctx in pairs]!=list(range(spec['expected_calls'])):
            raise ValueError('Native CartesianProduct lost complete ordered pair identities')
        previews=[];width=len(table['right'])
        for ctx in pairs:
            idx=ctx.state.idx;_,prompt=ops[1]._build_prompts(ctx)
            chat=executor._build_chat_request(prompt,8,False,0)
            ids=manager.tokenizer.encode(manager.tokenizer.apply_chat_template(chat.messages,
                tokenize=False,add_generation_prompt=chat.add_generation_prompt),add_special_tokens=chat.add_special_tokens)
            if len(ids)+8>max_model_len:raise ValueError('Complete Join prompt and original output cap exceed model context')
            previews.append(dict(idx=idx,row_id=spec['query_id']+f'-pair-{idx//width}-{idx%width}',
                left_position=idx//width,right_position=idx%width,messages=prompt,messages_sha256=digest(prompt),
                prompt_tokens=len(ids),token_ids=ids))
        (output/(spec['query_id']+'-native-prompts.json')).write_text(json.dumps(previews,ensure_ascii=False,indent=2)+'\n')
        plans.append(dict(query_id=spec['query_id'],left_rows=len(table['left']),right_rows=width,
            logical_occurrences=len(pairs),physical_call_cap=len(pairs),plan_segments=len(plan),
            max_prompt_tokens=max((row['prompt_tokens'] for row in previews),default=None)))
    return dict(status='passed_real_native_CPU_Join_planning_no_model_execution',plans=plans,
        frozen_input_binding=manifest['binding'],frozen_input_manifest_sha256=cfg['native_cpu_inputs']['sha256'],
        native_retry='none; native CartesianProduct and one Filter per pair',CPU_numeric_KV_budget_GiB=6,performance_claim=False)
