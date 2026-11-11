"""Complete Q9 prompt/planner preflight in the existing native CPU environment."""
import json
from pathlib import Path

from . import native_server
from .map_observation import digest, install
from .movie_map import make_query, prepare


def check(cfg,output,executor,manager,planner_class,data,max_model_len):
    from vllm.kalypso.sem_ops.map import SemMap
    from vllm.kalypso.controller.map_estimator import MapRatioEstimator
    if len(cfg['units'])!=1 or any(q['task']!='movie-q9' for q in cfg['units'][0]['queries']):
        raise ValueError('Q9 CPU preflight requires one dedicated Map unit')
    unit=cfg['units'][0]
    native_server.physical_call_caps(unit)
    local=dict(cfg,map_prompt_root=str(output/'prompts'))
    install(local,unit,output,native_server,SemMap,MapRatioEstimator)
    plans=[]
    for spec in unit['queries']:
        query=make_query(local,spec['query_id'])
        rows,path=prepare(local,spec,output/spec['query_id'],query.operation)
        manager.reset_stages()
        contexts=list(data._data_source(None,path,executor))
        planner=planner_class(executor,virtual_pinning=True,blocking=False)
        ops=planner.parse_ops([dict(op='sem_map',args=dict(prompt=query.operation.stage.method.instruction))])
        from types import SimpleNamespace
        for ctx in contexts:
            ctx.state.raw_request=SimpleNamespace(headers={'X-Kalypso-Observation':spec['query_id']})
        plan=planner.build(contexts,ops)
        if len(ops)!=1 or type(ops[0]) is not SemMap or ops[0].pin or ops[0].unpin:
            raise ValueError('Original one-stage unpinned Map plan changed')
        # The native contexts need only the observation header for this field binding.
        previews=[]
        for idx,ctx in enumerate(contexts):
            _,prompt=ops[0]._build_prompt(ctx)
            chat=executor._build_chat_request(prompt,ops[0]._planned_max_tokens(ctx),False,0)
            ids=manager.tokenizer.encode(manager.tokenizer.apply_chat_template(chat.messages,
                tokenize=False,add_generation_prompt=chat.add_generation_prompt),
                add_special_tokens=chat.add_special_tokens)
            if len(ids)+max(chat.max_tokens,8192)>max_model_len:
                raise ValueError('Complete Q9 prompt and native retry cap exceed model context')
            previews.append(dict(idx=idx,row_id=rows[idx]['row_id'],messages_sha256=digest(prompt),
                prompt_tokens=len(ids),token_ids=ids,first_max_tokens=chat.max_tokens))
        (output/(spec['query_id']+'-native-prompts.json')).write_text(json.dumps(previews,indent=2)+'\n')
        plans.append(dict(query_id=spec['query_id'],logical_occurrences=len(rows),physical_call_cap=512,
            plan_segments=len(plan),max_prompt_tokens=max(r['prompt_tokens'] for r in previews)))
    return dict(status='passed_real_native_CPU_Q9_planning_no_model_execution',plans=plans,
        native_retry='original length-to8192 branch, at most two calls per occurrence',
        CPU_numeric_KV_budget_GiB=6,performance_claim=False)
