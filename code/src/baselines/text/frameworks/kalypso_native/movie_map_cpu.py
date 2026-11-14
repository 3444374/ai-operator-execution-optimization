"""Complete SemBench text prompt/planner preflight in the native CPU environment."""
import json
from pathlib import Path

from . import native_server
from .map_observation import digest, install
from .field_binding import install_prompt
from .movie_map_inputs import load_inputs


def check(cfg,output,executor,manager,planner_class,data,max_model_len,*,kind=None):
    tasks=(native_server.ROW_TASKS if kind is None else
           native_server.MAP_TASKS if kind=='map' else ('sembench-filter',))
    if len(cfg['units'])!=1 or any(q['task'] not in tasks for q in cfg['units'][0]['queries']):
        raise ValueError('CPU preflight requires one text Filter/Map/Extract sequence')
    unit=cfg['units'][0]
    caps=native_server.physical_call_caps(unit)
    manifest,inputs=load_inputs(cfg)
    prompt_roots={item['paths']['prompt'].parent for item in inputs}
    if len(prompt_roots)!=1:raise ValueError('Text CPU prompt tables require one frozen directory')
    local=dict(cfg,map_prompt_root=str(next(iter(prompt_roots))))
    classes={}
    maps=[q for q in unit['queries'] if q['task'] in native_server.MAP_TASKS]
    filters=[q for q in unit['queries'] if q['task']=='sembench-filter']
    if maps:
        from vllm.kalypso.sem_ops.map import SemMap
        from vllm.kalypso.controller.map_estimator import MapRatioEstimator
        classes['map']=SemMap
        install(local,dict(unit,queries=maps),output,native_server,SemMap,MapRatioEstimator)
    if filters:
        from vllm.kalypso.sem_ops.filters import SemFilter
        from vllm.kalypso.sem_ops.base import OpName
        classes['filter']=SemFilter
        install_prompt(local,dict(unit,queries=filters),SemFilter,'_build_prompts',OpName.SEM_FILTER)
    plans=[]
    for item in inputs:
        spec=item['spec'];rows=item['table']['rows'];path=item['paths']['csv']
        kind='map' if spec['task'] in native_server.MAP_TASKS else 'filter'
        operator_class=classes[kind]
        manager.reset_stages()
        contexts=list(data._data_source(None,path,executor))
        if [ctx.state.idx for ctx in contexts]!=list(range(len(rows))):
            raise ValueError('Native CPU data source changed the frozen occurrence order')
        planner=planner_class(executor,virtual_pinning=True,blocking=False)
        ops=planner.parse_ops([dict(op='sem_'+kind,args=dict(prompt=item['table']['instruction']))])
        from types import SimpleNamespace
        for ctx in contexts:
            ctx.state.raw_request=SimpleNamespace(headers={'X-Kalypso-Observation':spec['query_id']})
        plan=planner.build(contexts,ops)
        if len(ops)!=1 or type(ops[0]) is not operator_class or ops[0].pin or ops[0].unpin:
            raise ValueError('Original one-stage unpinned text plan changed')
        # The native contexts need only the observation header for this field binding.
        previews=[]
        for idx,ctx in enumerate(contexts):
            if kind=='map':
                _,prompt=ops[0]._build_prompt(ctx)
                first_cap=ops[0]._planned_max_tokens(ctx)
            else:
                _,prompt=ops[0]._build_prompts(ctx)
                first_cap=ops[0].max_tokens
                if first_cap!=8:raise ValueError('Original Filter output cap changed')
            chat=executor._build_chat_request(prompt,first_cap,False,0)
            ids=manager.tokenizer.encode(manager.tokenizer.apply_chat_template(chat.messages,
                tokenize=False,add_generation_prompt=chat.add_generation_prompt),
                add_special_tokens=chat.add_special_tokens)
            if len(ids)+(max(chat.max_tokens,8192) if kind=='map' else chat.max_tokens)>max_model_len:
                raise ValueError('Complete text prompt and '+('native retry cap' if kind=='map' else 'Filter output cap')+' exceed model context')
            previews.append(dict(idx=idx,row_id=rows[idx]['row_id'],source_position=rows[idx]['source_position'],
                source_index=rows[idx]['source_index'],
                **(dict(reviewId=rows[idx]['reviewId']) if 'reviewId' in rows[idx] else {}),
                messages=prompt,messages_sha256=digest(prompt),
                prompt_tokens=len(ids),token_ids=ids,first_max_tokens=chat.max_tokens))
        (output/(spec['query_id']+'-native-prompts.json')).write_text(json.dumps(previews,indent=2)+'\n')
        plans.append(dict(query_id=spec['query_id'],logical_occurrences=len(rows),physical_call_cap=caps[spec['query_id']],
            operator=kind,native_retry=('original length-to8192 branch, at most two calls per occurrence' if kind=='map' else 'none'),
            plan_segments=len(plan),max_prompt_tokens=max((r['prompt_tokens'] for r in previews),default=None)))
    return dict(status='passed_real_native_CPU_text_planning_no_model_execution',plans=plans,
        frozen_input_binding=manifest['binding'],frozen_input_manifest_sha256=cfg['native_cpu_inputs']['sha256'],
        native_retry=('per-query original policies; see plans' if len(classes)>1 else plans[0]['native_retry']),
        CPU_numeric_KV_budget_GiB=6,performance_claim=False)
