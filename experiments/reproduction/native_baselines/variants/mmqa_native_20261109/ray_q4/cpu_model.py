"""Actual official template/tokenize UDF and SDK graph; no model construction."""
from pathlib import Path
import asyncio,hashlib,json,os,pickle,time,importlib.metadata as metadata
ROOT=Path(__file__).resolve().parent
SOURCE=Path(os.environ['CASE_SOURCE'])
os.environ.update(RAY_Q4_SOURCE=str(SOURCE),RAY_Q4_ARTIFACT=str(ROOT),RAY_Q4_BUDGET_ID='CPU-hook-import-only')
import ray,ray.cloudpickle as cp
from vllm.v1.engine.async_llm import AsyncLLM
import ray_q4_native,ray_q4_probe
from src.infrastructure.private_artifacts import write_private_json
from ray._private.runtime_env.setup_hook import export_setup_func_module
from ray.llm._internal.batch.stages.chat_template_stage import ChatTemplateUDF
from ray.llm._internal.batch.stages.tokenize_stage import TokenizeUDF
from ray.llm._internal.batch.stages.vllm_engine_stage import vLLMEngineWrapper, vLLMTaskType
rows=json.loads((ROOT/'messages-PRIVATE.json').read_text());refs=json.loads((ROOT/'CPU-TOKEN-REFERENCES.json').read_text());calls=[];original=AsyncLLM.__init__
def forbidden(*a,**kw):calls.append(True);raise AssertionError('CPU SDK check attempted a model')
AsyncLLM.__init__=forbidden
try:
 ray.init(address='local',num_cpus=1,num_gpus=0,object_store_memory=83886080,include_dashboard=False,_temp_dir=str(Path(os.environ['CASE_CPU_TEMP'])/'r'))
 processor=ray_q4_native.build(json.loads((ROOT/'CONFIG.json').read_text())['model']);cfg=processor.config.model_dump(mode='json');names=processor.list_stage_names()
 assert len(names)==4 and cfg['batch_size']==32 and cfg['concurrency']==1 and cfg['max_concurrent_batches']==8
 engine=processor.get_stage_by_name('vLLMEngineStage');assert engine.map_batches_kwargs['max_restarts']==engine.map_batches_kwargs['max_task_retries']==0
 for fn in (ray_q4_native.preprocess,ray_q4_native.postprocess,ray_q4_probe.native_observer_setup):cp.loads(cp.dumps(fn))
 env=dict(env_vars={k:os.environ[k] for k in ('RAY_Q4_SOURCE','RAY_Q4_ARTIFACT','RAY_Q4_BUDGET_ID')},worker_process_setup_hook='ray_q4_probe.native_observer_setup')
 json.loads(json.dumps(export_setup_func_module(env,env['worker_process_setup_hook'])))
 model=json.loads((ROOT/'CONFIG.json').read_text())['model']
 template=ChatTemplateUDF(data_column='__data',expected_input_keys=['messages'],model=model)
 tokenize=TokenizeUDF(data_column='__data',expected_input_keys=['prompt'],model=model)
 async def actual_stages():
  batch=[dict(messages=r['messages'],**{template.IDX_IN_BATCH_COLUMN:i}) for i,r in enumerate(rows)]
  rendered=[r async for r in template.udf(batch)]
  ids=[r async for r in tokenize.udf(rendered)]
  assert [r['prompt'] for r in rendered]==refs['prompts']
  assert [r['tokenized_prompt'] for r in ids]==refs['ids'] and len(ids)==24
  wrapper=object.__new__(vLLMEngineWrapper)
  wrapper.model=model;wrapper.task_type=vLLMTaskType.GENERATE
  wrapper.idx_in_batch_column=template.IDX_IN_BATCH_COLUMN;wrapper.request_id=0
  for i,(source,rendered_row,tokenized) in enumerate(zip(rows,rendered,ids,strict=True)):
   row=ray_q4_native.preprocess(source)
   row.update(prompt=rendered_row['prompt'],tokenized_prompt=tokenized['tokenized_prompt'])
   row[wrapper.idx_in_batch_column]=i
   request=await wrapper._prepare_llm_request(row)
   params=request.params;structured=params.structured_outputs
   assert structured.json_object is True
   assert all(getattr(structured,name,None) is None for name in ('json','regex','choice','grammar','structural_tag'))
   assert params.max_tokens==8192 and params.temperature==0 and params.top_p==1 and params.seed==0 and params.stop==[]
   assert params.presence_penalty==params.frequency_penalty==0 and params.repetition_penalty==1 and params.n==1
   assert request.prompt_token_ids==refs['ids'][i] and request.idx_in_batch==i
   assert 'response_format' not in ray_q4_native.SAMPLING
  assert wrapper.request_id==24
  assert ray_q4_native.SAMPLING['structured_outputs']=={'json_object':True}

 asyncio.run(actual_stages())
 from src.experiments.attempt_ledger import AttemptBudget,BudgetExhausted
 from src.experiments.cell_budget import CellBudgetLedger
 b=AttemptBudget('CPU-RAY-MEDICAL-Q10-boundary',24);ledger=CellBudgetLedger.create(ROOT/'CPU-ONLY-budget.sqlite',b,deadline_utc=time.time()+600)
 ledger.reserve_unit(b.budget_id,24);unit=ledger.claim_shared_unit(b.budget_id)
 for i in range(24):unit.reserve(hashlib.sha256(str(i).encode()).hexdigest())
 rejected=False
 try:unit.reserve(hashlib.sha256(b'25').hexdigest())
 except BudgetExhausted:rejected=True
 assert rejected and unit.attempts==24 and unit.remaining==0 and not calls
finally:
 AsyncLLM.__init__=original;ray.shutdown()
assert not ray.is_initialized()
write_private_json(ROOT/'CPU-SDK.json',dict(status='passed',versions={k:metadata.version(k) for k in ('ray','vllm','torch','transformers')},actual_ChatTemplateUDF_prompts_match=24,actual_TokenizeUDF_ids_match=24,actual_SDK_stages=list(names),processor_config=cfg,setup_hook_pickle_and_JSON=True,actual_official_prepare_llm_request_typed_JSONobject=24,structured_schema_or_enum_added=False,sampling_dict_JSON_roundtrip=json.loads(json.dumps(ray_q4_native.SAMPLING))==ray_q4_native.SAMPLING,constructor_attempts=0,models=0,Ray_CPU_shutdown=True,CPU_budget_reservations=24,CPU_attempt25_rejected=True,engine_restarts=0,engine_task_retries=0))
print((ROOT/'CPU-SDK.json').read_text())
