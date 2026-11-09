import sys,json,pathlib,builtins,importlib.metadata as metadata
from src.experiments.native_daft_query import run_worker
from physical_budget import install, unit, budgeted_generate
install()
from src.infrastructure.private_artifacts import read_json,write_private_json
from tests.baselines.text.native_sdk_fixtures import recorded_map_provider
ROOT=pathlib.Path(__file__).resolve().parent
config=read_json(ROOT/'cpu-worker-config-PRIVATE.json');requests=read_json(config['requests_path'])['requests'];outputs=read_json(ROOT/'recorded-outputs-PRIVATE.json')['outputs']
from transformers import AutoTokenizer
tok=AutoTokenizer.from_pretrained(config['model'],local_files_only=True,trust_remote_code=False)
expected=read_json(ROOT/'expected-prompt-token-ids-PRIVATE.json')['ids']
import pickle
assert pickle.loads(pickle.dumps((config,requests)))==(config,requests)
assert json.loads(json.dumps(config))==config
default_matches=sum(tok.encode(row['prompt'])==want for row,want in zip(requests,expected,strict=True))
direct_matches=sum(tok.encode(row['prompt'],add_special_tokens=False)==want for row,want in zip(requests,expected,strict=True))
assert default_matches==direct_matches==25
write_private_json(ROOT/'CPU-MODEL-TOKENIZER-RESULT.json',dict(status='passed',Transformers=metadata.version('transformers'),default_encode_matches=default_matches,direct_encode_matches=direct_matches,denominator=25,real_model_calls=0))

original_import=builtins.__import__
def model_only(name,*args,**kwargs):
 if name.startswith(('lotus','duckdb','src.semantic_methods','src.baselines.text.sembench_text')):
  raise AssertionError('model worker imported driver semantic dependency: '+name)
 return original_import(name,*args,**kwargs)
builtins.__import__=model_only
try:
 with recorded_map_provider(requests,outputs) as cpu:
  report=run_worker(config,ROOT/'CPU-SDK-model-worker')
  assert all(model.closed for model in cpu.engines)
 assert not any(x.loop_thread.is_alive() for x in cpu.executors)
 assert all(x.loop.is_closed() for x in cpu.executors)
finally:builtins.__import__=original_import
from src.experiments.attempt_ledger import BudgetExhausted
assert unit().attempts==25 and unit().remaining==0
forwarded=[]
try:budgeted_generate(lambda *a,**k:forwarded.append(1))(request_id='CPU-attempt-26')
except BudgetExhausted:pass
else:raise AssertionError('native attempt26 accepted')
assert not forwarded and unit().attempts==25
native=report['native_api'];assert (native['generate_calls'],native['accepted'],native['final_events'],native['unresolved'])==(25,25,25,0)
assert not native['errors']
ready=read_json(ROOT/'CPU-SDK-model-worker/result-ready.json')['result'];by_id={r['call_id']:r['generated_text'] for r in ready['predictions']}
assert [by_id[r['call_id']] for r in requests]==outputs
summary=dict(status='passed',physical_pre_generate_reservations=25,attempt26_rejected_before_delegate=True,Daft=metadata.version('daft'),generation_provider='recorded CPU stand-in; NOT real vLLM or AI collect',real_AI_expression_constructed=len(cpu.expressions),real_Daft_CPU_collect=True,real_Daft_LocalVLLMExecutor=True,original_LLM_dependencies_not_imported=True,recorded_requests=native['generate_calls'],recorded_adds=native['accepted'],recorded_terminals=native['final_events'],unknown=native['unresolved'],model_close_receipt=report['model_close_receipt'],loop_threads_closed=True,real_generation_calls=0,weights_loaded=False)
write_private_json(ROOT/'CPU-SDK-RESULT.json',summary);print(json.dumps(summary))
