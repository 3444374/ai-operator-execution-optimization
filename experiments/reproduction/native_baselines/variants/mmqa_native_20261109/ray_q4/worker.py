import os,sys,json,time
from pathlib import Path
started=time.monotonic_ns()
ROOT=Path(sys.argv[1])
if not ROOT.is_dir() or not (ROOT/"RUN-BOUND.json").is_file():
    raise RuntimeError("Outer supervisor must create a fresh lease/budget-bound run first")
binding=json.loads((ROOT/"RUN-BOUND.json").read_text())
if binding["physical_attempt_limit"]!=24:raise ValueError("Exactly 24 attempts per round required")
SOURCE=Path(binding["source"])
os.environ.update(RAY_Q4_SOURCE=str(SOURCE),RAY_Q4_ARTIFACT=str(ROOT),RAY_Q4_BUDGET_ID=binding["budget_id"])
sys.path[:0]=[str(Path(__file__).parent),str(SOURCE/"code")]
import ray,ray.data.llm
from ray_q4_native import execute
from ray._private.runtime_env.setup_hook import export_setup_func_module
original=ray.data.llm.build_processor
def observed_builder(*args,**kwargs):
    processor=original(*args,**kwargs)
    count=0
    for name in processor.list_stage_names():
        stage=processor.get_stage_by_name(name)
        if type(stage).__name__=="vLLMEngineStage":
            count+=1
            stage.map_batches_kwargs.update(max_restarts=0,max_task_retries=0)
            env=dict(stage.map_batches_kwargs.get("runtime_env") or {})
            env["env_vars"]={**env.get("env_vars",{}),**{k:os.environ[k] for k in ("RAY_Q4_SOURCE","RAY_Q4_ARTIFACT","RAY_Q4_BUDGET_ID")},
                "PYTHONPATH":str(Path(__file__).parent)+os.pathsep+str(SOURCE/"code")}
            env["worker_process_setup_hook"]="ray_q4_probe.native_observer_setup"
            stage.map_batches_kwargs["runtime_env"]=export_setup_func_module(env,env["worker_process_setup_hook"])
    if count!=1:raise RuntimeError("Expected one observed native engine stage")
    return processor
import importlib.metadata
report=dict(versions={k:importlib.metadata.version(k) for k in ("ray","vllm","torch","transformers")},pid=os.getpid(),input_profile="frozen preprepared original messages; source scan/preparation excluded",status="failed",profile="independent cold query",warm_query=False,worker_started_monotonic_ns=started,physical_attempt_limit=24)
try:
    ray.init(address="local",num_cpus=8,num_gpus=1,object_store_memory=536870912,include_dashboard=False,_temp_dir=binding["ray_temp_dir"])
    ray.data.llm.build_processor=observed_builder
    rows=json.loads((ROOT/"messages-PRIVATE.json").read_text())
    report["query_started_monotonic_ns"]=time.monotonic_ns()
    predictions=execute(rows,json.loads((Path(__file__).resolve().parent/'CONFIG.json').read_text())['model'])
    report["result_ready_monotonic_ns"]=time.monotonic_ns()
    (ROOT/"predictions-PRIVATE.json").write_text(json.dumps(predictions))
    report["status"]="completed"
finally:
    ray.data.llm.build_processor=original
    ray.shutdown()
    report.update(ray_driver_disconnected=not ray.is_initialized(),worker_closed_monotonic_ns=time.monotonic_ns())
    (ROOT/"worker.json").write_text(json.dumps(report,indent=2))
