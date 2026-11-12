"""Real Ray CPU worker hook export/install probe; never constructs an engine."""
import argparse
import json
import os
from pathlib import Path
import time


def inspect_worker():
    from vllm.v1.engine.async_llm import AsyncLLM
    import torch
    return dict(pid=os.getpid(), init_wrapped=hasattr(AsyncLLM.__init__, '__wrapped__'),
                generate_wrapped=hasattr(AsyncLLM.generate, '__wrapped__'),
                cuda_initialized=torch.cuda.is_initialized())


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output',type=Path)
    args=parser.parse_args()
    if os.environ.get('CUDA_VISIBLE_DEVICES') != '':
        raise ValueError('Set CUDA_VISIBLE_DEVICES empty for this zero-model probe')
    import ray
    from ray._private.runtime_env.setup_hook import export_setup_func_module
    from src.experiments.attempt_ledger import AttemptBudget
    from src.experiments.cell_budget import CellBudgetLedger
    root=args.output.resolve();root.mkdir(exist_ok=False)
    budget=AttemptBudget('q7-hook-cpu',128)
    (root/'RUN-BOUND.json').write_text(json.dumps(dict(budget_id=budget.budget_id,physical_attempt_limit=128)))
    ledger=CellBudgetLedger.create(root/'logical-unit.sqlite',budget,deadline_utc=time.time()+600)
    ledger.reserve_unit(budget.budget_id,128);unit=ledger.claim_shared_unit(budget.budget_id)
    keys=dict(Q7_SOURCE=os.environ['Q7_SOURCE'],Q7_CONFIG=os.environ['Q7_CONFIG'],
              Q7_OUTPUT=str(root),Q7_BUDGET_ID=budget.budget_id,CUDA_VISIBLE_DEVICES='',
              PYTHONPATH=str(Path(os.environ['Q7_SOURCE'])/'code'))
    hook='src.baselines.image.ray_animals_q7.native_probe.native_observer_setup'
    try:
        ray.init(address='local',num_cpus=1,num_gpus=0,include_dashboard=False,
                 object_store_memory=83886080,_temp_dir=str(root/'ray-temp'))
        env=export_setup_func_module(dict(env_vars=keys,worker_process_setup_hook=hook),hook)
        result=ray.get(ray.remote(inspect_worker).options(runtime_env=env,max_retries=0).remote())
        assert result['init_wrapped'] and result['generate_wrapped'] and not result['cuda_initialized']
        events=[json.loads(line) for f in root.glob('native-events-*.jsonl') for line in f.read_text().splitlines()]
        assert any(e['event']=='native_observer_installed' and e['pid']==result['pid'] for e in events)
        assert not any(e['event'].startswith(('native_constructor','native_generate')) for e in events)
        assert unit.attempts==0
        (root/'HOOK-CPU.json').write_text(json.dumps(dict(status='passed',worker=result,
             model_calls=0,model_constructor_events=0,scope='actual Ray worker hook export/install only'),indent=2))
    finally:
        ray.shutdown()


if __name__ == '__main__':
    main()
