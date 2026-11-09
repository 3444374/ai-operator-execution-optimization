"""Finite native MMQAQ3a CPU qualification using existing host lease/supervision."""
import importlib.util
import json
import os
from pathlib import Path
import sys
import traceback

ROOT=Path(__file__).resolve().parent
SOURCE=Path(os.environ['CASE_SOURCE'])


def main():
    assert not (ROOT/'CPU-FINAL.json').exists() and not (ROOT/'CPU-before.json').exists()
    spec=importlib.util.spec_from_file_location('host',os.environ['CASE_HOST_HELPER'])
    host=importlib.util.module_from_spec(spec);spec.loader.exec_module(host)
    api=host.production_api(SOURCE/'code')
    from src.observability.process_resources.supervised import ResourceMonitor
    resources=json.loads(Path(os.environ['CASE_RESOURCES']).read_text())
    os.sched_setaffinity(0,set(resources['shared_resources']['cpu_affinity_slots']))
    before=host.snapshot(api,resources,'Daft-native-MMQAQ3a-CPU-before')
    host.private_json(ROOT/'CPU-before.json',before);host.assert_idle(before,resources)
    lease=api.acquire_runner_lease(Path(os.environ['CASE_LEASE_SCOPE']),
        config_fingerprint=api.HOST_SCOPE_FINGERPRINT,repository_commit='1d35348134683ffbd3c0f3c9b3718ad032439ab5',recover_stale=False)
    monitor=ResourceMonitor(resources['shared_resources']);owned=set();failure=None;steps=[]
    try:
        os.environ.update(PYTHONHASHSEED='3',PYTHONPATH=str(SOURCE/'code')+os.pathsep+str(ROOT),CUDA_VISIBLE_DEVICES='',
            HF_HUB_OFFLINE='1',TRANSFORMERS_OFFLINE='1',LITELLM_LOCAL_MODEL_COST_MAP='True',
            PYTHONDONTWRITEBYTECODE='1',TOKENIZERS_PARALLELISM='false',OMP_NUM_THREADS='1')
        from src.experiments.cell_budget import CellBudgetLedger
        from src.experiments.attempt_ledger import AttemptBudget
        physical=CellBudgetLedger.create(ROOT/'cpu-native-budget.sqlite',AttemptBudget('DaftMMQAQ3a-CPU-physical',25),deadline_utc=__import__('time').time()+900)
        physical.reserve_unit('native-generate',25);physical.claim_shared_unit('native-generate')
        os.environ.update(DAFT_PHYSICAL_BUDGET_PATH=str(ROOT/'cpu-native-budget.sqlite'),DAFT_PHYSICAL_BUDGET_ID='DaftMMQAQ3a-CPU-physical',DAFT_PHYSICAL_UNIT='native-generate')
        model_python=os.environ['CASE_MODEL_PYTHON']
        for name,python in [('prepare_cpu',sys.executable),('model_sdk_cpu',model_python),('finish_cpu',sys.executable)]:
            report=api.supervise([python,'-u','-B',str(ROOT/(name+'.py'))],ROOT/(name+'-supervision'),
                timeout_s=180,grace_s=8,natural_exit_grace_s=4,monitor=monitor)
            owned.update(host.report_identities(report))
            host.private_json(ROOT/(name+'-SUPERVISOR.json'),report)
            assert report['worker_exit']==0
            steps.append(name)
        assert json.loads((ROOT/'CPU-END-TO-END-RESULT.json').read_text())['status']=='passed'
    except BaseException as error:
        report=getattr(error,'report',None)
        if report:
            owned.update(host.report_identities(report));host.private_json(ROOT/'CPU-SUPERVISOR-ERROR.json',report)
        failure=dict(type=type(error).__name__,message=str(error),traceback=traceback.format_exc())
    finally:
        if host.live_owned(api,owned):host.cleanup_owned(api,owned,20)
        live=host.live_owned(api,owned);resource_report=monitor.finish()
        host.private_json(ROOT/'CPU-RESOURCES.json',resource_report)
        after=host.snapshot(api,resources,'Daft-native-MMQAQ3a-CPU-after',owned);host.private_json(ROOT/'CPU-after.json',after)
        idle_error=None
        try:host.assert_idle(after,resources)
        except Exception as error:idle_error=str(error)
        clean=not live and idle_error is None and resource_report['passed']
        if clean:host.check_lease(lease);lease.release()
        host.private_json(ROOT/'CPU-FINAL.json',dict(status='passed' if failure is None and clean else 'failed',
            failure=failure,steps=steps,real_model_calls=0,real_Ray=0,GPU=0,remaining_owned=live,
            resource_passed=resource_report['passed'],physical_idle=clean,idle_error=idle_error,lease_released=clean,
            scope='Real Daft CPU collection and original MMQAQ3a source/sink with recorded engine stand-in'))


if __name__=='__main__':
    main()
