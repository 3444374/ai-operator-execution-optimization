"""One native MMQAQ3a cold query, using the existing host supervision and prepaid unit."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import time
import traceback

ROOT = Path(__file__).resolve().parent
SOURCE = Path(os.environ['CASE_SOURCE'])
OUTPUT = Path(os.environ['CASE_OUTPUT'])
TEMP = Path(os.environ['CASE_TEMP'])

def main():
    config_path = ROOT / 'CONFIG.json'
    frozen=json.loads((ROOT/'FROZEN.json').read_text())
    for name,digest in frozen['files_sha256'].items():
        assert hashlib.sha256((ROOT/name).read_bytes()).hexdigest()==digest,name
    for path,digest in frozen['external_files_sha256'].items():
        assert hashlib.sha256(Path(path).read_bytes()).hexdigest()==digest,path
    assert json.loads((ROOT/'CPU-FINAL.json').read_text())['status']=='passed'
    config = json.loads(config_path.read_text())
    assert not OUTPUT.exists() and not (ROOT / 'STARTED.json').exists() and not (ROOT / 'FINAL.json').exists()
    base = subprocess.check_output(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD'], text=True).strip()
    tree = subprocess.check_output(['git', '-C', str(SOURCE), 'write-tree'], text=True).strip()
    assert base == '1d35348134683ffbd3c0f3c9b3718ad032439ab5'
    assert tree == '10c6560c643a47a1905c058c130766e78d1b5a94'
    assert not subprocess.check_output(['git', '-C', str(SOURCE), 'diff', '--name-only'], text=True)
    helper = Path(os.environ['CASE_HOST_HELPER'])
    assert hashlib.sha256(helper.read_bytes()).hexdigest() == '340bb1acf01b8e3e8faded3f21bc11177bfb49a31b670a31e101f2568d733760'
    spec = importlib.util.spec_from_file_location('native_q4_host', helper)
    host = importlib.util.module_from_spec(spec); spec.loader.exec_module(host)
    api = host.production_api(SOURCE / 'code')
    from src.observability.process_resources.supervised import ResourceMonitor
    from src.experiments.cell_budget import CellBudgetLedger
    from src.experiments.attempt_ledger import AttemptBudget
    resource_path = Path(os.environ['CASE_RESOURCES'])
    assert hashlib.sha256(resource_path.read_bytes()).hexdigest() == 'acd1f3fe6fae8dea71b3887334aabd508a02b3850686bf17479bd6e0a0a86a23'
    resources = json.loads(resource_path.read_text())
    host.private_json(ROOT / 'RESOURCE-CONFIG.json', resources)
    os.sched_setaffinity(0, set(resources['shared_resources']['cpu_affinity_slots']))
    before = host.snapshot(api, resources, 'native-MMQAQ3a-before')
    host.private_json(ROOT / 'before.json', before); host.assert_idle(before, resources)
    lease = api.acquire_runner_lease(Path(os.environ['CASE_LEASE_SCOPE']), config_fingerprint=api.HOST_SCOPE_FINGERPRINT, repository_commit=base, recover_stale=False)
    owned = set(); report = {}; failure = None; monitor = None; ledger = None
    try:
        (TEMP).mkdir()
        os.chdir(SOURCE)
        os.environ.update(PYTHONHASHSEED='3',PYTHONPATH=str(SOURCE / 'code'), CUDA_VISIBLE_DEVICES='0',
            HF_HUB_OFFLINE='1', TRANSFORMERS_OFFLINE='1', LITELLM_LOCAL_MODEL_COST_MAP='True',
            PYTHONDONTWRITEBYTECODE='1', TMPDIR=str(TEMP), VLLM_RPC_BASE_PATH=str(TEMP),
            VLLM_CACHE_ROOT=str(Path(os.environ['CASE_CACHE_ROOT'])/'vllm'),
            TRITON_CACHE_DIR=str(Path(os.environ['CASE_CACHE_ROOT'])/'triton'),
            TORCHINDUCTOR_CACHE_DIR=str(Path(os.environ['CASE_CACHE_ROOT'])/'torchinductor'),
            CUDA_CACHE_PATH=str(Path(os.environ['CASE_CACHE_ROOT'])/'cuda'),
            TORCH_HOME=str(Path(os.environ['CASE_CACHE_ROOT'])/'torch'),
            MPLCONFIGDIR=str(Path(os.environ['CASE_CACHE_ROOT'])/'matplotlib'))
        ledger = CellBudgetLedger.create(ROOT / 'logical-unit.sqlite', AttemptBudget('native-daft-mmqa-q3a-cold-1109-r1', 25), deadline_utc=time.time() + 3600)
        ledger.reserve_unit(config['query_id'], 25); ledger.claim_unit(config['query_id'])
        physical=CellBudgetLedger.create(ROOT/'physical-native.sqlite',AttemptBudget(config['query_id']+'-physical',25),deadline_utc=time.time()+3600)
        physical.reserve_unit('native-generate',25);physical.claim_shared_unit('native-generate')
        os.environ.update(DAFT_PHYSICAL_BUDGET_PATH=str(ROOT/'physical-native.sqlite'),DAFT_PHYSICAL_BUDGET_ID=config['query_id']+'-physical',DAFT_PHYSICAL_UNIT='native-generate')
        host.private_json(ROOT / 'STARTED.json', dict(started_monotonic_ns=time.monotonic_ns(), source_base=base, source_tree=tree,
            matching_candidate='1d353481-qualified-runtime-plus-native-MMQAQ3a-binding', budget=ledger.snapshot(), warmup=0,
            whole_query_retries=0, expected_native_requests=25, hard_native_physical_cap=True,
            baseline_name='Daft native AI prompt / independent cold', semantic_operator='MMQAQ3a taken_3 full25 sentiment Map then ordinaryvalue_counts', execution_backend='Daft Native vllm-prefix-caching; driver original formatting/parser/sink',
            native_engine_overrides=config['engine_kwargs'], provider_overrides=config['provider_options']))
        command = [os.environ['CASE_DRIVER_PYTHON'], '-u', '-B',
            str(ROOT/'native_daft_mmqa_q3a.py'), '--config', str(config_path),
            '--model-python', str(ROOT/'model-python.sh'), '--output-dir', str(OUTPUT)]
        monitor = ResourceMonitor(resources['shared_resources'])
        report = api.supervise(command, ROOT / 'supervision', timeout_s=3600, grace_s=8,
            natural_exit_grace_s=4, monitor=monitor)
        assert report['worker_exit']==0
        driver=json.loads((OUTPUT/'driver.json').read_text())
        assert driver['status']=='completed'
        native=driver['native_api']
        assert (native['generate_calls'],native['accepted'],native['final_events'],native['unresolved'])==(25,25,25,0)
        from physical_budget import unit
        assert unit().attempts==25 and unit().remaining==0
        host.private_json(ROOT/'PHYSICAL-BUDGET-FINAL.json',dict(limit=25,pre_generate_reservations=unit().attempts,remaining=unit().remaining,native_generate=native['generate_calls'],native_final=native['final_events']))
    except BaseException as error:
        report = getattr(error, 'report', report)
        failure = dict(type=type(error).__name__, message=str(error), traceback=traceback.format_exc())
    finally:
        owned |= host.report_identities(report)
        if host.live_owned(api, owned): host.cleanup_owned(api, owned, 20)
        live = host.live_owned(api, owned)
        resource_report = monitor.finish() if monitor is not None else None
        host.private_json(ROOT / 'SUPERVISOR.json', report)
        host.private_json(ROOT / 'RESOURCES.json', resource_report)
        after = host.snapshot(api, resources, 'native-MMQAQ3a-after', owned)
        host.private_json(ROOT / 'after.json', after)
        gpu_processes = subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid,used_memory', '--format=csv,noheader,nounits'], text=True).strip()
        socket_cleanup = None
        socket_helper = Path(os.environ['CASE_SOCKET_HELPER'])
        if not live and not gpu_processes and (TEMP).exists():
            assert hashlib.sha256(socket_helper.read_bytes()).hexdigest() == '64bb48944f91524eea254a7f7097175165af9d128fd1b459fc6a830be15b7577'
            temp_spec = importlib.util.spec_from_file_location('native_q4_temp', socket_helper)
            temp_module = importlib.util.module_from_spec(temp_spec); temp_spec.loader.exec_module(temp_module)
            socket_cleanup = temp_module.cleanup_inactive_socket_nodes(TEMP, Path('/proc/net/unix').read_text())
        sockets = [str(p.relative_to(TEMP)) for p in (TEMP).rglob('*') if p.is_socket()]
        shm_clear = resource_report is None or (not resource_report['remaining_new_shm_names'] and resource_report['final_host_shm_used_delta_bytes'] == 0)
        cleanup_confirmed = not live and not gpu_processes and not sockets and shm_clear
        released = False
        if cleanup_confirmed: host.check_lease(lease); lease.release(); released = True
        final = dict(status='completed' if failure is None and resource_report is not None and resource_report['passed'] and cleanup_confirmed else 'failed',
            failure=failure, remaining_owned=live, GPU_compute_processes=gpu_processes,
            resource_passed=None if resource_report is None else resource_report['passed'],
            remaining_private_sockets=sockets, socket_cleanup=socket_cleanup,
            cleanup_confirmed=cleanup_confirmed, lease_released=released,
            logical_budget=None if ledger is None else ledger.snapshot(),
            actual_native_attempts='derive from native sidecar and terminal receipts; not inferred from logical claim',
            source_base=base, source_tree=tree, matching_candidate='1d353481-qualified-runtime-plus-native-MMQAQ3a-binding',
            completed_monotonic_ns=time.monotonic_ns(), no_whole_query_retry=True)
        host.private_json(ROOT / 'FINAL.json', final)
        print(json.dumps(final))

if __name__ == '__main__':
    main()
