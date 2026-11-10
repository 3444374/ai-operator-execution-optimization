"""Thin supervised one-cold-Q7 entrypoint; no lease or scheduling replacement."""
import argparse
from contextlib import contextmanager
import gzip
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def event(root, kind, **fields):
    value = dict(event=kind, monotonic_ns=time.monotonic_ns(), pid=os.getpid(), **fields)
    with (root / 'runner-events.jsonl').open('a') as stream:
        stream.write(json.dumps(value) + '\n')
    return value['monotonic_ns']


@contextmanager
def observed_builder():
    import ray.data.llm
    from ray._private.runtime_env.setup_hook import export_setup_func_module
    original = ray.data.llm.build_processor
    def build(*args, **kwargs):
        processor = original(*args, **kwargs)
        engines = 0
        for name in processor.list_stage_names():
            stage = processor.get_stage_by_name(name)
            if type(stage).__name__ != 'vLLMEngineStage':
                continue
            engines += 1
            stage.map_batches_kwargs.update(max_restarts=0, max_task_retries=0)
            env = dict(stage.map_batches_kwargs.get('runtime_env') or {})
            keys = ('Q7_SOURCE', 'Q7_OUTPUT', 'Q7_BUDGET_ID', 'Q7_CONFIG')
            env['env_vars'] = {**env.get('env_vars', {}), **{k: os.environ[k] for k in keys},
                               'PYTHONPATH': str(Path(os.environ['Q7_SOURCE']) / 'code')}
            hook = 'src.baselines.image.ray_animals_q7.native_probe.native_observer_setup'
            env['worker_process_setup_hook'] = hook
            stage.map_batches_kwargs['runtime_env'] = export_setup_func_module(env, hook)
        if engines != 1:
            raise ValueError('Expected exactly one native engine stage')
        return processor
    ray.data.llm.build_processor = build
    try:
        yield
    finally:
        ray.data.llm.build_processor = original


def native_stage(root, stage, bound):
    import ray
    from .native_query import execute
    import importlib.metadata
    config = json.loads(Path(os.environ['Q7_CONFIG']).read_text())
    versions = {k: importlib.metadata.version(k) for k in ('ray', 'vllm', 'torch', 'transformers')}
    expected = {'official-ray-0.22': '0.22.0', 'unified-candidate-0.25.1': '0.25.1'}
    if versions['ray'] != '2.56.1' or versions['vllm'] != expected[config['runtime_profile']]:
        raise ValueError('Native runtime does not match selected profile')
    event(root, 'native_worker_entered', stage=stage, versions=versions)
    try:
        ray.init(address='local', num_cpus=bound['num_cpus'], num_gpus=bound['num_gpus'],
                 object_store_memory=bound['object_store_memory'], include_dashboard=False,
                 _temp_dir=str(Path(os.environ['RAY_TMPDIR']) / 'ray'))
        with gzip.open(root / f'input-{stage}.json.gz', 'rt') as stream:
            rows = json.load(stream)
        with observed_builder():
            event(root, 'native_pipeline_entered', stage=stage)
            predictions = execute(rows, bound['model'])
        event(root, 'native_pipeline_returned', stage=stage)
        (root / f'predictions-{stage}.json').write_text(json.dumps(predictions))
    finally:
        event(root, 'public_ray_close_started', stage=stage)
        ray.shutdown()
        event(root, 'public_ray_close_finished', stage=stage, disconnected=not ray.is_initialized())


def run_query(root, bound, supervise_stage=None):
    if supervise_stage is None:
        raise RuntimeError('Per-stage host supervision and physical-exit capability required; direct launch disabled')
    from .binding import run
    from src.baselines.image.sembench_animals import AnimalsDataset
    manifest = Path(bound['manifest'])
    dataset = AnimalsDataset(Path(bound['checkout']), manifest, bound['manifest_digest'])
    stage = 0
    def execute(rows):
        nonlocal stage
        current = stage
        stage += 1
        with gzip.open(root / f'input-{current}.json.gz', 'wt') as stream:
            json.dump(rows, stream)
        event(root, 'stage_worker_launch', stage=current)
        receipt = supervise_stage([bound['native_python'], '-m', __package__ + '.runner', str(root),
                                   '--native-stage', str(current)], current, root)
        # Only the existing host supervisor can establish descendant/GPU/SHM exit.
        required = ('worker_exit_zero', 'physical_idle', 'resource_passed', 'owned_processes_exited', 'private_sockets_clear')
        if not isinstance(receipt, dict) or any(receipt.get(key) is not True for key in required):
            event(root, 'stage_exit_unconfirmed', stage=current)
            raise RuntimeError('Native stage physical exit unconfirmed; retain outputs and stop Q7')
        (root / f'lifecycle-{current}.json').write_text(json.dumps(receipt, indent=2))
        event(root, 'stage_physical_exit_confirmed', stage=current)
        return json.loads((root / f'predictions-{current}.json').read_text())
    query_entered = event(root, 'fresh_source_query_entered')
    result, quality, passes, timing = run(dataset, execute)
    report = dict(status='completed', result=result.to_dict(orient='split'), quality=quality,
                  passes=passes, timing=dict(query_entered_monotonic_ns=query_entered, **timing),
                  controller_entry_monotonic_ns=None, first_engine_accept_monotonic_ns=None,
                  lifecycle='two sequential native workers; each owns a fresh Ray runtime/processor; no shared-engine claim',
                  request_span_is_GPU_time=False, warmup=0, retries=0, logical_occurrences=128)
    event(root, 'report_persistence_started')
    (root / 'QUERY.json').write_text(json.dumps(report, indent=2))
    event(root, 'report_persistence_finished')
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    parser.add_argument('--native-stage', type=int, choices=(0, 1))
    args = parser.parse_args()
    root = args.output.resolve()
    event(root, 'runner_entered', stage=args.native_stage)
    bound = json.loads((root / 'RUN-BOUND.json').read_text())
    if bound['physical_attempt_limit'] != 128 or not (root / 'logical-unit.sqlite').is_file():
        raise ValueError('Supervisor must reserve and claim one shared 128-call unit before launch')
    os.environ.update(Q7_SOURCE=bound['source'], Q7_OUTPUT=str(root),
                      Q7_BUDGET_ID=bound['budget_id'], Q7_CONFIG=bound['config'])
    if args.native_stage is None:
        from .host_adapter import from_bound
        run_query(root, bound, supervise_stage=from_bound(bound))
    else:
        native_stage(root, args.native_stage, bound)


if __name__ == '__main__':
    main()
