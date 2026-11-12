"""Shared supervised Animals image Filter queries; no scheduling replacement."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import gzip
import json
import os
from pathlib import Path
import time

from src.infrastructure.private_artifacts import publish_private_json, write_private_json
from src.infrastructure.redact import redact_text


FINAL_RELEASE = 'final-stage-release.json'


def wait_for_final_release(root, timeout_s):
    """Hold the materialized native owner until the original complete sink exists."""
    deadline = time.monotonic() + timeout_s
    path = root / FINAL_RELEASE
    while not path.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError('Animals Filter final owner did not receive a result handoff within its stage budget')
        time.sleep(.01)
    value = json.loads(path.read_text())
    if value.get('action') != 'complete' or type(value.get('sink_ready_monotonic_ns')) is not int:
        raise RuntimeError('Animals Filter original query aborted before its final owner was released')
    observed = time.monotonic_ns()
    if not 0 <= value['sink_ready_monotonic_ns'] <= observed:
        raise ValueError('Animals Filter sink clock follows native release acceptance')
    publish_private_json(root / 'final-stage-handoff.json', dict(**value,
        release_observed_monotonic_ns=observed, worker_pid=os.getpid()))


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
    from .binding import filter_passes, rows_per_pass
    last_stage = filter_passes(bound.get('query', 7)) - 1
    if not 0 <= stage <= last_stage:
        raise ValueError('native stage is outside the original query Filter passes')
    versions = {k: importlib.metadata.version(k) for k in ('ray', 'vllm', 'torch', 'transformers')}
    expected = {'official-ray-0.22': '0.22.0', 'unified-candidate-0.25.1': '0.25.1'}
    if versions['ray'] != '2.56.1' or versions['vllm'] != expected[config['runtime_profile']]:
        raise ValueError('Native runtime does not match selected profile')
    if (root / f'predictions-{stage}.json').exists() or (stage == last_stage and (root / FINAL_RELEASE).exists()):
        raise ValueError('Animals Filter native stage refuses stale prediction or release evidence')
    event(root, 'native_worker_entered', stage=stage, versions=versions)
    failure = None
    try:
        ray.init(address='local', num_cpus=bound['num_cpus'], num_gpus=bound['num_gpus'],
                 object_store_memory=bound['object_store_memory'], include_dashboard=False,
                 _temp_dir=str(Path(os.environ['RAY_TMPDIR']) / 'ray'))
        with gzip.open(root / f'input-{stage}.json.gz', 'rt') as stream:
            rows = json.load(stream)
        if len(rows) != rows_per_pass(bound['physical_attempt_limit'], bound.get('query', 7)):
            raise ValueError('Native Animals Filter stage differs from the frozen complete source size')
        with observed_builder():
            event(root, 'native_pipeline_entered', stage=stage)
            predictions = execute(rows, bound['model'])
        event(root, 'native_pipeline_returned', stage=stage)
        publish_private_json(root / f'predictions-{stage}.json', predictions)
        if stage == last_stage:
            wait_for_final_release(root, bound['host_adapter']['stage_timeout_s'])
    except BaseException as error:
        failure = error
        raise
    finally:
        close_started = time.monotonic_ns()
        try:
            try:
                event(root, 'public_ray_close_started', stage=stage)
            finally:
                ray.shutdown()
            close_finished = time.monotonic_ns()
            event(root, 'public_ray_close_finished', stage=stage, disconnected=not ray.is_initialized())
            publish_private_json(root / f'native-close-{stage}.json', dict(
                started_monotonic_ns=close_started, finished_monotonic_ns=close_finished,
                disconnected=not ray.is_initialized()))
        except BaseException as closing:
            if failure is None:
                raise
            failure.add_note('native close also failed: ' + redact_text(str(closing)))


def run_query(root, bound, supervise_stage=None):
    if supervise_stage is None:
        raise RuntimeError('Per-stage host supervision and physical-exit capability required; direct launch disabled')
    from .binding import run, filter_passes
    from src.baselines.image.sembench_animals import AnimalsDataset
    stale = [name for name in (FINAL_RELEASE, 'QUERY.json', 'QUERY-failure.json',
        'input-0.json.gz', 'input-1.json.gz', 'predictions-0.json', 'predictions-1.json',
        'final-stage-handoff.json', 'native-close-0.json', 'native-close-1.json') if (root / name).exists()]
    if stale:
        raise ValueError('Animals Filter requires fresh query paths; existing evidence: ' + ', '.join(stale))
    query_entered = event(root, 'fresh_source_query_entered')
    manifest = Path(bound['manifest'])
    dataset = AnimalsDataset(Path(bound['checkout']), manifest, bound['manifest_digest'])
    query_options = {'query_id': bound['query']} if 'query' in bound else {}
    last_stage = filter_passes(bound.get('query', 7)) - 1
    stage, final_future, pool, frame, failure = 0, None, None, None, None
    report = dict(status='failed', query=bound.get('query', 7), result=None, quality=None, passes=[],
        timing=dict(query_entered_monotonic_ns=query_entered, sink_ready_monotonic_ns=None,
            source_to_sink_seconds=None), controller_entry_monotonic_ns=None,
        first_engine_accept_monotonic_ns=None, stage_lifecycle=[],
        lifecycle='native worker per original Filter; final owner released after complete original query sink',
        source_to_sink_scope='fresh source, native startups, required transport and nonfinal-stage cleanup; excludes final teardown, audit, evaluation and persistence',
        request_span_is_GPU_time=False, warmup=0, retries=0,
        logical_occurrences=bound['physical_attempt_limit'])

    def confirm(receipt, current):
        required = ('worker_exit_zero', 'physical_idle', 'resource_passed', 'owned_processes_exited', 'private_sockets_clear')
        if not isinstance(receipt, dict) or any(receipt.get(key) is not True for key in required):
            event(root, 'stage_exit_unconfirmed', stage=current)
            raise RuntimeError('Native stage physical exit unconfirmed; retain outputs and stop the query')
        write_private_json(root / f'lifecycle-{current}.json', receipt)
        report['stage_lifecycle'].append(dict(stage=current, receipt=receipt))
        event(root, 'stage_physical_exit_confirmed', stage=current)

    def execute(rows):
        nonlocal stage, final_future, pool
        current = stage
        stage += 1
        with gzip.open(root / f'input-{current}.json.gz', 'wt') as stream:
            json.dump(rows, stream)
        event(root, 'stage_worker_launch', stage=current)
        command = [bound['native_python'], '-m', __package__ + '.runner', str(root),
                   '--native-stage', str(current)]
        if current < last_stage:
            # The second model must not start until the first lifecycle is clean.
            confirm(supervise_stage(command, current, root), current)
        elif current == last_stage:
            deadline = time.monotonic() + bound['host_adapter']['stage_timeout_s']
            pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='q7-final-supervisor')
            final_future = pool.submit(supervise_stage, command, current, root)
            prediction = root / f'predictions-{current}.json'
            while not prediction.exists():
                if final_future.done():
                    final_future.result()  # Preserve the original supervisor failure.
                    raise RuntimeError('Final native stage exited before publishing its predictions')
                if time.monotonic() >= deadline:
                    raise TimeoutError('Animals Filter final predictions exceeded the existing stage budget')
                time.sleep(.01)
            if final_future.done():
                final_future.result()
                raise RuntimeError('Final native owner exited before the complete original query sink')
        else:
            raise ValueError('cannot dispatch more than the original query Filter passes')
        return json.loads((root / f'predictions-{current}.json').read_text())

    def accept(result, sink_ready):
        nonlocal frame, final_future
        frame = result
        report['timing'].update(sink_ready_monotonic_ns=sink_ready,
            source_to_sink_seconds=(sink_ready-query_entered)/1e9)
        if final_future is None or final_future.done():
            if final_future is not None:
                final_future.result()
            raise RuntimeError('Animals Filter final owner is not held at the complete original query sink')
        event(root, 'full_memory_sink_ready', sink_ready_monotonic_ns=sink_ready)
        publish_private_json(root / FINAL_RELEASE, dict(action='complete', sink_ready_monotonic_ns=sink_ready))
        future = final_future
        final_future = None
        receipt = future.result()
        confirm(receipt, last_stage)
        handoff = json.loads((root / 'final-stage-handoff.json').read_text())
        close = json.loads((root / f'native-close-{last_stage}.json').read_text())
        if (handoff['sink_ready_monotonic_ns'] != sink_ready or
                not sink_ready <= handoff['release_observed_monotonic_ns'] <= close['started_monotonic_ns']):
            raise ValueError('Final native release/close does not follow the original query sink')
        report['timing'].update(final_release_observed_monotonic_ns=handoff['release_observed_monotonic_ns'],
            final_close_started_monotonic_ns=close['started_monotonic_ns'],
            final_close_finished_monotonic_ns=close['finished_monotonic_ns'])

    try:
        result, quality, passes, timing = run(dataset, execute, on_result_ready=accept,
            physical_attempt_limit=bound['physical_attempt_limit'], **query_options)
        if report['timing']['sink_ready_monotonic_ns'] is None:
            raise RuntimeError('Original Animals Filter query omitted its complete memory-sink callback')
        report.update(status='completed', quality=quality, passes=passes)
        report['timing'].update(timing)
    except BaseException as error:
        failure = error
    finally:
        if final_future is not None:
            try:
                if not (root / FINAL_RELEASE).exists():
                    publish_private_json(root / FINAL_RELEASE, dict(action='abort'))
                confirm(final_future.result(), 1)
            except BaseException as closing:
                if failure is None:
                    failure = closing
                else:
                    failure.add_note('final supervision also failed: ' + redact_text(str(closing)))
        if pool is not None:
            pool.shutdown(wait=True)
    try:
        if frame is not None:
            report['timing']['serialization_started_monotonic_ns'] = time.monotonic_ns()
            report['result'] = frame.to_dict(orient='split')
            report['timing']['serialization_finished_monotonic_ns'] = time.monotonic_ns()
    except BaseException as serializing:
        if failure is None:
            failure = serializing
        else:
            failure.add_note('result encoding also failed: ' + redact_text(str(serializing)))
    if failure is not None:
        report.update(status='failed', failure=dict(type=type(failure).__name__, message=redact_text(str(failure))))
        failure.query_report = report
    try:
        event(root, 'report_persistence_started')
        write_private_json(root / ('QUERY.json' if failure is None else 'QUERY-failure.json'), report)
        event(root, 'report_persistence_finished')
    except BaseException as recording:
        if failure is None:
            recording.query_report = dict(report, status='failed')
            raise
        failure.add_note('query recording also failed: ' + redact_text(str(recording)))
    if failure is not None:
        raise failure
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path)
    parser.add_argument('--native-stage', type=int, choices=(0, 1))
    args = parser.parse_args()
    root = args.output.resolve()
    event(root, 'runner_entered', stage=args.native_stage)
    bound = json.loads((root / 'RUN-BOUND.json').read_text())
    from .binding import rows_per_pass
    rows_per_pass(bound['physical_attempt_limit'], bound.get('query', 7))
    if not (root / 'logical-unit.sqlite').is_file():
        raise ValueError('Supervisor must reserve and claim the complete shared query unit before launch')
    os.environ.update(Q7_SOURCE=bound['source'], Q7_OUTPUT=str(root),
                      Q7_BUDGET_ID=bound['budget_id'], Q7_CONFIG=bound['config'])
    if args.native_stage is None:
        from .host_adapter import from_bound
        run_query(root, bound, supervise_stage=from_bound(bound))
    else:
        native_stage(root, args.native_stage, bound)


if __name__ == '__main__':
    main()
