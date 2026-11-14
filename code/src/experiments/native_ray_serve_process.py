"""One isolated native Serve caller, with driver-owned tasks and evaluation.

Only native request batches cross this process boundary. Existing Ray Serve
owns inference and the existing records supervisor owns process-tree cleanup.
"""

import argparse
from copy import deepcopy
import json
import math
from pathlib import Path
import threading
import time

from src.infrastructure.private_artifacts import (
    new_private_directory, publish_private_json, read_json,
)
from src.infrastructure.redact import redact_text


def _read_packet(path):
    # These exclusive private packets already have a finite actual file size.
    # The generic small-JSON default must not truncate legitimate image batches.
    return read_json(path, max_bytes=path.stat().st_size)


def _owner_summary(owner):
    return {key: value for key, value in owner.report.items() if key != 'batches'}


def serve(config_path, *, native_factory=None):
    from src.baselines.text.frameworks.ray_native_serve_session import NativeServeSession

    config = _read_packet(Path(config_path))
    directory = Path(config['directory'])
    owner = (native_factory or NativeServeSession)(**config['native_options'])
    failure = None
    try:
        owner.start()
        publish_private_json(directory/'ready.json', dict(status='ready', owner=_owner_summary(owner)))
        index = 1
        while True:
            path = directory/('command-'+str(index)+'.json')
            while not path.exists():
                if (directory/'stop').exists():
                    break
                if time.monotonic() >= config['work_deadline']:
                    raise TimeoutError('native Serve session exceeded its work allowance')
                time.sleep(.01)
            if not path.exists():
                break
            if (directory/'stop').exists():
                break
            if time.monotonic() >= config['work_deadline']:
                raise TimeoutError('native Serve work allowance expired before command acceptance')
            command = _read_packet(path)
            if command['op'] == 'close':
                break
            if command['op'] != 'predict':
                raise ValueError('native Serve process accepts predict or close only')
            batch = owner.predict(command['requests'], sampling_params=command['sampling_params'])
            publish_private_json(directory/('reply-'+str(index)+'.json'), dict(status='completed', batch=batch))
            index += 1
    except BaseException as error:
        failure = error
    finally:
        try:
            owner.close()
        except BaseException as error:
            if failure is None:
                failure = error
            else:
                failure.add_note('native Serve close also failed: '+type(error).__name__)
        publish_private_json(directory/'terminal.json', dict(
            status='closed' if failure is None else 'failed', owner=_owner_summary(owner),
            error_type=None if failure is None else type(failure).__name__,
            error=None if failure is None else redact_text(str(failure)),
            notes=[] if failure is None else [redact_text(note) for note in getattr(failure, '__notes__', [])]))
    if failure is not None:
        raise failure


class NativeServeProcess:
    """A single persistent native caller; no semantic or batching policy."""

    def __init__(self, *, model_python, directory, startup_timeout_s, session_timeout_s,
                 cleanup_s, native_options, supervise=None):
        from src.baselines.text.frameworks.ray_native_serve_session import check_options

        check_options(native_options['runtime_options'], native_options['processor_options'])
        for name, value in (('startup_timeout_s', startup_timeout_s), ('session_timeout_s', session_timeout_s),
                            ('cleanup_s', cleanup_s)):
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError(name+' must be finite and positive')
        if cleanup_s >= session_timeout_s:
            raise ValueError('native Serve cleanup must fit inside its session allowance')
        self.model_python, self.directory = model_python, Path(directory)
        self.startup_timeout_s, self.session_timeout_s, self.cleanup_s = startup_timeout_s, session_timeout_s, cleanup_s
        self.native_options, self.supervise = deepcopy(native_options), supervise
        self.report = dict(profile='native-ray-serve-isolated-caller', events={}, batches=[],
            caller_profile='isolated-native-caller', caller_python=model_python,
            evaluation_in_driver=True, physical_gpu_exit_confirmed=None)
        self.sequence, self.thread, self.failure, self.closed = 0, None, None, False
        self.done = threading.Event()
        self._predict_lock = threading.Lock()

    def _command(self, path):
        return ['env', 'PYTHONPATH='+str(Path(__file__).resolve().parents[2]),
                self.model_python, '-B', '-m', __name__, '--serve', str(path)]

    def _wait(self, path, deadline):
        while True:
            if path.exists():
                return _read_packet(path)
            terminal = self.directory/'terminal.json'
            if terminal.exists():
                value = _read_packet(terminal)
                self.report.update(value['owner'], native_caller_terminal=value)
                if value['status'] == 'closed':
                    raise EOFError('native Serve caller closed before its complete reply')
                raise RuntimeError('native Serve caller '+str(value['error_type'])+': '+str(value['error']))
            if self.done.is_set():
                if self.failure is not None:
                    raise self.failure
                raise EOFError('native Serve caller exited before its complete reply')
            if time.monotonic() >= deadline:
                raise TimeoutError('native Serve caller did not reply within its allowance')
            time.sleep(.01)

    def start(self):
        if self.thread is not None or self.closed:
            raise RuntimeError('native Serve caller cannot restart')
        from src.experiments.lotus_records_supervisor import supervise

        new_private_directory(self.directory)
        started = time.monotonic()
        self.deadline = started+self.session_timeout_s
        self.work_deadline = self.deadline-min(self.cleanup_s, self.session_timeout_s/3)
        path = self.directory/'native-config.json'
        publish_private_json(path, dict(directory=str(self.directory), native_options=self.native_options,
                                       work_deadline=self.work_deadline))
        def run():
            try:
                self.report['supervision'] = (self.supervise or supervise)(self._command(path),
                    self.directory/'supervision', timeout_s=self.session_timeout_s, grace_s=self.cleanup_s,
                    natural_exit_grace_s=self.cleanup_s)
            except BaseException as error:
                self.failure = error
                if hasattr(error, 'report'):
                    self.report['supervision'] = error.report
            finally:
                self.done.set()
        self.thread = threading.Thread(target=run, name='native-ray-serve-caller')
        self.thread.start()
        ready = self._wait(self.directory/'ready.json', min(self.work_deadline, started+self.startup_timeout_s))
        self.report.update(ready['owner'])
        self.report['native_caller_startup_seconds'] = time.monotonic()-started

    def predict(self, requests, *, sampling_params):
        # Original LOTUS groupby threads share this one sequential IPC caller.
        with self._predict_lock:
            if self.thread is None or self.closed:
                raise RuntimeError('native Serve caller is not open')
            if time.monotonic() >= self.work_deadline:
                raise TimeoutError('native Serve work allowance expired before submission')
            self.sequence += 1
            publish_private_json(self.directory/('command-'+str(self.sequence)+'.json'),
                dict(op='predict', requests=requests, sampling_params=sampling_params))
            value = self._wait(self.directory/('reply-'+str(self.sequence)+'.json'), self.work_deadline)
            self.report['batches'].append(value['batch'])
            return value['batch']

    def identity_snapshot(self):
        """Fresh host identities, bound to the native actor constructor event."""
        import psutil

        output = Path(json.loads(self.native_options['model_runtime_env']['env_vars']['SEMLOOM_RAY_NATIVE_OBSERVER'])['output'])
        events = [json.loads(line) for path in output.glob('native-events-*.jsonl') for line in path.read_text().splitlines()]
        actors = {tuple(row.get(key) for key in ('pid', 'node_id', 'model_actor_id', 'model_actor_create_time'))
                  for row in events if row['event'] == 'native_constructor_returned'}
        value = dict(observed_monotonic_ns=time.monotonic_ns(), model_processes=[],
                     scope='native actor context plus fresh host PID/birth; not cache or weight identity')
        if len(actors) != 1:
            return value
        pid, node, actor, birth = next(iter(actors))
        if not all((pid, node, actor, birth)):
            return value
        try:
            actor_process = psutil.Process(pid)
            if actor_process.create_time() != birth:
                return value
            value.update(node_id=node, model_actor_id=actor, model_actor_pid=pid, model_actor_create_time=birth)
            for child in actor_process.children(recursive=True):
                command = child.cmdline()
                if command and command[0].startswith('VLLM::EngineCore'):
                    value['model_processes'].append(dict(pid=child.pid, create_time=child.create_time()))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            value['model_processes'] = []
        value['observed_monotonic_ns'] = time.monotonic_ns()
        return value

    def close(self):
        if self.closed:
            return
        self.closed = True
        if self.thread is None:
            return
        # Stop is also visible when a failed command left no usable reply.
        (self.directory/'stop').touch(mode=0o600, exist_ok=False)
        self.sequence += 1
        publish_private_json(self.directory/('command-'+str(self.sequence)+'.json'), dict(op='close'))
        self.thread.join(timeout=max(0, self.deadline-time.monotonic()))
        terminal = self.directory/'terminal.json'
        if terminal.exists():
            value = _read_packet(terminal)
            self.report.update(value['owner'], native_caller_terminal=value)
        else:
            value = None
        if self.thread.is_alive():
            raise TimeoutError('native Serve caller cleanup did not finish within its session allowance')
        if value is not None and value['status'] != 'closed':
            raise RuntimeError('native Serve caller '+str(value['error_type'])+': '+str(value['error']))
        if self.failure is not None:
            raise self.failure
        if value is None:
            raise RuntimeError('native Serve caller did not confirm native close')


def _serve_attempt_window(directory, unit, first_attempt, before, predictions=None):
    """Match API allowance to complete native responses, after the query sink."""
    from src.baselines.text.frameworks.ray_native_observer import summarize

    expected = list(range(first_attempt+before, first_attempt+unit.attempts))
    associations = []
    if predictions is not None:
        response_ids = [row['response_id'] for row in predictions]
        if len(predictions) != len(expected) or len(set(response_ids)) != len(response_ids):
            raise RuntimeError('Serve complete API response IDs do not cover the charged invocations')
        associations = [dict(call_id=row['call_id'], response_id=row['response_id']) for row in predictions]
    return dict(attempts=expected, response_associations=associations,
        complete_native_API_results=None if predictions is None else len(predictions),
        completion_source='complete non-streaming native API responses; not an independent engine-output trace'), summarize(directory)


def _owner_snapshot(snapshot):
    if snapshot is None:
        return None, None
    before = time.monotonic_ns()
    value = snapshot()
    after = time.monotonic_ns()
    fields = {'observed_monotonic_ns', 'node_id', 'model_actor_id', 'model_actor_pid',
              'model_actor_create_time', 'model_processes'}
    if not isinstance(value, dict) or not fields <= value.keys():
        return value, None
    children = value['model_processes']
    if (not before <= value['observed_monotonic_ns'] <= after
            or not value['node_id'] or not value['model_actor_id']
            or type(value['model_actor_pid']) is not int or value['model_actor_pid'] <= 0
            or not isinstance(children, list) or not children):
        return value, None
    processes = [(value['model_actor_pid'], value['model_actor_create_time'])]
    for child in children:
        if not isinstance(child, dict) or not {'pid', 'create_time'} <= child.keys():
            return value, None
        processes.append((child['pid'], child['create_time']))
    if any(type(pid) is not int or pid <= 0 or type(birth) not in (int, float)
           or not math.isfinite(birth) or birth <= 0 for pid, birth in processes):
        return value, None
    return value, (value['node_id'], value['model_actor_id'], processes[0], tuple(sorted(processes[1:])))


def _serve_constructor_events(directory):
    events = [json.loads(line) for path in Path(directory).glob('native-events-*.jsonl')
              for line in path.read_text().splitlines()]
    return sorted((row['monotonic_ns'], row.get('pid'), row['event']) for row in events
        if row['event'] in ('native_constructor_started', 'native_constructor_returned'))


def _native_serve_process(config, directory, runtime_env):
    options = {key: config[key] for key in ('model', 'model_source', 'app_name',
        'deployment_name', 'engine_kwargs', 'processor_options', 'runtime_options')}
    return NativeServeProcess(model_python=config['model_python'], directory=directory,
        startup_timeout_s=config['startup_timeout_s'], session_timeout_s=config['session_timeout_s'],
        cleanup_s=config['cleanup_s'], native_options=dict(options, model_runtime_env=runtime_env))


def run_serve_queries(config, directory, *, prepare_queries, metadata,
                      session_factory=None, identity_snapshot=None):
    """Run fresh complete queries on one ready native Serve deployment.

    Query callbacks retain source, semantics, sink, evaluation and evidence.
    This shared Ray lifecycle owns only the model, allowance and identity proof.
    """
    from .attempt_ledger import AttemptBudget
    from .cell_budget import CellBudgetLedger
    from src.infrastructure.private_artifacts import write_private_json

    directory = Path(directory).resolve()
    new_private_directory(directory)
    report = dict(status='failed',
        execution_owner='Ray Data ServeDeploymentProcessor and owned Ray Serve deployment',
        target_native_model_qualified=None, performance_claim=False, queries=[],
        native_output_artifacts=False, native_output_evidence='complete native API response',
        model_owner_reused=None, warm_qualified=None, extra_query_warmup=0,
        warm_qualification_scope='requires fresh native model actor and actual EngineCore subprocess identities plus '
            'unchanged native constructor events; software fixtures do not qualify a target model',
        native_startup_attempts=None, worker_entry_ns=time.monotonic_ns(),
        timing_scope='deployment startup separately measured; each fresh source, native Serve graph, '
            'dependent batches, native complete response checks, strict task parsing and full memory sink measured directly; API allowance '
            'and necessary cross-environment batch IPC included; no per-request success persistence; post-sink audits/scoring/persistence '
            'and final deployment/caller close excluded')
    report.update(metadata)
    primary = ledger = unit = owner = current = None

    def failed(error, phase):
        nonlocal primary
        report['status'] = 'failed'
        report['warm_qualified'] = None
        if current is not None:
            current['status'] = 'failed'
            current.pop('quality', None)
            current.setdefault('error_type', type(error).__name__)
        if primary is None:
            primary = error
            report.update(error_type=type(error).__name__, error=redact_text(str(error)))
        elif primary is not error:
            primary.add_note(phase+' also failed: '+type(error).__name__)

    try:
        queries = prepare_queries()
        value = config['budget']
        ledger = CellBudgetLedger(Path(value['path']), AttemptBudget(value['id'], value['limit']))
        unit = ledger.claim_shared_unit(value['unit_id'])
        first = next(row['first_attempt'] for row in ledger.snapshot()['units'] if row['unit_id'] == value['unit_id'])
        runtime_env = dict(py_executable=config['model_python'], env_vars=dict(
            PYTHONPATH=str(Path(__file__).parents[2]),
            SEMLOOM_RAY_NATIVE_OBSERVER=json.dumps(dict(value, output=str(directory),
                observe_model_actor_identity=True))))
        if session_factory is None:
            owner = _native_serve_process(config, directory/'native-caller', runtime_env)
        else:
            owner = session_factory(model=config['model'], model_source=config['model_source'],
                app_name=config['app_name'], deployment_name=config['deployment_name'],
                engine_kwargs=config['engine_kwargs'], processor_options=config['processor_options'],
                runtime_options=config['runtime_options'], model_runtime_env=runtime_env)
        owner.start()
        if identity_snapshot is None:
            identity_snapshot = getattr(owner, 'identity_snapshot', None)
        report['native'] = owner.report
        report['native_startup_attempts'], initial = _serve_attempt_window(directory, unit, first, 0)
        constructors = initial['model_constructor_returned_ns']
        if not initial['available'] or not constructors:
            raise RuntimeError('Serve startup did not establish its mandatory native observer')
        constructor_events = _serve_constructor_events(directory)
        baseline_raw, baseline_identity = _owner_snapshot(identity_snapshot)
        report['initial_model_owner'] = baseline_raw
        report['deployment_startup_seconds'] = owner.report['deployment_startup_seconds']
        report['native_caller_startup_seconds'] = owner.report.get('native_caller_startup_seconds')
        identity_complete = baseline_identity is not None
        accounted = unit.attempts
        for index, execute_query in enumerate(queries):
            current = dict(round=index+1, phase='ready-first' if index == 0 else 'same-owner-repeat',
                status='failed', result=None, model_ready_before_source=None,
                query_clock_version='native_ray_serve.source_to_sink.v2')
            report['queries'].append(current)
            phase = directory/('round-'+str(index+1))
            new_private_directory(phase)
            before = unit.attempts
            try:
                if before != accounted or _serve_constructor_events(directory) != constructor_events:
                    raise RuntimeError('Serve submitted new work or constructed a model between complete queries')
                raw_identity, actual_identity = _owner_snapshot(identity_snapshot)
                current['owner_before_source'] = raw_identity
                identity_complete = identity_complete and actual_identity is not None
                if baseline_identity is not None and actual_identity is not None and actual_identity != baseline_identity:
                    raise RuntimeError('Serve model owner changed before fresh source')
                predictions = execute_query(owner, unit, current, phase)
                current.setdefault('post_sink_audit_started_ns', time.monotonic_ns())
                current['native_observation'], observed = _serve_attempt_window(directory, unit, first, before, predictions)
                accounted = unit.attempts
                if len(predictions) != current['logical_requests']:
                    raise RuntimeError('Serve complete predictions differ from task occurrences')
                if _serve_constructor_events(directory) != constructor_events:
                    raise RuntimeError('Serve constructed a new native model during the session')
                raw_identity, actual_identity = _owner_snapshot(identity_snapshot)
                current['model_owner'] = raw_identity
                identity_complete = identity_complete and actual_identity is not None
                if baseline_identity is not None and actual_identity is not None and actual_identity != baseline_identity:
                    raise RuntimeError('Serve model owner changed during the session')
                current['model_ready_before_source'] = (
                    owner.report['events']['deployment_ready_ns'] <= current['source_started_ns']
                    and max(constructors) <= current['source_started_ns'])
                current['status'] = 'completed'
                if index == 1:
                    report['model_owner_reused'] = (True if identity_complete and actual_identity == baseline_identity
                        and current['logical_requests'] and report['queries'][0]['logical_requests'] else None)
                    report['warm_qualified'] = report['model_owner_reused']
            except BaseException as error:
                failed(error, 'query')
            if primary is not None:
                raise primary
        report['status'] = 'completed'
    except BaseException as error:
        failed(error, 'session')
    finally:
        if unit is not None:
            try:
                ledger.close_shared_unit(config['budget']['unit_id'])
                report['physical_attempts_reserved'] = unit.attempts
                report['permanent_budget'] = ledger.snapshot()
            except BaseException as error:
                failed(error, 'budget close')
        if owner is not None:
            report['native'] = owner.report
            try:
                owner.close()
            except BaseException as error:
                failed(error, 'owned Serve close')
        for query in report['queries']:
            try:
                write_private_json(directory/('round-'+str(query['round']))/'query.json', query)
            except BaseException as error:
                failed(error, 'query report persistence')
        try:
            write_private_json(directory/'session.json', report)
        except BaseException as error:
            failed(error, 'session evidence persistence')
    if primary is not None:
        primary.query_report = report
        raise primary
    return report



def check_sdk(config):
    """Import and validate the native caller config, without starting Ray/model."""
    import sys
    from importlib.metadata import version
    from src.baselines.text.frameworks.ray_native_serve_session import _load_native, _input, check_options

    check_options(config['runtime_options'], config['processor_options'])
    sdk = _load_native()
    if sdk.ray.__version__ != '2.56.1':
        raise ValueError('the native Serve caller is fixed to Ray 2.56.1')
    sdk.LLMConfig(model_loading_config=dict(model_id=config['model'], model_source=config['model_source']),
        engine_kwargs=config['engine_kwargs'], runtime_env=dict(py_executable=config['model_python']),
        callback_config=dict(callback_class=sdk.NativeObserverCallback),
        deployment_config=dict(name=config['deployment_name'], num_replicas=1))
    sdk.ServeDeploymentProcessorConfig(app_name=config['app_name'], deployment_name=config['deployment_name'],
        dtype_mapping={'ChatCompletionRequest': sdk.ChatCompletionRequest}, should_continue_on_error=False,
        **config['processor_options'])
    request = _input(dict(messages=[dict(role='user', content='configuration check only')]),
                     model=config['model'], sampling_params=config['sampling_params'])
    sdk.ChatCompletionRequest(**request['request_kwargs'])
    torch = sys.modules.get('torch')
    return dict(status='checked_native_caller_config_only', python=sys.executable,
        versions={name: version(name) for name in ('ray', 'vllm')},
        ray_runtime_started=sdk.ray.is_initialized(), CUDA_initialized=None if torch is None else torch.cuda.is_initialized(),
        model_calls=0, model_constructed=False, evaluation_dependencies_required=False,
        scope='native public Serve/processor/request configuration; not deployment or model qualification')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('--serve')
    action.add_argument('--check-sdk')
    args = parser.parse_args(argv)
    if args.check_sdk:
        print(json.dumps(check_sdk(_read_packet(Path(args.check_sdk))), ensure_ascii=False, allow_nan=False))
    else:
        serve(args.serve)


if __name__ == '__main__':
    main()
