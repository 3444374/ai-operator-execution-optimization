"""Sequential native method queries with group-owned reusable runtimes."""
import argparse
from contextlib import contextmanager, ExitStack
from dataclasses import asdict, replace
import json
import hashlib
from pathlib import Path
import threading
import time
import uuid

from src.baselines.common.private_artifacts import new_private_directory, open_private_text, write_private_json
from src.baselines.common.redact import redact_json_values
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import load_fixed_model_config
from src.execution_provider.adapters.native_tasks import build_native_execution
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.buffered_events import BufferedEvents
from src.experiments.shared_request_budget import CellBudgetLedger
from src.observability.request_gateway import GatewayRoute, ObservationGateway
from src.scheduling.core.session_contract import SessionTimeouts
from .cell_evidence import CellErrors
from .native_adapter_query import ARMS, _runtime, run_prepared_map_query
from .supplier_adapter_query import (
    SUPPLIER_ARMS, _drain_execution, prepare_duckdb_connection, prepare_lotus_lm,
    replace_duckdb_inputs, run_supplier_query,
)


class _ArmOwner:
    def __init__(self, group, arm):
        self.group, self.arm = group, arm
        self.identity = uuid.uuid4().hex
        self.root = group.root / 'owners' / arm
        new_private_directory(self.root)
        self.stack = ExitStack()
        self.events = self.stack.enter_context(BufferedEvents(self.root / 'owner-events.jsonl'))
        self.binding = self.service = None
        self.execution = self.lm = self.connection = self.native = None
        self.physical = group.physical if 'semloom' in arm and not arm.endswith('diagnostic') else None
        self.gateway = self.stack.enter_context(ObservationGateway(
            routes=(GatewayRoute(self.identity, 'model', group.model.endpoint_url),),
            trace_path=self.root / 'http-trace.jsonl',
            request_timeout_s=min(group.query_timeout_s, group.model.timeout_ms/1000),
            before_forward=self.before, after_forward=self.after,
            query_identity=lambda: self.binding['unit_id'] if self.binding else None))
        token = group.model.bearer_token
        if arm.startswith('lotus-'):
            token = token or 'local-fixture'
        elif arm.startswith('duckdb-'):
            token = token or 'EMPTY'
        self.model = replace(group.model, endpoint_url=self.gateway.endpoint_url(self.identity, 'model'),
                             bearer_token=token)
        try:
            if 'semloom' in arm and not arm.startswith('sema-'):
                self.execution = build_native_execution(self.model, physical=self.physical,
                    max_tasks=group.options.concurrency, max_active_requests=group.options.concurrency,
                    observer=self.observe, timeouts=SessionTimeouts(backend_s=max(45, group.query_timeout_s)))
            if arm.startswith('lotus-'):
                self.lotus_lm()
        except BaseException:
            self.stack.close()
            raise
        self.started_ns = time.monotonic_ns()
        self.queries = 0

    def before(self, route, body):
        if self.binding is None:
            raise RuntimeError('persistent owner has no active query')
        return self.binding['before'](route, body)

    def after(self, route, body, response, status):
        if self.binding is None:
            raise RuntimeError('persistent owner lost its response consumer')
        return self.binding['after'](route, body, response, status)

    def observe(self, event):
        value = dict(event, monotonic_ns=time.monotonic_ns())
        value['query_id'] = self.binding['unit_id'] if self.binding else None
        self.events.record(value)
        if self.binding is not None:
            self.binding['record'](event)
        if self.service is not None:
            self.service._observe(event)

    def lifecycle(self):
        return dict(owner_id=self.identity, ray_session_id=self.group.ray_session_id,
            execution_id=None if self.execution is None else str(id(self.execution)),
            lm_id=None if self.lm is None else str(id(self.lm)),
            duckdb_connection_id=None if self.connection is None else str(id(self.connection)),
            sema_pid=None if self.native is None else self.native.pid,
            completed_queries=self.queries, result_cache=False,
            core_usage=None if self.execution is None else asdict(self.execution.engine.capacity.usage()),
            core_jobs=None if self.execution is None else len(self.execution.engine.jobs.jobs),
            native_ray_actor_scope=('original Ray Data graph creates its own actors' if self.arm=='fixed-map-native-ray'
                                    else 'not a native Ray Data graph'),
            request_service_scope=('per query, including Sema service Core and workers' if self.arm.startswith('sema-')
                                   else 'no Sema request service'),
            native_http_client_scope='supplier owns its SDK/client lifecycle')

    @contextmanager
    def query(self, unit_id, root, before, after, record):
        if self.binding is not None:
            raise RuntimeError('one persistent arm cannot overlap query consumers')
        self.binding = dict(unit_id=unit_id, before=before, after=after, record=record)
        errors = CellErrors()
        primary = None
        try:
            yield self
        except BaseException as error:
            primary = error
            raise
        finally:
            if self.execution is not None:
                errors.attempt('query_core_drain', lambda: _drain_execution(self.execution, close=False))
            traces = errors.attempt('query_http_drain', lambda: self.gateway.snapshot(
                self.group.model.timeout_ms/1000 + 1))
            if traces is not None:
                selected = [row for row in traces if row.get('query_id') == unit_id]
                def save_trace():
                    with open_private_text(Path(root) / 'http-trace.jsonl') as stream:
                        for row in selected:
                            stream.write(json.dumps(redact_json_values(row), sort_keys=True) + '\n')
                errors.attempt('query_trace_write', save_trace)
            self.binding = self.service = None
            if errors.first is not None:
                self.group.poisoned = True
                if primary is not None:
                    primary.add_note('Persistent query cleanup also failed: ' + type(errors.first).__name__)
                else:
                    errors.raise_if_failed()

    def lotus_lm(self):
        if self.lm is None:
            self.lm = prepare_lotus_lm(self.group.plan, self.model, self.group.options, self.group.tokenizer_path)
        return self.lm

    def duckdb_connection(self, values):
        if self.connection is None:
            self.connection = prepare_duckdb_connection(self.stack, self.group.plan, self.model,
                                                       self.group.options, self.group.duckdb_library)
        replace_duckdb_inputs(self.connection, values, self.group.plan)
        return self.connection

    def sema_native(self, values):
        if self.native is None:
            from src.baselines.text.products.sema import prepare_projection
            root = self.root / 'native'
            new_private_directory(root)
            self.native = self.stack.enter_context(prepare_projection(values, self.group.plan, self.model,
                binary=self.group.sema_binary, root=root, num_threads=self.group.options.concurrency))
        return self.native

    def close(self):
        errors = CellErrors()
        if self.execution is not None:
            errors.attempt('owner_execution_close', lambda: _drain_execution(self.execution))
        errors.attempt('owner_components_close', self.stack.close)
        write_private_json(self.root / 'owner-summary.json', redact_json_values(dict(
            lifecycle=self.lifecycle(), cleanup_errors=errors.details, closed_ns=time.monotonic_ns())))
        errors.raise_if_failed()


class PersistentAdapterGroup:
    """At most three comparison arms share one driver; each arm owns its backend."""
    def __init__(self, arms, *, plan, model, ledger, root, options=NativeGraphOptions(), physical=None,
                 ray_temp_root=None, ray_address=None, query_timeout_s=120, owner_timeout_s=1800,
                 stages=None, tokenizer_path=None, duckdb_library=None, sema_binary=None):
        self.arms = tuple(arms)
        if (not 1 <= len(self.arms) <= 3 or len(set(self.arms)) != len(self.arms)
                or any(arm not in ARMS+SUPPLIER_ARMS for arm in self.arms)):
            raise ValueError('persistent measurement requires one to three distinct integrated arms')
        if sum('semloom' in arm for arm in self.arms) > 2:
            raise ValueError('persistent group supports at most two SemLoom backends')
        if not 0 < query_timeout_s <= 120 or not query_timeout_s+120 < owner_timeout_s <= 1800:
            raise ValueError('query and owner durations must leave 120 seconds for final cleanup')
        self.plan, self.model, self.ledger = plan, model, ledger
        self.root, self.options, self.physical = Path(root), options, physical
        self.ray_temp_root, self.ray_address = ray_temp_root, ray_address
        self.query_timeout_s, self.owner_timeout_s = query_timeout_s, owner_timeout_s
        self.stages, self.tokenizer_path = stages, tokenizer_path
        self.duckdb_library, self.sema_binary = duckdb_library, sema_binary
        self.stack = ExitStack()
        self.owners, self.used_units = {}, set()
        self.poisoned = False
        self.ray_session_id = None
        self.runtime = dict(owner='persistent group', source='finite raw input')
        self._lock = threading.Lock()

    def __enter__(self):
        self.started_ns = time.monotonic_ns()
        self.deadline = time.monotonic()+self.owner_timeout_s
        new_private_directory(self.root)
        try:
            if 'fixed-map-native-daft' in self.arms:
                _runtime(self.stack, 'fixed-map-native-daft', self.options, None, None, None)
            core = any('semloom' in arm and not arm.endswith('diagnostic') for arm in self.arms)
            if core or 'fixed-map-native-ray' in self.arms:
                if core and self.physical is None:
                    raise ValueError('persistent SemLoom requires the existing physical configuration')
                self.physical, self.runtime = _runtime(self.stack,
                    'fixed-map-semloom' if core else 'fixed-map-native-ray', self.options,
                    self.physical, self.ray_temp_root, self.ray_address)
                self.ray_session_id = uuid.uuid4().hex
            for arm in self.arms:
                owner = _ArmOwner(self, arm)
                self.owners[arm] = owner
                self.stack.callback(owner.close)
            self.ready_ns = time.monotonic_ns()
            write_private_json(self.root/'startup.json', redact_json_values(dict(
                schema='semloom.persistent_adapter_group.v1', arms=self.arms, started_ns=self.started_ns,
                ready_ns=self.ready_ns, startup_seconds=(self.ready_ns-self.started_ns)/1e9,
                runtime=self.runtime, ray_session_id=self.ray_session_id,
                owner_timeout_s=self.owner_timeout_s, cleanup_reserve_s=120,
                hard_timeout_owner='calling process supervisor', result_cache=False)))
            return self
        except BaseException:
            self.stack.close()
            raise

    def run(self, arm, *, unit_id, root, load_source, phase, reference_outputs=None, allowed_outputs=None):
        if phase not in ('qualification', 'warmup', 'measurement'):
            raise ValueError('query phase must be explicitly declared')
        with self._lock:
            if self.poisoned or arm not in self.owners or unit_id in self.used_units:
                raise RuntimeError('persistent group stopped or query identity already used')
            if time.monotonic()+self.query_timeout_s+120 >= self.deadline:
                raise TimeoutError('persistent group cannot fit another query and final cleanup')
            self.used_units.add(unit_id)
            owner = self.owners[arm]
            resources_before = self.ray_resources()
            arguments = dict(load_source=load_source, plan=self.plan, model=self.model, ledger=self.ledger,
                unit_id=unit_id, root=root, options=self.options, physical=owner.physical,
                query_timeout_s=self.query_timeout_s, reference_outputs=reference_outputs,
                allowed_outputs=allowed_outputs, owner=owner)
            try:
                if arm in SUPPLIER_ARMS:
                    result = run_supplier_query(arm, **arguments, stages=self.stages,
                        tokenizer_path=self.tokenizer_path, duckdb_library=self.duckdb_library,
                        sema_binary=self.sema_binary)
                else:
                    result = run_prepared_map_query(arm, **arguments)
                owner.queries += 1
                result['persistent_lifecycle'] = owner.lifecycle()
                result['measurement_phase'] = phase
                write_private_json(Path(root)/'persistent-query.json', redact_json_values(dict(
                    schema='semloom.persistent_adapter_query.v1', unit_id=unit_id, arm=arm,
                    measurement_phase=phase, persistent_lifecycle=owner.lifecycle(),
                    query_summary_sha256=hashlib.sha256((Path(root)/'summary.json').read_bytes()).hexdigest(),
                    t_release_ns=result['query_preparation_started_ns'],
                    t_submit_ns=result['execution']['t_submit_ns'],
                    t_eof_ns=result['execution']['t_query_terminal_ns'],
                    release_query_seconds=result['full_query_seconds'],
                    release_scope='raw input reading, query preparation, formatter, graph, materialization and consumer EOF',
                    submit_scope=result['execution']['ready_query_timing_scope'],
                    actual_posts=result['actual_posts'], rows=result['rows'])))
                write_private_json(Path(root)/'runtime-resources.json', dict(
                    before_query=resources_before, after_query=self.ray_resources(),
                    scope='same group Ray cluster; other persistent arm actors retain their declared CPUs',
                    resource_matching=('dedicated arm Ray cluster' if len(self.arms)==1 else
                                       'shared deployment recorded; not a dedicated per-arm cluster')))
                return result
            except BaseException as failure:
                self.poisoned = True
                summary_path = Path(root)/'summary.json'
                sidecar = Path(root)/'persistent-query.json'
                if summary_path.is_file() and not sidecar.exists():
                    try:
                        write_private_json(sidecar, dict(
                            schema='semloom.persistent_adapter_query.v1', status='failed',
                            unit_id=unit_id, arm=arm, measurement_phase=phase,
                            persistent_lifecycle=owner.lifecycle(),
                            query_summary_sha256=hashlib.sha256(summary_path.read_bytes()).hexdigest()))
                    except BaseException as observation:
                        failure.add_note('Persistent query metadata also failed: '+type(observation).__name__)
                raise

    def ray_resources(self):
        if self.ray_session_id is None:
            return dict(status='unavailable', reason='this group uses no Ray runtime')
        import ray
        keys=('CPU','GPU','object_store_memory')
        total, available=ray.cluster_resources(), ray.available_resources()
        return dict(cluster={key:total.get(key,0) for key in keys},
                    available={key:available.get(key,0) for key in keys},
                    semloom_actor_pools=[dict(arm=arm,workers=owner.physical.workers,
                                             declared_cpus=owner.physical.workers)
                        for arm,owner in self.owners.items() if owner.physical is not None and owner.execution is not None])

    def __exit__(self, error_type, error, traceback):
        errors = CellErrors()
        errors.attempt('group_close', self.stack.close)
        write_private_json(self.root/'group-summary.json', redact_json_values(dict(
            status='failed' if error is not None or self.poisoned or errors.first is not None else 'passed',
            queries=len(self.used_units), poisoned=self.poisoned,
            ended_ns=time.monotonic_ns(), cleanup_errors=errors.details,
            owners={arm:owner.lifecycle() for arm,owner in self.owners.items()})))
        if errors.first is not None:
            if error is not None:
                error.add_note('Persistent group cleanup also failed: '+type(errors.first).__name__)
            else:
                errors.raise_if_failed()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('schedule','plan','model','budget','output'):
        parser.add_argument('--'+name, type=Path, required=True)
    parser.add_argument('--budget-id', required=True)
    parser.add_argument('--max-attempts', type=int, required=True)
    for name in ('options','ray-physical','ray-temp-root','stages','tokenizer','duckdb-library','sema-binary'):
        parser.add_argument('--'+name, type=Path)
    parser.add_argument('--ray-address')
    parser.add_argument('--allowed-output', action='append')
    parser.add_argument('--query-timeout-s', type=float, default=120)
    parser.add_argument('--owner-timeout-s', type=float, default=1800)
    args = parser.parse_args(argv)
    schedule = json.loads(args.schedule.read_text())
    if not isinstance(schedule,list) or not 1 <= len(schedule) <= 195:
        parser.error('schedule must explicitly list at most 195 queries')
    for item in schedule:
        if (not isinstance(item,dict) or set(item)-{'arm','unit_id','input','phase','references'}
                or not {'arm','unit_id','input','phase'} <= set(item)
                or item['phase'] not in ('qualification','warmup','measurement')):
            parser.error('schedule entries require arm, unit_id, raw input and phase')
    arms = tuple(dict.fromkeys(item['arm'] for item in schedule))
    options = NativeGraphOptions(**json.loads(args.options.read_text())) if args.options else NativeGraphOptions()
    ledger = CellBudgetLedger(args.budget, AttemptBudget(args.budget_id,args.max_attempts))
    with PersistentAdapterGroup(arms, plan=SemanticMapPlan(**json.loads(args.plan.read_text())),
            model=load_fixed_model_config(args.model), ledger=ledger, root=args.output, options=options,
            physical=RayMapConfig.load(args.ray_physical) if args.ray_physical else None,
            ray_temp_root=args.ray_temp_root, ray_address=args.ray_address,
            stages=json.loads(args.stages.read_text()) if args.stages else None,
            tokenizer_path=args.tokenizer, duckdb_library=args.duckdb_library, sema_binary=args.sema_binary,
            query_timeout_s=args.query_timeout_s, owner_timeout_s=args.owner_timeout_s) as group:
        write_private_json(group.root/'schedule.json',schedule)
        for ordinal, item in enumerate(schedule):
            source = Path(item['input'])
            if source.stat().st_size > 64*1024*1024:
                raise ValueError('raw input file exceeds its declared byte allowance')
            def load_source():
                with source.open() as stream:
                    for line in stream:
                        yield json.loads(line)
            group.run(item['arm'], unit_id=item['unit_id'], root=group.root/('query-'+str(ordinal)),
                load_source=load_source, phase=item['phase'], allowed_outputs=args.allowed_output,
                reference_outputs=json.loads(Path(item['references']).read_text()) if item.get('references') else None)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
