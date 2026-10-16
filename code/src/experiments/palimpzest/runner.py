"""Public Movie entry: original PZ system or one selected fixed-plan backend.

Model services, Ray clusters and author Coordinators are caller-owned and ready.
This runner owns each query's fresh plan, executor/progress, gateway and SDK stack.
"""
import argparse
from dataclasses import asdict, replace
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from src.baselines.common.private_artifacts import new_private_directory, write_private_json
from src.baselines.common.redact import redact_json_values, redact_text
from src.baselines.text.frameworks.imlane_calls import IMLaneCallExecutor
from src.baselines.text.frameworks.native_calls import NativeCallExecutor
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.ray_map_transport import RayMapConfig
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.buffered_events import BufferedEvents
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.native_http_observer import observe_native_httpx
from src.experiments.postgresql.imlane_query import load_imlane_call_configs
from src.infrastructure.runner_lease import HOST_SCOPE_FINGERPRINT, acquire_runner_lease
from src.observability.request_gateway import GatewayRoute, ObservationGateway
from src.semantic_methods.palimpzest.query_execution import NativeAdaptedExecutor, PreparedRequestRoute, QueryExecutionLimits
from . import native
from .fixed import ADAPTED_ARMS, ORIGINAL_NATIVE_ARM, BackendRunProfile, FixedEnvironment, FixedMovieProfile, _RUN_ID, select_fixed_movie, verify_fixed_libraries
from .fixed_runner import execute_fixed_movie
from .ledger import PalimpzestLedger
from .model import fetch_model_metadata


def fixed_settings(config):
    selected = config['fixed_execution']
    required = {'arm', 'run_id', 'environment', 'backend_profile', 'limits', 'backend_options'}
    if (type(selected) is not dict or set(selected) != required or config['query_id'] != 3
            or selected['arm'] not in (ORIGINAL_NATIVE_ARM, *ADAPTED_ARMS)
            or type(selected['run_id']) is not str or not _RUN_ID.fullmatch(selected['run_id'])
            or type(selected['backend_options']) is not dict):
        raise ValueError('fixed PZ configuration requires one supported Q3 arm and explicit run settings')
    environment = FixedEnvironment(**selected['environment'])
    profile = BackendRunProfile(**selected['backend_profile'])
    arm, options = selected['arm'], selected['backend_options']
    limits = None if selected['limits'] is None else QueryExecutionLimits(**selected['limits'])
    if arm == ORIGINAL_NATIVE_ARM:
        if limits is not None or options:
            raise ValueError('original-native uses only the original PZ controls')
    elif limits is None or limits.pz_workers != native.NativeOptions(**config['native_options']).max_workers:
        raise ValueError('fixed preparation limits must match the original PZ worker identity')
    graph, imlane, physical, address = None, None, None, None
    if arm == 'native-adapted':
        if (set(options) != {'max_workers'} or type(options['max_workers']) is not int
                or not 1 <= options['max_workers'] <= 256):
            raise ValueError('native-adapted requires its own explicit worker count')
    elif arm in ('daft', 'ray-data', 'imlane-cpython', 'imlane-ray'):
        extra = {'ray_address_env'} if arm == 'ray-data' else {'call_config_env'} if arm.startswith('imlane-') else set()
        if set(options) != {'graph_options'} | extra:
            raise ValueError('fixed native backend requires its existing graph/runtime settings')
        graph = NativeGraphOptions(**options['graph_options'])
        graph.validate_backend('imlane' if arm.startswith('imlane-') else arm)
        if arm == 'ray-data':
            name = options['ray_address_env']
            address = os.environ.get(name) if type(name) is str else None
        elif arm.startswith('imlane-'):
            runtime = arm.removeprefix('imlane-')
            imlane = load_imlane_call_configs({runtime: native._env_path(options['call_config_env'])})[runtime]
            if (limits.max_batch_rows > imlane.max_batch_rows or limits.max_batch_bytes > imlane.max_input_payload_bytes
                    or limits.max_batch_rows * limits.max_response_bytes > imlane.max_batch_response_bytes):
                raise ValueError('fixed preparation snapshot exceeds the declared IMLane call capacity')
    elif arm == 'semloom':
        if (set(options) != {'max_active_requests', 'physical_config_env'}
                or type(options['max_active_requests']) is not int or options['max_active_requests'] < 1):
            raise ValueError('public SemLoom execution requires explicit physical configuration and request capacity')
        physical = RayMapConfig.load(native._env_path(options['physical_config_env']))
        if (physical.response_mode != 'native-call-full' or physical.payload_backend != 'daft'
                or physical.preparation is not None or physical.worker_pool is not None):
            raise ValueError('public PZ SemLoom requires query-owned native-call-full Daft/Ray workers')
        address = physical.address
    if (arm in ('ray-data', 'semloom') and (type(address) is not str or not address or address in ('auto', 'local'))):
        raise ValueError('fixed PZ requires an explicit caller-owned Ray cluster address')
    if limits is not None and limits.max_batch_rows * limits.max_response_bytes > limits.max_batch_bytes:
        raise ValueError('fixed backend byte allowance must also hold all responses in one ready snapshot')
    return dict(environment=environment, profile=profile, limits=limits, graph=graph,
                imlane=imlane, physical=physical, address=address)


def preflight(config, *, libraries=True):
    prepared = native.preflight(config, libraries=libraries)
    if config['schema'] == 'semloom.sembench_palimpzest_fixed.v1':
        settings = fixed_settings(config)
        evidence = prepared[-1]
        evidence.update(fixed_execution=config['fixed_execution'],
            execution_scope='one selected original Q3 physical plan; no system optimizer comparison',
            backend_runtime_owner='caller-owned ready model service / Ray cluster / author Coordinator',
            physical_config=asdict(settings['physical']) if settings['physical'] is not None else None,
            imlane_config=asdict(settings['imlane']) if settings['imlane'] is not None else None)
        if libraries:
            evidence['versions'] = verify_fixed_libraries()
    return prepared


def _executor(fresh, binding, selected, settings, route, record):
    arm, options, limits = selected['arm'], selected['backend_options'], settings['limits']
    if arm == ORIGINAL_NATIVE_ARM:
        return None
    if arm == 'native-adapted':
        return NativeAdaptedExecutor(max_workers=options['max_workers'], max_batch_rows=limits.max_batch_rows,
                                    max_batch_bytes=limits.max_batch_bytes)
    if arm in ('daft', 'ray-data'):
        return NativeCallExecutor(arm, settings['graph'], max_batch_rows=limits.max_batch_rows,
                                  max_batch_bytes=limits.max_batch_bytes)
    if arm.startswith('imlane-'):
        return IMLaneCallExecutor(settings['imlane'], settings['graph'], max_batch_rows=limits.max_batch_rows,
                                  max_batch_bytes=limits.max_batch_bytes)
    from src.semantic_methods.palimpzest.semloom import SemLoomProgress
    return SemLoomProgress(replace(binding.model, endpoint_url=route.effective_endpoint),
        query_id=fresh.run_id, operator_ids=fresh.semantic_operator_ids, limits=limits,
        max_active_requests=options['max_active_requests'], physical=settings['physical'], observer=record)


def run_fixed(config):
    preparation_started = time.monotonic_ns()
    dataset, rows, pz_root, binding, output, host_scope, evidence = preflight(config)
    settings, selected = fixed_settings(config), config['fixed_execution']
    commit = subprocess.check_output(['git', '-C', str(Path(__file__).resolve().parents[4]),
                                      'rev-parse', 'HEAD'], text=True).strip()
    lease = acquire_runner_lease(host_scope, config_fingerprint=HOST_SCOPE_FINGERPRINT,
                                repository_commit=commit, recover_stale=False)
    lease.__enter__()
    owner, summary, execution = {'lease': lease, 'closed_resources': []}, dict(schema='semloom.sembench_palimpzest_fixed_run.v1',
        status='failed', repository_commit=commit, arm=selected['arm'],
        input_preparation_seconds=(time.monotonic_ns()-preparation_started)/1e9,
        service_startup_seconds=None, service_startup_scope='external ready services; startup not measured by this runner',
        performance_qualified=False, backend_qualified=False, model_quality_qualified=False), {}
    ledger, cell, budget, pending, failure, created = None, None, None, False, None, False
    try:
        new_private_directory(output)
        created = True
        write_private_json(output/'preflight.json', redact_json_values(evidence))
        write_private_json(output/'config.json', redact_json_values(config))
        write_private_json(output/'started.json', redact_json_values(summary))
        cell = CellBudgetLedger.create(output/'budget.sqlite', AttemptBudget('palimpzest-fixed-movie', config['max_attempts']),
                                       deadline_utc=time.time()+config['query_timeout_s'])
        cell.reserve_unit(selected['run_id'], config['max_attempts'])
        budget = cell.claim_unit(selected['run_id'])
        verified = fetch_model_metadata(binding)
        fixed = select_fixed_movie(dataset, pz_root, binding, verified, environment=settings['environment'],
            profile=FixedMovieProfile(native.NativeOptions(**config['native_options'])))
        write_private_json(output/'selection.json', redact_json_values(fixed.describe()))
        summary['selection_accounting'] = fixed.selection_accounting
        query_started = time.monotonic_ns()
        execution['query_submitted_ns'] = query_started
        owner['events'] = events = BufferedEvents(output/'events.jsonl')
        ledger = PalimpzestLedger(max_calls=config['max_attempts'], record=events.record)
        fresh = fixed.fresh(selected['run_id'])
        if settings['address'] is not None:
            import ray
            owner['ray'] = ray
            owner['ray_connection_owned'] = not ray.is_initialized()
            if owner['ray_connection_owned']:
                ray.init(address=settings['address'])
            if ray.get_runtime_context().gcs_address != settings['address']:
                raise ValueError('connected Ray runtime differs from the explicit fixed backend address')
        route = None
        if selected['arm'] != ORIGINAL_NATIVE_ARM:
            registered = GatewayRoute(fresh.run_id, 'model', binding.model.endpoint_url)
            def before(_route, body):
                budget.reserve(hashlib.sha256(body).hexdigest())
            owner['gateway'] = gateway = ObservationGateway(routes=(registered,), preserve_http=True,
                trace_path=output/'http-trace.jsonl', request_timeout_s=binding.model.timeout_ms/1000,
                before_forward=before)
            gateway.start()
            route = PreparedRequestRoute.from_gateway(gateway, registered)
        owner['executor'] = executor = _executor(fresh, binding, selected, settings, route, events.record)
        if selected['arm'].startswith('imlane-'):
            executor.start()
        with native._private_working_directory(output):
            if selected['arm'] == ORIGINAL_NATIVE_ARM:
                with observe_native_httpx(budget, events.record, binding.model.endpoint_url, timeout_s=config['query_timeout_s']):
                    result = execute_fixed_movie(fresh, arm=selected['arm'], backend_profile=settings['profile'],
                        ledger=ledger, evidence=execution, query_started_ns=query_started, owner=owner)
            else:
                result = execute_fixed_movie(fresh, arm=selected['arm'], backend_profile=settings['profile'],
                    executor=executor, limits=settings['limits'], ledger=ledger, record=events.record,
                    cancelled=lambda: time.monotonic_ns()-query_started >= config['query_timeout_s']*1e9,
                    evidence=execution, request_route=route, query_started_ns=query_started, owner=owner)
        summary['status'] = result.evidence['status']
        from palimpzest.core.elements.records import DataRecordCollection
        write_private_json(output/'original-result.json', json.loads(DataRecordCollection(result.records).to_df().to_json(orient='split')))
    except BaseException as error:
        failure = error
        raise
    finally:
        def failed(error):
            nonlocal failure
            if failure is None:
                failure = error
            elif error is not failure:
                failure.add_note('PZ fixed runner cleanup also failed: '+type(error).__name__)
        strategy = owner.get('strategy')
        if strategy is not None and not strategy.snapshot()['closed']:
            try:
                strategy.close()
            except BaseException as error:
                failed(error)
        state = strategy.snapshot() if strategy is not None else {}
        pending = bool(state and (not state['closed'] or state['unconfirmed_work']))
        if state.get('closed', True) and owner.get('executor') is not None:
            try:
                report = owner['executor'].close()
                if report is None:
                    report = getattr(owner['executor'], 'last_close_report', None)
                pending |= report is False or isinstance(report, dict) and (
                    report.get('closed') is False or bool(report.get('unconfirmed_work'))
                    or bool(report.get('external_coordinator_requires_cleanup')))
                if not pending:
                    owner['closed_resources'].append('executor')
            except BaseException as error:
                pending = True
                failed(error)
        if not pending:
            for name, close in (('gateway', owner['gateway'].stop if 'gateway' in owner else None),
                                ('ray_connection', owner['ray'].shutdown if owner.get('ray_connection_owned') else None),
                                ('events', owner['events'].close if 'events' in owner else None)):
                if close is not None:
                    try:
                        close()
                        owner['closed_resources'].append(name)
                    except BaseException as error:
                        pending = True
                        failed(error)
        if not pending:
            try:
                lease.__exit__(None, None, None)
                owner['closed_resources'].append('lease')
            except BaseException as error:
                pending = True
                failed(error)
        if pending:
            failure = failure or RuntimeError('PZ query owner still holds unfinished backend work')
            failure.pz_execution_owner = owner
        summary.update(execution=execution, owner_cleanup_pending=pending,
            owner_cleanup_completed=list(owner['closed_resources']),
            prepared_execution=strategy.snapshot() if strategy is not None else None,
            executor_close_report=getattr(owner.get('executor'), 'last_close_report', None))
        for name, read in (('actual_posts', lambda: budget.attempts if budget is not None else 0),
                           ('budget', lambda: cell.snapshot() if cell is not None else None),
                           ('completion_ledger', lambda: ledger.snapshot() if ledger is not None else None)):
            try:
                summary[name] = read()
            except BaseException as error:
                failed(error)
        if created:
            try:
                write_private_json(output/'generator-calls.json', redact_json_values(ledger.calls() if ledger is not None else []))
            except BaseException as error:
                failed(error)
            if failure is not None:
                summary.update(status='failed', error=dict(type=type(failure).__name__, message=redact_text(str(failure))))
            try:
                write_private_json(output/'summary.json', redact_json_values(summary))
            except BaseException as error:
                failed(error)
        if failure is not None and sys.exception() is None:
            raise failure
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    action = parser.add_mutually_exclusive_group(required=True)
    for name in ('data-preflight', 'preflight', 'execute'):
        action.add_argument('--'+name, action='store_true')
    args = parser.parse_args(argv)
    config = native.load_config(args.config)
    if args.execute:
        value = run_fixed(config) if 'fixed_execution' in config else native.run(config)
    else:
        value = preflight(config, libraries=args.preflight)[-1]
    print(json.dumps(redact_json_values(value), ensure_ascii=False, allow_nan=False))
    return 0 if not args.execute or value['status'] in ('passed', 'completed') else 2
