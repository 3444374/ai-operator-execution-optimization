"""Execute a fresh fixed Movie plan through a separately named execution arm.

This helper owns one PZ query, not the host lease, request budget, service or backend
runtime. The caller installs the existing actual-send accounting and closes its
executor. A returned software result never grants model, performance or vendor
qualification by itself.
"""

from dataclasses import asdict, dataclass, replace
import time

from src.baselines.common.private_artifacts import content_digest
from src.baselines.text.frameworks.native_calls import NativeCallExecutor
from src.baselines.text.frameworks.imlane_calls import IMLaneCallExecutor
from src.baselines.text.sembench_movie import classification_audit
from src.semantic_methods.palimpzest.backend import PalimpzestAdapterError
from .fixed import ADAPTED_ARMS, ORIGINAL_NATIVE_ARM, BackendRunProfile, FreshFixedMovie
from .ledger import PalimpzestLedger
from .model import bind_local_completion, fetch_model_metadata
from .native import movie_quality


def executor_identity(arm, executor):
    """Check the real implementation, rather than granting identity from its label."""
    from src.semantic_methods.palimpzest.query_execution import NativeAdaptedExecutor
    if arm == ORIGINAL_NATIVE_ARM:
        if executor is not None:
            raise ValueError('original native uses its own original PZ executor')
        return dict(arm=arm, scheduler_owner='original PZ ParallelExecutionStrategy',
                    scope='fixed execute_plan; separate from original Dataset.run system baseline')
    if arm not in ADAPTED_ARMS:
        raise ValueError('unknown fixed PZ execution arm')
    if arm == 'semloom':
        from src.semantic_methods.palimpzest.semloom import SemLoomProgress
        if type(executor) is not SemLoomProgress:
            raise ValueError('SemLoom requires its typed query Job/operator progress adapter')
        state = executor.snapshot()
        return dict(arm=arm, scheduler_owner=state['scheduler_owner'],
                    physical_backend=state['physical_backend'], ray_backend=state['ray_backend'],
                    declared_request_capacity=state['declared_request_capacity'],
                    scope='query-thread Job/operator sessions over '+state['physical_backend'],
                    local_diagnostic=not state['ray_backend'],
                    main_system_qualification='pending independent physical/backend/model qualification',
                    distributed_backend_qualified=False)
    if arm == 'native-adapted' and type(executor) is NativeAdaptedExecutor:
        return dict(arm=arm, scheduler_owner='native-adapted original SDK I/O pool',
                    max_workers=executor.max_workers,
                    scope='same prepared/finish control; adaptation cost reference')
    if type(executor) is NativeCallExecutor:
        if arm in ('daft', 'ray-data') and executor.backend == arm and executor._imlane is None:
            return dict(arm=arm, scheduler_owner=arm+' native complete-call graph',
                        options=asdict(executor.options), scope='complete ready model requests')
        implementation = executor._imlane if executor.backend == 'imlane' else None
    else:
        implementation = executor
    if (arm in ('imlane-cpython', 'imlane-ray') and type(implementation) is IMLaneCallExecutor
            and implementation.config.runtime == arm.removeprefix('imlane-')):
        return dict(arm=arm, scheduler_owner='author DBEnd '+implementation.config.runtime,
                    declared_author_executors=implementation.config.n_executors,
                    options=asdict(implementation.options), call_limits=asdict(implementation.limits),
                    scope='existing IMLane bridge; author source unchanged',
                    coordinator_lifecycle='caller-owned and independently verified')
    raise ValueError('executor implementation does not match the selected fixed PZ arm')


def result_status(prepared, records, ledger):
    """Preserve native relation behavior while auditing every Filter occurrence."""
    from palimpzest.core.elements.records import DataRecordCollection
    result = DataRecordCollection(records)
    frame = result.to_df()
    rows = prepared._rows
    labels = prepared.dataset.references(rows)
    selected = {row['row_id']: labels[row['row_id']] for row in rows if row['movie_id'] == 'taken_3'}
    predictions = ledger.predictions(prepared.source_occurrences)
    task = [(key, label) for key, label in predictions if key in selected]
    audit = classification_audit(selected, task)
    all_evaluated = classification_audit(labels, predictions, require_complete=False)
    kept = sum(call['native_passed_operator'] is True for call in ledger.calls()
               if prepared.source_occurrences[call['source_indices'][0]] in selected)
    evidence = dict(classification_audit=audit, all_evaluated_classification_audit=all_evaluated,
                    extra_movie_filter_calls=sum(key not in selected for key, _ in predictions),
                    relation_rows=len(frame), relation_columns=list(frame.columns))
    if frame.empty and len(frame.columns) == 0 and kept == 0:
        return dict(evidence, status='compatibility_invalid_empty_count',
                    relation_compatible=False, original_empty_relation_preserved=True,
                    quality=dict(status='unavailable', reason='original PZ emitted no COUNT row'))
    if (list(frame.columns) != ['count'] or len(frame) != 1
            or frame.iloc[0, 0] != kept):
        raise PalimpzestAdapterError('fixed Q3 relation differs from original Filter outcomes/count schema')
    return dict(evidence,
                status='completed_with_invalid_predictions' if audit['invalid'] else 'completed',
                relation_compatible=True, original_empty_relation_preserved=False,
                quality=movie_quality(prepared.dataset, rows, 3, result, ledger))


@dataclass
class FixedRunResult:
    records: list
    plan_stats: object
    ledger: PalimpzestLedger
    evidence: dict


def execute_fixed_movie(fresh, *, arm, backend_profile, executor=None, limits=None, ledger=None,
                        record=None, cancelled=None, evidence=None, request_route=None):
    """Execute once; actual-send budget/lease and executor teardown stay caller-owned.

    Metadata is checked anew before this run. The selection manifest and fresh state
    are verified before any Generator call. Fatal identity/capacity/association or
    cleanup failures poison the selection; ordinary provider/parser defaults retain
    the original semantics and remain invalid in the separate quality ledger.
    """
    if type(fresh) is not FreshFixedMovie:
        raise TypeError('fixed execution requires one fresh Movie run')
    from src.semantic_methods.palimpzest import completion_bridge
    from src.semantic_methods.palimpzest.query_execution import (
        PreparedExecutionStrategy, PreparedRequestRoute, QueryExecutionLimits,
    )
    prepared = fresh.owner
    # Public contract references are checked by begin(); execution always uses the
    # independently captured immutable selection, including the metadata GET.
    binding, selected_verified = prepared._selected_binding, prepared._selected_verified
    evidence = evidence if evidence is not None else {}
    evidence.update(schema='semloom.palimpzest_fixed_run.v1', status='started', arm=arm,
        fixed_identity=prepared.identity, run_id=fresh.run_id, selected_plan=fresh.frozen.describe(),
        fresh_state_preparation_seconds=fresh.preparation_seconds,
        configuration_ownership=dict(deployment='CPU/GPU/memory/service budget and fixed/native-auto mode',
            frontend='original PZ semantic dependencies and ready work',
            backend='worker dispatch and backpressure within its declared configuration',
            preparation_limits='adapter-held SDK continuations and request/response memory; not backend concurrency',
            http_and_vllm_capacity='separate actual HTTP budget and vLLM sequence/token evidence'),
        selection_accounting=prepared.selection_accounting,
        sampling_validation=prepared.describe()['manifest']['sampling_validation'],
        actual_http_attempts='separate actual-send request budget required',
        executor_lifecycle='caller-owned; runtime close and residual work require separate evidence',
        performance_qualified=False, backend_qualified=False, model_quality_qualified=False)
    started, strategy, bridge = time.monotonic_ns(), None, None
    try:
        if type(backend_profile) is not BackendRunProfile:
            raise TypeError('fixed execution requires an explicit backend deployment profile')
        identity = executor_identity(arm, executor)
        if arm == ORIGINAL_NATIVE_ARM:
            if limits is not None or cancelled is not None or request_route is not None:
                raise ValueError('original native supports its fixed direct PZ controls without adapted callbacks/routes')
        elif (type(limits) is not QueryExecutionLimits
              or limits.pz_workers != fresh.native_strategy.max_workers):
            raise ValueError('prepared execution requires limits matching the original PZ configuration identity')
        if request_route is not None and (type(request_route) is not PreparedRequestRoute
                or request_route.route.upstream_url != binding.model.endpoint_url
                or request_route.route.job_id != fresh.run_id):
            raise ValueError('prepared request route differs from the fixed model or query registration')
        if arm == 'semloom':
            endpoint = binding.model.endpoint_url if request_route is None else request_route.effective_endpoint
            state = executor.snapshot()
            if (backend_profile.mode != 'fixed' or executor.query_id != fresh.run_id
                    or set(executor.flows) != set(fresh.semantic_operator_ids) or executor.limits != limits
                    or executor.transport.config != replace(binding.model, endpoint_url=endpoint)
                    or state['closed'] or state['active'] or state['calls_submitted'] or state['outcomes_received']):
                raise PalimpzestAdapterError('SemLoom progress differs from the fresh fixed query/model/operator configuration')
        ledger = ledger if ledger is not None else PalimpzestLedger(max_calls=prepared.max_completion_calls, record=record)
        if (type(ledger) is not PalimpzestLedger or ledger.snapshot()['completion_submitted']
                or ledger.max_calls < prepared.max_completion_calls):
            raise ValueError('each fixed run needs a fresh ledger covering all source rows/method steps')
        physical = dict(backend=identity, mode=backend_profile.mode,
                        deployment_budget_sha256=backend_profile.deployment_budget_sha256,
                        preparation_limits=asdict(limits) if limits is not None else None,
                        request_route=request_route.describe() if request_route is not None else None)
        evidence.update(arm=identity, backend_configuration=physical,
            execution_identity=content_digest(dict(fixed_identity=prepared.identity, physical=physical)))
        fresh.begin()
        metadata_started = time.monotonic_ns()
        verified = fetch_model_metadata(binding)
        evidence['metadata_check_seconds'] = (time.monotonic_ns()-metadata_started)/1e9
        if verified != selected_verified:
            raise PalimpzestAdapterError('local model metadata changed from the complete fixed identity')
        from palimpzest.query.generators import generators
        local = bind_local_completion(generators.litellm.completion, binding, verified)
        def request_kwargs(kwargs):
            values = local.request_kwargs(kwargs)
            if request_route is not None:
                values['api_base'] = request_route.effective_endpoint.removesuffix('/chat/completions')
            return values
        execute_started = time.monotonic_ns()
        if arm == ORIGINAL_NATIVE_ARM:
            with completion_bridge(completion=local, observer=ledger) as bridge:
                records, stats = fresh.native_strategy.execute_plan(plan=fresh.frozen.plan)
        else:
            strategy = PreparedExecutionStrategy(fresh.native_strategy, binding.model,
                original_completion=local.original, request_kwargs=request_kwargs,
                validate_response=local.validate_response, executor=executor, limits=limits,
                query_id=fresh.run_id, observer=ledger, cancelled=cancelled, record=record,
                request_route=request_route, progress=executor if arm == 'semloom' else None)
            records, stats = strategy.execute_plan(plan=fresh.frozen.plan)
            state = strategy.snapshot()
            if not state['closed'] or state['active_stacks'] or state['unconfirmed_work']:
                raise PalimpzestAdapterError('fixed execution retains unconfirmed backend or original continuation ownership')
        evidence['plan_execution_seconds'] = (time.monotonic_ns()-execute_started)/1e9
        quality_started = time.monotonic_ns()
        evidence.update(result_status(prepared, records, ledger))
        evidence['quality_evaluation_seconds'] = (time.monotonic_ns()-quality_started)/1e9
        return FixedRunResult(records, stats, ledger, evidence)
    except BaseException as error:
        prepared.invalidate(error)
        evidence.update(status='failed', error_type=type(error).__name__)
        raise
    finally:
        evidence.update(query_seconds=(time.monotonic_ns()-started)/1e9,
                        total_seconds_with_fresh_state=fresh.preparation_seconds+(time.monotonic_ns()-started)/1e9,
                        ledger=ledger.snapshot() if type(ledger) is PalimpzestLedger else None,
                        prepared_execution=strategy.snapshot() if strategy is not None else None,
                        native_bridge=bridge.snapshot() if bridge is not None else None)
