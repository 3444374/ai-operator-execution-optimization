"""Select one old-PZ Movie plan and create independent runs without reoptimizing."""
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from importlib.metadata import version
from pathlib import Path
import re
import threading
import time

from src.baselines.common.private_artifacts import content_digest
from src.experiments.postgresql.sembench_queries import MovieDataset
from src.experiments.postgresql.query_workloads import file_identity
from src.semantic_methods.palimpzest.backend import PalimpzestAdapterError
from .model import LocalModelBinding, PZ_VLLM_ENUM, PZ_VLLM_MODEL, VerifiedLocalModel
from .native import NativeOptions, config_identity, verify_libraries
from .source import build_movie_query, load_palimpzest, source_identity


ADAPTED_ARMS = ('native-adapted', 'daft', 'ray-data', 'imlane-cpython', 'imlane-ray', 'semloom')
ORIGINAL_NATIVE_ARM = 'original-native'
_SHA256 = re.compile(r'[0-9a-f]{64}\Z')
_RUN_ID = re.compile(r'[A-Za-z0-9_.-]{1,128}\Z')


def verify_fixed_libraries():
    from src.semantic_methods.palimpzest.query_execution import validate_prepared_runtime
    validate_prepared_runtime()
    return dict(verify_libraries(), greenlet=version('greenlet'))


def adapter_source_identity():
    """Record the local method, ready-work, transport and Core code used by this slice."""
    root = Path(__file__).resolve().parents[2]
    files = set()
    for group in ('experiments/palimpzest', 'semantic_methods/palimpzest', 'scheduling/core'):
        files.update(path.relative_to(root).as_posix() for path in (root/group).glob('*.py'))
    # The approved extraction may not exist in an older local diagnostic checkout.
    # Its appearance/removal changes the identity rather than borrowing old evidence.
    common_transport = 'execution_provider/adapters/native_call_transport.py'
    if (root/common_transport).is_file():
        files.add(common_transport)
    files.update((
        'baselines/common/private_artifacts.py', 'baselines/text/sembench_movie.py',
        'baselines/text/frameworks/native_calls.py', 'baselines/text/frameworks/prepared_map.py',
        'baselines/text/frameworks/imlane_calls.py', 'baselines/text/frameworks/imlane_limits.py',
        'execution_provider/adapters/native_tasks.py', 'execution_provider/adapters/full_response.py',
        'execution_provider/adapters/model_config.py', 'execution_provider/adapters/incremental_execution.py',
        'execution_provider/adapters/ray_map_transport.py', 'execution_provider/adapters/sema_imlane.py',
        'execution_provider/completion.py', 'execution_provider/limits.py', 'execution_provider/wire/framing.py',
        'experiments/postgresql/sembench_queries.py', 'experiments/postgresql/query_workloads.py',
        'observability/request_gateway.py', 'planning/work.py', 'scheduling/runtime/async_backend.py',
        'scheduling/endpoint_routing/policies.py', 'scheduling/organization/session_window.py',
        'scheduling/submission_control/admission.py',
    ))
    return {name: file_identity(root/name) for name in sorted(files)}


@dataclass(frozen=True)
class FixedEnvironment:
    """Caller-verified external facts; their digests are declarations, not discovery."""
    service_config_sha256: str
    service_cache_sha256: str
    dependency_lock_sha256: str

    def __post_init__(self):
        if any(type(value) is not str or not _SHA256.fullmatch(value) for value in asdict(self).values()):
            raise ValueError('fixed execution requires explicit complete service/cache/dependency digests')


@dataclass(frozen=True)
class BackendRunProfile:
    """Deployment owns the budget and mode; each backend owns its dispatch policy."""
    deployment_budget_sha256: str
    mode: str = 'fixed'

    def __post_init__(self):
        if (self.mode != 'fixed'
                or type(self.deployment_budget_sha256) is not str
                or not _SHA256.fullmatch(self.deployment_budget_sha256)):
            raise ValueError('fixed backend profile needs a resource-budget digest; native-auto is not yet qualified')


@dataclass(frozen=True)
class FixedMovieProfile:
    options: NativeOptions = field(default_factory=NativeOptions)
    query_id: int = 3
    sampling_validation: str = 'none'

    def __post_init__(self):
        if type(self.options) is not NativeOptions or type(self.query_id) is not int or self.query_id != 3:
            raise ValueError('the first fixed-plan runner supports original Movie Q3 only; LIMIT is unsupported')
        if self.sampling_validation != 'none':
            raise ValueError('sampling/validation must run as a separately accounted original-system phase')

    def query_config(self, pz, binding):
        return pz.QueryProcessorConfig(policy=pz.MaxQuality(), execution_strategy='parallel',
            optimizer_strategy='pareto', sentinel_execution_strategy=None,
            # This old QPC annotates num_samples as int with an unvalidated None
            # default; passing None explicitly is rejected by its own validator.
            # The actual processor/strategy None value is checked below.
            scan_start_idx=0,
            available_models=[getattr(pz.Model, PZ_VLLM_ENUM)],
            max_workers=self.options.max_workers, join_parallelism=self.options.join_parallelism,
            batch_size=self.options.batch_size, api_base=binding.api_base, use_vertex=False,
            progress=False, verbose=False, allow_bonded_query=True,
            allow_model_selection=False, allow_rag_reduction=False, allow_mixtures=False,
            allow_critic=False, allow_split_merge=False)


def _source_contents(plan):
    """MemoryDataset input is bounded by MovieDataset and contains only raw text fields."""
    return [dict(source_id=operator.datasource.id, rows=len(operator.datasource),
                 values_sha256=content_digest([operator.datasource[index]
                                               for index in range(len(operator.datasource))]))
            for operator in plan if hasattr(operator, 'datasource')]


def _copy_plan(node):
    # Reconstruct operators through their original constructors, so Generator,
    # PromptFactory and operator state are fresh. Immutable schemas/UDFs may be shared.
    operator = type(node.operator)(**deepcopy(node.operator.get_op_params()))
    return type(node)(operator=operator, subplans=[_copy_plan(child) for child in node.subplans],
                      plan_cost=deepcopy(node.plan_cost))


def _check_fresh(template, actual):
    for previous, current in zip(template, actual, strict=True):
        if previous is current:
            raise PalimpzestAdapterError('fixed-plan run reused an operator object')
        for name in ('generator', 'datasource'):
            before, after = getattr(previous, name, None), getattr(current, name, None)
            if before is not None and before is after:
                raise PalimpzestAdapterError('fixed-plan run reused '+name+' state')
        if hasattr(previous, 'generator') and previous.generator.prompt_factory is current.generator.prompt_factory:
            raise PalimpzestAdapterError('fixed-plan run reused prompt factory state')


class FreshFixedMovie:
    """One run's plan, original strategy and generation state; single execution only."""
    def __init__(self, owner, run_id, frozen, native_strategy, preparation_seconds):
        self.owner, self.run_id, self.frozen, self.native_strategy = owner, run_id, frozen, native_strategy
        self.preparation_seconds = preparation_seconds
        self.semantic_operator_ids = tuple(f'{index}-{operator.get_full_op_id()}'
            for index, operator in enumerate(frozen.plan) if hasattr(operator, 'generator'))
        self._started = False
        self._lock = threading.Lock()

    def begin(self):
        with self._lock:
            if self._started:
                raise PalimpzestAdapterError('a fixed-plan run cannot execute more than once')
            try:
                self.owner.validate()
                from src.semantic_methods.palimpzest.query_execution import validate_frozen_plan
                validate_frozen_plan(self.frozen)
                if (_source_contents(self.frozen.plan) != self.owner._source_contents
                        or vars(self.native_strategy) != vars(self.owner._native_strategy)):
                    raise PalimpzestAdapterError('fresh source content or original strategy changed before execution')
            except Exception as error:
                self.owner.invalidate(error)
                raise
            self._started = True


class PreparedFixedMovie:
    """A never-executed selection template; every arm obtains an independent copy."""
    def __init__(self, *, dataset, rows, binding, verified, profile, environment,
                 frozen, native_strategy, manifest, source_contents, pz_checkout):
        self.dataset, self.binding, self.verified = dataset, binding, verified
        self._selected_binding, self._selected_verified = deepcopy(binding), deepcopy(verified)
        self.profile, self.environment = profile, environment
        self._rows, self._template, self._native_strategy = deepcopy(rows), frozen, native_strategy
        self._pz_checkout = pz_checkout
        self._source_contents = deepcopy(source_contents)
        self._manifest = deepcopy(manifest)
        self.identity = content_digest(manifest)
        self._used_run_ids, self._lock, self._unhealthy = set(), threading.Lock(), None

    def describe(self):
        return dict(schema='semloom.palimpzest_fixed_movie.v1', identity=self.identity,
                    manifest=deepcopy(self._manifest), unhealthy=self._unhealthy,
                    identity_scope='plan/schema/prompt/parser/source/config/runtime/model/service/cache declarations',
                    request_bytes_scope='actual per-call request hashes are retained during execution',
                    original_native_arm=ORIGINAL_NATIVE_ARM, adapted_target_arms=list(ADAPTED_ARMS),
                    performance_qualified=False, backend_qualified=False)

    @property
    def source_occurrences(self):
        return {'reviews-'+str(index): row['row_id'] for index, row in enumerate(self._rows)}

    @property
    def max_completion_calls(self):
        return len(self._rows) * sum(hasattr(op, 'generator') for op in self._template.plan)

    def invalidate(self, error):
        with self._lock:
            self._unhealthy = self._unhealthy or type(error).__name__

    def validate(self):
        from src.semantic_methods.palimpzest.query_execution import validate_frozen_plan
        if self._unhealthy is not None:
            raise PalimpzestAdapterError('fixed query is unhealthy after '+self._unhealthy)
        try:
            if (type(self.binding) is not LocalModelBinding or type(self.verified) is not VerifiedLocalModel
                    or self.binding != self._selected_binding or self.verified != self._selected_verified
                    or self._manifest['local_model'] != self._selected_verified.summary()):
                raise PalimpzestAdapterError('fixed local model binding or verified metadata changed after selection')
            validate_frozen_plan(self._template)
            data = self.dataset.preflight(require_libraries=False)
            if (self.dataset.load_rows() != self._rows
                    or {name: data[name] for name in self._manifest['data']} != self._manifest['data']
                    or source_identity(self._pz_checkout, self.dataset.checkout, self.profile.query_id)
                        != self._manifest['source']
                    or adapter_source_identity() != self._manifest['adapter_source_files']
                    or _source_contents(self._template.plan) != self._source_contents
                    or verify_fixed_libraries() != self._manifest['runtime_versions']):
                raise PalimpzestAdapterError('fixed source content or dependency environment changed')
            import litellm
            if litellm.cache is not None or litellm.drop_params != self._manifest['litellm_drop_params']:
                raise PalimpzestAdapterError('fixed client cache or SDK parameter policy changed')
        except Exception as error:
            self.invalidate(error)
            raise error

    def fresh(self, run_id):
        from src.semantic_methods.palimpzest.query_execution import freeze_existing_plan
        started = time.monotonic_ns()
        if type(run_id) is not str or not _RUN_ID.fullmatch(run_id):
            raise ValueError('fixed execution requires an explicit bounded run occurrence id')
        self.validate()
        with self._lock:
            if run_id in self._used_run_ids:
                raise PalimpzestAdapterError('a fixed query run id cannot be reused')
            self._used_run_ids.add(run_id)
        plan = _copy_plan(self._template.plan)
        _check_fresh(self._template.plan, plan)
        frozen = freeze_existing_plan(plan)
        if frozen.fingerprint != self._template.fingerprint or _source_contents(plan) != self._source_contents:
            error = PalimpzestAdapterError('fresh physical plan differs from the selected complete identity')
            self.invalidate(error)
            raise error
        settings = self._manifest['effective_config']['execution_strategy']['settings']
        strategy = type(self._native_strategy)(**settings)
        return FreshFixedMovie(self, run_id, frozen, strategy, (time.monotonic_ns()-started)/1e9)


def select_fixed_movie(dataset, pz_checkout, binding, verified, *, environment,
                       profile=FixedMovieProfile()):
    """No execute_plan, sampling, validation or model call happens during selection."""
    if (type(dataset) is not MovieDataset or type(binding) is not LocalModelBinding
            or type(verified) is not VerifiedLocalModel or type(environment) is not FixedEnvironment
            or type(profile) is not FixedMovieProfile
            or verified.binding_fingerprint != binding.fingerprint
            or verified.served_model_id != binding.model.model_id):
        raise ValueError('fixed Movie selection requires matching explicit source/model/environment contracts')
    from src.semantic_methods.palimpzest import freeze_best_plan
    from src.semantic_methods.palimpzest.query_execution import validate_prepared_plan
    started = time.monotonic_ns()
    data = dataset.preflight(require_libraries=False)
    rows = dataset.load_rows()
    pz = load_palimpzest(pz_checkout)
    versions = verify_fixed_libraries()
    import litellm
    if litellm.cache is not None:
        raise PalimpzestAdapterError('fixed execution requires an explicit disabled client response cache')
    config = profile.query_config(pz, binding)
    selected_config = config_identity(config)
    query = build_movie_query(dataset.checkout, pz, profile.query_id, rows, config)
    query._generate_unique_logical_op_ids()
    from palimpzest.query.processor.query_processor_factory import QueryProcessorFactory
    processor = QueryProcessorFactory.create_processor(query, config)
    flags = {name: getattr(processor.optimizer, name) for name in (
        'allow_bonded_query', 'allow_rag_reduction', 'allow_mixtures', 'allow_critic', 'allow_split_merge')}
    if (flags != dict(allow_bonded_query=True, allow_rag_reduction=False, allow_mixtures=False,
                      allow_critic=False, allow_split_merge=False)
            or [model.value for model in processor.optimizer.available_models] != [PZ_VLLM_MODEL]
            or processor.validator is not None or processor.sentinel_execution_strategy is not None
            or processor.num_samples is not None or processor.scan_start_idx != 0):
        raise PalimpzestAdapterError('actual PZ optimizer/model/sampling configuration differs from fixed profile')
    optimizer_started = time.monotonic_ns()
    frozen = freeze_best_plan(processor)
    optimizer_finished = time.monotonic_ns()
    checked = validate_prepared_plan(frozen.plan)
    contents = _source_contents(frozen.plan)
    manifest = dict(schema='semloom.palimpzest_fixed_identity.v1', query_id=profile.query_id,
        source=source_identity(pz_checkout, dataset.checkout, profile.query_id),
        adapter_source_files=adapter_source_identity(),
        data={name: data[name] for name in ('role', 'manifest_content_digest', 'manifest_file', 'files', 'csv')},
        source_contents=contents, physical_plan=frozen.describe(), selected_plan_capability=checked,
        selected_config=selected_config, effective_config=config_identity(config),
        effective_optimizer_flags=flags, optimizer_models=[PZ_VLLM_MODEL],
        allow_model_selection_scope='old optimizer ignores this QPC field; singleton model and selected ops enforce it',
        runtime_versions=versions, litellm_drop_params=litellm.drop_params,
        runtime_source_validation='fixed old PZ and HostedVLLM SDK file hashes verified before selection/reuse',
        local_model=verified.summary(), external_environment=asdict(environment),
        sampling_validation=dict(mode='none', model_calls=0, seconds=0.0),
        result_contract='original Q3 count schema; old zero-input empty relation is compatibility-invalid',
        preparation_seconds=(optimizer_started-started)/1e9,
        optimizer_selection_seconds=(optimizer_finished-optimizer_started)/1e9)
    # Timing is evidence, never part of the identity shared across fresh runs.
    timings = {name: manifest.pop(name) for name in ('preparation_seconds', 'optimizer_selection_seconds')}
    prepared = PreparedFixedMovie(dataset=dataset, rows=rows, binding=binding, verified=verified,
        profile=profile, environment=environment, frozen=frozen,
        native_strategy=processor.execution_strategy, manifest=manifest, source_contents=contents,
        pz_checkout=pz_checkout)
    prepared.selection_accounting = dict(timings, optimizer_invocations=1,
        sampling_model_calls=0, validation_model_calls=0,
        scope='one shared selection; not executed again or charged once per backend run')
    return prepared
