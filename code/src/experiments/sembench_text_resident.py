"""Explicit resident LOTUS adapter for the existing SemBench text/lifecycle CLI.

The original accessor/LM owns execution. This driver owns one native model and
fresh source-to-sink queries; the existing supervisor and caller own the host.
"""
import json
import os
from pathlib import Path
import subprocess
import time

from src.infrastructure.private_artifacts import content_digest, file_sha256, json_metric
from src.observability.native_query_timing import observe_native_requests, native_request_boundaries
from .attempt_ledger import AttemptBudget
from .cell_budget import CellBudgetLedger
from .native_query_lifecycle import run_sequence


def validate_configuration(config):
    spec = config['resident']
    if config['backend'] != 'lotus-native' or set(spec) != {
            'repetitions', 'timeout_s', 'cleanup_s', 'python_hash_seed', 'binding'}:
        raise ValueError('explicit native LOTUS resident configuration required')
    if type(spec['repetitions']) is not int or spec['repetitions'] < 1:
        raise ValueError('resident requires at least one complete cold query; later queries are fresh warm queries')
    if (type(spec['timeout_s']) not in (int, float) or type(spec['cleanup_s']) not in (int, float)
            or not 0 < spec['cleanup_s'] < spec['timeout_s'] < float('inf')):
        raise ValueError('finite resident supervision and cleanup allowance required')
    if type(spec['python_hash_seed']) is not int or not 0 <= spec['python_hash_seed'] < 2**32:
        raise ValueError('explicit Python hash seed required for native prompt column order')
    binding = spec['binding']
    if set(binding) != {'source', 'input_files', 'occurrences_sha256', 'model_files', 'tokenizer_files'}:
        raise ValueError('source/input/model/tokenizer identities must be declared')
    if set(binding['source']) != {'commit', 'code_tree'}:
        raise ValueError('source binding requires the selected maintained commit and code tree')
    budget = config['budget']
    if set(budget) != {'path', 'id', 'limit', 'unit_ids'}:
        raise ValueError('resident requires existing per-query permanent budget units')
    units = budget['unit_ids']
    if (not isinstance(units, list) or len(units) != spec['repetitions']
            or any(not isinstance(unit, str) or not unit for unit in units)
            or len(set(units)) != len(units)):
        raise ValueError('each resident query needs its own distinct permanent budget unit')


def occurrence_digest(query):
    # row_id includes the fresh execution ID. Source position, label and values
    # identify the input occurrence independently of its cold/warm invocation.
    records = () if query.operation is None else query.operation.records
    return content_digest([dict(source_position=r.source_position,
        source_index=r.source_index, values=r.values) for r in records])


def verify_inputs(config, query):
    binding = config['resident']['binding']
    if ({name: value['sha256'] for name, value in query.files.items()} != binding['input_files']
            or occurrence_digest(query) != binding['occurrences_sha256']):
        raise ValueError('resident input bytes, order or source occurrences changed')


def verify_binding(config, query, engine, *, assets=True):
    """Validate declared current source; no historical commit or machine path fallback."""
    spec = config['resident']
    if os.environ.get('PYTHONHASHSEED') != str(spec['python_hash_seed']):
        raise ValueError('launch Python with the declared PYTHONHASHSEED')
    root = Path(__file__).resolve().parents[3]
    git = lambda *args: subprocess.check_output(['git', '-C', str(root), *args], text=True).strip()
    source = spec['binding']['source']
    if (git('rev-parse', 'HEAD') != source['commit'] or git('rev-parse', 'HEAD:code') != source['code_tree']
            or git('status', '--porcelain')):
        raise ValueError('resident requires the declared clean maintained source checkout')
    verify_inputs(config, query)
    count = query.describe()['expected_model_calls']
    maximum = spec['repetitions'] * max(1, count)
    if engine.max_model_calls != maximum or config['budget']['limit'] != maximum:
        raise ValueError('resident model and permanent allowances must cover exactly the declared queries')
    digests = {}
    if assets:
        for base, key in ((engine.model_path, 'model_files'),
                          (engine.tokenizer_path or engine.model_path, 'tokenizer_files')):
            files = spec['binding'][key]
            if not isinstance(files, dict) or not files:
                raise ValueError('declare nonempty model and tokenizer file identities')
            for name, expected in files.items():
                path = Path(base) / name
                if Path(name).is_absolute() or '..' in Path(name).parts:
                    raise ValueError('asset identity paths must be relative to their declared root')
                actual = digests.setdefault(str(path), None)
                if actual is None:
                    actual = digests[str(path)] = file_sha256(path)
                if actual != expected:
                    raise ValueError('resident asset hash mismatch: ' + key + '/' + name)
    return dict(source=source, input_files=spec['binding']['input_files'],
        occurrences_sha256=spec['binding']['occurrences_sha256'], config_sha256=content_digest(config),
        model_files=spec['binding']['model_files'], tokenizer_files=spec['binding']['tokenizer_files'],
        asset_hashes_checked=assets, logical_requests=count * spec['repetitions'],
        request_count_kind=('upper_bound' if query.describe().get('ranking') == 'topk' else 'exact'),
        per_query_requests=count, phase_labels=['cold'] + ['warm'] * (spec['repetitions'] - 1))


class ResidentDriver:
    def __init__(self, config, checkout, data_dir, engine, generation, resources, ledger):
        self.config, self.checkout, self.data_dir = config, checkout, data_dir
        self.engine, self.generation, self.resources, self.ledger = engine, generation, resources, ledger
        self.owner, self.previous = None, None
        self.request_ids = set()

    def start(self):
        # run_sequence's owner-ready is this adapter, not model readiness. The
        # actual native model starts inside the first nonempty source query.
        return dict(model_startup_scope='lazy first nonempty query', model_ready=False,
                    protocol='original LOTUS LM / LiteLLM / local AsyncLLM IPC')

    def query(self, spec, emit):
        import lotus
        from src.baselines.text.frameworks.lotus_native_local import LocalNativeModel
        from src.baselines.text.frameworks.lotus_native_stage import execute_native
        from src.baselines.text.sembench_prepared import finish, result_value
        from src.semantic_methods.lotus.record_method import LotusRecordMethod
        from .sembench_text_query import query_from_config, complete_calls

        started = time.monotonic_ns()
        report = dict(status='failed', query_id=spec['query_id'], phase=spec['temperature'],
            source_start_ns=started, result_cache_enabled=False, extra_warmup=0, retries=0,
            first_engine_accept_ns=None, kernel_seconds=None)
        unit = primary = None
        try:
            with lotus.settings.context(enable_cache=False):
                query = query_from_config(dict(self.config, query_id=spec['query_id']),
                    self.checkout, self.data_dir, self.generation)
                verify_inputs(self.config, query)
                report['query'] = query.describe()
                unit = self.ledger.claim_shared_unit(spec['unit_id'])
                expected_count = query.describe()['expected_model_calls']
                if unit.attempts or unit.remaining != max(1, expected_count):
                    raise ValueError('resident query requires its fresh exact permanent allowance')
                if self.owner is None and query.operation is not None:
                    self.owner = LocalNativeModel(self.engine, query.operation.generation,
                        self.resources, self.config['profile']['native_lm_max_batch_size'])
                first_model = self.owner is not None and self.owner.ready_report is None
                events = dict(submit=[], terminal=[])
                report['native_io_events'] = events
                relations = []

                def execute(operation, invoke):
                    topk = operation.describe()['kind'] == 'topk'
                    def accessor():
                        if self.owner.transport is None:
                            relation = invoke()
                        else:
                            with observe_native_requests(self.owner.transport, events):
                                relation = invoke()
                        # Retain the native relation before original grouping can
                        # mutate it; serialize and validate it only after full sink.
                        relations.append((relation[0] if topk else relation).copy(deep=True))
                        return relation
                    if topk:
                        from src.baselines.text.frameworks.lotus_native_topk import execute_native_topk
                        return execute_native_topk(operation, invoke=accessor, shared_owner=self.owner,
                            query_id=query.query_id, budget=unit)
                    return execute_native(operation.frame, operation.stage, operation.records,
                        invoke=accessor, engine=self.engine, resources=self.resources,
                        query_id=query.query_id, profile=self.config['profile'], budget=unit,
                        shared_owner=self.owner)

                report['prepare_finished_ns'] = time.monotonic_ns()
                result, operator = query.execute(execute)
                sink = emit('result_ready')
                report.update(sink_ready_ns=sink, source_to_sink_seconds=(sink-started)/1e9,
                    native_query_seconds=(sink-report['prepare_finished_ns'])/1e9)
                report['operator'] = {} if operator is None else {k:v for k,v in operator.items() if k != 'frame'}
                state = None if self.owner is None else self.owner.snapshot()
                report['snapshot_finished_ns'] = time.monotonic_ns()
                if state is not None:
                    transport = state['transport']
                    if state['active_query'] or state['tainted'] or (transport is not None and
                            (not transport['child_alive'] or any(transport[k] for k in
                             ('held_requests', 'input_bytes', 'reserved_result_bytes', 'unknown_requests', 'ready_results')))):
                        raise RuntimeError('resident model is not live and quiescent at the query barrier')
                    if self.previous is not None:
                        for key in ('engine_uid', 'model_API_pid', 'process_created', 'owner_id', 'provider_name'):
                            if state[key] != self.previous[key]:
                                raise RuntimeError('resident model identity changed: ' + key)
                    self.previous = state
                calls = complete_calls(query, operator)
                boundaries = native_request_boundaries(events, calls)
                if unit.attempts != len(calls):
                    raise RuntimeError('permanent native attempts differ from complete original occurrences')
                ids = {c['engine_request_id'] for c in calls}
                if self.request_ids & ids:
                    raise RuntimeError('native request identity reused across fresh queries')
                self.request_ids |= ids
                if operator is not None and query.describe().get('ranking') == 'topk':
                    from src.baselines.text.frameworks.lotus_native_topk import audit
                    if len(relations) != 1:
                        raise ValueError('TopK must return one complete native relation')
                    audit(query, operator, result, relations[0])
                    report['operator_relation'] = result_value(relations[0])
                elif operator is not None:
                    method = LotusRecordMethod(query.operation.stage)
                    prepared = method.prepare_rows(query.operation.records, query_id=query.query_id)
                    actual = {operator['stage_call_ids'][c['call_id']]: c for c in calls}
                    for row in prepared:
                        call = actual[row['call_id']]
                        if call['messages_sha256'] != content_digest(json.loads(row['messages_json'])):
                            raise ValueError('actual native messages differ from the original source occurrence')
                    predictions = [dict(call_id=key, generated_text=call['text']) for key, call in actual.items()]
                    expected, semantic = finish(query, method, prepared, predictions)
                    if (len(relations) != 1 or result_value(relations[0]) != semantic['operator_relation']
                            or result_value(result) != result_value(expected)):
                        raise ValueError('native original relation or full sink differs from original parser replay')
                    report.update(operator_relation=result_value(relations[0]), prepared_calls=prepared)
                report['audit_finished_ns'] = time.monotonic_ns()
                report['evaluation_started_ns'] = time.monotonic_ns()
                quality = json_metric(query.evaluate(result))
                report['evaluation_finished_ns'] = time.monotonic_ns()
                report.update(status='completed', query=query.describe(), result=result_value(result),
                    quality=quality, snapshot=state, native_io_events=events, native_request_events=boundaries,
                    model_ready_observed_ns=None if operator is None else operator.get('model_ready_observed_ns'),
                    startup_this_round_seconds=(state['ready']['startup_seconds'] if first_model and state['ready'] else 0),
                    budget_attempts=unit.attempts, budget_remaining=unit.remaining,
                    timing_scope='fresh source/prepare -> original full sink; snapshot/audit/evaluation/persistence/close separate')
                return report
        except BaseException as error:
            primary = error
            report['error_type'] = type(error).__name__
            if getattr(error, 'operator_report', None) is not None:
                report['operator'] = {k:v for k,v in error.operator_report.items() if k != 'frame'}
            error.query_report = report
            raise
        finally:
            if unit is not None:
                try:
                    self.ledger.close_shared_unit(spec['unit_id'])
                except BaseException as cleanup:
                    if primary is None:
                        raise
                    primary.add_note('native permanent budget close failed: ' + type(cleanup).__name__)

    def close(self):
        if self.owner is None:
            return dict(status='closed', cleanup_scope='no model allocated', cleanup_errors=[])
        return self.owner.close(timeout=self.config['resident']['cleanup_s'])


def run(config, checkout, data_dir, engine, generation, resources, output):
    """Use the existing finite sequence lifecycle; no host lease or new executor."""
    value = config['budget']
    ledger = CellBudgetLedger(Path(value['path']), AttemptBudget(value['id'], value['limit']))
    units = {unit['unit_id']: unit for unit in ledger.snapshot()['units']}
    expected = value['limit'] // config['resident']['repetitions']
    if any(unit not in units or units[unit]['claimed'] or units[unit]['requests'] != expected
           for unit in value['unit_ids']):
        raise ValueError('reserve all fresh resident query units before creating a model')
    queries = [dict(query_id=config['query_id']+'-'+str(i), phase='measured',
        temperature='cold' if i == 0 else 'warm', unit_id=unit)
        for i, unit in enumerate(value['unit_ids'])]
    driver = ResidentDriver(config, checkout, data_dir, engine, generation, resources, ledger)
    return run_sequence(queries, lambda directory, first: driver, output,
        backend='lotus-native', lifecycle='resident',
        setup=dict(config_sha256=content_digest(config), source=config['resident']['binding']['source'],
            adapter_owner_ready_is_model_ready=False, extra_warmup=0, automatic_retries=0))
