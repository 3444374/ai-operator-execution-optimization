"""Finite original SemBench text queries over existing native baseline operators.

Run workers inside the experiment's existing host lease/resource supervisor.
The entry creates no host lease, tuning loop, retries or performance claim.
"""

import argparse
from collections import Counter
from dataclasses import asdict
import json
from pathlib import Path
import sys
import time

from src.infrastructure.private_artifacts import (
    json_metric, new_private_directory, read_json, write_private_json,
)
from src.infrastructure.redact import redact_text


BACKENDS = ('lotus-native', 'ray-data', 'daft', 'imlane')


def configuration(path):
    from src.semantic_methods.lotus.records import RecordsResources
    from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec

    value = read_json(path)
    required = {'schema', 'scenario', 'query', 'query_id', 'backend', 'model_name',
                'generation', 'engine', 'resources', 'profile'}
    if (type(value) is not dict or not required <= value.keys()
            or value.keys() - required - {'budget', 'owner_config', 'scale_factor', 'lifecycle', 'resident', 'ray_native'}
            or value['schema'] != 'semloom.sembench_text_run.v1'
            or value['backend'] not in BACKENDS or type(value['profile']) is not dict):
        raise ValueError('an explicit single-operator SemBench text configuration is required')
    engine = EngineSpec.from_dict(value['engine'])
    generation_config = value['generation']
    if isinstance(generation_config, dict):
        # The original SemBench runner uses 8192 tokens and no stop sequence.
        # Short Filter sampling remains an explicit experiment override.
        generation_config = {'max_tokens': 8192, 'stop': [], **generation_config}
    generation = GenerationSpec.from_dict(generation_config)
    resources = RecordsResources(**value['resources'])
    profile = value['profile']
    if 'ray_native' in value:
        spec = value['ray_native']
        if (value['backend'] != 'ray-data' or type(spec) is not dict
                or set(spec) != {'engine_kwargs', 'processor_options'}
                or any(type(item) is not dict for item in spec.values())
                or profile.get('address', 'local') != 'local'
                or resources.query_timeout_s is None):
            raise ValueError('ray_native requires native engine/processor options and an owned local runtime')
        from src.baselines.text.frameworks.ray_data_sembench_native import _options
        _options(spec['processor_options'])
    if value['backend'] == 'lotus-native':
        if (set(profile) != {'native_lm_max_batch_size'}
                or type(profile['native_lm_max_batch_size']) is not int
                or profile['native_lm_max_batch_size'] < 1):
            raise ValueError('native LOTUS requires explicit native_lm_max_batch_size')
    elif value['backend'] == 'daft' and profile.keys() - {'runner_threads'}:
        raise ValueError('the existing Daft chain profile accepts runner_threads only')
    elif value['backend'] == 'imlane' and profile:
        raise ValueError('IMLane uses its existing author owner/resource configuration')
    if value['backend'] != 'lotus-native' and resources.cpu_workers != 1:
        raise ValueError('the existing finite native record graphs support cpu_workers=1')
    if value['backend'] == 'imlane':
        resources.request_capacity()
    lifecycle = value.get('lifecycle', 'isolated')
    if lifecycle not in ('isolated', 'resident'):
        raise ValueError('choose isolated or explicit native LOTUS resident lifecycle')
    if lifecycle == 'resident':
        from .sembench_text_resident import validate_configuration
        validate_configuration(value)
    elif 'resident' in value:
        raise ValueError('resident settings require lifecycle=resident')
    return value, engine, generation, resources


def query_from_config(config, checkout, data_dir, generation):
    from src.baselines.text.sembench_text import SemBenchTextQuery

    return SemBenchTextQuery(checkout, data_dir, config['scenario'], config['query'],
        execution_id=config['query_id'], model_name=config['model_name'],
        generation=generation, scale_factor=config.get('scale_factor'))


def check(config, query, engine):
    expected = query.describe()['expected_model_calls']
    if engine.max_model_calls is None or engine.max_model_calls < expected:
        raise ValueError('engine.max_model_calls must cover the complete original operator')
    if engine.max_model_len is not None and query.generation.max_tokens > engine.max_model_len:
        raise ValueError('generation exceeds the explicit model context limit')
    return dict(status='checked_cpu_only', inference_calls=0, query=query.describe(),
        backend=config['backend'], engine=asdict(engine), generation=asdict(query.generation),
        effective_operator_generation=(None if query.operation is None else
                                       asdict(query.operation.stage.generation)),
        profile=config['profile'], native_processor=config.get('ray_native'), unsupported_backends={
            'kalypso': 'current binding is Movie Filter only; use existing Q4 route; MMQA/Medical/Map/Extract not bound'},
        execution_layout=('public Ray Data LLM processor; complete original sink before close' if 'ray_native' in config
                          else 'original LOTUS accessor / LM / batch_completion' if config['backend']=='lotus-native'
                          else 'existing native finite record-chain graph; one terminal F/M/E operator'),
        lifecycle='isolated cold query; native runtime/model startup and close included',
        environment_scope=('source/method/configuration only; native Ray SDK, engine interpreter and model remain unverified'
                           if 'ray_native' in config else
                           'source/method/configuration only; actual driver and model checked before run'))


def _frame_value(frame):
    import pandas as pd
    if isinstance(frame, pd.Series):
        frame = frame.to_frame()
    return json_metric(frame.to_dict(orient='split'))


def complete_calls(query, operator):
    """Require one confirmed successful result for each original occurrence."""
    raw = {} if operator is None else operator
    calls = raw.get('calls', [])
    calls = calls['records'] if type(calls) is dict else calls
    keys = [(row['row_id'], row['source_position']) for row in calls]
    declared = [] if query.operation is None else [(r.row_id,r.source_position) for r in query.operation.records]
    if (len(keys) != len(set(keys)) or set(keys) != set(declared)
            or any(row['status'] != 'succeeded' for row in calls)):
        raise RuntimeError('the complete original operator lacks unique successful occurrence calls')
    return calls


def run_query(config, query, engine, resources, directory, *, budget=None, context=None,
              execute_operator=None):
    """Legacy isolated clock; it is not the separate source-to-sink metric."""
    report = dict(schema='semloom.sembench_text_result.v1', status='failed',
        query=query.describe(), backend=config['backend'], production_core_used=False,
        lifecycle='cold isolated query', result_cache_enabled=False,
        warmup_calls=0, retries=0, performance_claim=False,
        query_timing_scope='CSV scan and identity check, original selection, native graph/model startup, '
            'complete operator interpretation, adapter audit and close, original relational sink',
        source_to_sink_seconds=None,
        source_to_sink_unavailable_reason='isolated operator API returns after adapter audit and close; '
            'no independent pre-close full-sink boundary was observed',
        evaluation_in_query_seconds=False, physical_gpu_exit_confirmed=None,
        physical_gpu_exit_scope='independent existing outer host supervisor')
    started = time.monotonic()
    try:
        def execute(operation, invoke):
            if execute_operator is not None:
                return execute_operator(operation, invoke)
            if config['backend'] == 'lotus-native':
                from src.baselines.text.frameworks.lotus_native_stage import execute_native
                return execute_native(operation.frame, operation.stage, operation.records, invoke=invoke,
                    engine=engine, resources=resources, query_id=query.query_id,
                    profile=config['profile'], budget=budget)
            from src.baselines.text.frameworks.sembench_text import execute_records
            return execute_records(operation, backend=config['backend'], engine=engine,
                resources=resources, query_id=query.query_id, profile=config['profile'],
                control_dir=directory, context=context)

        result, operator = query.execute(execute)
        report['query_seconds'] = time.monotonic() - started
        report['operator'] = {} if operator is None else {k:v for k,v in operator.items() if k != 'frame'}
        calls = complete_calls(query, operator)
        expected = query.describe()['expected_model_calls']
        report['model_calls'] = len(calls)
        report['call_statuses'] = dict(Counter(row['status'] for row in calls))
        report['expected_model_calls'] = expected
        report['result'] = _frame_value(result)
        evaluated = time.monotonic()
        report['quality'] = json_metric(query.evaluate(result))
        report['evaluation_seconds'] = time.monotonic() - evaluated
        report['status'] = 'completed'
    except BaseException as error:
        report.update(error_type=type(error).__name__, error=redact_text(str(error)))
        operator = getattr(error, 'operator_report', None)
        if operator is not None:
            report['operator'] = {k:v for k,v in operator.items() if k != 'frame'}
        report['elapsed_seconds'] = time.monotonic() - started
        error.query_report = report
        raise
    report['elapsed_seconds'] = time.monotonic() - started
    return report


def fixture(config, query, responses, directory):
    """Real formatter/parser and original query/evaluator, fixed software answers."""
    from src.semantic_methods.lotus.record_chain import LotusRecordChain

    expected = query.describe()['expected_model_calls']
    if (type(responses) is not list or len(responses) != expected
            or any(type(text) is not str for text in responses)):
        raise ValueError('fixture responses must cover every original operator input')

    def execute(operation, invoke):
        by_position = {r.source_position:text for r,text in zip(operation.records,responses,strict=True)}
        def infer(prepared, *, generation):
            return [dict(row, status='succeeded', text=by_position[row['source_position']],
                prompt_tokens=1, completion_tokens=1, total_tokens=2, finish_reason='stop',
                engine_request_id='fixed-'+row['call_id'], submission_confirmed=True,
                logprobs_json='null') for row in prepared]
        chain = LotusRecordChain((operation.stage,), query_id=query.query_id)
        output = chain.run_batch(list(operation.records), infer,
            columns=list(operation.stage.method.input_columns if operation.stage.kind == 'extract'
                         else operation.stage.method.resolved_input_columns))
        return dict(status='completed', frame=operation.result_frame(output['records']), calls=output['calls'])

    return run_query(config, query, None, None, directory, execute_operator=execute)


def run_worker(config, query, engine, resources, directory):
    if 'ray_native' in config:
        from .native_ray_sembench import run_query as run_native_ray
        # Native Ray owns its engine/dependencies. Do not impose the separate
        # SemLoom bridge's vLLM version on the author's official native profile.
        return run_native_ray(config, query, engine, resources, directory)
    engine.validate_driver_environment()
    if config['backend'] == 'imlane':
        from integrations.imlane_records.chain_runtime import read_owner, preflight_owner, AuthorRunGuard
        from .imlane_owned_query import run_owned_query
        owner = read_owner(config['owner_config'])
        return run_owned_query(directory, owner, query_id=query.query_id, backend='imlane',
            required_requests=max(1, query.describe()['expected_model_calls']),
            preflight=lambda: preflight_owner(owner, engine),
            execute=lambda args, guard: run_query(config, query, engine, resources, directory,
                context=dict(author_args=args, model_environment=owner['model_environment'], cancelled=guard)),
            guard_factory=AuthorRunGuard, write_report=write_private_json)
    from .attempt_ledger import AttemptBudget
    from .cell_budget import CellBudgetLedger
    value = config['budget']
    ledger = CellBudgetLedger(Path(value['path']), AttemptBudget(value['id'], value['limit']))
    try:
        claim = ledger.claim_unit(value['unit_id'])
        if claim.remaining < query.describe()['expected_model_calls']:
            raise ValueError('the already-reserved permanent unit cannot cover the original query')
        claim.bind_execution()
        report = run_query(config, query, engine, resources, directory, budget=claim)
    except BaseException as error:
        report = getattr(error, 'query_report', dict(status='failed', error_type=type(error).__name__,
                                                   error=redact_text(str(error))))
        report['permanent_budget'] = ledger.snapshot()
        write_private_json(directory/'query.json', report)
        raise
    report['permanent_budget'] = ledger.snapshot()
    write_private_json(directory/'query.json', report)
    return report


def main(argv=None, *, controller_entry_ns=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='action', required=True)
    prepare = commands.add_parser('prepare-mmqa-text')
    prepare.add_argument('--source-dir', required=True)
    prepare.add_argument('--sembench-checkout', required=True)
    prepare.add_argument('--output-dir', required=True)
    prepare.add_argument('--scale-factor', required=True, type=int)
    prepare_medical = commands.add_parser('prepare-medical-text',
        help='copy three existing Medical CSVs without multimodal preparation or downloads')
    prepare_medical.add_argument('--source-dir', required=True)
    prepare_medical.add_argument('--sembench-checkout', required=True)
    prepare_medical.add_argument('--output-dir', required=True)
    prepare_medical.add_argument('--scale-factor', type=int, default=11112)
    for name in ('check','fixture','run'):
        command = commands.add_parser(name)
        for field in ('config','data-dir','sembench-checkout'):
            command.add_argument('--'+field, required=True)
        if name != 'check':
            command.add_argument('--output-dir', required=True)
        if name == 'fixture':
            command.add_argument('--responses', required=True)
        if name == 'run':
            command.add_argument('--execute-model', action='store_true')
            command.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.action == 'prepare-mmqa-text':
        from src.baselines.text.sembench_mmqa import prepare_text_inputs
        report = prepare_text_inputs(args.sembench_checkout,args.source_dir,args.output_dir,args.scale_factor)
        print(json.dumps(report, ensure_ascii=False))
        return 0
    if args.action == 'prepare-medical-text':
        from src.baselines.text.sembench_medical import prepare_text_inputs
        report = prepare_text_inputs(args.sembench_checkout,args.source_dir,args.output_dir,args.scale_factor)
        print(json.dumps(report, ensure_ascii=False))
        return 0
    config, engine, generation, resources = configuration(args.config)
    query = query_from_config(config,args.sembench_checkout,args.data_dir,generation)
    checked = check(config,query,engine)
    resident = config.get('lifecycle', 'isolated') == 'resident'
    if resident:
        from .sembench_text_resident import verify_binding
        checked.update(lifecycle='explicit same-owner resident; fresh source per query',
            binding=verify_binding(config,query,engine,assets=not getattr(args,'worker',False)))
    if args.action == 'check' or args.action == 'run' and not args.execute_model:
        print(json.dumps(checked, ensure_ascii=False))
        return 0
    root = Path(args.output_dir).resolve()
    if args.action == 'fixture':
        if resident:
            raise ValueError('resident CPU qualification uses tests.baselines.text.replay_lotus_resident')
        new_private_directory(root)
        try:
            report = fixture(config,query,read_json(args.responses),root)
        except BaseException as error:
            report = getattr(error,'query_report',dict(status='failed',error_type=type(error).__name__,
                                                       error=redact_text(str(error))))
            report.update(inference_calls=0, execution_kind='fixed-response software qualification')
            write_private_json(root/'query.json',report)
            raise
        report.update(inference_calls=0, execution_kind='fixed-response software qualification')
        write_private_json(root/'query.json',report)
        print(json.dumps(dict(status=report['status'],inference_calls=0)))
        return 0
    if args.worker:
        if resident:
            from .sembench_text_resident import run
            from src.infrastructure.private_artifacts import content_digest
            if read_json(root/'preflight.json')['binding']['config_sha256'] != content_digest(config):
                raise ValueError('resident worker differs from the validated launch configuration')
            engine.validate_driver_environment()
            run(config,args.sembench_checkout,args.data_dir,engine,generation,resources,root/'session')
        else:
            run_worker(config,query,engine,resources,root)
    else:
        from .lotus_records_supervisor import supervise
        new_private_directory(root)
        write_private_json(root/'preflight.json',checked)
        command = [sys.executable,str(Path(__file__).parents[2]/'scripts/experiments/run_sembench_text_query.py'),
            'run','--config',str(Path(args.config).resolve()),'--data-dir',str(Path(args.data_dir).resolve()),
            '--sembench-checkout',str(Path(args.sembench_checkout).resolve()),'--output-dir',str(root),
            '--execute-model','--worker']
        if resident:
            from src.observability.native_query_timing import resident_launch_timing
            spec = config['resident']
            supervision = supervise(command,root,timeout_s=spec['timeout_s'],grace_s=spec['cleanup_s'])
            timing = resident_launch_timing(supervision,read_json(root/'session/session.json'),controller_entry_ns)
            write_private_json(root/'resident-timing.json',timing)
        else:
            supervise(command,root,timeout_s=None if resources.query_timeout_s is None else resources.startup_timeout_s+resources.query_timeout_s+30)
    return 0
