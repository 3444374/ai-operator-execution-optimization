"""Run one original text operation through the existing finite Core owner."""

from copy import deepcopy

from src.execution_provider.adapters.lotus_record_chain_core import SemLoomRecordChainCore
from src.execution_provider.adapters.records_capacity import CoreChainCapacity, CoreRecordsCapacity
from src.semantic_methods.lotus.record_chain import LotusRecordChain


def configuration(profile, resources):
    if (type(profile) is not dict or 'execution' not in profile
            or profile.keys() - {'execution', 'capacity', 'ray_init'}):
        raise ValueError('Core profile requires execution and optional capacity/ray_init')
    execution = profile['execution']
    if execution not in ('fused', 'record_local_sections'):
        raise ValueError('Core execution must be fused or record_local_sections')
    if type(profile.get('capacity', {})) is not dict or type(profile.get('ray_init', {})) is not dict:
        raise ValueError('Core capacity and ray_init must be objects')
    resources.request_capacity()
    if resources.cpu_workers != 1:
        raise ValueError('finite Core text execution requires one CPU worker')
    if execution == 'fused':
        if resources.max_active_batches != 1 or 'ray_init' in profile:
            raise ValueError('fused Core requires max_active_batches=1 and no Ray configuration')
        capacity = CoreChainCapacity(**profile.get('capacity', {}))
    else:
        if resources.prepare_batch_rows != 1 or resources.model_batch_rows != 1:
            raise ValueError('record-local Core requires prepare_batch_rows=model_batch_rows=1')
        capacity = CoreRecordsCapacity(**profile.get('capacity', {}))
        if capacity.budget is not None:
            raise ValueError('this initial Core binding uses count admission, not rolling resource budgets')
    return dict(execution=execution, capacity=capacity)


def execute(operation, *, engine, resources, query_id, profile, control_dir):
    options = configuration(profile, resources)
    report = dict(backend='semloom-core', execution=options['execution'], status='failed',
                  production_core_used=False, scheduler_owner='SemLoom SessionEngine')
    if not operation.records:
        return dict(report, status='completed', frame=operation.result_frame(()), calls=[],
                    backend_close=dict(status='closed', query_completed=True, cleanup_errors=[]),
                    scheduler_owner='empty relation; no Core, framework or model allocated')
    owner = ray = None
    created_ray = False
    primary = None
    try:
        chain = LotusRecordChain((operation.stage,), query_id=query_id)
        if options['execution'] == 'record_local_sections':
            import ray
            if ray.is_initialized():
                raise ValueError('this cold Core query requires an isolated Ray runtime')
            created_ray = True
            ray.init(**profile.get('ray_init', {}))
        owner = SemLoomRecordChainCore(engine, resources, control_dir=control_dir, **options)
        report['production_core_used'] = True
        columns = (list(operation.stage.method.input_columns if operation.stage.kind == 'extract'
                        else operation.stage.method.resolved_input_columns)
                   if options['execution'] == 'record_local_sections' else None)
        stream = owner.evaluate(iter(operation.records), chain, expected=operation.records,
                                query_id=query_id, columns=columns)
        rows = list(stream)
        if stream.completed is not True:
            raise RuntimeError('Core operator did not establish complete successful EOF')
        frame = operation.result_frame(rows)
        report['status'] = 'completed'
    except BaseException as error:
        primary = error
    finally:
        if owner is not None:
            try:
                report['backend_close'] = owner.close()
                close = report['backend_close']
                if close['status'] != 'closed' or close.get('cleanup_errors'):
                    raise RuntimeError('Core operator cleanup remains unconfirmed')
                if primary is None and close.get('query_completed') is not True:
                    raise RuntimeError('Core owner did not confirm query completion')
            except BaseException as error:
                report['status'] = 'failed'
                report['backend_close'] = deepcopy(owner.last_close_report) or dict(
                    status='unknown', cleanup_errors=[])
                report['backend_close'].update(status='unknown', close_error_type=type(error).__name__)
                if primary is None:
                    primary = error
                else:
                    primary.add_note('Core operator close failed: ' + type(error).__name__)
            try:
                report['calls'] = deepcopy(owner.calls)
            except BaseException as error:
                report['status'] = 'failed'
                if primary is None:
                    primary = error
                else:
                    primary.add_note('Core call accounting failed: ' + type(error).__name__)
        if created_ray:
            report['ray_driver_disconnected'] = None
            try:
                ray.shutdown()
                report['ray_driver_disconnected'] = not ray.is_initialized()
                if not report['ray_driver_disconnected']:
                    raise RuntimeError('the Core query Ray runtime is still active')
            except BaseException as error:
                report['status'] = 'failed'
                if primary is None:
                    primary = error
                else:
                    primary.add_note('Ray runtime close failed: ' + type(error).__name__)
    if primary is not None:
        primary.operator_report = report
        raise primary
    return dict(report, frame=frame)
