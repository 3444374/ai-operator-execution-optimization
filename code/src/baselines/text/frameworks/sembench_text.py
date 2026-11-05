"""Connect one original SemBench text operator to existing native baseline graphs."""

from copy import deepcopy
import json

from src.semantic_methods.lotus.record_chain import LotusRecordChain


def execute_records(operation, *, backend, engine, resources, query_id, profile,
                    control_dir, context=None):
    """The chosen framework owns its graph, batching and private model process."""
    context = {} if context is None else context
    chain = LotusRecordChain((operation.stage,), query_id=query_id)
    records = operation.records
    columns = list(operation.stage.method.input_columns if operation.stage.kind == 'extract'
                   else operation.stage.method.resolved_input_columns)
    stream = ray = None
    created_ray = False
    primary = None
    report = dict(backend=backend, status='failed', production_core_used=False)
    if not records:
        return dict(report, status='completed', frame=operation.result_frame(()), calls=[],
            scheduler_owner='empty relation; no framework or model allocated',
            backend_close=dict(status='closed', cleanup_errors=[]))
    try:
        if backend == 'ray-data':
            import ray
            import ray.data
            from .ray_data_lotus_chain import evaluate
            if ray.is_initialized():
                raise ValueError('this cold query entry requires an isolated Ray runtime')
            created_ray = True
            ray.init(**profile)
            dataset = ray.data.from_items([dict(record_json=json.dumps(row.describe(), ensure_ascii=False))
                                           for row in records])
            stream = evaluate(dataset, chain, engine, resources, expected=records,
                query_id=query_id, columns=columns, control_dir=control_dir)
            report['scheduler_owner'] = 'Ray Data Dataset / ActorPoolStrategy'
        elif backend == 'daft':
            import daft
            from .daft_lotus_chain import configure_native_runner, evaluate
            configure_native_runner(profile.get('runner_threads'))
            frame = daft.from_pylist([dict(record_json=json.dumps(row.describe(), ensure_ascii=False))
                                      for row in records])
            stream = evaluate(frame, chain, engine, resources, expected=records,
                query_id=query_id, columns=columns, control_dir=control_dir, **profile)
            report['scheduler_owner'] = 'Daft native class / batch graph'
        elif backend == 'imlane':
            from integrations.imlane_records.chain_backend import IMLaneChainBackend
            owner = IMLaneChainBackend(engine=engine, resources=resources,
                author_args=context['author_args'], model_environment=context['model_environment'],
                cancelled=context.get('cancelled'))
            stream = owner.evaluate(iter(records), chain, expected=records, columns=columns)
            report['scheduler_owner'] = 'author IMLane DBEnd / Coordinator / Lane'
        else:
            raise ValueError('no native record graph for backend ' + backend)
        output = list(stream)
        if stream.completed is not True:
            raise RuntimeError('native operator did not establish complete successful EOF')
        result = operation.result_frame(output)
        report['status'] = 'completed'
    except BaseException as error:
        primary = error
        report['calls'] = deepcopy(getattr(error, 'chain_calls', []))
    finally:
        if stream is not None:
            try:
                report['backend_close'] = stream.close()
                if (report['backend_close'].get('status') != 'closed'
                        or report['backend_close'].get('cleanup_errors')):
                    raise RuntimeError('native operator cleanup remains unconfirmed')
            except BaseException as error:
                report['backend_close'] = deepcopy(stream.last_close_report) or dict(
                    status='unknown', cleanup_errors=[])
                report['backend_close'].update(status='unknown', close_error_type=type(error).__name__)
                report['status'] = 'failed'
                if primary is None:
                    primary = error
                else:
                    primary.add_note('native operator close failed: ' + type(error).__name__)
            try:
                report['calls'] = stream.calls
                report['stage_stats'] = stream.batch_stage_stats
            except BaseException as error:
                report['status'] = 'failed'
                if primary is None:
                    primary = error
                else:
                    primary.add_note('native operator accounting failed: ' + type(error).__name__)
        if created_ray:
            report['ray_runtime_closed'] = None
            try:
                ray.shutdown()
                report['ray_runtime_closed'] = not ray.is_initialized()
                if not report['ray_runtime_closed']:
                    raise RuntimeError('the original query Ray runtime is still active')
            except BaseException as error:
                report['status'] = 'failed'
                if primary is None:
                    primary = error
                else:
                    primary.add_note('Ray runtime close failed: ' + type(error).__name__)
    if primary is not None:
        primary.operator_report = report
        raise primary
    return dict(report, frame=result)
