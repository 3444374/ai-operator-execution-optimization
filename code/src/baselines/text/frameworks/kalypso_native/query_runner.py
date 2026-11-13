"""One query lifecycle for Kalypso's native semantic operators.

Original SemBench query functions still own the relational computation and
scoring. Operator handlers only adapt a native call and restore its relation.
"""
import time

from src.baselines.text.sembench_prepared import result_value
from . import adapter as native
from .sembench_inputs import make_query


class KalypsoRunner(native.NativeQueryDriver):
    def query(self, spec, emit):
        if spec != self.unit['queries'][self.next_query]:
            raise ValueError('Query sequence changed')
        self.next_query += 1
        directory = self.output / spec['query_id']
        directory.mkdir(mode=0o700)
        operator_type = operator_handler(spec)
        with self.query_evidence(
            spec, directory,
            sink_scope='complete original SemBench query relation in memory',
            source_scope='fresh original query source and author-required native inputs',
        ) as (report, artifacts, timing):
            operator = operator_type(self, spec, directory, artifacts, timing)
            with native.query_phase(timing, 'source_preflight'):
                query = make_query(self.cfg, spec['query_id'], spec, kind=operator.kind)
                execute_query = query.bind_execution()
                operator.check_query(query)
            timing.update(
                query_clock_version=('kalypso.source_to_sink.v1' if operator.kind=='aggregate'
                                     else 'sembench.source_to_sink.v3'),
                source_started_ns=time.monotonic_ns(),
                model_ready_before_source=self.model_ready_observed_ns is not None,
                model_ready_before_source_observed_ns=self.model_ready_observed_ns,
                phase_overlap='original_query includes source, HTTP and relation; do not add nested phases',
            )
            with native.query_phase(timing, 'original_query'):
                frame, _ = execute_query(operator.execute)
            timing['memory_sink_ready_ns'] = time.monotonic_ns()
            emit('memory_sink_ready')
            with native.query_phase(timing, 'result_encoding'):
                result = result_value(frame)
                artifacts['result.json'] = result
            with native.query_phase(timing, 'trace_audit'):
                timing['trace_audit_status'] = 'running'
                operator.audit()
                timing['trace_audit_status'] = 'passed'
            with native.query_phase(timing, 'source_snapshot'):
                query.verify_source()
                source = query.describe()
                source.update(operator.source_metadata)
                artifacts['source.json'] = source
            emit('result_ready')
            with native.query_phase(timing, 'quality_evaluation'):
                started = time.monotonic_ns()
                quality = query.evaluate(frame)
                quality.update(operator.quality_metadata())
                evaluation_seconds = (time.monotonic_ns() - started) / 1e9
                artifacts['quality.json'] = quality
            report.update(
                status='completed', query_id=spec['query_id'], backend='kalypso-native',
                result=result, quality=quality, evaluation_seconds=evaluation_seconds,
                **operator.report_metadata(source),
            )
        return report


class NativeOperation:
    def __init__(self, driver, spec, directory, artifacts, timing):
        self.driver, self.spec, self.directory = driver, spec, directory
        self.cfg, self.artifacts, self.timing = driver.cfg, artifacts, timing

    def check_query(self, query):
        pass

    def quality_metadata(self):
        return {}


def operator_handler(spec):
    from .native_server import MAP_TASKS
    from .movie_map import MapOperation
    from .sembench_filter import FilterOperation
    from .sembench_join import JoinOperation
    from .sembench_topk import TopKOperation
    from .sembench_aggregate import AggregateOperation
    if spec['task'] in MAP_TASKS:
        return MapOperation
    return {'sembench-filter': FilterOperation, 'sembench-join': JoinOperation,
            'sembench-topk': TopKOperation, 'sembench-aggregate': AggregateOperation}[spec['task']]
