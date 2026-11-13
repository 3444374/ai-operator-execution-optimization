"""Native SemAgg handler; the shared Kalypso runner owns the query lifecycle."""
import pandas as pd

from . import adapter as native
from .aggregate_binding import audit, response_text
from .native_server import physical_call_caps
from .query_runner import NativeOperation
from .sembench_inputs import prepare
from .topk_binding import criterion


class AggregateOperation(NativeOperation):
    kind = 'aggregate'
    source_metadata = dict(
        policy='original complete source and evaluator; native Kalypso SemAgg algorithm',
        native_aggregation='author data-dependent chunks, BlockingExecutor concurrency8, max_tokens8192',
        source_lineage='native context indices only; generated summary source lineage unknown',
        timing_scope='independent kalypso.source_to_sink.v1; native source/handoff/reduction and full original sink; '
                     'synchronous observer hashes and receipts remain included; post-sink audit/evaluation excluded',
    )

    def check_query(self, query):
        self.cap = physical_call_caps(self.driver.unit)[self.spec['query_id']]
        self.rows, self.text, self.response = [], None, None
        if len(query.operation.records) != self.spec['expected_rows']:
            raise ValueError('Original aggregate source differs from the declared row count')

    def execute(self, operation, invoke):
        with native.query_phase(self.timing, 'source_and_required_input'):
            self.rows, path = prepare(self.cfg, self.spec, self.directory, operation, kind=self.kind)
            payload = dict(data_path=str(path), ops=[dict(op='sem_agg', args=dict(
                instruction=criterion(operation.instruction, operation.columns)))], blocking=False)
            self.artifacts.update({'request.json': payload, 'occurrences.json': self.rows})
        with native.query_phase(self.timing, 'native_HTTP_and_JSON_parse'):
            self.response = self.driver.transport(payload, self.spec['query_id'], self.directory)
        with native.query_phase(self.timing, 'original_aggregate_relation'):
            self.text = response_text(self.response, self.cfg['model'], self.rows)
            return dict(frame=pd.DataFrame({'_output': [] if not self.rows else [self.text]}))

    def audit(self):
        events = native.read_trace(self.cfg['native_trace'], self.spec['query_id'])
        self.ends, ids = audit(self.text, events, rows=len(self.rows), cap=self.cap,
            model_uid=self.driver.model_uid, previous_ids=self.driver.request_ids)
        self.driver.request_ids.update(ids)

    def report_metadata(self, source):
        return dict(
            baseline_name='Kalypso native SemAgg with original SemBench source and evaluator',
            semantic_operator='author chunked multi-level SemAgg; original selection and final projection',
            execution_backend='author Kalypso BlockingExecutor and persistent native vLLM service',
            logical_occurrences=len(self.rows), physical_call_cap=self.cap,
            physical_native_calls=len(self.ends), expected_model_calls=None,
            native_API_units=self.ends, native_context_index_scope='native indices; source lineage unknown after merge',
            native_retry_policy='none', native_server_latency_s=self.response['latency_sec'],
            generation='author max_tokens8192, concurrency8, seed42/frequency.5/repetition1.3',
        )
