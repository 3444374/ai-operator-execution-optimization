"""Native full-ranking adaptation; query lifetime belongs to KalypsoRunner."""
from pathlib import Path

from . import adapter as native
from .native_server import physical_call_caps
from .query_runner import NativeOperation
from .sembench_inputs import prepare
from .topk_binding import audit, criterion


class TopKOperation(NativeOperation):
    kind = 'topk'
    source_metadata = dict(
        policy='original full query; native Kalypso TopK instead of LOTUS quick ranking',
        native_ranking='author insertion below8 rows, recursive merge at8; K=N,5tokens,no retry',
    )

    def check_query(self, query):
        self.cap = physical_call_caps(self.driver.unit)[self.spec['query_id']]
        self.order = []
        self.response = None
        if query.operation is None and self.spec['expected_rows'] != 0:
            raise ValueError('Original empty ranking differs from the bound source count')

    def execute(self, operation, invoke):
        with native.query_phase(self.timing, 'source_and_required_input'):
            rows, path = prepare(self.cfg, self.spec, self.directory, operation, kind='topk')
            payload = dict(data_path=str(path), ops=[dict(op='sem_topk', args=dict(
                instruction=criterion(operation.instruction, operation.columns), k=len(rows)))], blocking=False)
            self.artifacts.update({'request.json': payload, 'occurrences.json': rows})
        with native.query_phase(self.timing, 'native_HTTP_and_JSON_parse'):
            self.response = self.driver.transport(payload, self.spec['query_id'], self.directory)
        with native.query_phase(self.timing, 'original_ranked_relation'):
            results = native.response_results(self.response, self.cfg['model'], len(rows))
            if len(results) != len(rows):
                raise ValueError('Native full ranking lost source occurrences')
            self.order = [row['idx'] for row in results]
            return dict(frame=operation.frame.iloc[self.order].copy(deep=True),
                native_stats=dict(owner='Kalypso SemTopK', algorithm='author insertion/merge full ranking'))

    def audit(self):
        qid = self.spec['query_id']
        events = native.read_trace(self.cfg['native_trace'], qid)
        comparisons = native.read_trace(Path(self.cfg['native_trace']).with_name('topk-trace.jsonl'), qid)
        self.ends, ids = audit(self.order, events, comparisons, rows=len(self.order), cap=self.cap,
            model_uid=self.driver.model_uid, previous_ids=self.driver.request_ids)
        self.driver.request_ids.update(ids)
        self.artifacts['comparisons.json'] = comparisons

    def report_metadata(self, source):
        return dict(
            baseline_name='Kalypso native SemTopK with original SemBench query and evaluator',
            semantic_operator='author full-ranking SemTopK, original selection and rank-to-score sink',
            execution_backend='author Kalypso blocking operator and persistent native vLLM service',
            logical_occurrences=len(self.order), physical_call_cap=self.cap, physical_native_calls=len(self.ends),
            native_API_units=self.ends, ranked_source_positions=self.order,
            native_retry_policy='none', native_server_latency_s=self.response['latency_sec'] if self.response else None,
            generation='author max_tokens5, seed42/frequency.5/repetition1.3',
        )
