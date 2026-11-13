"""Native Filter adaptation; query lifetime belongs to KalypsoRunner."""
from . import adapter as native
from .query_runner import NativeOperation
from .sembench_inputs import prepare


class FilterOperation(NativeOperation):
    kind = 'filter'
    operator_name = 'SemFilter'
    prompt_parser_identity = 'original fields in author Filter template; absence of false passes; not LOTUS parser'

    @property
    def source_metadata(self):
        return dict(policy='original full query with author ' + self.operator_name + '; cache disabled; no warmup or transport retry')

    def prepare_operator(self, operation):
        values, path = prepare(self.cfg, self.spec, self.directory, operation, kind='filter')
        payload = dict(data_path=str(path), ops=[dict(op='sem_filter',
            args=dict(prompt=operation.stage.method.instruction))], blocking=False)
        return values, payload

    def operator_relation(self, operation, invoke, decisions):
        kept = [record for record, decision in zip(operation.records, decisions, strict=True) if decision['decision']]
        return operation.result_frame(kept)

    def execute(self, operation, invoke):
        with native.query_phase(self.timing, 'source_and_required_input'):
            self.rows, payload = self.prepare_operator(operation)
        self.artifacts.update({'request.json': payload, 'occurrences.json': self.rows})
        with native.query_phase(self.timing, 'native_HTTP_and_JSON_parse'):
            self.response = self.driver.transport(payload, self.spec['query_id'], self.directory)
        with native.query_phase(self.timing, 'original_' + self.kind.title() + '_relation'):
            decisions = native.response_decisions(self.rows, self.response, self.cfg['model'])
            return dict(frame=self.operator_relation(operation, invoke, decisions), calls=[])

    def audit(self):
        events = native.read_trace(self.cfg['native_trace'], self.spec['query_id'])
        self.decisions = native.reconcile(self.rows, self.response, events, self.cfg['model'])
        self.ends = [e for e in events if e['event'] == 'native_API_end']
        ids = {e['request_id'] for e in self.ends}
        if ids & self.driver.request_ids or any(e['model_uid'] != self.driver.model_uid for e in self.ends):
            raise ValueError('Owner or request IDs changed')
        self.driver.request_ids.update(ids)
        self.artifacts['decisions.json'] = self.decisions

    def quality_metadata(self):
        return dict(native_nonliteral_verdicts=sum(r['native_verdict'] not in ('true', 'false') for r in self.decisions))

    def report_metadata(self, source):
        return dict(
            baseline_name='Kalypso author ' + self.operator_name + ' with original SemBench instruction-field binding',
            semantic_operator=self.operator_name + ', original ' + source['scenario'] + ' ' + source['query'] + ' relation and evaluator',
            execution_backend='author Kalypso controller and native vLLM persistent service',
            logical_occurrences=self.spec['expected_calls'], physical_call_cap=self.spec['expected_calls'],
            physical_native_calls=len(self.ends), native_API_units=self.ends,
            native_retry_policy='none; original Filter has one request per occurrence',
            native_server_latency_s=self.response['latency_sec'], prompt_parser_identity=self.prompt_parser_identity,
            generation='author max_tokens8, seed42/frequency.5/repetition1.3',
        )
