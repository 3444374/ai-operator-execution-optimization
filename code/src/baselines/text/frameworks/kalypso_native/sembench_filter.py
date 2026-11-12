"""Original text Filter queries using the author's predicate and service owner."""
import time

from src.baselines.text.sembench_prepared import result_value
from . import adapter as native
from .sembench_inputs import make_query, prepare


class FilterDriver(native.NativeQueryDriver):
    def query(self,spec,emit):
        if spec!=self.unit['queries'][self.next_query]:
            raise ValueError('Query sequence changed')
        self.next_query+=1
        directory=self.output/spec['query_id'];directory.mkdir(mode=0o700)
        with self.query_evidence(spec,directory,
                sink_scope='complete original SemBench query using native Filter membership in memory',
                source_scope='fresh original query source plus author-required CSV and field table') as (report,artifacts,timing):
            timing['phase_overlap']='original_query includes input, HTTP and original relational work; do not add nested phase durations'
            with native.query_phase(timing,'source_preflight'):
                query=make_query(self.cfg,spec['query_id'],spec,kind='filter')
                execute_query=query.bind_execution()
            responses=[];rows=[]
            def execute(operation,unused_original):
                with native.query_phase(timing,'source_and_required_input'):
                    values,path=prepare(self.cfg,spec,directory,operation,kind='filter')
                    rows.extend(values)
                    payload=dict(data_path=str(path),ops=[dict(op='sem_filter',
                        args=dict(prompt=operation.stage.method.instruction))],blocking=False)
                artifacts['request.json']=payload;artifacts['occurrences.json']=values
                with native.query_phase(timing,'native_HTTP_and_JSON_parse'):
                    response=self.transport(payload,spec['query_id'],directory)
                responses.append(response)
                with native.query_phase(timing,'original_Filter_relation'):
                    decisions=native.response_decisions(values,response,self.cfg['model'])
                    kept=[record for record,decision in zip(operation.records,decisions,strict=True) if decision['decision']]
                    return dict(frame=operation.result_frame(kept),calls=[])
            timing.update(query_clock_version='sembench.source_to_sink.v3',source_started_ns=time.monotonic_ns(),
                model_ready_before_source=self.model_ready_observed_ns is not None,
                model_ready_before_source_observed_ns=self.model_ready_observed_ns)
            with native.query_phase(timing,'original_query'):
                frame,_=execute_query(execute)
                if len(responses)!=1:raise ValueError('Binding requires one original native Filter query')
            timing['memory_sink_ready_ns']=time.monotonic_ns();emit('memory_sink_ready')
            with native.query_phase(timing,'trace_audit'):
                timing['trace_audit_status']='running'
                events=native.read_trace(self.cfg['native_trace'],spec['query_id'])
                decisions=native.reconcile(rows,responses[0],events,self.cfg['model'])
                ends=[e for e in events if e['event']=='native_API_end']
                ids={e['request_id'] for e in ends}
                if ids&self.request_ids or {e['model_uid'] for e in ends}!={self.model_uid}:
                    raise ValueError('Owner or request IDs changed')
                self.request_ids.update(ids)
                artifacts['decisions.json']=decisions
                timing['trace_audit_status']='passed'
            with native.query_phase(timing,'source_snapshot'):
                query.verify_source()
                source=query.describe()
                source['policy']='original full query with author Filter predicate; cache disabled; no warmup or transport retry'
                artifacts['source.json']=source
            with native.query_phase(timing,'result_encoding'):
                result=result_value(frame);artifacts['result.json']=result
            emit('result_ready')
            with native.query_phase(timing,'quality_evaluation'):
                quality=query.evaluate(frame)
                quality['native_nonliteral_verdicts']=sum(r['native_verdict'] not in ('true','false') for r in decisions)
                artifacts['quality.json']=quality
            report.update(status='completed',query_id=spec['query_id'],backend='kalypso-native',
                baseline_name='Kalypso author SemFilter with original SemBench instruction-field binding',
                semantic_operator='SemFilter, original '+source['scenario']+' '+source['query']+' relation and evaluator',
                execution_backend='author Kalypso controller and native vLLM persistent service',
                logical_occurrences=spec['expected_calls'],physical_call_cap=spec['expected_calls'],
                physical_native_calls=len(ends),native_API_units=ends,result=result,quality=quality,
                native_retry_policy='none; original Filter has one request per occurrence',
                native_server_latency_s=responses[0]['latency_sec'],
                prompt_parser_identity='original fields in author Filter template; absence of false passes; not LOTUS parser',
                generation='author max_tokens8, seed42/frequency.5/repetition1.3')
        return report
