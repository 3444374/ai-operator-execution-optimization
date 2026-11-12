"""Original full-query sink around the author's full-ranking TopK algorithm."""
import time
from pathlib import Path

from src.baselines.text.sembench_prepared import result_value
from . import adapter as native
from .native_server import physical_call_caps
from .sembench_inputs import make_query,prepare
from .topk_binding import audit,criterion


class TopKDriver(native.NativeQueryDriver):
    def query(self,spec,emit):
        if spec!=self.unit['queries'][self.next_query]:raise ValueError('Query sequence changed')
        self.next_query+=1
        cap=physical_call_caps(self.unit)[spec['query_id']]
        directory=self.output/spec['query_id'];directory.mkdir(mode=0o700)
        with self.query_evidence(spec,directory,
                sink_scope='complete original SemBench query using author full-ranking order',
                source_scope='fresh original query source and author-required CSV') as (report,artifacts,timing):
            with native.query_phase(timing,'source_preflight'):
                query=make_query(self.cfg,spec['query_id'],spec,kind='topk');execute_query=query.bind_execution()
                if query.operation is None and spec['expected_rows']!=0:
                    raise ValueError('Original empty ranking differs from the bound source count')
            responses=[];order=[]
            def execute(operation,invoke):
                with native.query_phase(timing,'source_and_required_input'):
                    rows,path=prepare(self.cfg,spec,directory,operation,kind='topk')
                    payload=dict(data_path=str(path),ops=[dict(op='sem_topk',args=dict(
                        instruction=criterion(operation.instruction,operation.columns),k=len(rows)))],blocking=False)
                    artifacts['request.json']=payload;artifacts['occurrences.json']=rows
                with native.query_phase(timing,'native_HTTP_and_JSON_parse'):
                    response=self.transport(payload,spec['query_id'],directory)
                responses.append(response)
                with native.query_phase(timing,'original_ranked_relation'):
                    results=native.response_results(response,self.cfg['model'],len(rows))
                    if len(results)!=len(rows):raise ValueError('Native full ranking lost source occurrences')
                    order.extend(row['idx'] for row in results)
                    return dict(frame=operation.frame.iloc[order].copy(deep=True),
                        native_stats=dict(owner='Kalypso SemTopK',algorithm='author insertion/merge full ranking'))
            timing.update(query_clock_version='sembench.source_to_sink.v3',source_started_ns=time.monotonic_ns(),
                model_ready_before_source=self.model_ready_observed_ns is not None,
                model_ready_before_source_observed_ns=self.model_ready_observed_ns,
                phase_overlap='original_query includes source, HTTP and relation; do not add nested phases')
            with native.query_phase(timing,'original_query'):
                frame,_=execute_query(execute)
                if len(responses)!=(0 if query.operation is None else 1):
                    raise ValueError('Binding requires one original full-ranking query')
            timing['memory_sink_ready_ns']=time.monotonic_ns();emit('memory_sink_ready')
            with native.query_phase(timing,'trace_audit'):
                timing['trace_audit_status']='running'
                events=native.read_trace(self.cfg['native_trace'],spec['query_id'])
                comparisons=native.read_trace(Path(self.cfg['native_trace']).with_name('topk-trace.jsonl'),spec['query_id'])
                ends,ids=audit(order,events,comparisons,rows=len(order),cap=cap,
                    model_uid=self.model_uid,previous_ids=self.request_ids)
                self.request_ids.update(ids);artifacts['comparisons.json']=comparisons
                timing['trace_audit_status']='passed'
            with native.query_phase(timing,'source_snapshot'):
                query.verify_source();source=query.describe()
                source.update(policy='original full query; native Kalypso TopK instead of LOTUS quick ranking',
                    native_ranking='author insertion below8 rows, recursive merge at8; K=N,5tokens,no retry')
                artifacts['source.json']=source
            with native.query_phase(timing,'result_encoding'):
                result=result_value(frame);artifacts['result.json']=result
            emit('result_ready')
            with native.query_phase(timing,'quality_evaluation'):
                quality=query.evaluate(frame);artifacts['quality.json']=quality
            report.update(status='completed',query_id=spec['query_id'],backend='kalypso-native',
                baseline_name='Kalypso native SemTopK with original SemBench query and evaluator',
                semantic_operator='author full-ranking SemTopK, original selection and rank-to-score sink',
                execution_backend='author Kalypso blocking operator and persistent native vLLM service',
                logical_occurrences=len(order),physical_call_cap=cap,physical_native_calls=len(ends),
                native_API_units=ends,ranked_source_positions=order,result=result,quality=quality,
                native_retry_policy='none',native_server_latency_s=responses[0]['latency_sec'] if responses else None,
                generation='author max_tokens5, seed42/frequency.5/repetition1.3')
        return report
