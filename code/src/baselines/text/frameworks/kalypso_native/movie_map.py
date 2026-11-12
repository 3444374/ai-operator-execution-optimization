"""Original Movie Map queries using the existing native owner session."""
import csv
from dataclasses import replace
from numbers import Integral
import json
from pathlib import Path
import time

from src.baselines.text.frameworks.kalypso_native import adapter as native
from src.baselines.text.sembench_text import SemBenchTextQuery
from src.serving.backends.vllm_protocol import GenerationSpec
from .native_server import MOVIE_MAP_QUERIES


def make_query(cfg,qid,task='movie-q9'):
    if task not in MOVIE_MAP_QUERIES:
        raise ValueError('A supported original Movie Map task is required; not TopK')
    query_id=MOVIE_MAP_QUERIES[task]
    hashes=cfg['source_data_sha256']
    required={'Reviews.csv','Movies.csv'} if query_id=='10' else {'Reviews.csv'}
    if not required<=set(hashes) or set(hashes)-{'Reviews.csv','Movies.csv'}:
        raise ValueError('Map requires the complete original source and evaluation file hashes')
    for name, expected in hashes.items():
        if native.sha(Path(cfg['data_dir'])/name) != expected:
            raise ValueError('Bound Map source data changed: '+name)
    # These stage metadata fields do not set author first-request SamplingParams.
    return SemBenchTextQuery(cfg['sembench'],cfg['data_dir'],'movie',query_id,execution_id=qid,
        model_name=cfg['model_name'],generation=GenerationSpec(max_tokens=8192),
        **({'ranking':'map'} if query_id=='9' else {}))


def prepare(cfg,spec,directory,operation):
    if (operation is None or operation.stage.kind != 'map'
            or len(operation.records) != spec['expected_calls']):
        raise ValueError('Original Map selection differs from the bound complete input')
    frame = operation.frame
    records = operation.records
    values = [dict(idx=i,row_id=r.row_id,source_position=r.source_position,source_index=r.source_index,
        reviewId=(int(frame.iloc[i]['reviewId']) if isinstance(frame.iloc[i]['reviewId'],Integral)
                  else frame.iloc[i]['reviewId']),reviewText=r.values['reviewText']) for i,r in enumerate(records)]
    if len({v['row_id'] for v in values}) != len(values):
        raise ValueError('Duplicate occurrence identity')
    directory.mkdir(mode=0o700,parents=True,exist_ok=True)
    path = directory/'native-input.csv'
    with path.open('x',newline='') as out:
        w=csv.DictWriter(out,fieldnames=['data']);w.writeheader();w.writerows({'data':v['reviewText']} for v in values)
    table=Path(cfg['map_prompt_root'])/(spec['query_id']+'.json');table.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    with table.open('x') as out:json.dump(dict(query_id=spec['query_id'],instruction=operation.stage.method.instruction,rows=values),out,ensure_ascii=False)
    return values,path


def reconcile(response,events,qid,expected):
    """Audit physical attempts without replacing the native retry algorithm."""
    events=[e for e in events if e.get('query_id')==qid]
    ends=[e for e in events if e['event']=='native_API_end']
    mapped=[e for e in events if e['event']=='map_result']
    starts=[e for e in events if e['event']=='native_API_start']
    sampling=[e for e in events if e['event']=='native_sampling']
    if (len(mapped)!=expected or any(type(e.get('idx')) is not int for e in mapped)
            or {e['idx'] for e in mapped}!=set(range(expected))):
        raise ValueError('Incomplete, duplicate or unknown Map outputs')
    if (not expected<=len(ends)<=2*expected
            or any(e.get('status')!='succeeded' or type(e.get('idx')) is not int
                   or not 0<=e['idx']<expected or e.get('pin') is not False
                   or type(e.get('max_tokens')) is not int or e['max_tokens']<=0
                   or not isinstance(e.get('request_id'),str) or not e['request_id'] for e in ends)
            or len({e['request_id'] for e in ends})!=len(ends)):
        raise ValueError('Invalid native terminal identity or physical call bound')
    indices=[]
    for group in (starts,sampling,ends):
        if (len(group)!=len(ends) or any(type(e.get('sequence')) is not int for e in group)
                or len({e['sequence'] for e in group})!=len(group)):
            raise ValueError('Incomplete or duplicate native API event sequence')
        indices.append({e['sequence']:e for e in group})
    if any(set(group)!=set(indices[0]) for group in indices[1:]):
        raise ValueError('Native API start/sampling/terminal sequence differs')
    for sequence,end in indices[2].items():
        start,sample=indices[0][sequence],indices[1][sequence]
        for key in ('idx','model','model_uid','max_tokens','pin','priority','messages_sha256'):
            if start.get(key)!=end.get(key):
                raise ValueError('Native API event identity differs: '+key)
        if sample['idx']!=end['idx'] or sample['model']!=end['model']:
            raise ValueError('Native sampling occurrence/model differs')
        values=sample['sampling']
        if (values['max_tokens']!=end['max_tokens'] or values['temperature']!=0
                or values['seed']!=42 or values['top_p']!=1 or values['frequency_penalty']!=.5
                or values['repetition_penalty']!=1.3 or values['stop'] not in (None,[])):
            raise ValueError('Original native Map sampling changed')
    for idx in range(expected):
        calls=sorted((e for e in ends if e['idx']==idx),key=lambda e:e['sequence'])
        if not 1<=len(calls)<=2:
            raise ValueError('Occurrence has an invalid native attempt count')
        retry=calls[0]['finish_reason']=='length' and calls[0]['max_tokens']<8192
        if len(calls)!=(2 if retry else 1) or (retry and calls[1]['max_tokens']!=8192):
            raise ValueError('Physical retry differs from original SemMap length branch')
    returned=response['results']
    if len(returned)!=expected or {r['idx'] for r in returned}!=set(range(expected)):
        raise ValueError('Native semantic response lost occurrence identities')
    by_idx={r['idx']:r['text'] for r in mapped}
    for row in returned:
        if len(row['outputs'])!=1 or row['outputs'][0]['value']!=by_idx[row['idx']]:
            raise ValueError('Native response differs from observed final Map result')
    return [by_idx[i] for i in range(expected)],ends


class MapDriver(native.NativeQueryDriver):
    def query(self,spec,emit):
        from lotus.sem_ops.postprocessors import map_postprocess
        from src.semantic_methods.lotus.record_method import LotusRecordMethod
        if spec!=self.unit['queries'][self.next_query]:raise ValueError('Query sequence changed')
        self.next_query+=1
        directory=self.output/spec['query_id'];directory.mkdir(mode=0o700)
        with self.query_evidence(spec,directory,
                sink_scope='complete original SemBench Map parser and final query relation in memory',
                source_scope='fresh original query source plus author-required CSV and prompt table') as (report,artifacts,timing):
            timing['phase_overlap']='original_query includes input, HTTP, parser and original relational work; do not add nested phase durations'
            native_report={};responses=[]
            with native.query_phase(timing,'source_preflight'):
                query=make_query(self.cfg,spec['query_id'],spec['task'])
                execute_query=query.bind_execution()
            def execute(operation,unused_original):
                with native.query_phase(timing,'source_and_required_input'):
                    values,path=prepare(self.cfg,spec,directory,operation)
                    payload=dict(data_path=str(path),ops=[dict(op='sem_map',args=dict(prompt=operation.stage.method.instruction))],blocking=False)
                artifacts['request.json']=payload;artifacts['occurrences.json']=values
                with native.query_phase(timing,'native_HTTP_and_JSON_parse'):
                    response=self.transport(payload,spec['query_id'],directory)
                responses.append(response)
                with native.query_phase(timing,'required_Map_parser'):
                    texts=native.response_map_texts(response,self.cfg['model'],spec['expected_calls'])
                    method=LotusRecordMethod(operation.stage)
                    parsed=map_postprocess(texts,method.lm,False).outputs
                    completed=[replace(r,values={**r.values,operation.stage.method.output_column:text}) for r,text in zip(operation.records,parsed,strict=True)]
                    native_report.update(rows=values,raw_outputs=texts,latency_s=response['latency_sec'])
                    return dict(frame=operation.result_frame(completed),calls=[])
            timing.update(query_clock_version='sembench.source_to_sink.v3',
                          source_started_ns=time.monotonic_ns(),
                          model_ready_before_source=self.model_ready_observed_ns is not None,
                          model_ready_before_source_observed_ns=self.model_ready_observed_ns)
            with native.query_phase(timing,'original_query'):
                frame,_=execute_query(execute)
                if len(responses)!=1:raise ValueError('Binding requires one original native Map query')
            timing['memory_sink_ready_ns']=time.monotonic_ns()
            emit('memory_sink_ready')
            with native.query_phase(timing,'result_encoding'):
                result=frame.to_dict(orient='split');artifacts['result.json']=result
            with native.query_phase(timing,'trace_audit'):
                timing['trace_audit_status']='running'
                events=native.read_trace(self.cfg['native_trace'],spec['query_id'])
                events+=native.read_trace(Path(self.cfg['native_trace']).with_name('map-trace.jsonl'),spec['query_id'])
                texts,ends=reconcile(responses[0],events,spec['query_id'],spec['expected_calls'])
                ids={e['request_id'] for e in ends}
                if ids&self.request_ids or {e['model_uid'] for e in ends}!={self.model_uid}:raise ValueError('Owner or request IDs changed')
                self.request_ids.update(ids)
                native_report.update(native_API_units=ends,
                    first_max_tokens=[min([e for e in ends if e['idx']==i],key=lambda e:e['sequence'])['max_tokens'] for i in range(spec['expected_calls'])])
                artifacts['native-map.json']=native_report
                timing['trace_audit_status']='passed'
            with native.query_phase(timing,'source_snapshot'):
                query.verify_source()
                source=query.describe()
                source['policy']='original Movie Map relation; cache disabled; no implicit warmup or transport retry'
                source['native_retry_policy']='author SemMap length branch: at most once at 8192'
                artifacts['source.json']=source
            emit('result_ready')
            with native.query_phase(timing,'quality_evaluation'):
                evaluation_started=time.monotonic_ns();quality=query.evaluate(frame);evaluated=time.monotonic_ns()
                artifacts['quality.json']=quality
            report.update(status='completed',query_id=spec['query_id'],backend='kalypso-native',
                baseline_name='Kalypso author SemMap with original SemBench instruction-field binding',
                semantic_operator='SemMap, original Movie '+source['query']+' parser, relation and evaluator',
                execution_backend='author Kalypso controller/KV/ratio estimator and native vLLM persistent service',
                logical_occurrences=spec['expected_calls'],physical_call_cap=spec['max_native_calls'],
                native_retry_policy='one length-triggered retry at 8192; no transport retry',physical_native_calls=len(native_report['native_API_units']),
                native_API_units=native_report['native_API_units'],result=result,quality=quality,
                evaluation_seconds=(evaluated-evaluation_started)/1e9,native_server_latency_s=native_report['latency_s'],
                generation='author dynamic first max_tokens, native length-to8192 retry, seed42/frequency.5/repetition1.3',
                prompt_scope='exact original Movie instruction/review substituted into author Map prompt template, different from LOTUS messages')
        return report

