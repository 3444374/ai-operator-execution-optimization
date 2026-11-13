"""Original SemBench Map queries using the existing native owner session."""
from dataclasses import replace
from pathlib import Path

from src.baselines.text.frameworks.kalypso_native import adapter as native
from .sembench_inputs import prepare
from .query_runner import NativeOperation


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


class MapOperation(NativeOperation):
    kind = 'map'
    source_metadata = dict(
        policy='original SemBench Map relation; cache disabled; no implicit warmup or transport retry',
        native_retry_policy='author SemMap length branch: at most once at 8192',
    )

    def execute(self, operation, unused_original):
        from lotus.sem_ops.postprocessors import map_postprocess
        from src.semantic_methods.lotus.record_method import LotusRecordMethod
        with native.query_phase(self.timing, 'source_and_required_input'):
            self.values, path = prepare(self.cfg, self.spec, self.directory, operation)
            payload = dict(data_path=str(path), ops=[dict(op='sem_map',
                args=dict(prompt=operation.stage.method.instruction))], blocking=False)
        self.artifacts.update({'request.json': payload, 'occurrences.json': self.values})
        with native.query_phase(self.timing, 'native_HTTP_and_JSON_parse'):
            self.response = self.driver.transport(payload, self.spec['query_id'], self.directory)
        with native.query_phase(self.timing, 'required_Map_parser'):
            self.texts = native.response_map_texts(self.response, self.cfg['model'], self.spec['expected_calls'])
            method = LotusRecordMethod(operation.stage)
            parsed = map_postprocess(self.texts, method.lm, False).outputs
            completed = [replace(r, values={**r.values, operation.stage.method.output_column: text})
                for r, text in zip(operation.records, parsed, strict=True)]
            return dict(frame=operation.result_frame(completed), calls=[])

    def audit(self):
        qid = self.spec['query_id']
        events = native.read_trace(self.cfg['native_trace'], qid)
        events += native.read_trace(Path(self.cfg['native_trace']).with_name('map-trace.jsonl'), qid)
        _, self.ends = reconcile(self.response, events, qid, self.spec['expected_calls'])
        ids = {e['request_id'] for e in self.ends}
        if ids & self.driver.request_ids or any(e['model_uid'] != self.driver.model_uid for e in self.ends):
            raise ValueError('Owner or request IDs changed')
        self.driver.request_ids.update(ids)
        self.artifacts['native-map.json'] = dict(
            rows=self.values, raw_outputs=self.texts, latency_s=self.response['latency_sec'], native_API_units=self.ends,
            first_max_tokens=[min([e for e in self.ends if e['idx'] == i], key=lambda e: e['sequence'])['max_tokens']
                for i in range(self.spec['expected_calls'])],
        )

    def report_metadata(self, source):
        return dict(
            baseline_name='Kalypso author SemMap with original SemBench instruction-field binding',
            semantic_operator='SemMap, original ' + source['scenario'] + ' ' + source['query'] + ' parser, relation and evaluator',
            execution_backend='author Kalypso controller/KV/ratio estimator and native vLLM persistent service',
            logical_occurrences=self.spec['expected_calls'], physical_call_cap=self.spec['max_native_calls'],
            native_retry_policy='one length-triggered retry at 8192; no transport retry', physical_native_calls=len(self.ends),
            native_API_units=self.ends, native_server_latency_s=self.response['latency_sec'],
            generation='author dynamic first max_tokens, native length-to8192 retry, seed42/frequency.5/repetition1.3',
            prompt_scope='exact original instruction and referenced field text bound to author Map prompt template, different from LOTUS messages',
        )
