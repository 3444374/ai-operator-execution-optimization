"""Source/sink and observation adapter; the author service owns all Filter work."""
import csv
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from urllib import request


def read_gpu_uuid(index):
    """Read the physical device identity; no model or CUDA runtime is touched."""
    import pynvml
    pynvml.nvmlInit()
    try:
        value=pynvml.nvmlDeviceGetUUID(pynvml.nvmlDeviceGetHandleByIndex(index))
        if isinstance(value,bytes):value=value.decode('ascii')
        return value
    finally:
        pynvml.nvmlShutdown()


def read_gpu_uuid_hash(index):
    return hashlib.sha256(read_gpu_uuid(index).encode()).hexdigest()


def uuid_bound_sample_source(cfg,sample_fn=None,uuid_reader=None):
    """The existing owner passes this as RecordsTelemetry.sample_fn."""
    index=cfg['physical_gpu_index'];expected=cfg['gpu_uuid_sha256']
    reader=uuid_reader or read_gpu_uuid_hash
    def verify():
        actual=reader(index)
        if actual!=expected:raise ValueError('Physical GPU UUID does not match the fixed baseline binding')
        return actual
    verify()  # Fail before service launch if the initial physical binding differs.
    if sample_fn is None:
        from src.observability.records_telemetry import MetricSources
        physical_uuid=read_gpu_uuid(index)
        if hashlib.sha256(physical_uuid.encode()).hexdigest()!=expected:
            raise ValueError('Physical GPU UUID changed before metric reader binding')
        sample_fn=MetricSources((physical_uuid,))
    def sample():
        begun=time.monotonic_ns();verify()
        result=sample_fn()
        actual=verify()  # Never publish values across an identity change.
        sources=dict(result.get('sources',{}))
        sources['physical_gpu_binding']=dict(status='verified',index=index,uuid_sha256=actual,
            expected_uuid_sha256=expected,started_ns=begun,finished_ns=time.monotonic_ns())
        return dict(result,sources=sources)
    return sample


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream,'sha256').hexdigest()


def load_dependencies(cfg):
    bundle=Path(cfg['baseline_bundle'])
    for rel,expected in cfg['dependency_sha256'].items():
        if sha(bundle/rel)!=expected:
            raise ValueError('Existing comparison dependency changed: '+rel)
    sys.path[:0]=[str(Path(cfg['baseline_source'])/'code'),cfg['baseline_packet']]
    from src.baselines.common.private_artifacts import write_private_json
    return write_private_json


def read_trace(path, query_id):
    rows=[]
    with Path(path).open() as stream:
        for line in stream:
            event=json.loads(line)
            if event.get('query_id')==query_id:
                rows.append(event)
    return rows


def reconcile(eligible, response, events, model):
    """Validate every native decision, then restore occurrence order at the sink."""
    if response.get('model_name')!=model:
        raise ValueError('Semantic response model differs from the loaded 7B model')
    native=[e for e in events if e.get('event')=='filter_result']
    if any(type(e.get('idx')) is not int for e in native):
        raise ValueError('Invalid native occurrence index')
    by_idx={e['idx']:e for e in native}
    if (len(by_idx)!=len(native) or set(by_idx)!=set(range(len(eligible)))
            or any(type(e.get('decision')) is not bool for e in native)
            or any(e['decision']!=('false' not in str(e['verdict']).strip().lower()) for e in native)):
        raise ValueError('Incomplete, duplicate or unknown native Filter decisions')
    passed={idx for idx,e in by_idx.items() if e['decision']}
    results=response.get('results')
    if not isinstance(results,list):
        raise ValueError('Missing complete native results')
    indices=[r.get('idx') for r in results]
    if (any(type(i) is not int for i in indices) or len(set(indices))!=len(indices) or set(indices)!=passed
            or response.get('num_output_rows')!=len(indices)):
        raise ValueError('Native returned rows disagree with complete Filter decisions')
    ends=[e for e in events if e.get('event')=='native_API_end']
    if (len(ends)!=len(eligible) or {e.get('idx') for e in ends}!=set(range(len(eligible)))
            or any(type(e.get('idx')) is not int for e in ends)
            or any(not isinstance(e.get('request_id'),str) or not e['request_id'] for e in ends)
            or len({e.get('request_id') for e in ends})!=len(ends)
            or any(e.get('status')!='succeeded' for e in ends)
            or any(e.get('pin') or e.get('max_tokens')!=8 for e in ends)):
        raise ValueError('Native API completion/call budget differs from one Filter per row')
    return [dict(row_id=row['row_id'],source_position=row['source_position'],
                 decision=by_idx[i]['decision'],native_verdict=by_idx[i]['verdict'])
            for i,row in enumerate(eligible)]


class NativeQueryDriver:
    """Attach to a service already owned by the GPU owner's lifecycle wrapper."""
    def __init__(self,cfg,unit,output,transport=None):
        self.cfg,self.unit,self.output=cfg,unit,Path(output)
        self.output.mkdir(mode=0o700)
        self.transport=transport or self.post
        self.save=load_dependencies(cfg)
        self.next_query=0
        self.request_ids=set()
        self.model_uid=None

    def start(self):
        proof=json.loads(Path(self.cfg['loaded_model_proof']).read_text())
        if (proof['model']!=self.cfg['model'] or proof['hidden_size']!=3584
                or proof['first_parameter_shape'][-1]!=3584
                or proof['first_parameter_device'].split(':')[0]!='cuda'
                or proof['dtype']!='torch.bfloat16'):
            raise ValueError('Actual native loaded-model proof is not the existing BF16 7B model')
        if 'observer_revision' in self.cfg:
            status=json.loads((Path(self.cfg['loaded_model_proof']).parent/'worker-observer-status.json').read_text())
            expected=Path(self.cfg['install_root'])/'source/vllm/v1/worker/gpu_model_runner.py'
            if (proof.get('observer_revision')!=self.cfg['observer_revision']
                    or proof.get('physical_gpu_uuid_sha256')!=self.cfg['gpu_uuid_sha256']
                    or status.get('status')!='model_observed'
                    or status.get('model_uid')!=proof['model_uid']
                    or (not self.cfg.get('CPU_fixture_only') and Path(proof['runner_module_path'])!=expected)):
                raise ValueError('Actual worker model/physical UUID observation is not qualified')
        self.model_uid=proof['model_uid']
        return dict(status='attached_ready',loaded_model=proof,
                    model_startup_scope='external GPU owner lifecycle; not measured by client attach',
                    service_startup_scope='GPU owner external lifecycle, not this attach operation')

    def post(self,payload,query_id,directory):
        # Exactly one native semantic POST. No proxy, redirect, retry or model override.
        class NoRedirect(request.HTTPRedirectHandler):
            def redirect_request(self,*args,**kwargs):return None
        opener=request.build_opener(request.ProxyHandler({}),NoRedirect())
        req=request.Request(self.cfg['url']+'/v1/semantic/query',
            data=json.dumps(payload).encode(),headers={'Content-Type':'application/json',
                'X-Kalypso-Observation':query_id},method='POST')
        started=time.monotonic_ns()
        status=None
        try:
            with opener.open(req,timeout=self.cfg['query_s']) as response:
                body=response.read(32*1024**2+1)
                status=response.status
            if len(body)>32*1024**2:raise ValueError('Native response exceeds the declared 32 MiB limit')
            (directory/'response.body').write_bytes(body)
        except BaseException as error:
            self.save(directory/'HTTP-clock.json',dict(started_ns=started,returned_ns=time.monotonic_ns(),
                status='failed_or_unknown',error_type=type(error).__name__,automatically_replayed=False))
            raise
        ended=time.monotonic_ns()
        self.save(directory/'HTTP-clock.json',dict(started_ns=started,returned_ns=ended,
            status=status,elapsed_s=(ended-started)/1e9,records_per_POST='entire native Filter query'))
        if status!=200:raise ValueError('Native semantic query did not succeed')
        return json.loads(body)

    def query(self,spec,emit):
        from src.baselines.text.movie_records import MovieRecordSource
        from src.baselines.text.movie_tasks import MovieTask
        from src.baselines.text.sembench_movie import evaluate_original,classification_audit
        # The reused outer query clock has already started, before source validation/scan.
        if spec!=self.unit['queries'][self.next_query]:
            raise ValueError('Query order differs from the finite unit declaration')
        self.next_query+=1
        directory=self.output/spec['query_id'];directory.mkdir(mode=0o700)
        if sha(spec['manifest'])!=spec['manifest_sha256']:
            raise ValueError('Fixed input manifest changed')
        source=MovieRecordSource.from_manifest(spec['manifest'])
        rows=source.rows()
        task=MovieTask.from_config(spec['task'])
        eligible=[r for r in rows if task.accepts(r)]
        if len(eligible)!=spec['expected_calls']:
            raise ValueError('Unexpected ordinary predicate cardinality')
        csv_path=directory/'native-input.csv'
        with csv_path.open('x',newline='') as stream:
            writer=csv.DictWriter(stream,fieldnames=['data']);writer.writeheader()
            writer.writerows({'data':r['reviewText']} for r in eligible)
        payload=dict(data_path=str(csv_path),ops=[dict(op='sem_filter',
            args=dict(prompt=self.cfg['native_instruction']))],blocking=False)
        self.save(directory/'request.json',payload)
        self.save(directory/'occurrences.json',[{k:r[k] for k in ['row_id','source_position','reviewId']} for r in eligible])
        response=self.transport(payload,spec['query_id'],directory)
        events=read_trace(self.cfg['native_trace'],spec['query_id'])
        decisions=reconcile(eligible,response,events,self.cfg['model'])
        apis=[e for e in events if e.get('event')=='native_API_end']
        ids={e['request_id'] for e in apis}
        if ids&self.request_ids or {e.get('model_uid') for e in apis}!={self.model_uid}:
            raise ValueError('Request IDs reused or loaded model UID changed across complete queries')
        self.request_ids.update(ids)
        frame=task.result_frame(rows,decisions)
        result=task.encode_result(frame)
        emit('result_ready')
        # Original gold evaluator and separate per-row quality audit, after result_ready.
        labeled=source.labeled_frame()
        quality=evaluate_original(self.cfg['sembench'],task.query,frame,labeled)
        with (Path(spec['manifest']).parent/'references.csv').open() as reference_stream:
            labels={r['row_id']:label['scoreSentiment'] for r,label in zip(rows,
                csv.DictReader(reference_stream)) if task.accepts(r)}
        quality['row_audit']=classification_audit(labels,[(r['row_id'],
            'POSITIVE' if r['decision'] else 'NEGATIVE') for r in decisions])
        quality['native_nonliteral_verdicts']=sum(r['native_verdict'] not in ('true','false') for r in decisions)
        report=dict(status='completed',backend='Kalypso-native',query_id=spec['query_id'],
            result=result,quality=quality,calls=dict(expected=len(eligible),native_completed=len(apis)),
            source_prefix_rows=len(rows),ordinary_predicate_rows=len(eligible),
            scan_pass_count='not instrumented; existing input validator may reread inside the query clock',
            native_API_units=apis,native_server_latency_sec=response.get('latency_sec'),
            scheduling_owner='author Kalypso/vLLM; adapter has no row dispatch loop',
            MFU=dict(status='unavailable',reason='Fixed vLLM 0.13.0rc4 exports no verified native FLOPs numerator'),
            prompt_parser_identity='native sem_filter; absence of false passes; not matched LOTUS requests')
        self.save(directory/'decisions.json',decisions)
        self.save(directory/'quality.json',quality)
        return report

    def close(self):
        return dict(status='closed',cleanup_errors=[],scope='attached HTTP client only; service shutdown belongs to GPU owner')


def run_attached(cfg,unit,output,observer=None):
    """Compatibility entry for the comparison lifecycle harness."""
    from src.experiments.kalypso_query_lifecycle import run_attached as run
    return run(cfg,unit,output,observer=observer)


def main():
    """Retain the existing native-adapter CLI as a comparison entry point."""
    from src.experiments.kalypso_query_lifecycle import main as run
    return run()


if __name__=='__main__':main()
