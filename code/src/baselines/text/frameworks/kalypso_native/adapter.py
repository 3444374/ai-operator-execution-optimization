"""Source/sink and observation adapter; the author service owns all Filter work."""
import csv
from contextlib import contextmanager
import hashlib
import json
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
    from src.infrastructure.private_artifacts import write_private_json
    return write_private_json


def read_trace(path, query_id):
    rows=[]
    with Path(path).open() as stream:
        for line in stream:
            event=json.loads(line)
            if event.get('query_id')==query_id:
                rows.append(event)
    return rows


def response_results(response, model, expected):
    """Validate the complete native HTTP relation before task materialization."""
    if not isinstance(response,dict) or response.get('model_name')!=model:
        raise ValueError('Semantic response model differs from the loaded 7B model')
    results=response.get('results')
    if not isinstance(results,list) or any(not isinstance(row,dict) for row in results):
        raise ValueError('Missing complete native results')
    indices=[row.get('idx') for row in results]
    if (any(type(index) is not int or not 0<=index<expected for index in indices)
            or len(set(indices))!=len(indices)
            or type(response.get('num_output_rows')) is not int
            or response['num_output_rows']!=len(indices)):
        raise ValueError('Native returned rows have invalid occurrence indices or cardinality')
    return results


def response_decisions(eligible, response, model):
    """Restore Filter occurrences; trace reconciliation remains a post-sink audit.

    The native handler awaits the entire query before returning surviving contexts.
    """
    passed={row['idx'] for row in response_results(response,model,len(eligible))}
    return [dict(row_id=row['row_id'],source_position=row['source_position'],decision=index in passed)
            for index,row in enumerate(eligible)]


def response_map_texts(response, model, expected):
    """Read original Map texts in occurrence order, before its required parser."""
    results=response_results(response,model,expected)
    if len(results)!=expected:
        raise ValueError('Native Map response lost occurrence identities')
    texts={}
    for row in results:
        outputs=row.get('outputs')
        if (not isinstance(outputs,list) or len(outputs)!=1 or not isinstance(outputs[0],dict)
                or not isinstance(outputs[0].get('value'),str)):
            raise ValueError('Native Map requires one final text output per occurrence')
        texts[row['idx']]=outputs[0]['value']
    return [texts[index] for index in range(expected)]


@contextmanager
def query_phase(timing, name):
    phase=dict(started_ns=time.monotonic_ns(),status='running')
    timing['phases'][name]=phase
    try:
        yield
        phase['status']='completed'
    except BaseException:
        phase['status']='failed'
        raise
    finally:
        phase['finished_ns']=time.monotonic_ns()
        phase['elapsed_s']=(phase['finished_ns']-phase['started_ns'])/1e9


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
        self.model_ready_observed_ns=None
        self.http_body=None
        self.http_clock=None
        # Existing private Map/MMQA drivers override query() and still require
        # post() to publish its evidence. Only migrated queries opt into deferral.
        self.defer_http_evidence=False

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
        launch={}
        if self.cfg.get('native_launch') == 'author_defaults':
            from .native_server import server_argv, AUTHOR_SOURCE_COMMIT
            launch=json.loads((Path(self.cfg['loaded_model_proof']).parent/'native-launch.json').read_text())
            if (self.cfg.get('source_commit')!=AUTHOR_SOURCE_COMMIT
                    or launch.get('policy')!='author_defaults' or launch.get('author_commit')!=AUTHOR_SOURCE_COMMIT
                    or launch.get('argv')!=server_argv(self.cfg) or launch.get('capacity_overrides') is not False):
                raise ValueError('Native launch provenance differs from this binding')
            config=proof.get('native_engine_configuration',{})
            values=[config.get('model',{}).get('max_model_len'),
                    *(config.get('scheduler',{}).get(name) for name in ('max_num_seqs','max_num_batched_tokens'))]
            memory=config.get('cache',{}).get('gpu_memory_utilization')
            if (any(type(value) is not int or value<=0 for value in values)
                    or type(memory) not in (int,float) or not 0<memory<=1):
                raise ValueError('Actual native worker capacity observation is incomplete')
        self.model_ready_observed_ns=time.monotonic_ns()
        return dict(status='attached_ready',loaded_model=proof,native_launch=launch,
                    model_ready_observed_ns=self.model_ready_observed_ns,
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
        self.http_body=None
        self.http_clock=dict(schema='semloom.kalypso_http_clock.v2',started_ns=started,
            returned_ns=None,records_per_POST='entire native semantic query',
            scope='native HTTP response read/close; excludes JSON parsing and audit persistence',
            automatically_replayed=False)
        try:
            with opener.open(req,timeout=self.cfg['query_s']) as response:
                self.http_body=response.read()
                status=response.status
            ended=time.monotonic_ns()
            self.http_clock.update(returned_ns=ended,status=status,elapsed_s=(ended-started)/1e9)
            if status!=200:raise ValueError('Native semantic query did not succeed')
            return json.loads(self.http_body)
        except BaseException as error:
            if self.http_clock['returned_ns'] is None:
                ended=time.monotonic_ns()
                self.http_clock.update(returned_ns=ended,elapsed_s=(ended-started)/1e9)
            self.http_clock.update(status='failed_or_unknown',http_status=status,
                                   error_type=type(error).__name__)
            raise
        finally:
            if not self.defer_http_evidence:
                # Keep the existing post() contract for unmigrated owner packets.
                # Their result_ready is not a unified in-memory sink boundary.
                if self.http_body is not None:
                    (directory/'response.body').write_bytes(self.http_body)
                self.save(directory/'HTTP-clock.json',self.http_clock)

    def persist_query_evidence(self,directory,artifacts,timing):
        """Keep independent evidence writes after the sink, including on failure."""
        errors=[]
        if self.http_clock is not None:
            artifacts['HTTP-clock.json']=self.http_clock
        with query_phase(timing,'audit_persistence'):
            for name,value in artifacts.items():
                try:
                    self.save(directory/name,value)
                except BaseException as error:
                    errors.append(error)
            if self.http_body is not None:
                try:
                    (directory/'response.body').write_bytes(self.http_body)
                except BaseException as error:
                    errors.append(error)
        if errors:
            timing['phases']['audit_persistence']['status']='failed'
        # This receipt's own publication is outside the measured artifact writes.
        timing['evidence_write_errors']=[type(error).__name__ for error in errors]
        try:
            self.save(directory/'memory-sink-timing.json',timing)
        except BaseException as error:
            errors.append(error)
            timing['evidence_write_errors'].append(type(error).__name__)
        return errors

    @contextmanager
    def query_evidence(self,spec,directory,*,sink_scope,source_scope):
        """Observe an existing native query; never execute or schedule its operators."""
        self.http_body=self.http_clock=None
        self.defer_http_evidence=True
        timing=dict(schema='semloom.kalypso_memory_sink.v1',memory_sink_ready_ns=None,
            clock='time.monotonic_ns in client process',phases={},trace_audit_status='not_started',
            sink_scope=sink_scope,source_scope=source_scope,
            result_ready_scope='legacy trace-audited result; not the unified memory sink',
            historical_timings_reconstructed=False)
        report=dict(status='failed',backend='Kalypso-native',query_id=spec['query_id'],
                    memory_sink_timing=timing)
        artifacts={}
        failure=None
        try:
            yield report,artifacts,timing
        except BaseException as error:
            failure=error
            report.update(status='failed',failure_type=type(error).__name__)
            if timing['trace_audit_status']=='running':
                timing['trace_audit_status']='failed'
            error.query_report=report
            raise
        finally:
            self.defer_http_evidence=False
            recording_errors=self.persist_query_evidence(directory,artifacts,timing)
            if recording_errors:
                report['status']='failed'
                if failure is None:
                    failure=recording_errors.pop(0)
                    report['failure_type']=type(failure).__name__
                    failure.query_report=report
                    for recording in recording_errors:
                        failure.add_note('query evidence also failed: '+type(recording).__name__)
                    raise failure
                for recording in recording_errors:
                    failure.add_note('query evidence also failed: '+type(recording).__name__)

    def query(self,spec,emit):
        from src.baselines.text.movie_records import MovieRecordSource
        from src.baselines.text.movie_tasks import MovieTask
        from src.baselines.text.sembench_movie import evaluate_original,classification_audit
        # The reused outer query clock has already started, before source validation/scan.
        if spec!=self.unit['queries'][self.next_query]:
            raise ValueError('Query order differs from the finite unit declaration')
        self.next_query+=1
        directory=self.output/spec['query_id'];directory.mkdir(mode=0o700)
        with self.query_evidence(spec,directory,
                sink_scope='complete native HTTP relation plus original Movie projection/aggregation in memory',
                source_scope='fresh original Movie CSV scan and author-required native-input.csv materialization') as (report,artifacts,timing):
            eligible=None
            try:
                with query_phase(timing,'source_preflight'):
                    if sha(spec['manifest'])!=spec['manifest_sha256']:
                        raise ValueError('Fixed input manifest changed')
                    source=MovieRecordSource.from_manifest(spec['manifest'])
                    read_rows=source.bind_query_rows()
                    task=MovieTask.from_config(spec['task'])
                timing.update(query_clock_version='sembench.source_to_sink.v3',
                              source_started_ns=time.monotonic_ns(),
                              model_ready_before_source=self.model_ready_observed_ns is not None,
                              model_ready_before_source_observed_ns=self.model_ready_observed_ns)
                with query_phase(timing,'source_and_required_input'):
                    rows=read_rows()
                    eligible=[r for r in rows if task.accepts(r)]
                    if len(eligible)!=spec['expected_calls']:
                        raise ValueError('Unexpected ordinary predicate cardinality')
                    csv_path=directory/'native-input.csv'
                    with csv_path.open('x',newline='') as stream:
                        writer=csv.DictWriter(stream,fieldnames=['data']);writer.writeheader()
                        writer.writerows({'data':r['reviewText']} for r in eligible)
                    payload=dict(data_path=str(csv_path),ops=[dict(op='sem_filter',
                        args=dict(prompt=self.cfg['native_instruction']))],blocking=False)
                # Audit copies are retained in memory until the relation has reached its sink.
                artifacts['request.json']=payload
                with query_phase(timing,'native_HTTP_and_JSON_parse'):
                    response=self.transport(payload,spec['query_id'],directory)
                with query_phase(timing,'original_relation'):
                    frame=task.result_frame(rows,response_decisions(eligible,response,self.cfg['model']))
                timing['memory_sink_ready_ns']=time.monotonic_ns()
                emit('memory_sink_ready')
                with query_phase(timing,'result_encoding'):
                    report['result']=task.encode_result(frame)
                with query_phase(timing,'trace_audit'):
                    timing['trace_audit_status']='running'
                    events=read_trace(self.cfg['native_trace'],spec['query_id'])
                    decisions=reconcile(eligible,response,events,self.cfg['model'])
                    artifacts['decisions.json']=decisions
                    apis=[e for e in events if e.get('event')=='native_API_end']
                    ids={e['request_id'] for e in apis}
                    if ids&self.request_ids or any(e.get('model_uid')!=self.model_uid for e in apis):
                        raise ValueError('Request IDs reused or loaded model UID changed across complete queries')
                    self.request_ids.update(ids)
                    timing['trace_audit_status']='passed'
                emit('result_ready')
                with query_phase(timing,'quality_evaluation'):
                    labeled=source.labeled_frame()
                    quality=evaluate_original(self.cfg['sembench'],task.query,frame,labeled)
                    with (Path(spec['manifest']).parent/'references.csv').open() as reference_stream:
                        labels={r['row_id']:label['scoreSentiment'] for r,label in zip(rows,
                            csv.DictReader(reference_stream)) if task.accepts(r)}
                    quality['row_audit']=classification_audit(labels,[(r['row_id'],
                        'POSITIVE' if r['decision'] else 'NEGATIVE') for r in decisions])
                    quality['native_nonliteral_verdicts']=sum(r['native_verdict'] not in ('true','false') for r in decisions)
                    artifacts['quality.json']=quality
                    report.update(quality=quality,calls=dict(expected=len(eligible),native_completed=len(apis)),
                        source_prefix_rows=len(rows),ordinary_predicate_rows=len(eligible),
                        scan_pass_count=1,
                        scan_pass_count_scope='original CSV scan inside versioned query; preflight and post-sink source audits are separate',
                        native_API_units=apis,native_server_latency_sec=response.get('latency_sec'),
                        scheduling_owner='author Kalypso/vLLM; adapter has no row dispatch loop',
                        MFU=dict(status='unavailable',reason='Fixed vLLM 0.13.0rc4 exports no verified native FLOPs numerator'),
                        prompt_parser_identity='native sem_filter; absence of false passes; not matched LOTUS requests')
                report['status']='completed'
                return report
            finally:
                if eligible is not None:
                    artifacts['occurrences.json']=[{k:r[k] for k in ['row_id','source_position','reviewId']} for r in eligible]

    def close(self):
        return dict(status='closed',cleanup_errors=[],scope='attached HTTP client only; service shutdown belongs to GPU owner')
