"""Bounded public LOTUS/DuckDB/Sema entries on both original IMLane modes."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid

CODE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE))
from src.baselines.common.private_artifacts import write_private_json
from src.baselines.common.redact import redact_text
from src.baselines.text.frameworks.imlane_calls import AUTHOR_COMMIT
from integrations.sema import verify_imlane_calls as fixture_module


METHOD_CLIENT = r'''
import json,os,pathlib,sys,sysconfig,time
code,config,native_source,tokenizer,result_file=sys.argv[1:]
value=json.loads(pathlib.Path(config).read_text());value.pop('fixture_endpoint')
bridge=json.loads(pathlib.Path(value['bridge_config_path']).read_text())
driver_site=sysconfig.get_path('purelib')
author_sites={p for p in bridge.get('env',{}).get('PYTHONPATH','').split(os.pathsep)
 if p and pathlib.Path(p).name in ('site-packages','dist-packages') and p!=driver_site}
sys.path[:]=[p for p in sys.path if p not in author_sites]
os.environ['PYTHONPATH']=os.pathsep.join(p for p in os.environ.get('PYTHONPATH','').split(os.pathsep)
 if p and p not in author_sites)
author_root=pathlib.Path(value['author_build_identity']).parents[2]
for name in ('LD_LIBRARY_PATH','PATH'):
 os.environ[name]=os.pathsep.join(p for p in os.environ.get(name,'').split(os.pathsep)
  if p and not pathlib.Path(p).is_relative_to(author_root))
os.environ['PATH']=str(pathlib.Path(sys.executable).parent)+os.pathsep+os.environ['PATH']
os.environ['LITELLM_LOCAL_MODEL_COST_MAP']='True'
sys.path.insert(0,code)
from src.baselines.common.private_artifacts import write_private_json
from src.baselines.text.frameworks.imlane_calls import IMLaneCallConfig
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.shared_request_budget import CellBudgetLedger
from src.experiments.postgresql.supplier_adapter_query import run_supplier_query
from src.experiments.postgresql.persistent_native_adapter_query import PersistentAdapterGroup
from src.experiments.postgresql.sema_native_query import SemaServiceConfig
from src.experiments.postgresql import supplier_adapter_query as supplier_module
from src.experiments.postgresql import persistent_native_adapter_query as persistent_module
from tests.experiments import test_native_adapter_query as model_fixture
from tests.experiments.test_native_adapter_query import fixture_server
from integrations.sema.verify_imlane_methods import listen_server_factory
model_fixture.ThreadingHTTPServer=listen_server_factory(model_fixture.ThreadingHTTPServer,MODEL_FIXTURE_PORT)
if OBSERVATION_PORT:
 original_gateway=supplier_module.ObservationGateway
 def gateway(**kwargs):return original_gateway(**dict(kwargs,bind_port=OBSERVATION_PORT))
 supplier_module.ObservationGateway=persistent_module.ObservationGateway=gateway
def optional_path(value):return None if value in (None,'None','') else pathlib.Path(value)
tokenizer_path=optional_path(tokenizer)
sema_binary=optional_path(SEMA_BINARY)
duckdb_library=optional_path(DUCKDB_EXTENSION)
sema_config=SemaServiceConfig(listen_port=SERVICE_PORT)
cfg=IMLaneCallConfig(**value);mode=cfg.runtime
options=NativeGraphOptions(concurrency=8,num_threads=8)
root=pathlib.Path(result_file).parent/'method-queries';root.mkdir()
plan=SemanticMapPlan('Return ok.','fixture',16)
ledger=CellBudgetLedger.create(root/'budget.sqlite',AttemptBudget('imlane-public-methods',64),deadline_utc=time.time()+180)
values=[dict(row_id='a',text='same'),dict(row_id='b',text='same'),dict(row_id='c',text='中文'),dict(row_id='d',text='fourth')]
suffix='-ray' if mode=='ray' else ''
results=[];posts_total=0;models=0
for method in METHODS:
 arm=('sema-native-imlane'+suffix+'-request-service' if method=='sema' else method+'-method-native-imlane'+suffix)
 with fixture_server(content='"ok"' if method=='sema' else 'ok') as (endpoint,posts):
  result=run_supplier_query(arm,load_source=lambda:iter(values),plan=plan,
   model=FixedModelConfig(endpoint,'fixture',20000),ledger=ledger,unit_id=method+'-'+mode,
   root=root/method,options=options,query_timeout_s=20,
   imlane_call_configs={mode:cfg},tokenizer_path=tokenizer_path,
   duckdb_library=duckdb_library,sema_binary=sema_binary,sema_native_threads=2,
   sema_service_config=sema_config if method=='sema' else None,
   reference_outputs={r['row_id']:'ok' for r in values},allowed_outputs=('ok',))
  assert result['status']=='passed' and result['actual_posts']==len(posts)==4,result
  assert result['execution']['recorded_rows']==4,result['execution']
  assert result['identity']['imlane_runtime']==mode,result['identity']
  assert result['runtime']['owner']=='IMLane author Coordinator',result['runtime']
  if method=='sema':
   service=result['request_service']
   assert service['uncertain_requests']==0 and not service['native_bridge_thread_alive'],service
   assert service['held_requests']==0 and service['native_driver_stopped'],service
   close=service['native_close_reports']
   assert all(r['clean'] is True for r in close),close
  elif method=='duckdb':
   close=result['identity']['diagnostics']['executor']['last_close_report']
   assert close['clean'] is True and close['received']==4,close
  results.append(dict(arm=arm,passed=True,posts=len(posts),rows=4,identity=result['identity'],runtime=result['runtime']))
  posts_total+=len(posts)
  write_private_json(root/('progress-'+str(len(results))+'.json'),dict(fixture_posts=posts_total,results=results,models=0))
# Reuse the caller Coordinator and each original method across two public group queries.
for method in (m for m in ('lotus','sema') if m in METHODS):
 arm='sema-native-imlane'+suffix+'-request-service' if method=='sema' else 'lotus-method-native-imlane'+suffix
 with fixture_server(content='"ok"' if method=='sema' else 'ok') as (endpoint,posts):
  with PersistentAdapterGroup((arm,),plan=plan,model=FixedModelConfig(endpoint,'fixture',20000),
   ledger=ledger,root=root/('persistent-'+method),options=options,query_timeout_s=20,
   imlane_call_configs={mode:cfg},tokenizer_path=tokenizer_path,
   sema_binary=sema_binary,sema_native_threads=2,
   sema_service_config=sema_config if method=='sema' else None) as group:
   for i in range(2):
    result=group.run(arm,unit_id='persistent-'+method+'-'+mode+'-'+str(i),
     root=root/('persistent-'+method+'-query-'+str(i)),load_source=lambda:iter(values[:2]),
     phase='qualification',allowed_outputs=('ok',),reference_outputs={r['row_id']:'ok' for r in values[:2]})
    assert result['status']=='passed' and result['actual_posts']==2,result
    assert result['execution']['recorded_rows']==2,result
    if method=='sema':
     service=result['request_service']
     assert service['uncertain_requests']==0 and service['held_requests']==0,service
     assert service['native_driver_stopped'] and not service['native_bridge_thread_alive'],service
     assert service['imlane_runtime']==mode and service['imlane_runtime_verified'],service
     assert all(r['clean'] is True for r in service['native_close_reports']),service
  assert len(posts)==4,posts
  posts_total+=4
 results.append(dict(arm=arm,case='persistent-two-queries',passed=True,posts=4,rows=4))
assert posts_total==4*len(METHODS)+4*sum(m in METHODS for m in ('lotus','sema')),posts_total
write_private_json(pathlib.Path(result_file),dict(passed=True,fixture_posts=posts_total,
 methods=results,models=0,gpu_resources=0,retries=0,imlane_runtime=mode))
'''


_RUNTIME_PROBE = r'''
import hashlib,importlib,importlib.metadata,json,pathlib,sys
names=['psutil','numpy','pyarrow','ray','aiohttp']
if sys.argv[1]=='driver' and 'lotus' in sys.argv[2].split(','):names.append('lotus')
modules={}
for name in names:
 module=importlib.import_module(name)
 path=pathlib.Path(module.__file__)
 modules[name]=dict(version=(importlib.metadata.version('lotus-ai') if name=='lotus' else module.__version__),
  path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest())
print(json.dumps(dict(python_version=list(sys.version_info[:2]),prefix=sys.prefix,
 python_executable=sys.executable,modules=modules,import_order=names,
 ray_initialized=sys.modules['ray'].is_initialized())))
'''


def check_runtime_versions(identity, expected):
    if identity.get('python_version') != [3, 12]:
        raise ValueError('IMLane method verification requires the fixed Python 3.12 runtime')
    for name, version in expected.items():
        if identity.get('modules', {}).get(name, {}).get('version') != version:
            raise ValueError('IMLane method runtime differs from its fixed dependency: ' + name)
    if identity.get('ray_initialized') is not False:
        raise ValueError('method runtime probe must not initialize or reuse Ray')


def listen_server_factory(constructor, port):
    if not port:
        return constructor
    def create(_address, handler, *args, **kwargs):
        return constructor(('127.0.0.1', port), handler, *args, **kwargs)
    return create


def check_ray_temp_path(author_root, ray_temp_root=None):
    root = Path(author_root).absolute()
    if len(root.parents) < 2:
        raise ValueError('author root cannot provide the owned Ray temporary directory')
    parent = Path(ray_temp_root).absolute() if ray_temp_root is not None else root.parents[1]
    short = parent / ('ir-' + 'x' * 10)
    if len(os.fsencode(short)) > 50:
        raise ValueError('owned Ray temporary path must fit 50 bytes before any runtime starts')
    return dict(parent=str(short.parent), max_path_bytes=50, selected_path_bytes=len(os.fsencode(short)))


def check_bridge_identity(binary):
    linked = json.loads(binary.with_suffix('.build.json').read_text())
    if (linked.get('upstream_commit') != AUTHOR_COMMIT or linked.get('returncode') != 0
            or linked.get('binary_sha256') != fixture_module.digest(binary)
            or linked.get('source_sha256') != fixture_module.digest(CODE / 'integrations/sema/imlane_http.cpp')):
        raise ValueError('bridge binary/source identity differs from the six-capacity ABI')
    return {name: linked[name] for name in ('upstream_commit','binary_sha256','source_sha256')}


def method_runtime_preflight(args):
    if sys.platform != 'linux':
        raise RuntimeError('real IMLane method verification requires Linux; no author runtime started')
    cpus = sorted(os.sched_getaffinity(0))
    if args.expected_cpus is not None and cpus != list(args.expected_cpus):
        raise ValueError('verifier CPU affinity differs from the caller-assigned taskset; no author work started')
    bridge_identity = check_bridge_identity(args.bridge_binary)
    sema_identity = None
    if 'sema' in args.methods:
        from src.baselines.text.products.sema import BINARY_SHA256, UPSTREAM_COMMIT
        if fixture_module.digest(args.sema_binary) != BINARY_SHA256:
            raise ValueError('Sema binary differs from the fixed author artifact; no author work started')
        sema_identity = dict(upstream_commit=UPSTREAM_COMMIT, binary_sha256=BINARY_SHA256)
    source_probe = subprocess.run([sys.executable, '-B', '-c',
        'import json,pathlib,runpy,sys;sys.path.insert(0,str(pathlib.Path(sys.argv[1]).parent));'
        'module=runpy.run_path(sys.argv[1]);print(json.dumps(module["verify_source"]('
        'pathlib.Path(sys.argv[2]),patched=True)))',
        str(CODE / 'integrations/imlane/build.py'), str(args.author_root / 'source')],
        text=True, capture_output=True, timeout=30)
    fixture_module.write_log(args.output / 'author-source.log', source_probe.stdout + source_probe.stderr)
    if source_probe.returncode != 0:
        raise RuntimeError('fixed author source verification failed; no author runtime started')
    identity = json.loads(source_probe.stdout)
    build = json.loads((args.author_root / 'artifacts/build-patched/build-identity.json').read_text())
    if build.get('upstream_commit') != identity['upstream_commit'] or build.get('mode') != 'patched':
        raise ValueError('IMLane method verification requires the fixed patched author build')
    if build.get('python_arrow', {}).get('arrow') != '16.1.0':
        raise ValueError('author C++ build requires the matching Arrow 16.1.0 ABI')
    for name, digest in build['binaries_sha256'].items():
        if fixture_module.digest(args.author_root / 'prefix-patched' / name) != digest:
            raise ValueError('checked author binary changed after build')
    helper_path = args.author_root / 'harness/code/integrations/imlane/verify.py'
    if fixture_module.digest(helper_path) != build['owned_sha256']['verify.py']:
        raise ValueError('checked author helper changed after build')
    facts = dict(ray_temp=check_ray_temp_path(args.author_root, args.ray_temp_root),
                 author_commit=identity['upstream_commit'], bridge_identity=bridge_identity,
                 cpu_affinity=cpus, methods=list(args.methods), ray_address=args.ray_address,
                 sema_identity=sema_identity,
                 models=0, gpu_resources=0)
    environment = dict(os.environ, CUDA_VISIBLE_DEVICES='', OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1',
                       PYTHONDONTWRITEBYTECODE='1', LITELLM_LOCAL_MODEL_COST_MAP='True')
    for name in ('PYTHONHOME', 'PYTHONPATH', 'RAY_ADDRESS'):
        environment.pop(name, None)
    for kind, python in (('author', args.author_root / 'venv/bin/python'), ('driver', args.lotus_python)):
        selected_env = dict(environment)
        if kind == 'driver':
            for name in ('LD_LIBRARY_PATH', 'PATH'):
                selected_env[name] = os.pathsep.join(p for p in selected_env.get(name, '').split(os.pathsep)
                    if p and not Path(p).is_relative_to(args.author_root))
            selected_env['PATH'] = str(python.parent) + os.pathsep + selected_env['PATH']
        probe = subprocess.run([str(python), '-B', '-c', _RUNTIME_PROBE, kind, ','.join(args.methods)], env=selected_env,
                               text=True, capture_output=True, timeout=30)
        fixture_module.write_log(args.output / (kind + '-runtime.log'), probe.stdout + probe.stderr)
        if probe.returncode != 0:
            raise RuntimeError(kind + ' fixed runtime import failed; no author work started')
        facts[kind] = json.loads(probe.stdout)
    write_private_json(args.output / 'runtime-preflight.json', facts)
    versions = identity['validation_python']
    check_runtime_versions(facts['author'], {name: versions[name] for name in ('psutil','numpy','pyarrow','ray')}
                           | {'aiohttp': '3.14.5'})
    driver_versions = {'pyarrow': '24.0.0', 'ray': '2.56.1', 'aiohttp': '3.14.3'}
    if 'lotus' in args.methods:
        driver_versions['lotus'] = '1.2.4'
    check_runtime_versions(facts['driver'], driver_versions)
    if facts['author']['prefix'] == facts['driver']['prefix']:
        raise ValueError('IMLane author Arrow and semantic method driver require separate environments')
    return facts


def check_author_runtime_traces(report, preflight):
    traces = report.get('kernel_traces', [])
    if not traces:
        raise ValueError('author runtime trace is missing')
    kernel = fixture_module.digest(CODE / 'integrations/sema/imlane_complete_call.py')
    http = fixture_module.digest(CODE / 'src/baselines/text/frameworks/native_calls.py')
    for row in traces:
        runtime = row.get('runtime', {})
        modules = runtime.get('modules', {})
        if (row.get('kernel_sha256') != kernel or runtime.get('python_version', [])[:2] != [3, 12]
                or not runtime.get('python_prefix') or not runtime.get('python_executable')):
            raise ValueError('author worker kernel or Python identity differs')
        for name, version in (('pyarrow','16.1.0'), ('aiohttp','3.14.5')):
            actual = modules.get(name, {})
            if (actual.get('version') != version or actual.get('source_sha256') !=
                    preflight['author']['modules'][name]['sha256']):
                raise ValueError('author worker actual library differs: ' + name)
        if modules.get('src.baselines.text.frameworks.native_calls', {}).get('source_sha256') != http:
            raise ValueError('author worker loaded a different complete-HTTP source')
    complete = [row for row in traces if type(row.get('status')) is int]
    workers = sorted({row['worker_pid'] for row in complete
                      if type(row.get('worker_pid')) is int and row['worker_pid'] > 0})
    actors = sorted({row['ray_actor_id'] for row in complete
                     if type(row.get('ray_actor_id')) is str and row['ray_actor_id']})
    if len(workers) < 2 or report['runtime'] == 'ray' and len(actors) < 2:
        raise ValueError('complete author calls did not observe both workers or both Ray actors')
    return dict(passed=True, worker_pids=workers, ray_actor_ids=actors, trace_rows=len(traces),
                complete_http_trace_rows=len(complete), kernel_sha256=kernel, native_http_source_sha256=http,
                pyarrow='16.1.0', aiohttp='3.14.5')


def parse_methods(value):
    methods = tuple(value.split(','))
    if not methods or len(set(methods)) != len(methods) or set(methods) - {'lotus','duckdb','sema'}:
        raise argparse.ArgumentTypeError('--methods requires distinct comma-separated lotus, duckdb or sema')
    return methods


def parse_cpus(value):
    if re.fullmatch(r'[0-9]+(?:-[0-9]+)?(?:,[0-9]+(?:-[0-9]+)?)*', value) is None:
        raise argparse.ArgumentTypeError('--expected-cpus requires CPU IDs or ranges such as 8-15')
    cpus = []
    for item in value.split(','):
        bounds = item.split('-')
        start, end = int(bounds[0]), int(bounds[-1])
        if end < start or end - start > 4096:
            raise argparse.ArgumentTypeError('CPU range is invalid')
        cpus.extend(range(start, end + 1))
    if len(set(cpus)) != len(cpus):
        raise argparse.ArgumentTypeError('CPU IDs must be unique')
    return tuple(sorted(cpus))


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('author-root','output','bridge-binary'):
        parser.add_argument('--'+name, type=Path, required=True)
    parser.add_argument('--driver-python', '--lotus-python', dest='lotus_python', type=Path, required=True)
    for name in ('lotus-native-source','tokenizer','duckdb-extension','sema-binary','ray-temp-root'):
        parser.add_argument('--'+name, type=Path)
    parser.add_argument('--methods', type=parse_methods, default=('lotus','duckdb','sema'))
    for name in ('fixture-port','model-fixture-port','service-port','observation-port'):
        parser.add_argument('--'+name, type=int, default=0)
    parser.add_argument('--coordinator-port-start', type=int)
    parser.add_argument('--ray-address', help='Explicit loopback GCS address; caller owns the private CLI cluster')
    parser.add_argument('--expected-cpus', type=parse_cpus,
                        help='Check inherited taskset affinity; the verifier never changes CPU affinity')
    args = parser.parse_args(argv)
    required = ({'sema-binary'} if 'sema' in args.methods else set()) | (
        {'duckdb-extension'} if 'duckdb' in args.methods else set()) | (
        {'lotus-native-source','tokenizer'} if 'lotus' in args.methods else set())
    for name in sorted(required):
        if getattr(args, name.replace('-', '_')) is None:
            parser.error('--' + name + ' is required for the selected --methods')
    ports = [args.fixture_port, args.model_fixture_port, args.service_port, args.observation_port]
    if any(not 0 <= port <= 65535 for port in ports):
        parser.error('fixture and service ports must be from 0 to 65535')
    args.coordinator_ports = {}
    if args.coordinator_port_start is not None:
        if not 1 <= args.coordinator_port_start < 65535:
            parser.error('coordinator port start must allow two distinct nonzero ports')
        args.coordinator_ports = dict(cpython=args.coordinator_port_start, ray=args.coordinator_port_start + 1)
        ports += list(args.coordinator_ports.values())
    if args.ray_address is not None:
        match = re.fullmatch(r'(?:127\.0\.0\.1|localhost):([0-9]{1,5})', args.ray_address)
        if match is None or not 1 <= int(match.group(1)) <= 65535:
            parser.error('--ray-address requires an explicit loopback host:port')
        ports.append(int(match.group(1)))
    declared = [port for port in ports if port]
    if len(set(declared)) != len(declared):
        parser.error('declared fixture, service, Coordinator and Ray ports must be distinct')
    for name, value in vars(args).items():
        if isinstance(value, Path):
            setattr(args, name, value.resolve())
    return args


def method_client_program(args):
    constants = dict(DUCKDB_EXTENSION=None if args.duckdb_extension is None else str(args.duckdb_extension),
        SEMA_BINARY=None if args.sema_binary is None else str(args.sema_binary), METHODS=args.methods,
        MODEL_FIXTURE_PORT=args.model_fixture_port, SERVICE_PORT=args.service_port,
        OBSERVATION_PORT=args.observation_port)
    return ''.join(name + '=' + repr(value) + '\n' for name, value in constants.items()) + METHOD_CLIENT


def main(argv=None):
    args = parse_arguments(argv)
    args.output.mkdir(parents=True, exist_ok=False)
    token = 'im-method-' + uuid.uuid4().hex
    os.environ['SEMLOOM_IMLANE_METHOD_FIXTURE'] = token
    fixture = None
    reports = []
    try:
        runtime_preflight = method_runtime_preflight(args)
        path = args.author_root/'harness/code/integrations/imlane/verify.py'
        spec = importlib.util.spec_from_file_location('fixed_author_helpers', path)
        helper = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(helper)
        fixture_module.LOTUS_CLIENT = method_client_program(args)
        fixture = fixture_module.HTTPFixture(port=args.fixture_port)
        for mode in ('cpython','ray'):
            report = fixture_module.check_mode(args, mode, helper, fixture)
            if report.get('passed'):
                try:
                    report['author_runtime_check'] = check_author_runtime_traces(report, runtime_preflight)
                except ValueError as error:
                    report.update(passed=False, author_runtime_check=dict(passed=False, error=redact_text(str(error))))
            client = report.get('stages',{}).get('lotus',{})
            if not client:
                # Failed clients still retain each public query's proxy/SQL facts.
                mode_root = next(args.output.glob(mode+'-*'))
                sums = [json.loads(p.read_text()) for p in (mode_root/'method-queries').glob('*/summary.json')]
                client = dict(passed=False,fixture_posts=sum(v.get('attempted_posts',v.get('actual_posts',0)) for v in sums),
                              recovery='public query summaries; client did not produce its final file')
            report['public_method_client'] = client
            report['total_fixture_posts'] = report['fixture_posts'] + client.get('fixture_posts',0)
            reports.append(report)
            if not report['passed']:
                break
        import psutil
        survivors = []
        for process in psutil.process_iter(['pid']):
            if process.pid == os.getpid():
                continue
            try:
                if process.environ().get('SEMLOOM_IMLANE_METHOD_FIXTURE') == token:
                    survivors.append(process.pid)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        result = dict(passed=len(reports)==2 and all(r['passed'] for r in reports) and not survivors,
            methods=list(args.methods), modes=reports,fixture_posts=sum(r['total_fixture_posts'] for r in reports),
            owned_tagged_survivors=survivors,models=0,gpu_resources=0,retries=0,
            fixture_posts_limit=128,
            runtime_preflight=runtime_preflight,
            verifier_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())
        write_private_json(args.output/'public-method-summary.json', result)
        print(json.dumps({k:v for k,v in result.items() if k!='modes'}))
        return 0 if result['passed'] and result['fixture_posts']<=128 else 1
    except BaseException as error:
        write_private_json(args.output/'public-method-error.json',dict(
            type=type(error).__name__,error=redact_text(str(error)),models=0,gpu_resources=0))
        raise
    finally:
        if fixture is not None:
            fixture.close()


if __name__ == '__main__':
    raise SystemExit(main())
