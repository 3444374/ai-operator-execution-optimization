"""Bounded real-author complete-HTTP checks; all endpoints are local fixtures."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
import uuid

CODE = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE))
from src.baselines.text.frameworks.imlane_calls import (
    IMLaneCallConfig, IMLaneCallExecutor, process_start_ticks,
)
from src.baselines.text.frameworks.native_calls import NativeCall
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.baselines.common.private_artifacts import open_private_text, write_private_json
from src.baselines.common.redact import redact_text


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_log(path, value):
    with open_private_text(path) as stream:
        stream.write(redact_text(value))


class HTTPFixture:
    def __init__(self, port=0):
        self.receipts, self.lock = [], threading.Lock()
        self.slow_started = threading.Event()
        owner = self
        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            def log_message(self, *_args):
                pass
            def do_POST(self):
                raw = self.rfile.read(int(self.headers["Content-Length"]))
                with owner.lock:
                    if len(owner.receipts) >= 100:
                        self.send_error(500, "fixture allowance exhausted")
                        return
                    owner.receipts.append(dict(path=self.path, sha256=hashlib.sha256(raw).hexdigest(),
                        body=raw.decode("utf-8", errors="replace"),
                        headers=list(self.headers.items())))
                if self.path == "/slow":
                    owner.slow_started.set()
                    time.sleep(.5)
                status = 401 if self.path == "/unauthorized" else 429 if self.path == "/rate" else 200
                body = b"not-json\x00\n" if self.path == "/bad-json" else json.dumps(dict(
                    id="fixture-response", object="chat.completion", created=0, model="fixture",
                    choices=[dict(index=0, message=dict(role="assistant", content="ok"),
                                  finish_reason="length" if self.path == "/length" else "stop")],
                    usage=dict(prompt_tokens=2, completion_tokens=1, total_tokens=3))).encode()
                compressed = self.path == "/gzip"
                if compressed:
                    body = gzip.compress(body, mtime=0)
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("X-Repeat", "one")
                self.send_header("X-Repeat", "two")
                if compressed:
                    self.send_header("Content-Encoding", "gzip")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                try:
                    self.wfile.write(body)
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    pass
        self.server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)


LOTUS_CLIENT = """
import importlib.util,json,os,pathlib,sys
code,config,native_source,tokenizer,result_file=sys.argv[1:]
sys.path.insert(0,code)
os.environ['LITELLM_LOCAL_MODEL_COST_MAP']='True'
from src.baselines.text.frameworks.imlane_calls import IMLaneCallConfig,IMLaneCallExecutor
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.semantic_methods.lotus.batch import lotus_executor
spec=importlib.util.spec_from_file_location('src.semantic_methods.lotus.native',native_source)
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
import lotus,pandas as pd
from lotus.models import LM
from tokenizers import Tokenizer
value=json.loads(pathlib.Path(config).read_text())
endpoint=value.pop('fixture_endpoint')
adapter=IMLaneCallExecutor(IMLaneCallConfig(**value),NativeGraphOptions(concurrency=8))
lm=LM('openai/fixture',api_base=endpoint.removesuffix('/chat/completions'),
 api_key='local-fixture',max_batch_size=8,temperature=0,top_p=1,max_tokens=16,
 num_retries=0,max_retries=0,timeout=5,tokenizer=Tokenizer.from_file(tokenizer))
observed=[]
selected=module.LotusNativeBatchExecutor(adapter,
 FixedModelConfig(endpoint,'fixture',5000,bearer_token='local-fixture'),
 query_id='author-lotus-fixture',operator_id='map',
 on_response=lambda i,response:observed.append((i,response.status_code,len(response.body))))
try:
 with lotus.settings.context(lm=lm,enable_cache=False),lotus_executor(lm,selected):
  frame=pd.DataFrame({'row_id':['a','b','c','d'],'text':['same text','same text','中文','fourth']})
  out=frame.sem_map('Return ok for {text}.')
  assert out['row_id'].tolist()==['a','b','c','d']
  outputs=out['_map'].tolist()
  assert outputs==['ok']*4,outputs
 assert len(observed)==4,observed
 pathlib.Path(result_file).write_text(json.dumps(dict(passed=True,outputs=outputs,
  consumed_rows=len(out),responses=observed,close=adapter.last_close_report,
  model_runs=0,gpu_resources=0)))
finally:adapter.close()
"""


def check_mode(args, runtime, helper, fixture):
    import psutil
    token = uuid.uuid4().hex[:10]
    out = args.output / (runtime + "-" + token)
    out.mkdir()
    author = args.author_root
    prefix, python = author/"prefix-patched", author/"venv/bin/python"
    info = json.loads(subprocess.check_output([str(python), "-c",
        "import sys,sysconfig,pyarrow,json; print(json.dumps(dict(site=sysconfig.get_path('purelib'),"
        "lib=pyarrow.get_library_dirs()[0],base=sys.base_prefix)))"], text=True))
    cpp = Path(subprocess.check_output(["g++", "-print-file-name=libstdc++.so.6"], text=True).strip()).resolve()
    namespace = "ic-" + runtime + "-" + token
    trace_root = (getattr(args, "ray_worker_trace_root", None) if runtime == "ray" else None) or out/"worker-traces"
    trace_root = Path(trace_root)
    declared_ports = getattr(args, "coordinator_ports", {})
    port = declared_ports[runtime] if runtime in declared_ports else helper.free_port()
    env = dict(os.environ, CUDA_VISIBLE_DEVICES="", OMP_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1",
        PATH=str(python.parent)+os.pathsep+os.environ["PATH"],
        PYTHONPATH=os.pathsep.join([info["site"],str(CODE),str(CODE/"integrations/sema")]),
        LD_LIBRARY_PATH=os.pathsep.join([str(cpp.parent),str(prefix/"lib"),info["lib"],
            str(author/"source/third_party/fmt/dist/lib"),str(author/"source/third_party/boost/dist/lib"),
            info["base"]+"/lib"]),
        TMPDIR=str(out/"tmp"), IMLANE_NAMESPACE=namespace, IMLANE_GATEWAY_HOST="127.0.0.1",
        IMLANE_HTTP_TRACE_DIR=str(trace_root),
        RAY_USAGE_STATS_ENABLED="0",RAY_DEDUP_LOGS="0")
    Path(env["TMPDIR"]).mkdir()
    env.pop("PYTHONHOME",None)
    env.pop("RAY_ADDRESS",None)
    owner = service = writer = owner_writer = None
    executors = []
    known = {}
    report = dict(runtime=runtime, namespace=namespace, cpu_affinity=sorted(os.sched_getaffinity(0)),
                  declared_author_executors=2,
                  ray_cpu_resources=(None if getattr(args,"ray_address",None) else 2) if runtime=="ray" else 0,
                  gpu_resources=0, model_runs=0, retries=0, stages={})
    monitor_stop = threading.Event()
    def monitor():
        while not monitor_stop.wait(.1):
            for process in (owner,service):
                if process is None:
                    continue
                try:
                    parent=psutil.Process(process.pid)
                    children=[parent,*parent.children(recursive=True)]
                except psutil.Error:
                    continue
                for child in children:
                    try:
                        known[child.pid]=child.create_time()
                    except psutil.Error:
                        pass
    watcher=threading.Thread(target=monitor,daemon=True);watcher.start()
    before=len(fixture.receipts)
    try:
        command=[str(prefix/"bin/imlane"),"--working_dir",str(out/"state"),
                 "--n_executors","2","--runtime",runtime,"--gateway_port",str(port)]
        if runtime=="ray":
            ray_root=out/"ray";ray_root.mkdir()
            address=out/"ray-address.json"
            external_address = getattr(args, "ray_address", None)
            if external_address:
                if external_address in ("auto", "local"):
                    raise ValueError("author Ray requires the explicit caller-owned GCS address")
                selected = external_address
                init = "ctx=ray.init(address=sys.argv[1]);"
                report["ray_cluster_owner"] = "external caller; verifier disconnects its driver only"
            else:
                short_parent = getattr(args, "ray_temp_root", None) or author.parents[1]
                short=Path(short_parent)/("ir-"+token)
                if len(os.fsencode(short.absolute())) > 50 or short.exists() or short.is_symlink():
                    raise ValueError("author Ray temporary directory must be new and short before startup")
                short.parent.mkdir(parents=True,exist_ok=True)
                short.symlink_to(ray_root,target_is_directory=True)
                selected = str(short)
                init = ("ctx=ray.init(address='local',num_cpus=2,num_gpus=0,"
                    "include_dashboard=False,object_store_memory=134217728,_node_ip_address='127.0.0.1',"
                    "_temp_dir=sys.argv[1]);")
                report["ray_cluster_owner"] = "verifier"
            code=("import sys,ray,json,os;"+init+"data=json.dumps("
                "dict(address=ctx.address_info['gcs_address'],resources=ray.cluster_resources()));"
                "temporary=sys.argv[2]+'.pending';stream=open(temporary,'w');"
                "stream.write(data);stream.close();"
                "os.replace(temporary,sys.argv[2]);sys.stdin.readline();ray.shutdown()")
            owner,owner_writer=helper.spawn([str(python),"-c",code,str(selected),str(address)],
                                           env,out/"ray-owner.log",stdin=True)
            until=time.monotonic()+60
            while not address.is_file():
                if owner.poll() is not None or time.monotonic()>until:
                    raise RuntimeError("owned zero-GPU Ray did not start")
                time.sleep(.05)
            actual_ray=json.loads(address.read_text())
            report['ray_cluster_resources']=actual_ray['resources']
            report['ray_cpu_resources']=actual_ray['resources'].get('CPU',0)
            command+=["--ray_address",actual_ray["address"]]
        service,writer=helper.spawn(command,env,out/"coordinator.log")
        until=time.monotonic()+60
        while True:
            if service.poll() is not None:
                raise RuntimeError("author Coordinator exited during startup")
            try:
                with socket.create_connection(("127.0.0.1",port),timeout=.1):
                    break
            except OSError:
                if time.monotonic()>until:
                    raise TimeoutError("author Coordinator readiness exceeded 60s")
                time.sleep(.05)
        kernel=CODE/"integrations/sema/imlane_complete_call.py"
        probe=subprocess.run([str(args.bridge_binary),str(port),str(kernel),"0"],
                             input="",text=True,capture_output=True,env=env,timeout=30)
        write_log(out/"empty-probe.log", probe.stdout+probe.stderr)
        assert probe.returncode==0,probe.stderr
        meta=json.loads(next(line.partition(" ")[2] for line in probe.stdout.splitlines()
                             if line.startswith("SEMA_IMLANE_META ")))
        report["settings_report"]=meta
        assert meta["native_task_threads"]<=8,meta
        bridge=dict(author_commit="072f8257db80569e42c9f5727d839587df529652",
            bridge_binary=str(args.bridge_binary),bridge_sha256=digest(args.bridge_binary),
            gateway_port=port,udf_path=str(kernel),udf_sha256=digest(kernel),work_root=str(out/"batches"),
            namespace=namespace,native_task_threads=meta["native_task_threads"],
            env={k:env[k] for k in ("LD_LIBRARY_PATH","PATH","PYTHONPATH","TMPDIR","OMP_NUM_THREADS","OPENBLAS_NUM_THREADS")})
        bridge_path=out/"bridge-config.json";write_private_json(bridge_path,bridge)
        cfg=IMLaneCallConfig(runtime,str(bridge_path),service.pid,process_start_ticks(service.pid),
              str(author/"artifacts/build-patched/build-identity.json"),2)
        def executor():
            value=IMLaneCallExecutor(cfg,NativeGraphOptions(concurrency=8))
            executors.append(value)
            return value
        initial=executor()
        try:
            list(initial.iter_batch([NativeCall("cancel-before",0,b"{}",fixture.base+"/ok",(),2)],
                                    cancelled=lambda:True))
            raise AssertionError("pre-dispatch cancellation was ignored")
        except InterruptedError:
            report["stages"]["pre_dispatch_cancel"]=initial.last_close_report
        assert len(fixture.receipts)==before
        paths=["/ok","/unauthorized","/rate","/gzip","/bad-json","/length"]
        payload=b' { "model":"fixture", "messages":[{"role":"user","content":"same"}] } '
        calls=[NativeCall("http-"+str(i),5,payload,fixture.base+path,
            (("Content-Type","application/json"),("X-Repeat","a"),("X-Repeat","b")),2,
            max_response_bytes=65536) for i,path in enumerate(paths)]
        values=list(initial.iter_batch(calls,timeout_s=30))
        assert {v.call_id for v in values}=={c.call_id for c in calls}
        outcomes={v.call_id:v for v in values}
        assert [outcomes[c.call_id].response.status_code for c in calls]==[200,401,429,200,200,200]
        assert outcomes["http-4"].response.body==b"not-json\x00\n"
        assert json.loads(gzip.decompress(outcomes["http-3"].response.body))["choices"][0]["message"]["content"]=="ok"
        for value in values:
            assert [v for k,v in value.response.headers if k=="X-Repeat"]==["one","two"]
            assert value.response.http_version=="HTTP/1.1"
        assert [r["sha256"] for r in fixture.receipts[before:]]==[hashlib.sha256(payload).hexdigest()]*6
        report["stages"]["full_http"]=initial.last_close_report
        old_fixture=subprocess.run([str(prefix/"bin/imlane_fixture_cpp"),str(port),
            str(author/"harness/code/integrations/imlane/fixture_udf.py")],
            env=env,text=True,capture_output=True,timeout=30)
        write_log(out/"author-loop-regression.log", old_fixture.stdout+old_fixture.stderr)
        assert old_fixture.returncode==0,old_fixture.stderr[-2000:]
        report["stages"]["author_loop_after_http"]=dict(returncode=0,models=0,
            complete_arrow_fixture_rows=105)
        # Actual LOTUS formatter, LM sem_map, provider parser and row consumption.
        endpoint=fixture.base+"/v1/chat/completions"
        client_config=out/"lotus-config.json"
        write_private_json(client_config,dict(cfg.__dict__,fixture_endpoint=endpoint))
        child=out/"lotus-client.py";child.write_text(LOTUS_CLIENT)
        result=out/"lotus-result.json"
        client=subprocess.run([str(args.lotus_python),str(child),str(CODE),str(client_config),
            str(args.lotus_native_source),str(args.tokenizer),str(result)],env=env,text=True,capture_output=True,timeout=60)
        write_log(out/"lotus.log", client.stdout+client.stderr)
        assert client.returncode==0,(client.returncode,client.stderr[-3000:])
        report["stages"]["lotus"]=json.loads(result.read_text())
        # A completed author UDF can still report an uncertain remote HTTP timeout.
        timed=executor()
        value,=timed.iter_batch([NativeCall("http-timeout",0,b"{}",fixture.base+"/slow",(),.1)],timeout_s=10)
        assert value.response is None and value.transport_error
        report["stages"]["http_timeout"]=dict(timed.last_close_report,error_type=value.transport_error.split(":",1)[0])
        assert timed.last_close_report["external_coordinator_requires_cleanup"]
        # A timeout without a complete HTTP response keeps this Coordinator unknown.
        # The independent cancellation case must use a separately owned Coordinator.
        blocked=executor()
        with_error = None
        try:
            list(blocked.iter_batch([NativeCall("reuse-unknown",0,b"{}",fixture.base+"/slow",(),2)]))
        except RuntimeError as error:
            with_error = str(error)
        assert with_error and "cleanup" in with_error
        report["stages"]["unknown_coordinator_reuse_blocked"] = True
        time.sleep(.6)
        report["normal_checks_passed"]=True
    except Exception as error:
        report.update(normal_checks_passed=False,error_type=type(error).__name__,error=redact_text(str(error))[:4096])
    finally:
        for value in executors:
            value.close()
        if service is not None:
            report["coordinator_shutdown"]=helper.stop(service,writer)
        if owner is not None:
            report["ray_owner_shutdown"]=helper.stop(owner,owner_writer,owner=True)
        monitor_stop.set();watcher.join(2)
        survivors=[]
        for pid,started in known.items():
            try:
                process=psutil.Process(pid)
                if process.create_time()==started and process.status()!=psutil.STATUS_ZOMBIE:
                    survivors.append(pid)
            except psutil.Error:
                pass
        report["owned_survivors"]=survivors
        report["remaining_shm"]=helper.resources(namespace)
        report["fixture_posts"]=len(fixture.receipts)-before
        traces=[]
        for path in trace_root.glob("*.jsonl"):
            traces.extend(json.loads(line) for line in path.read_text().splitlines())
        report["kernel_traces"]=traces
        report["actual_worker_pids"]=sorted({row["worker_pid"] for row in traces})
        report["ray_actor_ids"]=sorted({str(row["ray_actor_id"]) for row in traces if "ray_actor_id" in row})
        report["passed"]=(report.get("normal_checks_passed") is True and not survivors
            and not report["remaining_shm"] and not report["coordinator_shutdown"]["forced"]
            and report["coordinator_shutdown"]["returncode"]==0
            and (runtime!="ray" or (not report["ray_owner_shutdown"]["forced"]
                 and report["ray_owner_shutdown"]["returncode"]==0 and bool(report["ray_actor_ids"]))))
        write_private_json(out/"result.json",report)
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ("author-root","output","bridge-binary","lotus-python","lotus-native-source","tokenizer"):
        parser.add_argument("--"+name,type=Path,required=True)
    args=parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=False)
    path=args.author_root/"harness/code/integrations/imlane/verify.py"
    spec=importlib.util.spec_from_file_location("fixed_imlane_fixture_helpers",path)
    helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
    fixture=HTTPFixture()
    try:
        reports=[check_mode(args,mode,helper,fixture) for mode in ("cpython","ray")]
        settings_match=(reports[0].get("settings_report") is not None and
                        reports[0].get("settings_report")==reports[1].get("settings_report"))
        result=dict(passed=all(r["passed"] for r in reports) and settings_match,modes=reports,
                    author_settings_equal=settings_match,
                    fixture_posts=len(fixture.receipts),models=0,gpu_resources=0,retries=0,
                    fixture_receipts=fixture.receipts)
        write_private_json(args.output/"summary.json",result)
        print(json.dumps({k:v for k,v in result.items() if k!="fixture_receipts" and k!="modes"}))
        return 0 if result["passed"] else 1
    finally:
        fixture.close()


if __name__=="__main__":
    raise SystemExit(main())
