"""Process ownership for the independent no-model IMLane echo verifier.

The selected lifecycle methods come from the e9a54e3e AuthorScope. Linux birth
start-ticks, exact random namespace recovery and post-exit SHM cleanup retain
that contract; this module imports no old executor or author runtime.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import threading
import time
import uuid

from src.baselines.common.private_artifacts import write_private_json
from src.baselines.common.redact import redact_text
from src.observability.process_resources.linux_procfs import process_start_time_ticks

AUTHOR = "072f8257db80569e42c9f5727d839587df529652"
SHM_ROOT = Path("/dev/shm")
PROC_ROOT = Path("/proc")


class ProcessIdentityUnknown(OSError):
    """A process cannot be declared gone from an unreadable birth identity."""


class OwnedIdentityUnknown(RuntimeError):
    def __init__(self, live, unknown):
        super().__init__("owned process identities remain unconfirmed")
        self.live, self.unknown = live, unknown


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def process_start_ticks(pid):
    ticks = process_start_time_ticks(pid)
    if ticks is None:
        if not PROC_ROOT.is_dir():
            raise ProcessIdentityUnknown("Linux process identity source is unavailable")
        try:
            (PROC_ROOT / str(pid) / "stat").stat()
        except FileNotFoundError:
            raise ProcessLookupError("process no longer has a Linux identity") from None
        except OSError as error:
            raise ProcessIdentityUnknown("Linux process identity cannot be verified") from error
        raise ProcessIdentityUnknown("Linux process start identity cannot be decoded")
    return ticks


def resource_state(scope):
    cleanup = next((item for item in reversed(scope.report.get("cleanup", []))
                    if item.get("namespace") == scope.namespace), {})
    confirmed = (scope.closed is True and cleanup.get("owned_survivors") == []
                 and cleanup.get("remaining_shm") == {}
                 and cleanup.get("owned_threads_stopped") is True
                 and not getattr(scope, "unconfirmed_roots", {}))
    return dict(namespace=scope.namespace, resources_confirmed=confirmed,
                owned_survivors=cleanup.get("owned_survivors"),
                remaining_shm=cleanup.get("remaining_shm"),
                owned_threads_stopped=cleanup.get("owned_threads_stopped"),
                unconfirmed_root_pids=sorted(getattr(scope, "unconfirmed_roots", {})))


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def resources(prefix: str) -> dict[str, int]:
    return {p.name: p.stat().st_size for p in SHM_ROOT.iterdir()
            if p.name.startswith(prefix) or p.name.startswith('.' + prefix)}


def spawn(command: list[str], env: dict[str, str], log: Path, *, stdin=False):
    process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               stdin=subprocess.PIPE if stdin else subprocess.DEVNULL,
                               text=True, start_new_session=True, cwd=log.parent)
    def save_output():
        with log.open('w') as stream:
            stream.write(json.dumps({'command': [redact_text(arg) for arg in command]}) + '\n')
            for line in process.stdout:
                stream.write(redact_text(line)); stream.flush()
    writer = threading.Thread(target=save_output, daemon=True)
    writer.start()
    return process, writer


class StopBudget:
    """Deadline signals stop work while allowing each owned teardown to finish."""
    def __init__(self, stopped):
        self.stopped=stopped;self.cleanup_depth=0;self.reason=None

    def signal(self, number, _frame):
        self.stopped.set();self.reason=signal.Signals(number).name
        if not self.cleanup_depth:raise TimeoutError('software stop signal: '+self.reason)

    @contextmanager
    def cleanup(self):
        self.cleanup_depth+=1
        try:yield
        finally:self.cleanup_depth-=1

    def check(self):
        if self.stopped.is_set():raise TimeoutError('software work has stopped')


class AuthorScope:
    def __init__(self, args, mode, helper, report, *, kernel):
        self.args, self.mode, self.helper, self.report = args, mode, helper, report
        self.kernel = Path(kernel)
        self.namespace = 'ia-' + mode + '-' + uuid.uuid4().hex[:10]
        self.root = args.output / self.namespace
        self.root.mkdir()
        self.service = self.owner = None
        self.writer = self.owner_writer = self.short = None
        self.executors, self.children = [], []
        self.known, self.roots = set(), {}
        self.unconfirmed_roots = {}
        self.identities_lock = threading.Lock()
        self.stop_sampler = threading.Event()
        self.closed, self.cleanup_attempts = False, 0
        args.scopes.append(self)

    def track_root(self, process):
        # Popen already created a child. A failed first identity read cannot
        # turn this process into an empty known set and an apparent clean exit.
        self.unconfirmed_roots[process.pid] = process
        ticks=process_start_ticks(process.pid)
        with self.identities_lock:
            self.roots[process.pid]=ticks;self.known.add((process.pid,ticks))
        self.unconfirmed_roots.pop(process.pid, None)
        self.snapshot()


    def snapshot(self, *, include_orphans=False):
        import psutil
        with self.identities_lock:roots=list(self.roots.items())
        observed=[]
        for pid,ticks in roots:
            try:
                if process_start_ticks(pid)!=ticks:continue
                root=psutil.Process(pid);family=[root,*root.children(recursive=True)]
                if process_start_ticks(pid)!=ticks:continue
                observed.extend(family)
            except (psutil.Error,OSError):continue
        if include_orphans:
            # Exact random namespace also identifies our descendants after reparenting.
            for process in psutil.process_iter():
                try:
                    if process.environ().get('IMLANE_NAMESPACE')==self.namespace:observed.append(process)
                except psutil.Error:continue
        for process in observed:
            try:
                ticks=process_start_ticks(process.pid)
                if process.is_running():
                    with self.identities_lock:self.known.add((process.pid,ticks))
            except (psutil.Error,OSError):pass


    def survivors(self):
        import psutil
        with self.identities_lock:identities=sorted(self.known)
        result=[];unknown=[]
        for pid,ticks in identities:
            try:
                if process_start_ticks(pid)!=ticks:
                    continue
                process=psutil.Process(pid)
                if process.is_running() and process.status() not in (psutil.STATUS_ZOMBIE,psutil.STATUS_DEAD):
                    result.append((pid,ticks))
            except (psutil.NoSuchProcess,ProcessLookupError,FileNotFoundError):pass
            except (psutil.Error,OSError) as error:
                unknown.append(dict(pid=pid,start_ticks=ticks,type=type(error).__name__,
                                    error=redact_text(str(error))))
        if unknown:
            raise OwnedIdentityUnknown(result,unknown)
        return result


    @staticmethod
    def signal_identity(identity, number):
        pid,ticks=identity
        try:
            if process_start_ticks(pid)==ticks:os.kill(pid,number)
        except ProcessLookupError:pass
        except FileNotFoundError:pass
        except OSError as error:
            return dict(pid=pid,start_ticks=ticks,signal=signal.Signals(number).name,
                        type=type(error).__name__,error=redact_text(str(error)))


    def stop_root(self, process, writer, *, owner=False, timeout_s=8):
        if type(timeout_s) not in (int, float) or not 0 < timeout_s < float("inf"):
            raise ValueError("owned root stop requires finite positive seconds")
        forced=False
        if process.poll() is None:
            ticks=self.roots[process.pid]
            if owner:
                try:process.stdin.write('\n');process.stdin.flush()
                except (BrokenPipeError,OSError):pass
            else:self.signal_identity((process.pid,ticks),signal.SIGTERM)
            try:process.wait(timeout=timeout_s)
            except subprocess.TimeoutExpired:
                forced=True;self.snapshot(include_orphans=True)
                for identity in self.survivors():self.signal_identity(identity,signal.SIGKILL)
                process.wait(timeout=2)
        writer.join(1)
        return dict(returncode=process.returncode,forced=forced)


    def stop_survivors(self):
        self.snapshot(include_orphans=True)
        def scan():
            try:return self.survivors(),[]
            except OwnedIdentityUnknown as error:return error.live,error.unknown
        remaining,unknown=scan();forced=list(remaining);errors=[]
        for number in (signal.SIGTERM,signal.SIGKILL):
            for identity in remaining:
                error=self.signal_identity(identity,number)
                if error:errors.append(error)
            until=time.monotonic()+2
            while (remaining or unknown) and time.monotonic()<until:
                time.sleep(.02);remaining,unknown=scan()
            remaining,unknown=scan()
        if unknown:
            errors.append(dict(action='owned-identity',identities=unknown,
                               error='owned process identities remain unconfirmed'))
        return forced,None if unknown else remaining,errors


    def abort_owned(self):
        if self.closed:return
        errors=[]
        try:self.snapshot(include_orphans=True)
        except Exception as error:errors.append(dict(type=type(error).__name__,error=redact_text(str(error))))
        for identity in self.survivors():
            error=self.signal_identity(identity,signal.SIGKILL)
            if error:errors.append(error)
        return errors


    def close(self):
        if self.closed:return
        self.cleanup_attempts+=1
        cleanup=dict(runtime=self.mode,namespace=self.namespace,attempt=self.cleanup_attempts,errors=[])
        def attempt(label, operation):
            try:return operation()
            except BaseException as error:
                cleanup['errors'].append(dict(action=label,type=type(error).__name__,error=redact_text(str(error))))
        with self.args.stop_budget.cleanup():
            for process in tuple(self.unconfirmed_roots.values()):
                attempt('root-identity-retry',lambda current=process:self.track_root(current))
            attempt('snapshot',lambda:self.snapshot(include_orphans=True))
            for index,value in enumerate(self.executors):attempt('executor-'+str(index),value.close)
            if self.service is not None:
                cleanup['coordinator']=attempt('coordinator',lambda:self.stop_root(self.service,self.writer))
            if self.owner is not None:
                cleanup['ray_owner']=attempt('ray_owner',lambda:self.stop_root(self.owner,self.owner_writer,owner=True))
            for index,(process,writer) in enumerate(self.children):
                cleanup['client-'+str(index)]=attempt('client-'+str(index),lambda:self.stop_root(process,writer))
            self.stop_sampler.set()
            if hasattr(self,'sampler'):attempt('sampler',lambda:self.sampler.join(1))
            fallback=attempt('owned-survivors',self.stop_survivors)
            cleanup['forced_identities']=None if fallback is None else [dict(pid=p,start_ticks=t) for p,t in fallback[0]]
            cleanup['owned_survivors']=None if fallback is None or fallback[1] is None else [dict(pid=p,start_ticks=t) for p,t in fallback[1]]
            cleanup['unconfirmed_root_pids']=sorted(self.unconfirmed_roots)
            if self.unconfirmed_roots:
                cleanup['owned_survivors']=None
            if fallback is not None:cleanup['errors'].extend(fallback[2])
            cleanup['remaining_shm']=attempt('shared-memory',lambda:self.helper.resources(self.namespace))
            cleanup['shared_memory_before_fallback']=cleanup['remaining_shm']
            cleanup['forced_shared_memory']=[]
            if cleanup['owned_survivors']==[] and cleanup['remaining_shm']:
                for name in cleanup['remaining_shm']:
                    if Path(name).name!=name or not name.startswith((self.namespace,'.'+self.namespace)):
                        cleanup['errors'].append(dict(action='shared-memory-name',error='unrecognized owned namespace member'))
                        continue
                    cleanup['forced_shared_memory'].append(name)
                    attempt('shared-memory-unlink',lambda:(SHM_ROOT/name).unlink(missing_ok=True))
                cleanup['remaining_shm']=attempt('shared-memory-recheck',lambda:self.helper.resources(self.namespace))
            writers=(self.writer,self.owner_writer,*(writer for _process,writer in self.children))
            for writer in writers:
                if writer is not None:attempt('log-writer',lambda:writer.join(1))
            cleanup['owned_threads_stopped']=all(not value.is_alive() for value in
                (*writers,getattr(self,'sampler',None)) if value is not None)
            cleanup['owned_process_identities']=[dict(pid=p,start_ticks=t) for p,t in sorted(self.known)]
            cleanup['passed']=(not cleanup['errors'] and cleanup['owned_survivors']==[]
                and cleanup['forced_identities']==[] and not cleanup['forced_shared_memory']
                and cleanup['remaining_shm']=={} and cleanup['owned_threads_stopped'] and all(
                    cleanup[k] and not cleanup[k]['forced'] and cleanup[k]['returncode']==0
                    for k in ('coordinator','ray_owner') if k in cleanup))
            if any(not row or row['forced'] or row['returncode']!=0 for key,row in cleanup.items() if key.startswith('client-')):cleanup['passed']=False
            if self.short is not None and self.short.is_symlink():attempt('temporary-link',self.short.unlink)
            if cleanup['errors']:cleanup['passed']=False
            self.report['cleanup'].append(cleanup)
            attempt('cleanup-evidence',lambda:write_private_json(self.root/f'cleanup-{self.cleanup_attempts:03}.json',cleanup))
            if cleanup['errors']:cleanup['passed']=False
            self.closed=(cleanup['owned_survivors']==[] and cleanup['remaining_shm']=={} and cleanup['owned_threads_stopped'])
        if not cleanup['passed']:raise RuntimeError('owned author cleanup failed')


    def __exit__(self,kind,error,traceback):
        try:self.close()
        except Exception as cleanup:
            if error is None:raise
            error.add_note('Author cleanup also failed: '+type(cleanup).__name__)
