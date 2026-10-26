"""Opt-in native update trace; no polling, GPU calls, or inference policy.

Rows are frontend observations of genuine scheduler/iteration updates, not
kernel timestamps. Gaps remain gaps. JSONL persistence uses one writer thread;
query-completion watermarks flush the final update before returning it.
"""
from __future__ import annotations

import atexit
from collections import deque
import functools
import json
import os
from pathlib import Path
import threading
import time
import uuid

from .mfu import NativeMfuTotals

_active = None


class Trace:
    def __init__(self, directory, engine_uid, milestones, *, clock=time.monotonic_ns,
                 start_writer=True):
        self.pid, self.uid, self.clock = os.getpid(), engine_uid, clock
        self.totals = NativeMfuTotals(engine_uid, clock=lambda: clock()/1e9)
        self.pending = deque()
        self._lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._stop = threading.Event()
        self.errors = self.dropped = self.rows = 0
        self.record_wall_s = self.record_CPU_s = self.write_wall_s = self.write_CPU_s = 0.
        self.milestones = set(milestones)
        self.path = Path(directory)/('native-mfu-'+str(self.pid)+'-'+uuid.uuid4().hex+'.jsonl')
        self.pending.append(dict(kind='trace_start',engine_uid=self.uid,pid=self.pid,
            observed_ns=clock(),epoch_s=time.time(),counters=dict(self.totals.totals),
            scope='native StatLoggerManager.record callback, frontend monotonic clock',
            not_GPU_kernel_timing=True))
        self.flush()
        self.thread = None
        if start_writer:
            self.thread = threading.Thread(target=self._writer,daemon=True,name='optional-native-MFU-writer')
            self.thread.start()
            atexit.register(self.close)

    def mark_ready(self):
        try:
            with self._lock:
                self.pending.append(dict(kind='engine_logger_ready',engine_uid=self.uid,
                    observed_ns=self.clock(),counters=dict(self.totals.totals)))
            self.flush()
        except Exception:
            self.errors += 1

    def record(self, scheduler, iteration, engine_idx=0):
        started, cpu = time.monotonic(), time.thread_time()
        try:
            observed = self.clock()
            self.totals.record(scheduler,iteration,engine_idx)
            counters = dict(self.totals.totals)
            row = dict(kind='native_update',engine_uid=self.uid,pid=self.pid,
                observed_ns=observed,sequence=self.totals.records,counters=counters,
                native_perf_present=scheduler is not None and scheduler.perf_stats is not None,
                source_iteration_epoch_s=getattr(iteration,'iteration_timestamp',None),
                missing_perf_updates=self.totals.missing_perf,observation_errors=self.totals.errors)
            if scheduler is not None:
                row.update(running=scheduler.num_running_reqs,waiting=scheduler.num_waiting_reqs,
                    kv_usage=scheduler.kv_cache_usage)
            if iteration is not None:
                row['preemptions_delta']=iteration.num_preempted_reqs
            with self._lock:
                if len(self.pending)<100000:
                    self.pending.append(row);self.rows += 1
                else:self.dropped += 1
            reached={n for n in self.milestones if counters['finished']>=n}
            if reached:
                self.milestones.difference_update(reached)
                self.flush()
        except Exception:
            self.errors += 1
        finally:
            self.record_wall_s += time.monotonic()-started
            self.record_CPU_s += time.thread_time()-cpu

    def flush(self):
        started, cpu = time.monotonic(),time.thread_time()
        try:
            with self._write_lock:
                with self._lock:
                    rows=list(self.pending);self.pending.clear()
                if rows:
                    raw=''.join(json.dumps(x,separators=(',',':'),allow_nan=False)+'\n' for x in rows)
                    fd=os.open(self.path,os.O_CREAT|os.O_APPEND|os.O_WRONLY,0o600)
                    with os.fdopen(fd,'a') as output:output.write(raw);output.flush()
        except Exception:
            self.errors += 1
            # Missing observations are reported, never retried as inference.
        finally:
            self.write_wall_s += time.monotonic()-started
            self.write_CPU_s += time.thread_time()-cpu

    def _writer(self):
        while not self._stop.wait(.5):self.flush()

    def close(self):
        self._stop.set()
        if self.thread is not None and self.thread is not threading.current_thread():self.thread.join(1.)
        with self._lock:
            self.pending.append(dict(kind='trace_close',engine_uid=self.uid,observed_ns=self.clock(),
                counters=dict(self.totals.totals),errors=self.errors,dropped=self.dropped,
                native_rows=self.rows,record_wall_s=self.record_wall_s,record_thread_CPU_s=self.record_CPU_s,
                write_wall_s=self.write_wall_s,write_thread_CPU_s=self.write_CPU_s))
        self.flush()


def hook(manager_class, trace):
    """Delegate first; preserve original arguments, result object and exception."""
    original=manager_class.record
    @functools.wraps(original)
    def record(self,scheduler_stats,iteration_stats,mm_cache_stats=None,engine_idx=None):
        result=original(self,scheduler_stats,iteration_stats,mm_cache_stats=mm_cache_stats,engine_idx=engine_idx)
        try:trace.record(scheduler_stats,iteration_stats,0 if engine_idx is None else engine_idx)
        except Exception:pass
        return result
    manager_class.record=record
    if hasattr(manager_class,'log_engine_initialized'):
        original_ready=manager_class.log_engine_initialized
        @functools.wraps(original_ready)
        def ready(self,*args,**kwargs):
            result=original_ready(self,*args,**kwargs)
            try:trace.mark_ready()
            except Exception:pass
            return result
        manager_class.log_engine_initialized=ready


def install(engine_uid=None):
    """Explicit private-run environment switch; disabled means no hook/thread."""
    global _active
    directory=os.environ.get('SEMLOOM_MFU_TRACE_DIR')
    if not directory:return None
    if _active is not None and _active.pid==os.getpid():return _active
    try:
        target=Path(directory)
        if target.is_symlink() or not target.is_dir() or target.stat().st_mode&0o777!=0o700:
            return None
        uid=engine_uid or os.environ.get('SEMLOOM_MFU_ENGINE_UID')
        if not uid:return None
        milestones=[int(x) for x in os.environ.get('SEMLOOM_MFU_TRACE_MILESTONES','').split(',') if x]
        from vllm.v1.metrics.loggers import StatLoggerManager
        trace=Trace(target,uid,milestones)
        hook(StatLoggerManager,trace)
        _active=trace
        return trace
    except Exception:
        return None  # Optional data unavailable; original query still executes.
