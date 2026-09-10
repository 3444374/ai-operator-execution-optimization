"""Bound a dedicated query worker and its observed descendants, preserving evidence.

This process owns no request scheduling. Cancellation closes the shared POST
allocation before stopping workers; remote compute completion remains unknown.
"""
import json
import signal
import subprocess
import time

from src.baselines.common.private_artifacts import open_private_text,write_private_json


def supervise(command, root, close_budget, *, query_timeout_s, preparation_s=120, finish_s=120, grace_s=10):
    import psutil
    if min(query_timeout_s,preparation_s,finish_s,grace_s)<=0:
        raise ValueError('supervisor durations must be positive')
    owned={}
    started=time.monotonic()
    deadline=started+preparation_s
    phase='preparation'
    report=dict(status='failed',started_ns=time.monotonic_ns(),remote_compute_end_known=False)
    process=None
    failure=None

    def remember_children():
        try:
            parent=psutil.Process(process.pid)
            for child in [parent,*parent.children(recursive=True)]:
                owned[(child.pid,child.create_time())]=child
        except psutil.NoSuchProcess:
            pass

    def live_owned():
        live=[]
        for (pid,created),child in owned.items():
            try:
                if child.is_running() and child.create_time()==created and child.status()!=psutil.STATUS_ZOMBIE:
                    live.append(child)
            except psutil.NoSuchProcess:
                pass
        return live

    try:
        with open_private_text(root/'worker.log') as log:
            process=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
            while process.poll() is None:
                remember_children()
                if phase=='preparation' and (root/'unit/q0/started.json').exists():
                    try:
                        value=json.loads((root/'unit/q0/started.json').read_text())
                    except json.JSONDecodeError:
                        continue
                    deadline=value['started_ns']/1e9+query_timeout_s+grace_s
                    phase='query'
                if phase=='query' and (root/'unit/q0/execution.json').exists():
                    deadline=time.monotonic()+finish_s
                    phase='evaluation_and_cleanup'
                if time.monotonic()>=deadline:
                    raise TimeoutError('query worker exceeded '+phase+' deadline')
                time.sleep(.1)
            report['worker_exit']=process.returncode
            if process.returncode:
                raise RuntimeError('query worker failed; see private worker log and unit evidence')
            report['status']='passed'
    except BaseException as error:
        failure=error
        report.update(error_type=type(error).__name__,failure_phase=phase,t_cancel_ns=time.monotonic_ns())
    finally:
        if process is not None:
            remember_children()
            try:
                if (root/'unit/unit-reserved.json').exists():
                    close_budget()
            except BaseException as closing:
                report['budget_close_error_type']=type(closing).__name__
                if failure is None:failure=closing
            try:
                live=live_owned()
                if live:
                    if failure is None:
                        failure=RuntimeError('query worker left live owned processes')
                    # Give the worker a chance to run its own finally blocks.
                    if process.poll() is None:
                        process.send_signal(signal.SIGTERM)
                        try:process.wait(timeout=grace_s)
                        except subprocess.TimeoutExpired:pass
                    live=live_owned()
                    for child in live:
                        try:child.terminate()
                        except psutil.NoSuchProcess:pass
                    _,alive=psutil.wait_procs(live,timeout=grace_s)
                    for child in alive:
                        try:child.kill()
                        except psutil.NoSuchProcess:pass
                    psutil.wait_procs(alive,timeout=grace_s)
                if process.poll() is None:
                    process.kill();process.wait(timeout=grace_s)
                report.update(worker_exit=process.returncode,remaining_owned_pids=[p.pid for p in live_owned()])
                if report['remaining_owned_pids']:
                    raise RuntimeError('owned query processes remain after cancellation')
            except BaseException as cleanup:
                report['cleanup_error_type']=type(cleanup).__name__
                if failure is None:failure=cleanup
        report.update(status='failed' if failure else 'passed',ended_ns=time.monotonic_ns())
        try:
            write_private_json(root/'supervisor.json',report)
        except BaseException as recording:
            if failure is None:failure=recording
            else:failure.add_note('Supervisor report also failed: '+type(recording).__name__)
    if failure:raise failure
    return report
