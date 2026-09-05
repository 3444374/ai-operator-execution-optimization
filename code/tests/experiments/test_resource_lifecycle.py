"""Phase progress, status composition and complete controlled runner paths."""
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from src.observability.process_resources.model import (
    FdIdentity, FdKind, ProcessSnapshot, SampleTick, SnapshotStatus)
from src.experiments.postgresql import semmap_resource_runner as runner
from src.experiments.postgresql.resource_lifecycle import (
    PhaseResult, RunSpec, REQUIRED_PHASES, assess_phases, case_report, run_report)
from src.experiments.postgresql.resource_phase import execute_phase, cleanup_settle, hashes

FAST = RunSpec(sample_seconds=.001, baseline_interval_seconds=0,
               baseline_timeout_seconds=.05, cleanup_interval_seconds=0,
               cleanup_timeout_seconds=.05)


class ControlledWorkload:
    """Persistent process identities; operation opens then closes a known session."""
    def __init__(self, *, leak=False, failed=False, sample_error=False):
        self.active = False
        self.events = []
        self.leak, self.failed, self.sample_error = leak, failed, sample_error
        self.sample_count = 0

    def sample_all(self, ns):
        self.sample_count += 1
        if self.sample_error and self.active:
            raise ValueError("controlled sampling failure")
        backend = [FdIdentity(1, "pipe:[1]", FdKind.PIPE, 1)]
        gateway = [FdIdentity(3, "socket:[3]", FdKind.PROVIDER_UDS_LISTENER, 3)]
        if self.active:
            backend.append(FdIdentity(9, "socket:[9]", FdKind.UNBOUND_UNIX_SOCKET, 9))
            gateway.append(FdIdentity(5, "socket:[5]", FdKind.PROVIDER_UDS_CONNECTED, 5))
        return SampleTick(ns, True, {
            role: ProcessSnapshot(pid, pid, ns, SnapshotStatus.VALID,
                rss_bytes=1024, thread_count=1, fds=tuple(fds))
            for role,pid,fds in (("backend",1,backend),("gateway",2,gateway))})

    def operation(self):
        self.active = True
        self.events.append({"event":"session_start", "session_id":1,
            "monotonic_ns":time.monotonic_ns(), "peer_pid":1, "gateway_pid":2,
            "accepted_fd":5, "accepted_socket_inode":5})
        self.events.append({"event":"task", "session_id":1, "task":1,
            "payload_digest":"fixture", "monotonic_ns":time.monotonic_ns()})
        time.sleep(.01)
        self.events.append({"event":"task_complete", "session_id":1, "task":1,
            "monotonic_ns":time.monotonic_ns()})
        self.active = self.leak
        self.events.append({"event":"session_end", "session_id":1,
            "monotonic_ns":time.monotonic_ns(), "connection_closed":True})
        if self.failed:
            raise RuntimeError("controlled operation failure")
        return {"output_matches":True}


def controlled_phase(root, name, spec=FAST, **kw):
    workload = ControlledWorkload(**kw)
    return execute_phase(root=root, phase=name, spec=spec, sampler=workload,
        operation=workload.operation, events=lambda:workload.events,
        expected_digest="fixture", check_result=lambda value: [] if value == {"output_matches":True} else [{"metric":"output"}])


class LifecycleTests(unittest.TestCase):
    def test_pending_cleanup_becomes_pass_only_when_completed(self):
        peak = PhaseResult("peak","completed","valid","passed",safe=True)
        cleanup = PhaseResult("cleanup","running")
        pending = assess_phases(("peak","cleanup"), [peak,cleanup])
        self.assertIsNone(pending["assessment"])
        complete = assess_phases(("peak","cleanup"), [peak,replace(cleanup,
            state="completed",measurement_status="valid",policy_status="passed",safe=True)])
        self.assertEqual(complete["assessment"]["qualification_status"],"passed")

    def test_phase_baseline_rejects_missing_changing_listener_and_live_accepted(self):
        for scenario in ("missing", "changing", "accepted"):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as directory:
                workload = ControlledWorkload()
                class Sampler:
                    def sample_all(self, ns):
                        value = workload.sample_all(ns)
                        gateway = value.processes["gateway"]
                        if scenario == "missing":
                            resources = ()
                        elif scenario == "changing":
                            inode = 100 + workload.sample_count
                            resources = (FdIdentity(3, f"socket:[{inode}]", FdKind.PROVIDER_UDS_LISTENER, inode),)
                        else:
                            resources = gateway.fds + (FdIdentity(5, "socket:[5]", FdKind.PROVIDER_UDS_CONNECTED, 5),)
                        return replace(value, processes={**value.processes, "gateway": replace(gateway, fds=resources)})
                called = []
                result = execute_phase(root=Path(directory)/"phase", phase="operation", spec=FAST,
                    sampler=Sampler(), operation=lambda: called.append(True), events=lambda: [])
                self.assertEqual(result.measurement_status, "invalid")
                self.assertIn("stable_baseline_unavailable", result.problems)
                self.assertEqual(called, [])

    def test_invalid_original_survives_failed_recovery(self):
        value = assess_phases(("fault","recovery"), [
            PhaseResult("fault","completed","invalid","not_evaluated",problems=("gap",)),
            PhaseResult("recovery","completed","valid","failed",failures=({"metric":"recovery"},))])
        self.assertEqual(value["assessment"]["measurement_status"],"invalid")
        self.assertEqual(len(value["failures"]),1)

    def test_missing_and_empty_required_sets_never_pass(self):
        with self.assertRaises(ValueError):
            assess_phases((), [])
        self.assertIsNone(assess_phases(("needed",),[])["assessment"])
        self.assertEqual(run_report({},FAST)["exit_code"],2)

    def test_real_phase_positive_failure_and_gap(self):
        for options, expected in (({},"passed"),({"failed":True},"failed"),
                                  ({"sample_error":True},"not_evaluated")):
            with self.subTest(options=options), tempfile.TemporaryDirectory() as directory:
                root=Path(directory)/"phase"
                result=controlled_phase(root,"operation",**options)
                self.assertEqual(result.policy_status,expected,result)
                self.assertTrue((root/"operation/operation_outcome.json").exists())
                self.assertTrue((root/"cleanup/process_samples.jsonl.gz").exists())

    def test_absent_backend_leak_fails_without_gateway_requirement(self):
        workload=ControlledWorkload()
        class BackendOnly:
            def sample_all(self,ns):
                value=workload.sample_all(ns)
                return replace(value,processes={"backend":value.processes["backend"]})
        def operation():
            workload.active=True
        with tempfile.TemporaryDirectory() as directory:
            value=execute_phase(root=Path(directory)/"absent",phase="absent",spec=FAST,
                sampler=BackendOnly(),operation=operation,events=lambda:[],
                roles=("backend",),require_sessions=False)
        self.assertEqual(value.policy_status,"failed")
        self.assertFalse(value.safe)
        self.assertTrue(any(f["metric"]=="total_fd_end_delta" for f in value.failures))

    def test_cleanup_bad_first_tick_is_retained_and_cannot_end_poll(self):
        workload=ControlledWorkload()
        base=workload.sample_all(1).processes
        calls=[]
        class Sampler:
            def sample_all(self,ns):
                calls.append(ns)
                value=workload.sample_all(ns)
                if len(calls)==1:
                    value=replace(value,processes={r:replace(s,status=SnapshotStatus.PARTIAL) for r,s in value.processes.items()})
                return value
        trace,settled=cleanup_settle(Sampler(),base,FAST,lambda:[],roles=("backend","gateway"))
        self.assertTrue(settled)
        self.assertEqual(len(trace.ticks),1+FAST.cleanup_samples)
        self.assertEqual(trace.ticks[0].processes["backend"].status,SnapshotStatus.PARTIAL)
        self.assertEqual(trace.baseline,base)

    def test_diagnostic_does_not_mutate_subsequent_formal_spec(self):
        self.assertEqual(RunSpec("diagnostic").rounds,1)
        self.assertEqual(RunSpec("formal").rounds,3)
        self.assertEqual(RunSpec("formal").rows_per_round,2000)

    def test_mismatched_completion_id_cannot_qualify(self):
        workload=ControlledWorkload()
        def operation():
            value=workload.operation()
            next(e for e in workload.events if e['event']=='task_complete')['task']=999
            return value
        with tempfile.TemporaryDirectory() as directory:
            value=execute_phase(root=Path(directory)/'phase',phase='operation',spec=FAST,
                sampler=workload,operation=operation,events=lambda:workload.events)
        self.assertNotEqual(value.policy_status,'passed')
        self.assertFalse(value.safe)

    def test_cleanup_interrupt_keeps_collected_ticks_before_propagation(self):
        import gzip
        import threading
        workload=ControlledWorkload()
        class Sampler:
            calls_after_operation=0
            def sample_all(self,ns):
                if (threading.current_thread() is threading.main_thread()
                        and workload.events and workload.events[-1]['event']=='session_end'):
                    self.calls_after_operation+=1
                    if self.calls_after_operation==3:
                        raise KeyboardInterrupt()
                return workload.sample_all(ns)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'phase'
            with self.assertRaises(KeyboardInterrupt):
                execute_phase(root=root,phase='operation',spec=FAST,sampler=Sampler(),
                    operation=workload.operation,events=lambda:workload.events)
            path=root/'cleanup_interrupted/process_samples.jsonl.gz'
            self.assertTrue(path.exists())
            with gzip.open(path,'rt') as handle:
                self.assertTrue(any(json.loads(line)['kind']=='tick' for line in handle))

    def test_valid_overlimit_sample_retains_failure_beside_partial_tick(self):
        from src.experiments.postgresql.resource_qualification import build_qualification_report
        from src.observability.process_resources.model import ResourceTrace
        workload=ControlledWorkload()
        initial=workload.sample_all(1)
        partial=replace(initial,processes={r:replace(v,status=SnapshotStatus.PARTIAL) for r,v in initial.processes.items()})
        overloaded=replace(initial,processes={**initial.processes,
            'backend':replace(initial.processes['backend'],rss_bytes=50*1024*1024)})
        report=build_qualification_report(initial.processes,ResourceTrace(initial.processes,(partial,overloaded)),phase='stress')
        self.assertEqual(report.measurement_status,'inconclusive')
        self.assertTrue(any(v.metric=='rss_peak_delta' for v in report.peak_policy))

    def test_absent_path_and_established_disconnect_have_distinct_error_contracts(self):
        self.assertEqual(runner.GATEWAY_ABSENT_CONNECT_SQLSTATE,'XX000')
        self.assertEqual(runner.DISCONNECT_SQLSTATE,'08006')
        workload=ControlledWorkload()
        class Sampler:
            def sample_all(self,ns):
                value=workload.sample_all(ns)
                return replace(value,processes={'backend':value.processes['backend']})
        class SocketAccessError(Exception):
            sqlstate='XX000'
        def operation():
            raise SocketAccessError()
        with tempfile.TemporaryDirectory() as directory:
            result=execute_phase(root=Path(directory)/'absent',phase='absent',spec=FAST,
                sampler=Sampler(),operation=operation,events=lambda:[],roles=('backend',),
                require_sessions=False,expected_sqlstate=runner.GATEWAY_ABSENT_CONNECT_SQLSTATE)
        self.assertEqual(result.policy_status,'passed')
        self.assertTrue(result.safe)


class RunnerOwnershipTests(unittest.TestCase):
    def args(self,root,diagnostic=False):
        return runner.parse_args(['--root', str(root), '--repo', str(Path.cwd()),
            '--prefix', str(root.parent), '--commit', 'fixture', '--pg-port', '55499',
            *(['--diagnostic'] if diagnostic else [])])

    @staticmethod
    def compiler(source,target,prefix):
        target.write_bytes(b"controlled executable")
        target.with_suffix(".build.json").write_text("{}")

    @staticmethod
    def cases(args,spec):
        for name, required in REQUIRED_PHASES.items():
            root=args.root/name
            root.mkdir()
            yield name,[controlled_phase(root/phase,phase,replace(FAST,mode=spec.mode)) for phase in required]

    def test_new_root_compiles_real_files_and_runs_all_phases_to_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            args=self.args(Path(directory)/"run")
            with patch.object(runner,"preflight",return_value={"source":"controlled"}), \
                 patch.object(runner,"build_client",side_effect=self.compiler), \
                 patch.object(runner,"execute_cases",side_effect=self.cases):
                code=runner.run(args)
            self.assertEqual(code,0)
            summary=json.loads((args.root/"summary.json").read_text())
            self.assertEqual(summary["qualification_status"],"passed")
            self.assertEqual(set(summary["cases"]),set(REQUIRED_PHASES))
            self.assertTrue((args.root/"build/resource_client_v3").exists())
            manifest=json.loads((args.root/'manifest.json').read_text())
            self.assertEqual(manifest['runtime_configuration']['pg_port'],55499)
            self.assertEqual(len(manifest['runtime_configuration']['pg_owner_sha256']),64)

    def test_existing_root_is_byte_identical_and_no_actions_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/"summary.json").write_text("sentinel")
            before=hashes(root)
            with patch.object(runner,"preflight") as preflight, patch.object(runner,"build_client") as compiler:
                self.assertEqual(runner.run(self.args(root)),3)
            self.assertEqual(hashes(root),before)
            preflight.assert_not_called()
            compiler.assert_not_called()

    def test_preflight_nonzero_and_build_failure_write_owned_summary(self):
        import subprocess
        for where,error in (("preflight",subprocess.CalledProcessError(1,["git"])),
                            ("build_client",RuntimeError("controlled"))):
            with self.subTest(where=where),tempfile.TemporaryDirectory() as directory:
                args=self.args(Path(directory)/"run")
                with patch.object(runner,"preflight",return_value={}),patch.object(runner,"build_client"),patch.object(runner,where,side_effect=error):
                    self.assertEqual(runner.run(args),3)
                self.assertTrue((args.root/"summary.json").exists())

    def test_diagnostic_phase_case_and_run_agree(self):
        with tempfile.TemporaryDirectory() as directory:
            args=self.args(Path(directory)/"run",True)
            with patch.object(runner,"preflight",return_value={}),patch.object(runner,"build_client",side_effect=self.compiler),patch.object(runner,"execute_cases",side_effect=self.cases):
                self.assertEqual(runner.run(args),2)
            for path in args.root.rglob("*report.json"):
                value=json.loads(path.read_text())
                self.assertEqual(value["qualification_status"],"not_evaluated",str(path))
                self.assertEqual(value["diagnostic_status"],"passed",str(path))
            summary=json.loads((args.root/"summary.json").read_text())
            self.assertEqual(summary["workload"]["rounds"],1)


if __name__ == "__main__":
    unittest.main()
