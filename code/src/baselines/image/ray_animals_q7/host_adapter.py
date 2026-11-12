"""Reuse pinned Q1 host lifecycle helpers at each Q7 native-stage boundary."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path

from src.infrastructure.redact import redact_text


def load_verified(name, spec):
    path = Path(spec['path'])
    if hashlib.sha256(path.read_bytes()).hexdigest() != spec['sha256']:
        raise ValueError('Host lifecycle helper hash mismatch: ' + name)
    module_spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module


def load_host(name, spec):
    """Bind the maintained resource guard to the pinned lifecycle/cleanup helper.

    Outer controllers use this same loader for their pre/post boundaries. The
    verified helper file stays unchanged; snapshot receipts name the new policy.
    """
    from src.observability.process_resources import host_idle
    host = load_verified(name, spec)
    host.snapshot = host_idle.snapshot
    host.assert_idle = host_idle.assert_idle
    host.gpu_snapshot = host_idle.gpu_snapshot
    return host


class StageSupervisor:
    def __init__(self, host, api, sockets, semaphores, resources, monitor_factory, timeout_s):
        self.host, self.api = host, api
        self.sockets, self.semaphores = sockets, semaphores
        self.resources, self.monitor_factory, self.timeout_s = resources, monitor_factory, timeout_s

    def __call__(self, command, stage, root):
        host, api = self.host, self.api
        evidence = root / f'stage-{stage}-supervision'
        evidence.mkdir(mode=0o700)
        temp = evidence / 'private-temp'
        temp.mkdir(mode=0o700)
        before = host.snapshot(api, self.resources, f'q7-stage-{stage}-before')
        host.private_json(evidence / 'before.json', before)
        host.assert_idle(before, self.resources)
        monitor = self.monitor_factory(self.resources['shared_resources'])
        owned = set()
        report = None
        worker_failure = None
        receipt = dict(status='failed', worker_exit_zero=None, physical_idle=False,
                       resource_passed=None, owned_processes_exited=None,
                       private_sockets_clear=None, host_idle_guard='not_reached',
                       socket_cleanup_attempted=False, semaphore_cleanup_attempted=False,
                       evidence=str(evidence),
                       lifecycle_owner='project supervisor with pinned cleanup helpers and explicit host resource policy')
        keys = ('TMPDIR', 'VLLM_RPC_BASE_PATH', 'RAY_TMPDIR')
        previous = {key: os.environ.get(key) for key in keys}
        os.environ.update({key: str(temp) for key in keys})
        try:
            try:
                report = api.supervise(command, evidence / 'worker', timeout_s=self.timeout_s,
                                       grace_s=8, natural_exit_grace_s=4, monitor=monitor)
                owned |= host.report_identities(report)
                host.private_json(evidence / 'supervisor.json', report)
            except BaseException as error:
                worker_failure = error
                failed_report = getattr(error, 'report', None)
                if failed_report is not None:
                    report = failed_report
                    owned |= host.report_identities(report)
                    host.private_json(evidence / 'supervisor.json', report)
                raise
            finally:
                worker_exit = report['worker_exit'] if report is not None else None
                receipt['worker_exit_zero'] = worker_exit == 0 if worker_exit is not None else None
                if host.live_owned(api, owned):
                    host.cleanup_owned(api, owned, 20)
                live = host.live_owned(api, owned)
                receipt['owned_processes_exited'] = not live
                resources = monitor.finish()
                after = host.snapshot(api, self.resources, f'q7-stage-{stage}-after', owned)
                host.private_json(evidence / 'after.json', after)
                host.private_json(evidence / 'resources-before-cleanup.json', resources)
                receipt['resource_passed'] = resources['passed']
                # GPU/workload conflicts are rejected before unlink. Unclassified
                # Python separately prevents time-window-only SHM attribution.
                try:
                    host.assert_idle(after, self.resources)
                except BaseException:
                    receipt['host_idle_guard'] = 'rejected'
                    raise
                receipt['host_idle_guard'] = 'passed'
                if live or resources['failures']:
                    raise RuntimeError('Owned process or resource failure remains after native stage')
                receipt['socket_cleanup_attempted'] = True
                socket_receipt = self.sockets.cleanup_inactive_socket_nodes(temp, Path('/proc/net/unix').read_text())
                host.private_json(evidence / 'socket-cleanup.json', socket_receipt)
                receipt['semaphore_cleanup_attempted'] = True
                semaphore_receipt = self.semaphores.cleanup_inactive_semaphores(
                    resources['remaining_new_shm_names'], before, after)
                host.private_json(evidence / 'semaphore-cleanup.json', semaphore_receipt)
                resources = monitor.finish()
                host.private_json(evidence / 'resources-final.json', resources)
                sockets_clear = not any(path.is_socket() for path in temp.rglob('*'))
                receipt.update(physical_idle=not live and resources['passed'] and sockets_clear,
                               resource_passed=resources['passed'], private_sockets_clear=sockets_clear)
                required = ('worker_exit_zero', 'physical_idle', 'resource_passed',
                            'owned_processes_exited', 'private_sockets_clear')
                receipt['status'] = 'completed' if all(receipt[key] is True for key in required) else 'failed'
        except BaseException as error:
            failure = worker_failure or error
            receipt.update(status='failed', physical_idle=False,
                           failure=dict(type=type(failure).__name__, message=redact_text(str(failure))))
            if error is not failure:
                failure.add_note('stage finalization also failed: ' + redact_text(str(error)))
                receipt['finalization_failure'] = dict(type=type(error).__name__, message=redact_text(str(error)))
            # Preserve the rejected boundary without granting permission to unlink.
            try:
                resources = monitor.finish()
                receipt['resource_passed'] = resources['passed']
                host.private_json(evidence / 'resources-at-failure.json', resources)
            except BaseException as recording:
                failure.add_note('resource failure evidence also failed: ' + redact_text(str(recording)))
            try:
                host.private_json(evidence / 'EXIT.json', receipt)
            except BaseException as recording:
                failure.add_note('stage exit evidence also failed: ' + redact_text(str(recording)))
            if failure is not error:
                raise failure from error
            raise
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
        host.private_json(evidence / 'EXIT.json', receipt)
        return receipt


def from_bound(bound):
    spec = bound['host_adapter']
    host = load_host('q7_pinned_host', spec['host_helper'])
    sockets = load_verified('q7_pinned_sockets', spec['socket_helper'])
    from src.observability.process_resources import inactive_semaphores as semaphores
    resources_path = Path(spec['resource_config']['path'])
    if hashlib.sha256(resources_path.read_bytes()).hexdigest() != spec['resource_config']['sha256']:
        raise ValueError('Host resource configuration hash mismatch')
    from src.observability.process_resources.supervised import ResourceMonitor
    return StageSupervisor(host, host.production_api(Path(bound['source']) / 'code'), sockets,
                           semaphores, json.loads(resources_path.read_text()), ResourceMonitor,
                           spec['stage_timeout_s'])
