"""Reuse pinned Q1 host lifecycle helpers at each Q7 native-stage boundary."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path


def load_verified(name, spec):
    path = Path(spec['path'])
    if hashlib.sha256(path.read_bytes()).hexdigest() != spec['sha256']:
        raise ValueError('Host lifecycle helper hash mismatch: ' + name)
    module_spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module


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
                report = getattr(error, 'report', None)
                if report is not None:
                    owned |= host.report_identities(report)
                    host.private_json(evidence / 'supervisor.json', report)
                raise
            finally:
                if host.live_owned(api, owned):
                    host.cleanup_owned(api, owned, 20)
                live = host.live_owned(api, owned)
                resources = monitor.finish()
                after = host.snapshot(api, self.resources, f'q7-stage-{stage}-after', owned)
                host.private_json(evidence / 'after.json', after)
                host.private_json(evidence / 'resources-before-cleanup.json', resources)
                # assert_idle rejects unknown GPU/foreign ownership before any unlink.
                host.assert_idle(after, self.resources)
                if live or resources['failures']:
                    raise RuntimeError('Owned process or resource failure remains after native stage')
                socket_receipt = self.sockets.cleanup_inactive_socket_nodes(temp, Path('/proc/net/unix').read_text())
                host.private_json(evidence / 'socket-cleanup.json', socket_receipt)
                semaphore_receipt = self.semaphores.cleanup_inactive_semaphores(
                    resources['remaining_new_shm_names'], before, after)
                host.private_json(evidence / 'semaphore-cleanup.json', semaphore_receipt)
                resources = monitor.finish()
                host.private_json(evidence / 'resources-final.json', resources)
                sockets_clear = not any(path.is_socket() for path in temp.rglob('*'))
                receipt = dict(worker_exit_zero=report is not None and report['worker_exit'] == 0,
                               physical_idle=not live and resources['passed'] and sockets_clear,
                               resource_passed=resources['passed'], owned_processes_exited=not live,
                               private_sockets_clear=sockets_clear, evidence=str(evidence),
                               lifecycle_owner='project binding using unchanged pinned Q1 host helpers')
                host.private_json(evidence / 'EXIT.json', receipt)
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
        return receipt


def from_bound(bound):
    spec = bound['host_adapter']
    host = load_verified('q7_pinned_host', spec['host_helper'])
    sockets = load_verified('q7_pinned_sockets', spec['socket_helper'])
    from src.observability.process_resources import inactive_semaphores as semaphores
    resources_path = Path(spec['resource_config']['path'])
    if hashlib.sha256(resources_path.read_bytes()).hexdigest() != spec['resource_config']['sha256']:
        raise ValueError('Host resource configuration hash mismatch')
    from src.observability.process_resources.supervised import ResourceMonitor
    return StageSupervisor(host, host.production_api(Path(bound['source']) / 'code'), sockets,
                           semaphores, json.loads(resources_path.read_text()), ResourceMonitor,
                           spec['stage_timeout_s'])
