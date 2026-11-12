"""No GPU commands: explicit process inventories and actual local helper binding."""
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.baselines.image.ray_animals_q7.host_adapter import load_host, StageSupervisor
from src.observability.process_resources import host_idle
from src.observability.process_resources.inactive_semaphores import cleanup_inactive_semaphores


class NoSuchProcess(Exception):
    pass


def process(pid=21, born=42., name='python', argv=None):
    return SimpleNamespace(pid=pid, info=dict(pid=pid, create_time=born, name=name,
        cmdline=argv or ['python', '-B', '-c', 'print("status only")'], status='sleeping'))


class HostIdleTests(unittest.TestCase):
    def setUp(self):
        self.cfg = dict(gpu_idle_max_mib=1, allowed_idle_processes=[])
        self.gpu = dict(compute_inventory_complete=True, compute=[], used_mib=1)
        self.rows = []
        own = SimpleNamespace(pid=os.getpid(), create_time=lambda: 3., parents=lambda: [])
        self.psutil = SimpleNamespace(Process=lambda _: own,
            process_iter=lambda _: self.rows, NoSuchProcess=NoSuchProcess,
            STATUS_ZOMBIE='zombie', STATUS_DEAD='dead')
        self.api = SimpleNamespace(psutil=self.psutil)

    def snapshot(self, owned=()):
        with patch.object(host_idle, 'gpu_snapshot', return_value=deepcopy(self.gpu)):
            return host_idle.snapshot(self.api, self.cfg, 'CPU-fixture', owned)

    def test_plain_python_is_observed_without_claiming_resource_conflict(self):
        for argv in (['python', '-B', 'status.py'], ['python', '-c', 'inspect_only()'],
                     ['python', 'notes.py', '--log', 'vllm/native_server.py'],
                     ['python', '-m', 'json.tool', 'qualification_controller.py']):
            with self.subTest(argv=argv):
                self.rows = [process(argv=argv)]
                result = self.snapshot()
                host_idle.assert_idle(result, self.cfg)
                self.assertEqual(result['foreign_work'], [])
                self.assertEqual(len(result['unclassified_python']), 1)
                self.assertEqual(result['unclassified_python'][0]['cmdline_sha256'],
                    hashlib.sha256(' '.join(argv).encode()).hexdigest())
                self.assertIn('CPU isolation unproven', result['scope'])

    def test_native_workload_entrypoints_remain_conflicts(self):
        for name, argv in [('raylet', ['raylet']), ('ray::Worker', ['python']),
                           ('python', ['python', '-m', 'vllm']),
                           ('python', ['python', '-B', '-m', 'vllm.entrypoints.openai.api_server']),
                           ('python', ['python', '-X', 'dev', '-m', 'src.experiments.native_imlane_filters']),
                           ('python', ['python', '/code/run_kalypso_sembench_query.py'])]:
            with self.subTest(name=name, argv=argv):
                self.rows = [process(name=name, argv=argv)]
                result = self.snapshot()
                self.assertEqual(len(result['foreign_work']), 1)
                with self.assertRaisesRegex(RuntimeError, 'native workload'):
                    host_idle.assert_idle(result, self.cfg)

    def test_gpu_use_is_rejected_even_for_unclassified_python(self):
        self.rows = [process()]
        for change in ({'compute': [dict(pid=21)]}, {'compute_inventory_complete': False}, {'used_mib': 2}):
            with self.subTest(change=change):
                self.gpu.update(compute=[], compute_inventory_complete=True, used_mib=1)
                self.gpu.update(change)
                with self.assertRaisesRegex(RuntimeError, 'GPU'):
                    host_idle.assert_idle(self.snapshot(), self.cfg)

    def test_missing_or_changed_allowed_service_is_not_implicitly_permitted(self):
        allowed = dict(pid=21, create_time=42., cmdline_sha256=hashlib.sha256(b'python status.py').hexdigest())
        self.cfg['allowed_idle_processes'] = [allowed]
        for rows in ([], [process(argv=['python', 'other.py'])], [process(born=43., argv=['python', 'status.py'])]):
            with self.subTest(rows=rows):
                self.rows = rows
                with self.assertRaisesRegex(RuntimeError, 'identity'):
                    host_idle.assert_idle(self.snapshot(), self.cfg)
        self.rows = [process(argv=['python', 'status.py'])]
        result = self.snapshot()
        host_idle.assert_idle(result, self.cfg)
        self.assertEqual(result['unclassified_python'], [])

    def test_only_exact_owned_birth_identity_is_excluded(self):
        self.rows = [process(name='raylet', argv=['raylet'])]
        host_idle.assert_idle(self.snapshot({(21, 42.)}), self.cfg)
        with self.assertRaisesRegex(RuntimeError, 'native workload'):
            host_idle.assert_idle(self.snapshot({(21, 41.)}), self.cfg)

    def test_incomplete_inventory_and_historical_snapshots_cannot_pass(self):
        self.rows = [process()]
        self.rows[0].info['create_time'] = float('nan')
        result = self.snapshot()
        self.assertFalse(result['inventory_complete'])
        with self.assertRaisesRegex(RuntimeError, 'inventory'):
            host_idle.assert_idle(result, self.cfg)
        with self.assertRaisesRegex(RuntimeError, 'historical'):
            host_idle.assert_idle(dict(inventory_complete=True, foreign_work=[]), self.cfg)
        self.rows = [process()]
        self.rows[0].info['status'] = None
        with self.assertRaisesRegex(RuntimeError, 'inventory'):
            host_idle.assert_idle(self.snapshot(), self.cfg)

    def test_dead_python_and_dead_native_are_not_active_conflicts(self):
        self.rows = [process(), process(pid=22, name='raylet', argv=['raylet'])]
        for row in self.rows:
            row.info['status'] = 'zombie'
        result = self.snapshot()
        host_idle.assert_idle(result, self.cfg)
        self.assertEqual(result['unclassified_python'], [])
        self.assertEqual(len(result['processes']), 2)

    def test_original_gpu_hardware_and_inventory_validation_remain(self):
        cfg = dict(gpu_expected_name='fixture GPU', gpu_expected_total_mib=10)
        for gpu_text, compute_text, expected in [
            ('0,fixture GPU,uuid,10,1\n', '', None),
            ('0,other GPU,uuid,10,1\n', '', 'hardware'),
            ('0,fixture GPU,uuid,10,1\n', 'bad\n', 'unsupported'),
        ]:
            with self.subTest(gpu_text=gpu_text, compute_text=compute_text):
                with patch.object(host_idle.subprocess, 'run', side_effect=[
                    SimpleNamespace(stdout=gpu_text), SimpleNamespace(stdout=compute_text)]):
                    if expected:
                        with self.assertRaisesRegex(RuntimeError, expected):
                            host_idle.gpu_snapshot(cfg)
                    else:
                        self.assertEqual(host_idle.gpu_snapshot(cfg)['compute'], [])

    def test_explicit_loader_changes_policy_without_modifying_frozen_helper(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / 'host.py'
            body = b'def assert_idle(*args): raise RuntimeError("old policy")\ndef cleanup_owned(*args): return "unchanged"\n'
            path.write_bytes(body)
            spec = dict(path=str(path), sha256=hashlib.sha256(body).hexdigest())
            host = load_host('host_fixture', spec)
            self.assertIs(host.assert_idle, host_idle.assert_idle)
            self.assertIs(host.snapshot, host_idle.snapshot)
            self.assertEqual(host.cleanup_owned(), 'unchanged')
            self.assertEqual(path.read_bytes(), body)

    def test_unclassified_python_cannot_authorize_global_semaphore_unlink(self):
        before = dict(unclassified_python=[])
        after = dict(unclassified_python=[dict(pid=21, create_time=42.)])
        with patch('ctypes.CDLL') as libc:
            with self.assertRaisesRegex(RuntimeError, 'ownership is unconfirmed'):
                cleanup_inactive_semaphores(['sem.mp-fixture'], before, after)
            libc.assert_not_called()
            self.assertEqual(cleanup_inactive_semaphores([], before, after), [])

    def test_stage_completes_with_unrelated_python_and_no_remaining_resources(self):
        self.rows = [process()]
        with TemporaryDirectory() as tmp:
            saved = {}
            host = SimpleNamespace(snapshot=lambda *args: self.snapshot(),
                assert_idle=host_idle.assert_idle, private_json=lambda p, v: saved.update({p.name: deepcopy(v)}),
                report_identities=lambda _: set(), live_owned=lambda *_: [], cleanup_owned=Mock())
            report = dict(worker_exit=0)
            api = SimpleNamespace(supervise=lambda *a, **k: report)
            resources = dict(passed=True, failures=[], remaining_new_shm_names=[])
            monitor = SimpleNamespace(finish=lambda: resources)
            sockets = SimpleNamespace(cleanup_inactive_socket_nodes=Mock(return_value=[]))
            semaphores = SimpleNamespace(cleanup_inactive_semaphores=cleanup_inactive_semaphores)
            adapter = StageSupervisor(host, api, sockets, semaphores,
                dict(self.cfg, shared_resources={}), lambda _: monitor, 10)
            with patch.object(Path, 'read_text', return_value=''):
                result = adapter(['software-worker'], 0, Path(tmp))
            self.assertEqual(result['status'], 'completed')
            self.assertTrue(result['physical_idle'])
            self.assertEqual(len(saved['after.json']['unclassified_python']), 1)
            host.cleanup_owned.assert_not_called()

    def test_outer_and_stage_use_the_same_policy_before_lease_release(self):
        from src.baselines.image.ray_animals_q7 import host_adapter
        self.rows = [process()]
        saved = {}
        old_guard = Mock(side_effect=AssertionError('historical Python-only guard was used'))
        cleanup = Mock()
        self.api.supervise = Mock(return_value=dict(worker_exit=0))
        resources = dict(self.cfg, shared_resources={})

        def legacy_helper():
            return SimpleNamespace(assert_idle=old_guard, snapshot=old_guard, gpu_snapshot=old_guard,
                production_api=lambda _: self.api,
                private_json=lambda p, value: saved.update({str(p): deepcopy(value)}),
                report_identities=lambda _: set(), live_owned=lambda *_: [], cleanup_owned=cleanup)

        sockets = SimpleNamespace(cleanup_inactive_socket_nodes=Mock(return_value=[]))
        monitor = SimpleNamespace(finish=lambda: dict(passed=True, failures=[], remaining_new_shm_names=[]))
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / 'resources.json'
            config.write_text(json.dumps(resources))
            bound = dict(source=tmp, host_adapter=dict(host_helper={}, socket_helper={},
                resource_config=dict(path=str(config), sha256=hashlib.sha256(config.read_bytes()).hexdigest()),
                stage_timeout_s=10))
            load = lambda name, spec: sockets if name == 'q7_pinned_sockets' else legacy_helper()
            with patch.object(host_adapter, 'load_verified', side_effect=load), \
                    patch.object(host_idle, 'gpu_snapshot', return_value=self.gpu), \
                    patch('src.observability.process_resources.supervised.ResourceMonitor', return_value=monitor):
                outer = host_adapter.load_host('outer', {})
                before = outer.snapshot(self.api, resources, 'outer-before')
                outer.assert_idle(before, resources)
                stage = host_adapter.from_bound(bound)
                original_read = Path.read_text
                with patch.object(Path, 'read_text', lambda p, *a, **kw:
                    '' if str(p) == '/proc/net/unix' else original_read(p, *a, **kw)):
                    receipt = stage(['software-worker'], 0, root)
                after = outer.snapshot(self.api, resources, 'outer-after')
                outer.assert_idle(after, resources)
                lease = Mock()
                if receipt['physical_idle']:
                    lease.release()
                lease.release.assert_called_once()
            boundaries = [before, after, *(v for k, v in saved.items()
                if k.endswith(('/before.json', '/after.json')))]
            self.assertEqual(len(boundaries), 4)
            self.assertEqual({v['schema'] for v in boundaries}, {host_idle.SCHEMA})
            self.assertEqual(len({v['policy_sha256'] for v in boundaries}), 1)
            self.assertTrue(all(len(v['unclassified_python']) == 1 for v in boundaries))
            old_guard.assert_not_called()
            cleanup.assert_not_called()
