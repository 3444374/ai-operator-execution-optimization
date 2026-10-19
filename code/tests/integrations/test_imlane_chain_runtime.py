"""CPU-only owner boundaries; every author, GPU and ABI probe is a fixture.

The persistent CellBudgetLedger, private JSON, byte hashing, lease parser and
scope resource receipts are real. No child, socket, author or model is started.
"""

from contextlib import ExitStack, contextmanager
from copy import deepcopy
import hashlib
import io
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch


OPTIONAL_PROBE_DENIALS = []
_GUARDS = []


def setUpModule():
    class NoSocket(socket.socket):
        def __init__(self, *args, **kwargs):
            raise AssertionError("NoSocket: owner CPU tests must not create sockets")

    def no_subprocess(command, *args, **kwargs):
        if command == "lscpu":
            OPTIONAL_PROBE_DENIALS.append("NumPy optional lscpu denied; no child launched")
            raise OSError("optional CPU probe unavailable in NoPopen tests")
        raise AssertionError("NoPopen: owner CPU tests must not run subprocesses")

    for guard in (
        patch.dict(os.environ, HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1",
                   LITELLM_LOCAL_MODEL_COST_MAP="True", CUDA_VISIBLE_DEVICES="-1"),
        patch("subprocess.Popen", side_effect=AssertionError("NoPopen: no author/model child")),
        patch("subprocess.run", side_effect=no_subprocess),
        patch("socket.socket", NoSocket),
        patch("socket.socketpair", side_effect=AssertionError("NoSocket: no socketpair")),
        patch("socket.create_connection", side_effect=AssertionError("NoSocket: no connection")),
    ):
        guard.start()
        _GUARDS.append(guard)


def tearDownModule():
    for guard in reversed(_GUARDS):
        guard.stop()


class PrivateFixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from integrations.imlane_records import chain_runtime, method_environment, owner, verify_filter
        from src.experiments import lotus_records_chain
        from src.experiments.attempt_ledger import AttemptBudget, BudgetError, BudgetExhausted
        from src.experiments.cell_budget import CellBudgetLedger
        from src.infrastructure import runner_lease

        cls.runtime, cls.methods, cls.owner = chain_runtime, method_environment, owner
        cls.verify_filter, cls.runner, cls.leases = verify_filter, lotus_records_chain, runner_lease
        cls.AttemptBudget, cls.BudgetError, cls.BudgetExhausted = AttemptBudget, BudgetError, BudgetExhausted
        cls.Ledger = CellBudgetLedger

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="imlane-owner-cpu-", dir="/tmp")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.serial = 0

    def write(self, name, value):
        from src.baselines.common.private_artifacts import write_private_json

        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        write_private_json(path, value)
        return path

    def owner_value(self):
        return dict(
            schema="semloom.imlane_owner.v1",
            **{name: str(self.root / name) for name in (
                "author_root", "method_python", "bridge_binary", "artifact_root", "preflight_report")},
            model_environment={"CUDA_VISIBLE_DEVICES": "fixture-device"},
            owner_resource_limits=dict(cpu_cores=2, rss_bytes=1024**3, shm_bytes=1024**2),
            lease_sha256="a" * 64,
            budget=dict(path=str(self.root / "budget.sqlite"), id="fixture.chain", limit=8, unit_id="query"),
            gpu_idle_max_mib=5,
        )

    def read_owner(self, value):
        self.serial += 1
        return self.runtime.read_owner(self.write("owner-" + str(self.serial) + ".json", value))


class OwnerInputTests(PrivateFixtureTests):
    def test_explicit_owner_roundtrips_without_probing_external_resources(self):
        value = self.owner_value()
        with patch.object(self.runtime, "check_external_lease") as lease:
            self.assertEqual(self.read_owner(value), value)
        lease.assert_not_called()

    def test_owner_top_level_schema_and_exact_fields_are_required(self):
        values = [None, [], dict(self.owner_value(), schema="old"), dict(self.owner_value(), typo=True)]
        for key in self.owner_value():
            value = self.owner_value()
            del value[key]
            values.append(value)
        for value in values:
            with self.subTest(value=value), self.assertRaises((ValueError, TypeError)):
                self.read_owner(value)

    def test_owner_absolute_private_paths_are_required(self):
        for key in ("author_root", "method_python", "bridge_binary", "artifact_root", "preflight_report"):
            for bad in (None, True, "relative/path", str(Path(__file__).resolve())):
                value = self.owner_value()
                value[key] = bad
                with self.subTest(field=key, bad=bad), self.assertRaises((ValueError, TypeError)):
                    self.read_owner(value)

    def test_owner_limits_must_be_explicit_and_strictly_positive(self):
        invalid = [None, {}, dict(cpu_cores=2, rss_bytes=3, shm_bytes=4, extra=1)]
        for key in ("cpu_cores", "rss_bytes", "shm_bytes"):
            for bad in (None, True, 0, -1, 2.0):
                invalid.append(dict(self.owner_value()["owner_resource_limits"], **{key: bad}))
        for limits in invalid:
            value = dict(self.owner_value(), owner_resource_limits=limits)
            with self.subTest(limits=limits), self.assertRaises((ValueError, TypeError)):
                self.read_owner(value)

    def test_owner_budget_identity_unit_and_path_are_explicit(self):
        invalid = [None, {}, dict(self.owner_value()["budget"], extra=True)]
        for key, bads in (
            ("id", (None, True, "", "bad identity")),
            ("limit", (None, True, 0, -1, 2.0)),
            ("unit_id", (None, True, "", " ", "invalid/unit", "a" * 129)),
            ("path", (None, True, "relative.sqlite", str(Path(__file__).resolve()))),
        ):
            for bad in bads:
                invalid.append(dict(self.owner_value()["budget"], **{key: bad}))
        for budget in invalid:
            with self.subTest(budget=budget), self.assertRaises((ValueError, TypeError)):
                self.read_owner(dict(self.owner_value(), budget=budget))

    def test_owner_environment_lease_hash_and_gpu_allowance_reject_invalid_values(self):
        invalid = []
        for environment in (None, {}, {"CUDA_VISIBLE_DEVICES": ""}, {"CUDA_VISIBLE_DEVICES": "0,1"},
                            {"CUDA_VISIBLE_DEVICES": "0", "EXTRA": "x"}, {"CUDA_VISIBLE_DEVICES": True}):
            invalid.append(dict(self.owner_value(), model_environment=environment))
        for digest in (None, True, "", "a" * 63, "G" * 64):
            invalid.append(dict(self.owner_value(), lease_sha256=digest))
        for allowance in (None, True, -1, 1.5):
            invalid.append(dict(self.owner_value(), gpu_idle_max_mib=allowance))
        for value in invalid:
            with self.subTest(value=value), self.assertRaises((ValueError, TypeError)):
                self.read_owner(value)

    def test_source_and_owner_identity_are_checked_on_the_bytes_actually_parsed(self):
        source = dict(schema="semloom.record_relation.v1", records=[], columns=["body"],
                      source_index=dict(dtype="int64", name=None),
                      empty_prototype=[dict(name="body", dtype="object")])
        for kind, value, reader in (("source", source, self.runner.read_relation),
                                    ("owner", self.owner_value(), self.runtime.read_owner)):
            with self.subTest(kind=kind):
                path = self.write(kind + ".json", value)
                original = path.read_bytes()
                digest = hashlib.sha256(original).hexdigest()
                self.assertIsNotNone(reader(path, expected_sha256=digest))
                # A fresh pathname digest cannot authorize different opened bytes.
                opened = original + b" "
                original_open = Path.open

                def changed_open(target, *args, **kwargs):
                    if target == path:
                        return io.BytesIO(opened)
                    return original_open(target, *args, **kwargs)

                with patch.object(Path, "open", changed_open), \
                     patch.object(self.runtime, "digest", return_value=digest), \
                     self.assertRaisesRegex(ValueError, "context changed"):
                    reader(path, expected_sha256=digest)

    def test_json_rejects_nonfinite_symlink_checkout_and_oversized_inputs(self):
        for literal in ("NaN", "Infinity", "-Infinity"):
            path = self.root / (literal + ".json")
            path.write_text('{"bad":' + literal + '}')
            with self.subTest(literal=literal), self.assertRaisesRegex(ValueError, "nonfinite"):
                self.runtime.read_json(path)
        target = self.write("regular.json", {})
        alias = self.root / "alias.json"
        alias.symlink_to(target)
        with self.assertRaises(ValueError):
            self.runtime.read_json(alias)
        with self.assertRaisesRegex(ValueError, "outside a Git checkout"):
            self.runtime.read_json(Path(__file__))
        huge = self.root / "huge.json"
        with huge.open("wb") as stream:
            stream.truncate(8 * 1024**2 + 1)
        with self.assertRaises(ValueError):
            self.runtime.read_json(huge)


class ExternalLeaseTests(PrivateFixtureTests):
    def setUp(self):
        super().setUp()
        self.value = self.owner_value()
        self.record = dict(hostname="fixture-host", pid=711, process_start_id="birth-711",
                           owner_token="fixture-owner", config_fingerprint=self.leases.HOST_SCOPE_FINGERPRINT,
                           repository_commit="fixture-source", started_epoch_s=1)
        self.path = Path(self.value["artifact_root"]) / self.leases.HOST_SCOPE_DIR_NAME / self.leases.LEASE_NAME
        self.write(str(self.path.relative_to(self.root)), self.record)
        self.value["lease_sha256"] = self.runtime.digest(self.path)

    @contextmanager
    def observations(self, *, birth="birth-711", parents=(711,), preserve_bytes=True):
        before = self.path.read_bytes()
        with ExitStack() as stack:
            stack.enter_context(patch.object(self.runtime.socket, "gethostname", return_value="fixture-host"))
            stack.enter_context(patch.object(self.leases, "_process_start_id", return_value=birth))
            process = stack.enter_context(patch("psutil.Process"))
            process.return_value.parents.return_value = [SimpleNamespace(pid=pid) for pid in parents]
            for name in ("acquire_host_runner_lease", "acquire_runner_lease"):
                stack.enter_context(patch.object(self.leases, name, side_effect=AssertionError("no lease recovery")))
            stack.enter_context(patch.object(self.leases.RunnerLease, "release",
                                             side_effect=AssertionError("no external lease release")))
            stack.enter_context(patch.object(Path, "unlink", side_effect=AssertionError("no lease unlink")))
            yield
        if preserve_bytes:
            self.assertEqual(self.path.read_bytes(), before)

    def test_normal_ancestor_lease_is_checked_without_acquisition_recovery_or_release(self):
        with self.observations():
            result = self.runtime.check_external_lease(self.value)
        self.assertEqual(result, dict(sha256=self.value["lease_sha256"], ownership="external owner",
                                      released_by_worker=False))

    def test_current_process_can_be_the_external_lease_owner(self):
        self.record["pid"] = os.getpid()
        self.path.write_text(json.dumps(self.record))
        self.value["lease_sha256"] = self.runtime.digest(self.path)
        with self.observations(parents=()):
            self.assertFalse(self.runtime.check_external_lease(self.value)["released_by_worker"])

    def test_changed_lease_hash_is_rejected_without_removing_external_lease(self):
        self.value["lease_sha256"] = "0" * 64
        with self.observations(), self.assertRaisesRegex(RuntimeError, "lease changed"):
            self.runtime.check_external_lease(self.value)

    def test_wrong_fingerprint_host_missing_birth_and_reused_pid_are_rejected(self):
        for key, bad in (("config_fingerprint", "different-scope"), ("hostname", "different-host"),
                         ("process_start_id", ""), ("process_start_id", "old-birth")):
            record = dict(self.record, **{key: bad})
            self.path.write_text(json.dumps(record))
            self.value["lease_sha256"] = self.runtime.digest(self.path)
            with self.subTest(field=key, bad=bad), self.observations(), \
                 self.assertRaisesRegex(RuntimeError, "owner ancestry"):
                self.runtime.check_external_lease(self.value)

    def test_live_nonancestor_owner_is_rejected(self):
        with self.observations(parents=()), self.assertRaisesRegex(RuntimeError, "owner ancestry"):
            self.runtime.check_external_lease(self.value)

    def test_replaced_lease_bytes_cannot_reuse_the_previously_checked_hash(self):
        # The bound bytes describe a forbidden scope. A later, valid record
        # cannot borrow their SHA after the independent pathname hash succeeds.
        bound = dict(self.record, config_fingerprint="not-normal-host-scope")
        self.path.write_text(json.dumps(bound))
        self.value["lease_sha256"] = self.runtime.digest(self.path)
        original_read = self.leases._read_lease

        def changed_read(path):
            path.write_text(json.dumps(self.record))
            return original_read(path)

        with self.observations(preserve_bytes=False), patch.object(self.leases, "_read_lease", side_effect=changed_read), \
             self.assertRaisesRegex(RuntimeError, "lease changed|owner ancestry"):
            self.runtime.check_external_lease(self.value)

    def test_malformed_or_symlink_external_lease_is_never_recovered(self):
        self.path.write_text(json.dumps(dict(hostname="fixture-host")))
        self.value["lease_sha256"] = self.runtime.digest(self.path)
        with self.observations(), self.assertRaisesRegex(RuntimeError, "invalid schema"):
            self.runtime.check_external_lease(self.value)
        target = self.write("other-lease.json", self.record)
        self.path.unlink()
        self.path.symlink_to(target)
        self.value["lease_sha256"] = self.runtime.digest(target)
        with self.observations(), self.assertRaisesRegex(RuntimeError, "lease changed"):
            self.runtime.check_external_lease(self.value)
        self.assertTrue(self.path.is_symlink())


class AuthorPhaseGuardTests(PrivateFixtureTests):
    def setUp(self):
        super().setUp()
        self.value = self.owner_value()
        self.args = SimpleNamespace(scopes=[], stop_budget=self.owner.StopBudget(threading.Event()))
        self.idle = dict(uuid="fixture-gpu", memory_mib=5, compute_processes=[])
        lease = patch.object(self.runtime, "check_external_lease", return_value={"fixture_read_only": True})
        self.lease = lease.start()
        self.addCleanup(lease.stop)

    def scope(self, namespace, *, closed=True, confirmed=True):
        receipt = dict(namespace=namespace, owned_survivors=[], remaining_shm={}, owned_threads_stopped=True)
        if not confirmed:
            receipt["owned_threads_stopped"] = False
        return SimpleNamespace(namespace=namespace, closed=closed, report=dict(cleanup=[receipt]),
                               unconfirmed_roots={})

    def test_two_closed_namespaces_each_gate_once_and_finish_observes_again(self):
        self.assertIs(self.runtime.gpu_exited, self.verify_filter.gpu_exited)
        self.assertIs(self.runtime.gpu_snapshot, self.verify_filter.gpu_snapshot)
        first, second = self.scope("fixture-phase-a", closed=False), self.scope("fixture-phase-b", closed=False)
        self.args.scopes.append(first)
        with patch.object(self.runtime, "gpu_snapshot", side_effect=lambda _: deepcopy(self.idle)) as snapshot:
            guard = self.runtime.AuthorRunGuard(self.value, self.args)
            self.assertFalse(guard())
            self.assertEqual(snapshot.call_count, 1)
            first.closed = True
            self.assertFalse(guard())
            self.assertFalse(guard())
            self.assertEqual(snapshot.call_count, 2)
            self.args.scopes.append(second)
            self.assertFalse(guard())
            second.closed = True
            self.assertFalse(guard())
            self.assertFalse(guard())
            self.assertEqual(snapshot.call_count, 3)
            result = guard.finish()
        self.assertEqual(snapshot.call_count, 4)
        self.assertEqual([row["namespace"] for row in result["phase_gpu_exit"]],
                         [first.namespace, second.namespace])
        self.assertTrue(all(row["gpu_exit_confirmed"] for row in result["phase_gpu_exit"]))
        self.assertTrue(result["gpu_exit_confirmed"])
        self.assertFalse(result["external_host_lease_released_by_worker"])
        self.assertEqual({call.args for call in snapshot.call_args_list}, {("fixture-device",)})

    def test_busy_initial_gpu_is_rejected_before_any_author_phase(self):
        for snapshot in (dict(self.idle, compute_processes=[["fixture-pid", "fixture-gpu", "1"]]),
                         dict(self.idle, memory_mib=6)):
            with self.subTest(snapshot=snapshot), patch.object(self.runtime, "gpu_snapshot", return_value=snapshot), \
                 self.assertRaisesRegex(RuntimeError, "not idle"):
                self.runtime.AuthorRunGuard(self.value, self.args)

    def test_gpu_process_memory_or_device_mismatch_blocks_successor(self):
        for snapshot in (dict(self.idle, compute_processes=[["fixture-pid", "fixture-gpu", "1"]]),
                         dict(self.idle, memory_mib=6), dict(self.idle, uuid="different-device")):
            successor = Mock()
            self.args.scopes = [self.scope("fixture-closed")]
            with self.subTest(snapshot=snapshot), \
                 patch.object(self.runtime, "gpu_snapshot", side_effect=[self.idle, snapshot]):
                guard = self.runtime.AuthorRunGuard(self.value, self.args)
                with self.assertRaisesRegex(RuntimeError, "next author phase"):
                    guard()
                    successor()
                self.assertNotIn("fixture-closed", guard.seen)
                self.assertFalse(guard.phase_gpu_exit[-1]["gpu_exit_confirmed"])
            successor.assert_not_called()

    def test_unknown_author_resources_block_successor_before_gpu_probe(self):
        self.args.scopes = [self.scope("fixture-unknown", confirmed=False)]
        successor = Mock()
        with patch.object(self.runtime, "gpu_snapshot", return_value=self.idle) as snapshot:
            guard = self.runtime.AuthorRunGuard(self.value, self.args)
            with self.assertRaisesRegex(RuntimeError, "resources remain unknown"):
                guard()
                successor()
            self.assertEqual(snapshot.call_count, 1)
        successor.assert_not_called()

    def test_stop_budget_blocks_successor_and_final_gpu_probe_still_runs(self):
        self.args.scopes = [self.scope("fixture-stopped")]
        successor = Mock()
        with patch.object(self.runtime, "gpu_snapshot", return_value=self.idle) as snapshot:
            guard = self.runtime.AuthorRunGuard(self.value, self.args)
            self.args.stop_budget.stopped.set()
            with self.assertRaisesRegex(TimeoutError, "work has stopped"):
                guard()
                successor()
            self.assertEqual(snapshot.call_count, 1)
            self.assertTrue(guard.finish()["gpu_exit_confirmed"])
            self.assertEqual(snapshot.call_count, 2)
        successor.assert_not_called()

    def test_unreadable_gpu_observation_blocks_successor(self):
        self.args.scopes = [self.scope("fixture-closed")]
        with patch.object(self.runtime, "gpu_snapshot", side_effect=[self.idle, OSError("fixture observation unavailable")]):
            guard = self.runtime.AuthorRunGuard(self.value, self.args)
            with self.assertRaises(OSError):
                guard()
        self.assertEqual(guard.seen, set())

    def test_final_gpu_is_observed_even_when_the_external_lease_has_changed(self):
        with patch.object(self.runtime, "gpu_snapshot", return_value=self.idle) as snapshot:
            guard = self.runtime.AuthorRunGuard(self.value, self.args)
            self.lease.side_effect = RuntimeError("declared changed fixture lease")
            result = guard.finish()
        self.assertEqual(snapshot.call_count, 2)
        self.assertTrue(result["gpu_exit_confirmed"])
        self.assertFalse(result["external_lease_valid"])
        self.assertEqual(result["lease_error_type"], "RuntimeError")
        self.assertFalse(result["external_host_lease_released_by_worker"])


class OwnerPreflightTests(PrivateFixtureTests):
    def setUp(self):
        super().setUp()
        self.value = self.owner_value()
        path = self.write("preflight.json", dict(status="ok", fixture=True))
        self.value["preflight_report"] = str(path)
        self.engine = SimpleNamespace(validate_driver_environment=Mock(return_value={"fixture": "engine"}))
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.lease = self.stack.enter_context(patch.object(self.runtime, "check_external_lease", return_value={"fixture": "lease"}))
        self.affinity = self.stack.enter_context(patch.object(os, "sched_getaffinity", create=True, return_value={0, 1}))
        self.build = self.stack.enter_context(patch.object(self.runtime, "validate_author_build", return_value={"fixture": "build"}))
        self.abi = self.stack.enter_context(patch.object(self.runtime, "preflight_method", return_value=[{"fixture": "abi"}]))

    def test_owner_preflight_checks_build_chain_abi_driver_and_cpu_allowance(self):
        result = self.runtime.preflight_owner(self.value, self.engine)
        self.build.assert_called_once_with(self.value["author_root"], self.value["bridge_binary"])
        self.abi.assert_called_once_with(self.value["method_python"], self.value["author_root"],
                                         Path(self.runtime.__file__).parents[2], chain=True)
        self.engine.validate_driver_environment.assert_called_once_with()
        self.affinity.assert_called_once_with(0)
        self.assertEqual(self.lease.call_count, 2)
        self.assertEqual(result["owner_resource_limits"], self.value["owner_resource_limits"])

    def test_unsuccessful_runtime_preflight_blocks_build_and_abi_probes(self):
        for status in ("failed", "pending", None):
            Path(self.value["preflight_report"]).write_text(json.dumps(dict(status=status)))
            with self.subTest(status=status), self.assertRaisesRegex(ValueError, "successful current runtime"):
                self.runtime.preflight_owner(self.value, self.engine)
        self.build.assert_not_called()
        self.abi.assert_not_called()
        self.engine.validate_driver_environment.assert_not_called()

    def test_wider_cpu_affinity_blocks_build_abi_and_driver(self):
        self.affinity.return_value = {0, 1, 2}
        with self.assertRaisesRegex(ValueError, "declared CPU allowance"):
            self.runtime.preflight_owner(self.value, self.engine)
        self.build.assert_not_called()
        self.abi.assert_not_called()
        self.engine.validate_driver_environment.assert_not_called()

    def test_unavailable_cpu_affinity_source_is_rejected(self):
        with patch.dict(os.__dict__):
            del os.__dict__["sched_getaffinity"]
            with self.assertRaisesRegex(ValueError, "declared CPU allowance"):
                self.runtime.preflight_owner(self.value, self.engine)
        self.build.assert_not_called()

    def test_build_or_chain_abi_failure_prevents_later_driver_validation(self):
        for failing in (self.build, self.abi):
            for mock in (self.build, self.abi, self.engine.validate_driver_environment):
                mock.reset_mock()
                mock.side_effect = None
            failing.side_effect = ValueError("declared fixture build or ABI rejection")
            with self.subTest(failing=failing), self.assertRaisesRegex(ValueError, "build or ABI rejection"):
                self.runtime.preflight_owner(self.value, self.engine)
            self.engine.validate_driver_environment.assert_not_called()
            if failing is self.build:
                self.abi.assert_not_called()


class AuthorBuildAndABITests(PrivateFixtureTests):
    def build_fixture(self):
        author = self.root / "fixture-author"
        original, patched = author / "source/original.cpp", author / "source/patched.cpp"
        author_binary = author / "prefix-patched/bin/fixture-author"
        bridge = self.root / "fixture-bridge"
        for path, payload in ((original, b"fixture original"), (patched, b"fixture patched"),
                              (author_binary, b"fixture author binary"), (bridge, b"fixture bridge binary")):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
        identity = dict(upstream_commit=self.owner.AUTHOR, mode="patched",
                        original_sha256={"original.cpp": self.owner.digest(original), "patched.cpp": "0" * 64},
                        patched_sha256={"patched.cpp": self.owner.digest(patched)},
                        binaries_sha256={"bin/fixture-author": self.owner.digest(author_binary)})
        author_metadata = self.write(str((author / "artifacts/build-patched/build-identity.json").relative_to(self.root)), identity)
        bridge_identity = dict(upstream_commit=self.owner.AUTHOR, binary_sha256=self.owner.digest(bridge),
                               sources_sha256={name: self.owner.digest(Path(self.methods.__file__).parent / name)
                                               for name in ("imlane_records.cpp", "record_dbend_adapter.hpp")})
        bridge_metadata = self.write(str(bridge.with_suffix(".build.json").relative_to(self.root)), bridge_identity)
        return SimpleNamespace(author=author, bridge=bridge, original=original, patched=patched,
                               author_binary=author_binary, author_metadata=author_metadata,
                               bridge_metadata=bridge_metadata, identity=identity, bridge_identity=bridge_identity)

    def test_extracted_build_check_accepts_bound_bytes_and_remains_the_legacy_filter_check(self):
        fixture = self.build_fixture()
        self.assertIs(self.verify_filter.validate_author_build, self.methods.validate_author_build)
        result = self.methods.validate_author_build(fixture.author, fixture.bridge)
        self.assertEqual(result, dict(author_commit=self.owner.AUTHOR,
                                      bridge_binary_sha256=self.owner.digest(fixture.bridge)))

    def test_changed_original_patched_author_binary_and_bridge_bytes_are_rejected(self):
        fixture = self.build_fixture()
        for name in ("original", "patched", "author_binary", "bridge"):
            path = getattr(fixture, name)
            before = path.read_bytes()
            path.write_bytes(before + b" changed")
            with self.subTest(file=name), self.assertRaisesRegex(ValueError, "changed|reviewed v2 source bytes"):
                self.methods.validate_author_build(fixture.author, fixture.bridge)
            path.write_bytes(before)

    def test_wrong_author_commit_mode_bridge_commit_binary_or_source_hash_are_rejected(self):
        fixture = self.build_fixture()
        invalid = [
            (fixture.author_metadata, dict(fixture.identity, upstream_commit="different-author")),
            (fixture.author_metadata, dict(fixture.identity, mode="original")),
            (fixture.bridge_metadata, dict(fixture.bridge_identity, upstream_commit="different-author")),
            (fixture.bridge_metadata, dict(fixture.bridge_identity, binary_sha256="0" * 64)),
            (fixture.bridge_metadata, dict(fixture.bridge_identity, sources_sha256={})),
        ]
        for path, value in invalid:
            before = path.read_bytes()
            path.write_text(json.dumps(value))
            with self.subTest(metadata=path.name, keys=list(value)), self.assertRaises(ValueError):
                self.methods.validate_author_build(fixture.author, fixture.bridge)
            path.write_bytes(before)

    def test_method_probe_routes_chain_or_filter_without_running_a_child(self):
        probe = dict(base=str(self.root / "fixture-python-base"), version=[3, 12], arrow="16.1.0")
        for chain, module, function in ((True, "record_chain", "validate_chain_source"),
                                        (False, "engine", "validate_filter_source")):
            with self.subTest(chain=chain), \
                 patch.object(self.methods.subprocess, "check_output", return_value=json.dumps(probe)) as check, \
                 patch.object(self.methods.subprocess, "run") as run:
                self.assertEqual(self.methods.preflight_method(self.root / "method-python", self.root / "author",
                                                               self.root / "code", chain=chain), [probe, probe])
            self.assertEqual([call.args[0][0] for call in check.call_args_list],
                             [str(self.root / "author/venv/bin/python"), str(self.root / "method-python")])
            self.assertTrue(all(call.args[0][1:4] == ["-I", "-B", "-c"] for call in check.call_args_list))
            command = run.call_args.args[0]
            self.assertIn("lotus." + module + " import " + function, command[4])
            self.assertEqual(command[-1], str(self.root / "code"))
            self.assertEqual(run.call_args.kwargs, dict(check=True, capture_output=True, timeout=15))

    def test_method_arrow_python_and_base_abi_mismatches_block_source_probe(self):
        good = dict(base=str(self.root / "fixture-python-base"), version=[3, 12], arrow="16.1.0")
        for changed in (dict(good, arrow="different"), dict(good, version=[3, 11]),
                        dict(good, base=str(self.root / "different-base"))):
            with self.subTest(changed=changed), \
                 patch.object(self.methods.subprocess, "check_output", side_effect=[json.dumps(good), json.dumps(changed)]), \
                 patch.object(self.methods.subprocess, "run") as run, \
                 self.assertRaisesRegex(ValueError, "Python and Arrow ABI"):
                self.methods.preflight_method(self.root / "method-python", self.root / "author", self.root / "code", chain=True)
            run.assert_not_called()


class PermanentWorkerBudgetTests(PrivateFixtureTests):
    def setUp(self):
        super().setUp()
        self.value = self.owner_value()
        self.budget = self.AttemptBudget(self.value["budget"]["id"], self.value["budget"]["limit"])
        self.path = Path(self.value["budget"]["path"])
        self.deadline = time.time() + 3600
        self.ledger = self.Ledger.create(self.path, self.budget, deadline_utc=self.deadline)
        self.ledger.reserve_unit("query", 4)
        self.config = dict(backend="imlane-author-local-model-chain")
        self.relation = SimpleNamespace(records=(object(), object()))
        self.chain = SimpleNamespace(query_id="fixture-worker-query", stages=(object(), object()))
        self.engine, self.resources = object(), object()
        self.events = []
        self.guards = []
        self.factory = Mock(side_effect=lambda **_: self.events.append("backend_factory"))
        self.outcome = dict(schema="fixture.query.v1", status="completed", query_id=self.chain.query_id,
                            calls=[dict(call_id="fixture-known", status="succeeded", prompt_tokens=5,
                                        completion_tokens=2, total_tokens=7)])
        self.primary = None
        self.guard_error = None
        self.gpu_confirmed = True
        self.lease_valid = True
        self.saved_snapshot = self.Ledger.snapshot
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.preflight = self.stack.enter_context(patch.object(self.runner, "preflight", side_effect=self.fixture_preflight))
        self.guard_factory = self.stack.enter_context(patch.object(self.runner, "AuthorRunGuard", side_effect=self.fixture_guard))
        self.query = self.stack.enter_context(patch.object(self.runner, "run_query", side_effect=self.fixture_query))
        self.stack.enter_context(patch.object(self.runner, "_make_backend", self.factory))

    def fixture_preflight(self, *args):
        self.events.append("fixture_preflight")
        return dict(fixture=True)

    def fixture_guard(self, value, args):
        self.events.append("fixture_guard")
        guard = Mock()

        def finish():
            self.events.append("fixture_gpu_final")
            if self.guard_error is not None:
                raise self.guard_error
            return dict(gpu_exit_confirmed=self.gpu_confirmed,
                        external_lease_valid=self.lease_valid,
                        external_host_lease_released_by_worker=False, fixture=True)

        guard.finish.side_effect = finish
        self.guards.append(guard)
        self.assertIsInstance(args.stop_budget, self.owner.StopBudget)
        self.assertEqual(args.scopes, [])
        return guard

    def fixture_query(self, *args, **kwargs):
        self.events.append("fixture_query")
        # The real ledger must show a completed permanent claim before a backend factory.
        state = self.saved_snapshot(self.ledger)
        self.assertEqual(next(row for row in state["units"] if row["unit_id"] == self.current_unit)["claimed"], 1)
        self.runner._make_backend(fixture=True)
        if self.primary is not None:
            self.primary.query_report = deepcopy(self.outcome)
            raise self.primary
        return deepcopy(self.outcome)

    def run_worker(self, *, value=None):
        self.serial += 1
        root = self.root / ("worker-" + str(self.serial))
        root.mkdir(mode=0o700)
        before = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
        self.last_output = root
        selected = self.value if value is None else value
        self.current_unit = selected["budget"]["unit_id"]
        try:
            return self.runner.run_worker(self.config, self.relation, self.chain, self.engine, self.resources,
                                          root, selected)
        finally:
            self.assertEqual({number: signal.getsignal(number) for number in before}, before)

    def read_report(self):
        path = self.last_output / "query.json"
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        return self.runtime.read_json(path)

    def test_real_existing_unit_is_claimed_once_before_backend_and_never_refunded(self):
        report = self.run_worker()
        self.assertEqual(self.events, ["fixture_preflight", "fixture_guard", "fixture_query",
                                      "backend_factory", "fixture_gpu_final"])
        self.assertEqual(report, self.read_report())
        self.assertEqual(report["permanent_budget"]["allocated_requests"], 4)
        self.assertEqual(report["permanent_budget"]["units"][0]["claimed"], 1)
        self.assertFalse(report["external_host_lease_released_by_worker"])
        self.assertTrue(report["physical_exit"]["gpu_exit_confirmed"])
        with self.assertRaises(self.BudgetError):
            self.run_worker()
        self.factory.assert_called_once_with(fixture=True)
        self.assertEqual(self.saved_snapshot(self.ledger)["allocated_requests"], 4)
        self.assertEqual(self.read_report()["status"], "failed")
        self.assertTrue(all(guard.finish.call_count == 1 for guard in self.guards))

    def test_missing_wrong_corrupt_or_unreserved_ledger_fails_before_backend_factory(self):
        invalid = []
        for changes in (dict(path=str(self.root / "missing.sqlite")), dict(id="wrong.identity"),
                        dict(limit=9), dict(unit_id="not-reserved")):
            value = deepcopy(self.value)
            value["budget"].update(changes)
            invalid.append(value)
        corrupt = self.root / "corrupt.sqlite"
        corrupt.write_bytes(b"fixture corrupt database")
        value = deepcopy(self.value)
        value["budget"]["path"] = str(corrupt)
        invalid.append(value)
        for value in invalid:
            with self.subTest(budget=value["budget"]), self.assertRaises(self.BudgetError):
                self.run_worker(value=value)
            self.assertEqual(self.read_report()["status"], "failed")
        self.assertFalse((self.root / "missing.sqlite").exists())
        self.factory.assert_not_called()
        self.query.assert_not_called()
        self.assertTrue(all(guard.finish.call_count == 1 for guard in self.guards))
        self.assertEqual(self.saved_snapshot(self.ledger)["units"][0]["claimed"], 0)

    def test_empty_relation_still_requires_and_consumes_one_existing_unit(self):
        self.relation.records = ()
        self.ledger.reserve_unit("empty", 1)
        value = deepcopy(self.value)
        value["budget"]["unit_id"] = "empty"
        report = self.run_worker(value=value)
        state = report["permanent_budget"]
        self.assertEqual(state["allocated_requests"], 5)
        self.assertEqual(next(row for row in state["units"] if row["unit_id"] == "empty")["claimed"], 1)
        with self.assertRaises(self.BudgetError):
            self.ledger.claim_unit("empty")
        self.assertEqual(self.read_report(), report)

    def test_expired_permanent_unit_fails_before_backend_factory(self):
        with patch("src.experiments.cell_budget.time.time", return_value=self.deadline + 1), \
             self.assertRaises(self.BudgetExhausted):
            self.run_worker()
        self.factory.assert_not_called()
        self.assertEqual(self.saved_snapshot(self.ledger)["allocated_requests"], 4)
        self.assertEqual(self.saved_snapshot(self.ledger)["units"][0]["claimed"], 0)
        self.assertEqual(self.read_report()["status"], "failed")
        self.guards[0].finish.assert_called_once_with()

    def test_preflight_rejection_cannot_claim_a_unit_or_construct_backend(self):
        self.preflight.side_effect = ValueError("declared fixture preflight rejection")
        with self.assertRaisesRegex(ValueError, "preflight rejection"):
            self.run_worker()
        self.guard_factory.assert_not_called()
        self.factory.assert_not_called()
        self.query.assert_not_called()
        self.assertEqual(self.saved_snapshot(self.ledger)["units"][0]["claimed"], 0)
        self.assertEqual(self.read_report()["status"], "failed")

    def test_too_small_reserved_unit_is_charged_and_rejected_before_backend_factory(self):
        self.ledger.reserve_unit("small", 1)
        value = deepcopy(self.value)
        value["budget"]["unit_id"] = "small"
        with self.assertRaisesRegex(ValueError, "cannot cover this complete query"):
            self.run_worker(value=value)
        self.factory.assert_not_called()
        self.query.assert_not_called()
        state = self.saved_snapshot(self.ledger)
        self.assertEqual(state["allocated_requests"], 5)
        self.assertEqual(next(row for row in state["units"] if row["unit_id"] == "small")["claimed"], 1)
        with self.assertRaises(self.BudgetError):
            self.ledger.claim_unit("small")
        self.assertEqual(self.read_report()["permanent_budget"], state)

    def test_failed_and_unknown_calls_keep_the_original_error_usage_and_claim(self):
        self.outcome.update(status="failed", calls=[
            dict(call_id="fixture-failed", status="failed", prompt_tokens=5, completion_tokens=2, total_tokens=7),
            dict(call_id="fixture-unknown", status="unknown", prompt_tokens=None, completion_tokens=None, total_tokens=None),
        ])
        self.primary = RuntimeError("declared fixture query failure")
        with self.assertRaises(RuntimeError) as caught:
            self.run_worker()
        self.assertIs(caught.exception, self.primary)
        report = self.read_report()
        self.assertEqual(report["calls"], self.outcome["calls"])
        self.assertEqual(report["permanent_budget"]["allocated_requests"], 4)
        self.assertEqual(report["permanent_budget"]["units"][0]["claimed"], 1)
        self.guards[0].finish.assert_called_once_with()
        with self.assertRaises(self.BudgetError):
            self.ledger.claim_unit("query")

    def test_unconfirmed_final_gpu_exit_fails_with_known_usage_and_budget_receipt(self):
        self.gpu_confirmed = False
        with self.assertRaisesRegex(RuntimeError, "GPU exit.*remains unconfirmed"):
            self.run_worker()
        report = self.read_report()
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["calls"], self.outcome["calls"])
        self.assertFalse(report["physical_exit"]["gpu_exit_confirmed"])
        self.assertEqual(report["permanent_budget"]["allocated_requests"], 4)

    def test_changed_final_lease_fails_even_with_confirmed_gpu_and_keeps_usage_and_budget(self):
        self.lease_valid = False
        with self.assertRaisesRegex(RuntimeError, "external lease remains unconfirmed"):
            self.run_worker()
        report = self.read_report()
        self.assertEqual(report["status"], "failed")
        self.assertTrue(report["physical_exit"]["gpu_exit_confirmed"])
        self.assertFalse(report["physical_exit"]["external_lease_valid"])
        self.assertEqual(report["calls"], self.outcome["calls"])
        self.assertEqual(report["permanent_budget"]["allocated_requests"], 4)

    def test_primary_query_error_survives_final_gpu_error_and_budget_is_still_reported(self):
        self.primary = ValueError("declared primary fixture failure")
        self.guard_error = OSError("declared fixture GPU read failure")
        with self.assertRaises(ValueError) as caught:
            self.run_worker()
        self.assertIs(caught.exception, self.primary)
        self.assertTrue(any("physical exit check also failed" in note for note in self.primary.__notes__))
        report = self.read_report()
        self.assertEqual(report["calls"], self.outcome["calls"])
        self.assertEqual(report["permanent_budget"]["allocated_requests"], 4)

    def test_budget_snapshot_error_never_skips_gpu_final_or_private_query_report(self):
        with patch.object(self.Ledger, "snapshot", side_effect=OSError("declared fixture snapshot failure")), \
             self.assertRaisesRegex(OSError, "snapshot failure"):
            self.run_worker()
        self.guards[0].finish.assert_called_once_with()
        report = self.read_report()
        self.assertTrue(report["physical_exit"]["gpu_exit_confirmed"])
        self.assertEqual(report["calls"], self.outcome["calls"])
        self.assertEqual(report["status"], "failed")
        self.assertEqual(self.saved_snapshot(self.ledger)["units"][0]["claimed"], 1)

    def test_budget_snapshot_and_gpu_errors_never_replace_primary_or_skip_query_report(self):
        self.primary = ValueError("declared primary fixture failure")
        self.guard_error = OSError("declared fixture GPU read failure")
        with patch.object(self.Ledger, "snapshot", side_effect=RuntimeError("declared fixture snapshot failure")), \
             self.assertRaises(ValueError) as caught:
            self.run_worker()
        self.assertIs(caught.exception, self.primary)
        self.guards[0].finish.assert_called_once_with()
        self.assertTrue(any("budget receipt also failed" in note for note in self.primary.__notes__))
        self.assertEqual(self.read_report()["calls"], self.outcome["calls"])
        self.assertEqual(self.read_report()["error_type"], "ValueError")

    def test_report_write_failure_preserves_primary_known_calls_and_both_final_receipts(self):
        self.primary = ValueError("declared primary fixture failure")
        with patch.object(self.runner, "write_private_json", side_effect=OSError("declared fixture report failure")) as write, \
             self.assertRaises(ValueError) as caught:
            self.run_worker()
        self.assertIs(caught.exception, self.primary)
        self.guards[0].finish.assert_called_once_with()
        report = self.primary.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["calls"], self.outcome["calls"])
        self.assertEqual(report["permanent_budget"]["allocated_requests"], 4)
        self.assertTrue(report["physical_exit"]["gpu_exit_confirmed"])
        self.assertEqual(report["report_error_type"], "OSError")
        self.assertTrue(any("query report also failed" in note for note in self.primary.__notes__))
        write.assert_called_once_with(self.last_output / "query.json", report)


if __name__ == "__main__":
    unittest.main(verbosity=2)
