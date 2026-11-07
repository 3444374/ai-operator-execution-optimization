"""Local filesystem/CPU mocks only; no cluster, model or GPU probes."""

from contextlib import ExitStack
from pathlib import Path, PurePosixPath
import tempfile
import unittest
from unittest.mock import patch

from src.infrastructure import cgroup, cpu_allocation as cpu, environment


class CgroupTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.root = Path(self.stack.enter_context(tempfile.TemporaryDirectory()))
        self.proc, self.mount = self.root / "proc", self.root / "cgroup mount"
        self.proc.mkdir()
        self.mount.mkdir()
        self.stack.enter_context(patch.object(cgroup.sys, "platform", "linux"))

    def membership(self, group="/parent/job", *, version=2, mount_root="/", controller="cpu"):
        self.proc.joinpath("cgroup").write_text("0::" + group + "\n" if version == 2 else "5:" + controller + ":" + group + "\n")
        escaped = str(self.mount).replace(" ", r"\040")
        filesystem = "cgroup2 cgroup rw" if version == 2 else "cgroup cgroup rw," + controller
        self.proc.joinpath("mountinfo").write_text(f"42 1 0:1 {mount_root} {escaped} rw - {filesystem}\n")
        if group.startswith(mount_root) and ".." not in PurePosixPath(group).parts:
            self.mount.joinpath(group[len(mount_root):].lstrip("/")).mkdir(parents=True, exist_ok=True)
        return cgroup.controller_hierarchy(controller, proc_root=self.proc)

    def write(self, directory, name, value):
        directory.mkdir(parents=True, exist_ok=True)
        directory.joinpath(name).write_text(value)

    def test_v2_nested_parent_quota_and_fractional_leaf_quota(self):
        version, paths = self.membership()
        self.assertEqual(paths, (self.mount / "parent/job", self.mount / "parent", self.mount))
        self.write(paths[0], "cpu.max", "max 100000")
        self.write(paths[1], "cpu.max", "250000 100000")
        self.assertEqual(cgroup.cpu_quota(version, paths), 2.5)
        self.write(paths[0], "cpu.max", "50000 100000")
        self.assertEqual(cgroup.cpu_quota(version, paths), .5)

    def test_v1_cpu_quota_intersects_ancestors_without_rounding_up(self):
        version, paths = self.membership(version=1, controller="cpu,cpuacct")
        # The controller argument names one controller, even for a combined mount.
        version, paths = cgroup.controller_hierarchy("cpu", proc_root=self.proc)
        for path, quota in zip(paths, ("-1", "125000", "-1")):
            self.write(path, "cpu.cfs_quota_us", quota)
            self.write(path, "cpu.cfs_period_us", "100000")
        self.assertEqual(cgroup.cpu_quota(version, paths), 1.25)

    def test_unlimited_and_controller_absence_are_distinct_from_unreadable(self):
        version, paths = self.membership()
        self.write(paths[0], "cpu.max", "max 100000")
        self.assertIsNone(cgroup.cpu_quota(version, paths))
        with patch.object(Path, "read_text", side_effect=PermissionError("fixture")):
            with self.assertRaises(PermissionError):
                cgroup.cpu_quota(version, paths)

    def test_mount_root_is_removed_and_namespace_root_uses_visible_mount(self):
        _, paths = self.membership("/owned/job", mount_root="/owned")
        self.assertEqual(paths, (self.mount / "job", self.mount))
        _, paths = self.membership("/", mount_root="/owned")
        self.assertEqual(paths, (self.mount,))
        outer = self.root / "outer"
        outer.joinpath("owned/job").mkdir(parents=True)
        self.proc.joinpath("cgroup").write_text("0::/owned/job\n")
        escaped = str(self.mount).replace(" ", r"\040")
        self.proc.joinpath("mountinfo").write_text(
            f"42 1 0:1 /owned {escaped} rw - cgroup2 cgroup rw\n"
            f"43 1 0:1 / {outer} rw - cgroup2 cgroup rw\n")
        version, paths = cgroup.controller_hierarchy("cpu", proc_root=self.proc)
        self.write(outer, "cpu.max", "50000 100000")
        self.assertEqual(paths[-1], outer)
        self.assertEqual(cgroup.cpu_quota(version, paths), .5)

    def test_missing_v1_mount_cannot_fall_back_to_an_unlimited_v2_mount(self):
        self.membership("/")
        self.proc.joinpath("cgroup").write_text("5:cpu:/limited\n0::/\n")
        with self.assertRaisesRegex(ValueError, "matching"):
            cgroup.controller_hierarchy("cpu", proc_root=self.proc)

    def test_unmapped_membership_and_malformed_metadata_are_unknown(self):
        for group, root in (("/foreign/job", "/owned"), ("/../job", "/")):
            with self.subTest(group=group), self.assertRaises(ValueError):
                self.membership(group, mount_root=root)
        self.proc.joinpath("cgroup").write_text("not-membership")
        with self.assertRaises(ValueError):
            cgroup.controller_hierarchy("cpu", proc_root=self.proc)

    def test_namespace_root_with_unrelated_bind_mounts_is_unknown(self):
        self.membership("/", mount_root="/owned")
        with self.proc.joinpath("mountinfo").open("a") as stream:
            escaped = str(self.mount).replace(" ", r"\040")
            stream.write(f"43 1 0:1 /foreign {escaped} rw - cgroup2 cgroup rw\n")
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            cgroup.controller_hierarchy("cpu", proc_root=self.proc)

    def test_v2_malformed_or_nonpositive_quota_is_not_unlimited(self):
        for raw in ("0 100000", "-1 100000", "10000 0", "max 0", "10000", "bad 100000"):
            with self.subTest(raw=raw):
                self.write(self.mount, "cpu.max", raw)
                with self.assertRaises((ValueError, TypeError)):
                    cgroup.cpu_quota(2, (self.mount,))

    def test_v1_incomplete_or_invalid_quota_is_unknown(self):
        for quota, period in (("0", "100000"), ("-2", "100000"), ("-1", "0"), ("1", None)):
            with self.subTest(quota=quota, period=period):
                self.write(self.mount, "cpu.cfs_quota_us", quota)
                self.mount.joinpath("cpu.cfs_period_us").unlink(missing_ok=True)
                if period is not None:
                    self.write(self.mount, "cpu.cfs_period_us", period)
                with self.assertRaises((ValueError, TypeError)):
                    cgroup.cpu_quota(1, (self.mount,))

    def test_cgroup_cpu_usage_has_correct_v1_and_v2_units(self):
        self.write(self.mount, "cpu.stat", "usage_usec 1250000\nuser_usec 500000\n")
        self.assertEqual(cgroup.cpu_usage_seconds(2, self.mount), 1.25)
        self.write(self.mount, "cpuacct.usage", "2500000000")
        self.assertEqual(cgroup.cpu_usage_seconds(1, self.mount), 2.5)
        with self.assertRaises(OSError):
            cgroup.cpu_usage_seconds(1, self.root / "separate_cpu")

    def test_memory_headroom_uses_ancestor_usage_instead_of_host_available_RAM(self):
        paths = (self.mount / "job", self.mount)
        self.write(paths[0], "memory.max", "2000")
        self.write(paths[0], "memory.current", "100")
        self.write(paths[1], "memory.max", "3000")
        self.write(paths[1], "memory.current", "2800")
        with patch.object(cgroup, "controller_hierarchy", return_value=(2, paths)):
            value = cgroup.memory_observation()
        self.assertEqual(value["memory_allocation_limit_bytes"], 2000)
        self.assertEqual(value["memory_allocation_headroom_bytes"], 200)

    def test_memory_unknown_and_unlimited_do_not_report_host_RAM_as_container_RAM(self):
        with patch.object(cgroup, "controller_hierarchy", side_effect=PermissionError("fixture")):
            value = cgroup.memory_observation()
        self.assertIsNone(value["memory_allocation_headroom_bytes"])
        self.write(self.mount, "memory.limit_in_bytes", str((1 << 63) - 4096))
        with patch.object(cgroup, "controller_hierarchy", return_value=(1, (self.mount,))):
            value = cgroup.memory_observation()
        self.assertIsNone(value["memory_allocation_limit_bytes"])
        self.write(self.mount, "memory.max", "0")
        self.write(self.mount, "memory.current", "0")
        with patch.object(cgroup, "controller_hierarchy", return_value=(2, (self.mount,))):
            value = cgroup.memory_observation()
        self.assertEqual(value["memory_allocation_limit_bytes"], 0)
        self.assertEqual(value["memory_allocation_headroom_bytes"], 0)


class CpuCapacityTests(unittest.TestCase):
    def capacity(self, affinity=64, quota=2.5):
        with patch.object(cpu.os, "sched_getaffinity", return_value=set(range(affinity)), create=True), \
                patch.object(cpu, "controller_hierarchy", return_value=(2, (Path("/fixture"),))), \
                patch.object(cpu, "cpu_quota", return_value=quota):
            return cpu.observe_cpu_capacity()

    def test_effective_capacity_intersects_affinity_and_fractional_quota(self):
        for affinity, quota, expected in ((64, 2.5, 2.5), (2, 8, 2), (64, .5, .5), (4, None, 4)):
            with self.subTest(affinity=affinity, quota=quota):
                self.assertEqual(self.capacity(affinity, quota).effective_slots, expected)

    def test_unreadable_quota_or_affinity_is_not_the_host_cpu_count(self):
        with patch.object(cpu.os, "sched_getaffinity", return_value={0, 1}, create=True), \
                patch.object(cpu, "controller_hierarchy", side_effect=PermissionError("fixture")):
            self.assertIsNone(cpu.observe_cpu_capacity().effective_slots)
        with patch.object(cpu.os, "sched_getaffinity", side_effect=OSError("fixture"), create=True):
            self.assertIsNone(cpu.observe_cpu_capacity().effective_slots)

    def test_launch_rejects_oversubscription_without_rounding_or_shrinking_actors(self):
        with patch.object(cpu, "observe_cpu_capacity", return_value=self.capacity()):
            self.assertEqual(cpu.checked_local_cpu_slots(2), 2)
            with self.assertRaisesRegex(ValueError, "effective allocation is 2.5"):
                cpu.checked_local_cpu_slots(3)
        with patch.object(cpu, "observe_cpu_capacity", return_value=cpu.CpuCapacity(64, None, None, "unknown")):
            with self.assertRaisesRegex(RuntimeError, "unknown"):
                cpu.checked_local_cpu_slots(1)

    def test_machine_observation_and_preflight_share_the_effective_capacity(self):
        with patch.object(environment, "observe_cpu_capacity", return_value=self.capacity()), \
                patch.object(environment.shutil, "which", return_value=None):
            observed = environment.observe_machine()
            rows = []
            environment._check_profile(dict(platforms=[environment.platform.system()], minimum_cpu_slots=3), {}, rows)
        self.assertEqual(observed.cpu_slots, 2.5)
        self.assertEqual(observed.cpu_quota_slots, 2.5)
        self.assertEqual(next(row for row in rows if row.check == "machine:cpu_slots").status, "missing")
