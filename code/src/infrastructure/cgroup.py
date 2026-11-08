"""Read the current process's visible cgroup hierarchy without changing it."""

from pathlib import Path, PurePosixPath
import re
import sys


def _path(value):
    value = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), value)
    path = PurePosixPath(value)
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("invalid cgroup path")
    return path


def controller_hierarchy(controller, *, proc_root=Path("/proc/self")):
    """Return version and directories from leaf to the visible mount root.

    Membership paths are resolved against mount roots, including bind mounts.
    Namespace-root membership '/' refers to the visible mount root. Missing or
    unreadable Linux metadata is unknown, never an unlimited allocation.
    """
    if sys.platform != "linux":
        return 0, ()
    memberships = []
    for line in (proc_root / "cgroup").read_text().splitlines():
        fields = line.split(":", 2)
        if len(fields) != 3:
            raise ValueError("invalid cgroup membership")
        if controller in fields[1].split(","):
            memberships.insert(0, (1, _path(fields[2])))
        elif fields[0] == "0" and fields[1] == "":
            memberships.append((2, _path(fields[2])))
    if any(version == 1 for version, _ in memberships):
        memberships = [item for item in memberships if item[0] == 1]
    mounts = []
    for line in (proc_root / "mountinfo").read_text().splitlines():
        left, separator, right = line.partition(" - ")
        fields, filesystem = left.split(), right.split()
        if not separator or len(fields) < 6 or len(filesystem) < 3:
            raise ValueError("invalid cgroup mount metadata")
        if filesystem[0] == "cgroup2":
            version = 2
        elif filesystem[0] == "cgroup" and controller in filesystem[2].split(","):
            version = 1
        else:
            continue
        mounts.append((version, _path(fields[3]), Path(str(_path(fields[4])))))
    for version, membership in memberships:
        candidates = []
        for mount_version, root, mount in mounts:
            if version != mount_version:
                continue
            if membership.is_relative_to(root):
                relative = membership.relative_to(root)
                direct = True
            elif membership == PurePosixPath("/"):
                relative = PurePosixPath(".")
                direct = False
            else:
                continue
            candidates.append((direct, len(root.parts), root, mount, mount / str(relative)))
        if candidates:
            if not any(item[0] for item in candidates) and len({item[2] for item in candidates}) != 1:
                raise ValueError("ambiguous namespace-root cgroup mounts")
            # Prefer the widest visible mount so a narrower bind mount cannot
            # hide a readable ancestor's tighter quota.
            _, _, _, mount, leaf = max(candidates, key=lambda item: (item[0], -item[1]))
            if not leaf.is_dir():
                raise ValueError("current cgroup directory is unavailable")
            return version, tuple(p for p in (leaf, *leaf.parents) if p.is_relative_to(mount))
    if memberships:
        raise ValueError("current cgroup has no matching visible controller mount")
    return 0, ()


def read_optional(path):
    try:
        return path.read_text().strip()
    except FileNotFoundError:
        return None


def cpu_quota(version, directories):
    limits = []
    for directory in directories:
        if version == 2:
            raw = read_optional(directory / "cpu.max")
            if raw is None:
                continue  # Root and groups without the CPU controller have no cpu.max.
            quota, period = raw.split()
            period = int(period)
            if period <= 0:
                raise ValueError("invalid CPU quota period")
            if quota == "max":
                continue
            quota = int(quota)
        else:
            quota = read_optional(directory / "cpu.cfs_quota_us")
            period = read_optional(directory / "cpu.cfs_period_us")
            if quota is None and period is None:
                continue
            quota, period = int(quota), int(period)
            if period <= 0:
                raise ValueError("invalid CPU quota period")
            if quota == -1:
                continue
        if quota <= 0:
            raise ValueError("invalid CPU quota")
        limits.append(quota / period)
    return min(limits) if limits else None


def cpu_usage_seconds(version, directory):
    if version == 2:
        fields = dict(line.split() for line in (directory / "cpu.stat").read_text().splitlines())
        value, scale = int(fields["usage_usec"]), 1e6
    else:
        # Only a combined cpu/cpuacct hierarchy proves the counters cover the
        # same quota group. A separately grouped cpuacct leaf is not equivalent.
        value, scale = int((directory / "cpuacct.usage").read_text()), 1e9
    if value < 0:
        raise ValueError("invalid cgroup CPU usage")
    return value / scale


def memory_observation():
    """Keep container memory separate from psutil's host-wide RAM fields."""
    try:
        version, directories = controller_hierarchy("memory")
        if not directories:
            return dict(memory_allocation_status="not_cgroup_scoped")
        limits, available = [], []
        for directory in directories:
            name = "memory.max" if version == 2 else "memory.limit_in_bytes"
            raw = read_optional(directory / name)
            if raw is None or raw == "max":
                continue
            limit = int(raw)
            if limit < 0:
                raise ValueError("invalid cgroup memory limit")
            if version == 1 and limit >= (1 << 60):
                continue  # Kernel PAGE_COUNTER_MAX sentinel, not assigned RAM.
            current = int((directory / ("memory.current" if version == 2 else "memory.usage_in_bytes")).read_text())
            if current < 0:
                raise ValueError("invalid cgroup memory usage")
            limits.append(limit)
            available.append(max(0, limit - current))
        return dict(memory_allocation_status="observed" if limits else "unlimited_visible_hierarchy",
                    memory_allocation_limit_bytes=min(limits) if limits else None,
                    memory_allocation_headroom_bytes=min(available) if available else None)
    except (OSError, ValueError, TypeError) as error:
        return dict(memory_allocation_status="unavailable:" + type(error).__name__,
                    memory_allocation_limit_bytes=None, memory_allocation_headroom_bytes=None)
