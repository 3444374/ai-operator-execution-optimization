"""Owner allowances and read-only device observations; echo defaults stay fixed."""

import subprocess


ECHO_LIMITS = dict(cpu_cores=8, rss_bytes=8 * 1024**3, shm_bytes=2 * 1024**3)


def owner_limits(value=None):
    if value is None:
        return dict(ECHO_LIMITS)
    if (type(value) is not dict or set(value) != set(ECHO_LIMITS)
            or any(type(number) is not int or number <= 0 for number in value.values())):
        raise ValueError("owner limits require positive integer CPU cores, RSS bytes and SHM bytes")
    return dict(value)


def check_resources(rss, shm, limits):
    if rss > limits["rss_bytes"] or shm > limits["shm_bytes"]:
        raise RuntimeError("records resource allowance exceeded")


def gpu_snapshot(device):
    def query(kind, fields):
        result = subprocess.run(["nvidia-smi", "-i", device, "--query-" + kind + "=" + fields,
                                 "--format=csv,noheader,nounits"], check=True,
                                capture_output=True, text=True, timeout=5)
        return [[field.strip() for field in line.split(",")] for line in result.stdout.splitlines() if line.strip()]
    devices = query("gpu", "uuid,memory.used")
    if len(devices) != 1 or len(devices[0]) != 2:
        raise ValueError("the assigned GPU must resolve to exactly one physical device")
    processes = query("compute-apps", "pid,gpu_uuid,used_memory")
    if any(len(row) != 3 or row[1] != devices[0][0] for row in processes):
        raise ValueError("GPU compute process observation is incomplete")
    return dict(uuid=devices[0][0], memory_mib=int(devices[0][1]), compute_processes=processes)


def gpu_exited(before, after):
    return (after["uuid"] == before["uuid"] and not after["compute_processes"]
            and after["memory_mib"] <= before["memory_mib"] + 1)
