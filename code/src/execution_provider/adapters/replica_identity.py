"""Read-only local verification of model API processes and their actual GPU descendants."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import ipaddress
import json
from pathlib import Path
import socket
import subprocess
import sys
from urllib.parse import urlsplit

from .model_replicas import ModelReplicaSet


@dataclass(frozen=True)
class ReplicaListener:
    family: str
    address: str
    port: int
    inode: str = ""

    def __post_init__(self):
        if type(self.address) is not str:
            raise ValueError("invalid model listener address")
        address = ipaddress.ip_address(self.address)
        if self.family != ("ipv4" if address.version == 4 else "ipv6"):
            raise ValueError("model listener address family differs")
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("invalid model listener port")
        if type(self.inode) is not str or (self.inode and not self.inode.isdecimal()):
            raise ValueError("invalid model listener inode")
        object.__setattr__(self, "address", str(address))


@dataclass(frozen=True)
class ReplicaProcess:
    pid: int
    parent_pid: int
    start_ticks: str
    argv: tuple[str, ...] = field(repr=False)
    listening_ports: tuple[int, ...] = ()
    # Port-only observations remain representable but cannot qualify a URL.
    listening_sockets: tuple[ReplicaListener, ...] = ()


@dataclass(frozen=True)
class GpuProcess:
    pid: int
    device_id: str


def _option(argv, name, *, required=True):
    values = []
    for index, token in enumerate(argv):
        if token == name:
            if index + 1 == len(argv) or argv[index + 1].startswith("--"):
                raise ValueError("model process has an incomplete serving option")
            values.append(argv[index + 1])
        elif token.startswith(name + "="):
            values.append(token[len(name) + 1:])
    if len(values) > 1 or (required and not values):
        raise ValueError("model process serving option is missing or repeated")
    return values[0] if values else None


def _serving_sha256(argv):
    # Compare actual serving arguments while withholding paths and credential values.
    module = argv.index("vllm.entrypoints.openai.api_server")
    result, index = [], module + 1
    physical = ("--port", "--host", "--api-key")
    while index < len(argv):
        token = argv[index]
        if token in physical:
            index += 2
        elif any(token.startswith(name + "=") for name in physical):
            index += 1
        else:
            result.append(token)
            index += 1
    return hashlib.sha256(json.dumps(result, separators=(",", ":")).encode()).hexdigest()


def _native_topology(sidecar, argv, device_ids):
    """Legacy TP1 endpoints or the opt-in pinned local native DP deployment."""
    argv = tuple(token.split("=", 1)[0].replace("_", "-") +
                 ("=" + token.split("=", 1)[1] if "=" in token else "")
                 if token.startswith("--") else token for token in argv)
    resolved = sidecar.get("resources_resolved")
    if resolved is None:
        if (len(device_ids) != 1 or _option(argv, "--tensor-parallel-size") != "1"
                or _option(argv, "--data-parallel-size", required=False) not in (None, "1")
                or _option(argv, "--pipeline-parallel-size", required=False) not in (None, "1")):
            raise ValueError("legacy qualification requires exactly one declared GPU per TP1 replica")
        return dict(data_parallel_size=1, tensor_parallel_size=1, pipeline_parallel_size=1)
    if type(resolved) is not dict:
        raise ValueError("invalid native vLLM resource identity")
    canonical = {"--data-parallel-size", "--data-parallel-backend", "--tensor-parallel-size",
                 "--pipeline-parallel-size", "--distributed-executor-backend"}
    for token in argv:
        flag = token.split("=", 1)[0].replace("_", "-")
        if (flag.startswith(("-dp", "-tp", "-pp", "-dcp", "-pcp", "-ep", "-n", "-r", "--device-ids", "--headless",
                             "--nnodes", "--node-rank", "--enable-expert-parallel", "--enable-elastic-ep", "--config"))
                or (flag.startswith(("--data-parallel", "--tensor-parallel", "--pipeline-parallel",
                                     "--distributed-executor", "--decode-context-parallel", "--prefill-context-parallel"))
                    and flag not in canonical)):
            raise ValueError("native vLLM resource identity forbids rank/device/backend overrides")
    dp, tp = resolved.get("data_parallel_size"), resolved.get("tensor_parallel_size")
    if (any(type(x) is not int or x <= 0 for x in (dp, tp))
            or sidecar.get("package_version") != "0.25.1" or resolved.get("version") != "0.25.1"
            or resolved.get("scope") != "single_node" or resolved.get("pipeline_parallel_size") != 1
            or resolved.get("load_balancing") != "vllm_internal"
            or dp * tp != len(device_ids) or resolved.get("resolved_count") != len(device_ids)
            or sorted(resolved.get("selected_gpu_uuids", [])) != sorted(device_ids)):
        raise ValueError("native vLLM DP topology differs from its declared physical allocation")
    for flag, expected in (("--data-parallel-size", str(dp)), ("--tensor-parallel-size", str(tp)),
                           ("--pipeline-parallel-size", "1"), ("--data-parallel-backend", "mp"),
                           ("--distributed-executor-backend", "mp")):
        if _option(argv, flag) != expected:
            raise ValueError("native vLLM serving argv differs from its resource identity")
    return dict(data_parallel_size=dp, tensor_parallel_size=tp, pipeline_parallel_size=1)


def verify_replica_processes(directory, sidecars, processes, gpu_processes, *, independent_devices=True,
                             localhost_addresses=None):
    """Verify local model deployments; CPU HTTP actors are not model replicas.

    sidecars use launch_vllm_with_identity.py's existing schema. A model content
    audit must separately establish deployment_sha256; this verifies the live
    process/device topology and equal serving argv/package versions only.
    """
    if type(directory) is not ModelReplicaSet or type(independent_devices) is not bool:
        raise ValueError("invalid model replica verification request")
    if set(sidecars) != {r.endpoint_id for r in directory.replicas}:
        raise ValueError("runtime sidecars must cover exactly the declared replicas")
    by_pid = {p.pid: p for p in processes}
    if len(by_pid) != len(processes):
        raise ValueError("duplicate process identities")

    def descendants(pid):
        members = {pid}
        while True:
            grown = members | {p.pid for p in processes if p.parent_pid in members}
            if grown == members:
                return members
            members = grown

    used_processes, used_devices, records, serving = set(), set(), [], set()
    for replica in directory.replicas:
        url = urlsplit(replica.endpoint_url)
        if url.hostname not in ("127.0.0.1", "localhost", "::1") or url.port is None:
            raise ValueError("process verification requires endpoint-machine loopback URLs with explicit ports")
        if url.hostname == "localhost":
            if type(localhost_addresses) is not tuple or not 1 <= len(localhost_addresses) <= 16:
                raise ValueError("localhost requires current resolver address evidence")
            addresses = tuple(ipaddress.ip_address(address) for address in localhost_addresses)
            if any(not address.is_loopback for address in addresses):
                raise ValueError("localhost resolver addresses must be loopback")
        else:
            addresses = (ipaddress.ip_address(url.hostname),)
        sidecar = sidecars[replica.endpoint_id]
        if (type(sidecar) is not dict or type(sidecar.get("pid")) is not int
                or type(sidecar.get("port")) is not int or sidecar.get("schema_version") != 1):
            raise ValueError("invalid model runtime sidecar")
        process = by_pid.get(sidecar["pid"])
        if (process is None or process.start_ticks != sidecar.get("process_start_time_ticks")
                or sidecar["port"] != url.port):
            raise ValueError("runtime sidecar has a stale process or different endpoint port")
        if ("vllm.entrypoints.openai.api_server" not in process.argv
                or _option(process.argv, "--port") != str(url.port)
                or _option(process.argv, "--served-model-name") != directory.model_id
                or _option(process.argv, "--model") is None):
            raise ValueError("replica is not the declared model API process")
        topology = _native_topology(sidecar, process.argv, replica.device_ids)
        members = descendants(process.pid)
        if members & used_processes:
            raise ValueError("replicas share a model API process tree")
        bindings = []
        for pid in sorted(members):
            for listener in by_pid[pid].listening_sockets:
                if type(listener) is not ReplicaListener:
                    raise ValueError("model listener address/family evidence is required")
                bound = ipaddress.ip_address(listener.address)
                if listener.port == url.port and any(
                        bound.version == address.version and (bound == address or bound.is_unspecified)
                        for address in addresses):
                    bindings.append((pid, listener))
        # Require every possible localhost resolution to reach this model tree.
        # An IPv6 wildcard is not assumed to accept IPv4 without socket-mode evidence.
        if not all(any(ipaddress.ip_address(listener.address).version == address.version
                       and (ipaddress.ip_address(listener.address) == address
                            or ipaddress.ip_address(listener.address).is_unspecified)
                       for _, listener in bindings) for address in addresses):
            raise ValueError("model process tree does not own the endpoint address/family listener")
        gpu_members = [g for g in gpu_processes if g.pid in members]
        devices = {g.device_id for g in gpu_members}
        if devices != set(replica.device_ids):
            raise ValueError("actual model GPU descendants differ from the declared devices")
        if independent_devices and (devices & used_devices or any(
                g.device_id in devices and g.pid not in members for g in gpu_processes)):
            raise ValueError("independent replicas require GPUs without other compute owners")
        version = sidecar.get("package_version")
        if type(version) is not str or not version:
            raise ValueError("model runtime package version is unavailable")
        signature = _serving_sha256(process.argv)
        serving.add((version, signature))
        used_processes.update(members)
        used_devices.update(devices)
        records.append(dict(endpoint_id=replica.endpoint_id, location_id=replica.location_id,
            api_pid=process.pid, process_start_time_ticks=process.start_ticks,
            gpu_process_ids=sorted({g.pid for g in gpu_members}), device_ids=sorted(devices),
            service_version=version, serving_arguments_sha256=signature,
            native_topology=topology,
            gpu_workers=[dict(pid=pid, process_start_time_ticks=by_pid[pid].start_ticks,
                device_ids=sorted({g.device_id for g in gpu_members if g.pid == pid}))
                for pid in sorted({g.pid for g in gpu_members})],
            owned_process_tree=[dict(pid=pid, parent_pid=by_pid[pid].parent_pid,
                process_start_time_ticks=by_pid[pid].start_ticks) for pid in sorted(members)],
            actual_dp_rank_identity="not inferred from PID order; process/GPU ownership only",
            listener_bindings=[dict(pid=pid, process_start_time_ticks=by_pid[pid].start_ticks,
                family=listener.family, address=listener.address, port=listener.port, inode=listener.inode)
                for pid, listener in bindings]))
    if len(serving) != 1:
        raise ValueError("replicas have different effective serving arguments or package versions")
    return dict(schema_version=2, replica_set_sha256=directory.sha256, replica_count=len(records),
        gpu_count=len(used_devices), independent_devices=independent_devices,
        model_content_identity="declared; separate content audit required", replicas=records)


def _process_stat(path):
    raw = path.read_text(encoding="utf-8")
    close = raw.rfind(")")
    fields = raw[close + 2:].split()
    if close < 0 or len(fields) <= 19:
        raise ValueError("model process stat is incomplete")
    return int(fields[1]), fields[19]


def _listeners(proc_root):
    sockets = {}
    for name in ("tcp", "tcp6"):
        for line in (proc_root / "net" / name).read_text().splitlines()[1:]:
            fields = line.split()
            if len(fields) > 9 and fields[3] == "0A":
                encoded, port = fields[1].rsplit(":", 1)
                expected = 8 if name == "tcp" else 32
                if len(encoded) != expected:
                    raise ValueError("model socket address encoding is incomplete")
                # /proc prints each native-endian 32-bit address word as hex.
                packed = b"".join(int(encoded[start:start + 8], 16).to_bytes(4, sys.byteorder)
                                  for start in range(0, len(encoded), 8))
                family = "ipv4" if name == "tcp" else "ipv6"
                sockets[fields[9]] = ReplicaListener(family, str(ipaddress.ip_address(packed)),
                                                     int(port, 16), fields[9])
    return sockets


def _process_listeners(path, sockets):
    listeners = set()
    for fd in (path / "fd").iterdir():
        try:
            target = fd.readlink().as_posix()
        except OSError:
            continue
        if target.startswith("socket:[") and target[8:-1] in sockets:
            listeners.add(sockets[target[8:-1]])
    return tuple(sorted(listeners, key=lambda item: (item.family, item.address, item.port, item.inode)))


def inspect_local_model_replicas(directory, sidecar_paths, *, independent_devices=True):
    """No HTTP, model launch, GPU allocation, downloads, or SSH connections."""
    proc_root = Path("/proc")
    if not proc_root.is_dir():
        raise ValueError("live replica verification requires Linux /proc on the endpoint machine")
    sidecars = {}
    for endpoint, path in sidecar_paths.items():
        path = Path(path)
        if path.stat().st_size > 65536:
            raise ValueError("model runtime sidecar exceeds its byte capacity")
        sidecars[endpoint] = json.loads(path.read_text())
    sockets, processes = _listeners(proc_root), []
    for path in proc_root.iterdir():
        if not path.name.isdecimal():
            continue
        try:
            parent, started = _process_stat(path / "stat")
            argv = tuple(t.decode("utf-8", "replace")
                         for t in (path / "cmdline").read_bytes().split(b"\0") if t)
            listeners = _process_listeners(path, sockets)
            # PID reuse or a process exiting during the snapshot cannot qualify.
            if (parent, started) != _process_stat(path / "stat"):
                continue
            processes.append(ReplicaProcess(int(path.name), parent, started, argv,
                tuple(sorted({listener.port for listener in listeners})), listeners))
        except (OSError, ValueError):
            continue
    probe = subprocess.run(["nvidia-smi", "--query-compute-apps=pid,gpu_uuid", "--format=csv,noheader,nounits"],
                           capture_output=True, text=True, timeout=10, check=True)
    gpu_processes = []
    for line in probe.stdout.splitlines():
        if not line.strip():
            continue
        fields = line.split(",")
        if len(fields) != 2 or not fields[0].strip().isdecimal() or not fields[1].strip().startswith("GPU-"):
            raise ValueError("GPU process identity is unavailable")
        gpu_processes.append(GpuProcess(int(fields[0].strip()), fields[1].strip()))
    localhost_addresses = None
    if any(urlsplit(replica.endpoint_url).hostname == "localhost" for replica in directory.replicas):
        localhost_addresses = tuple(sorted({str(ipaddress.ip_address(item[4][0])) for item in
            socket.getaddrinfo("localhost", None, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP)}))
    result = verify_replica_processes(directory, sidecars, tuple(processes), tuple(gpu_processes),
        independent_devices=independent_devices, localhost_addresses=localhost_addresses)
    current_sockets = _listeners(proc_root)
    # Recheck the API process identity after the GPU snapshot, not just at sidecar read time.
    for replica in result["replicas"]:
        if _process_stat(proc_root / str(replica["api_pid"]) / "stat")[1] != replica["process_start_time_ticks"]:
            raise ValueError("model API process changed during device verification")
        for worker in replica["owned_process_tree"]:
            if _process_stat(proc_root / str(worker["pid"]) / "stat")[1] != worker["process_start_time_ticks"]:
                raise ValueError("model worker process changed during device verification")
        for binding in replica["listener_bindings"]:
            path = proc_root / str(binding["pid"])
            expected = ReplicaListener(binding["family"], binding["address"], binding["port"], binding["inode"])
            if (_process_stat(path / "stat")[1] != binding["process_start_time_ticks"]
                    or expected not in _process_listeners(path, current_sockets)):
                raise ValueError("model endpoint listener changed during device verification")
    return result
