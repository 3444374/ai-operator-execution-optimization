"""Native Ray driver ownership shared by SemBench adapters; no SQL or Core policy."""
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
import time


@dataclass(frozen=True)
class NativeRayRuntimeConfig:
    """Driver connection settings independent of a SQL reader or execution graph."""
    ray_num_cpus: int
    ray_async_batches_per_actor: int
    ray_address: str | None = None
    ray_object_store_bytes: int = 268435456

    def __post_init__(self):
        import re
        from ipaddress import IPv4Address
        if any(type(value) is not int or not 1 <= value <= 256 for value in (
                self.ray_num_cpus, self.ray_async_batches_per_actor)):
            raise ValueError('native Ray CPU and async batch settings must be positive and bounded')
        if (type(self.ray_object_store_bytes) is not int
                or not 80*1048576 <= self.ray_object_store_bytes <= 4*1073741824):
            raise ValueError('Ray object store allocation exceeds the supported range')
        if self.ray_address is not None:
            address = re.fullmatch(r'(localhost|[0-9.]+):([0-9]{1,5})', self.ray_address) if isinstance(self.ray_address, str) else None
            if address is None or not 1 <= int(address[2]) <= 65535:
                raise ValueError('native Ray requires an explicit existing local cluster address')
            if address[1] != 'localhost':
                IPv4Address(address[1])



@contextmanager
def native_ray_runtime(ray, config, ray_temp_root):
    """Own a driver connection; an explicit shared cluster remains caller-owned."""
    if ray.__version__ != '2.56.1' or ray.is_initialized():
        raise RuntimeError('native query requires a fresh driver and pinned Ray 2.56.1')
    shared = config.ray_address is not None
    if shared:
        if ray_temp_root is not None:
            raise ValueError('shared Ray does not create a query runtime directory')
    elif ray_temp_root is None or len(str(ray_temp_root).encode()) > 40 or Path(ray_temp_root).exists():
        raise ValueError('Ray query needs a new short private runtime directory')
    started = time.monotonic_ns()
    runtime_env = dict(env_vars={
        'RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY': str(config.ray_async_batches_per_actor)})
    try:
        if shared:
            ray.init(address=config.ray_address, runtime_env=runtime_env)
        else:
            ray.init(address='local',num_cpus=config.ray_num_cpus,num_gpus=0,include_dashboard=False,
                     _node_ip_address='127.0.0.1',object_store_memory=config.ray_object_store_bytes,
                     _temp_dir=str(ray_temp_root), runtime_env=runtime_env)
        resources = ray.cluster_resources()
        if shared:
            import socket
            import psutil
            local_addresses = {entry.address for entries in psutil.net_if_addrs().values()
                               for entry in entries if entry.family == socket.AF_INET}
            nodes = [node for node in ray.nodes() if node['Alive']]
            if (resources.get('CPU') != config.ray_num_cpus or resources.get('GPU', 0) != 0
                    or resources.get('object_store_memory') != config.ray_object_store_bytes
                    or len(nodes) != 1 or nodes[0].get('NodeManagerAddress') not in local_addresses):
                raise ValueError('shared Ray topology or resources differ from the declared local single-node runtime')
        report = dict(owner='caller' if shared else 'query',
            startup='external' if shared else 'included',
            cluster_resources={key: resources.get(key, 0) for key in ('CPU', 'GPU', 'object_store_memory')},
            driver_connect_seconds=(time.monotonic_ns()-started)/1e9,
            shared_cluster_shutdown_requested=False, driver_disconnected=False)
        yield report
    finally:
        # Ray shutdown disconnects this driver; it stops only a locally started runtime.
        ray.shutdown()
        if 'report' in locals():
            report['driver_disconnected'] = not ray.is_initialized()
