"""Declared equivalent model services, composed through the existing session engine."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from functools import partial
import hashlib
import json
from pathlib import Path
import re
import time
from urllib.parse import urlsplit

from ...scheduling.core.errors import EndpointCapacityUnavailable
from ...scheduling.core.models import EndpointSnapshot, RoutingDecision, TopologySnapshot
from ...scheduling.core.session_policy import SessionPolicies
from ...scheduling.organization.session_window import WorkWindowOrganizer
from ...scheduling.submission_control.admission import StaticAdmissionController
from .async_fixed_model import AsyncFixedModelTransport
from .full_response import FullResponseTransport
from .incremental_execution import build_fixed_model_execution
from .model_config import FixedModelConfig


_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
MAX_REPLICAS = 64


def _identifier(value, name):
    if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"invalid {name}")


def _positive(value, name):
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be a positive integer")


def _url_identity(url):
    # Reuse fixed-model validation, including rejection of embedded credentials.
    FixedModelConfig(url, "validation", 1)
    parsed = urlsplit(url)
    return (parsed.scheme, parsed.hostname.lower(), parsed.port or
            (443 if parsed.scheme == "https" else 80), parsed.path or "/", parsed.query)


@dataclass(frozen=True)
class ModelReplica:
    endpoint_id: str
    endpoint_url: str = field(repr=False)
    location_id: str
    device_ids: tuple[str, ...]
    deployment_sha256: str
    max_active_requests: int
    max_active_work: int

    def __post_init__(self):
        _identifier(self.endpoint_id, "replica identity")
        _identifier(self.location_id, "replica location")
        _url_identity(self.endpoint_url)
        if type(self.device_ids) is not tuple or len(self.device_ids) > 64:
            raise ValueError("invalid replica devices")
        for device in self.device_ids:
            _identifier(device, "replica device")
        if len(set(self.device_ids)) != len(self.device_ids):
            raise ValueError("invalid replica devices")
        if type(self.deployment_sha256) is not str or not _SHA256.fullmatch(self.deployment_sha256):
            raise ValueError("replica deployment must have a SHA-256 identity")
        _positive(self.max_active_requests, "replica request capacity")
        _positive(self.max_active_work, "replica work capacity")


@dataclass(frozen=True)
class ModelReplicaSet:
    """One logical role; the caller attests audited model/serving/data-policy equivalence.

    A deployment digest is a declaration, not evidence that weights or devices
    have been inspected. GPU qualification uses replica_identity separately.
    Global capacities belong to one execution owner, not the entire server.
    """

    model_id: str
    replicas: tuple[ModelReplica, ...]
    max_active_requests: int
    max_active_work: int
    routing: str = "round_robin"
    role_id: str = "model"

    def __post_init__(self):
        if type(self.model_id) is not str or not 1 <= len(self.model_id.encode()) <= 128:
            raise ValueError("invalid replica model identity")
        if self.role_id != "model" or self.routing not in ("round_robin", "static"):
            raise ValueError("replicas require the model role and static or round_robin routing")
        if (type(self.replicas) is not tuple or not 1 <= len(self.replicas) <= MAX_REPLICAS
                or any(type(replica) is not ModelReplica for replica in self.replicas)):
            raise ValueError("invalid model replica directory")
        if len({r.endpoint_id for r in self.replicas}) != len(self.replicas):
            raise ValueError("replica identities must be unique")
        if len({_url_identity(r.endpoint_url) for r in self.replicas}) != len(self.replicas):
            raise ValueError("replicas must use distinct endpoint URLs")
        if len({r.deployment_sha256 for r in self.replicas}) != 1:
            raise ValueError("replicas must declare one equivalent deployment")
        _positive(self.max_active_requests, "global request capacity")
        _positive(self.max_active_work, "global work capacity")
        if (self.max_active_requests > sum(r.max_active_requests for r in self.replicas)
                or self.max_active_work > sum(r.max_active_work for r in self.replicas)):
            raise ValueError("global capacity exceeds the sum of replica capacities")

    @property
    def sha256(self):
        # Ordering participates because it controls deterministic routing.
        record = asdict(self)
        for replica in record["replicas"]:
            replica["endpoint_url"] = hashlib.sha256(replica["endpoint_url"].encode()).hexdigest()
        return hashlib.sha256(json.dumps(record, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    @classmethod
    def load(cls, path):
        """Strict private configuration; credentials remain in FixedModelConfig."""
        try:
            path = Path(path)
            if path.stat().st_size > 65536:
                raise ValueError("model replica configuration exceeds its byte capacity")
            value = json.loads(path.read_text(encoding="utf-8"))
            if type(value) is not dict or set(value) - set(cls.__dataclass_fields__):
                raise ValueError("invalid replica configuration fields")
            replicas = []
            for item in value["replicas"]:
                if type(item) is not dict or type(item.get("device_ids")) is not list:
                    raise ValueError("invalid replica configuration fields")
                replicas.append(ModelReplica(**dict(item, device_ids=tuple(item["device_ids"]))))
            return cls(**dict(value, replicas=tuple(replicas)))
        except (OSError, UnicodeError, ValueError, TypeError, KeyError):
            raise ValueError("invalid model replica configuration") from None


class StaticReplicaRouter:
    """Use Core's outstanding requests/work, including cancelled and uncertain calls."""

    def __init__(self, directory):
        self.directory = directory
        self._next = 0

    def route(self, request, topology, pool_id):
        if pool_id != self.directory.role_id:
            raise ValueError("replica router received a different model role")
        snapshots = {e.endpoint_id: e for e in topology.endpoints}
        if set(snapshots) != {r.endpoint_id for r in self.directory.replicas}:
            raise ValueError("replica topology differs from its directory")
        if request.estimated_work_units > max(r.max_active_work for r in self.directory.replicas):
            raise ValueError("request exceeds every replica's work capacity")
        count = len(self.directory.replicas)
        if self.directory.routing == "static":
            # SessionPolicies uses session:sequence; pin occurrences, not prompt hashes.
            start = int(request.request_id.rsplit(":", 1)[1]) % count
            indices = (start,)
        else:
            indices = tuple((self._next + offset) % count for offset in range(count))
        for index in indices:
            replica = self.directory.replicas[index]
            if self.directory.routing == "static" and request.estimated_work_units > replica.max_active_work:
                raise ValueError("request exceeds its static replica's work capacity")
            endpoint = snapshots[replica.endpoint_id]
            if (endpoint.pool_id != pool_id or not endpoint.healthy or not endpoint.available
                    or endpoint.running + endpoint.waiting >= replica.max_active_requests
                    or endpoint.estimated_active_work + request.estimated_work_units > replica.max_active_work):
                continue
            if self.directory.routing == "round_robin":
                self._next = (index + 1) % count
            return RoutingDecision(replica.endpoint_id, pool_id, "replica_" + self.directory.routing)
        raise EndpointCapacityUnavailable("declared replica capacity is occupied")


def _prepare_payload_backend(physical):
    # Child pools can materialize concurrently. Daft's process-wide runner must
    # be selected on the startup thread before those pools can race its setup.
    if any(config.payload_backend == "daft" for config in physical.values()):
        from ...data.materializers.text import configure_daft_runner
        configure_daft_runner("native")


class ReplicaTransport:
    """Resolve a Core-selected identity to one owned HTTP transport; never retry."""

    def __init__(self, config, capacity, observer=None, *, directory, response_mode,
                 physical=None, before_request=None):
        self.directory, self.transports = directory, {}
        self._observer = observer
        self._directory_sha256 = directory.sha256
        if response_mode not in ("completion", "full"):
            raise ValueError("unknown replica response mode")
        if physical is not None:
            from .ray_map_transport import RayMapConfig
            if (type(physical) is not dict or set(physical) != {r.endpoint_id for r in directory.replicas}
                    or any(type(p) is not RayMapConfig or p.preparation is not None for p in physical.values())):
                raise ValueError("replica Ray configuration must cover every endpoint without preparation")
            if len({p.address for p in physical.values()}) != 1:
                raise ValueError("replica Ray workers require one existing cluster")
            pools = [p.worker_pool for p in physical.values() if p.worker_pool is not None]
            if len(pools) != len(set(pools)):
                raise ValueError("replicas require distinct named Ray worker pools")
            for replica in directory.replicas:
                effective = min(capacity, replica.max_active_requests)
                p = physical[replica.endpoint_id]
                if max(p.workers, p.batch_rows) > effective:
                    raise ValueError("replica Ray workers and rows exceed its request capacity")
                from ..wire.framing import MAX_FRAME_BYTES
                if p.window_bytes < MAX_FRAME_BYTES + 24:
                    raise ValueError("replica Ray window cannot fit a legal protocol request")
            _prepare_payload_backend(physical)
        elif before_request is not None:
            raise ValueError("a remote request guard currently requires the Ray transport")
        try:
            for replica in directory.replicas:
                child_config = replace(config, endpoint_url=replica.endpoint_url)
                child_observer = partial(self._observe, replica)
                child_capacity = min(capacity, replica.max_active_requests)
                if physical is None:
                    factory = FullResponseTransport if response_mode == "full" else AsyncFixedModelTransport
                    child = factory(child_config, child_capacity, child_observer)
                else:
                    from .ray_map_transport import RayMapTransport
                    child = RayMapTransport(child_config, child_capacity, child_observer,
                        physical=replace(physical[replica.endpoint_id], response_mode=response_mode),
                        before_request=before_request)
                self.transports[replica.endpoint_id] = child
        except BaseException as error:
            try:
                self.abort_startup()
            except BaseException as cleanup:
                error.add_note("Replica startup cleanup also failed: " + type(cleanup).__name__)
            raise

    def _observe(self, replica, event):
        if self._observer:
            self._observer(dict(event, endpoint_id=replica.endpoint_id,
                model_role=self.directory.role_id, location_id=replica.location_id,
                deployment_sha256=replica.deployment_sha256, replica_set_sha256=self._directory_sha256))

    async def execute(self, task, endpoint):
        if endpoint not in self.transports:
            raise ValueError("unknown model replica")
        replica = next(r for r in self.directory.replicas if r.endpoint_id == endpoint)
        self._observe(replica, dict(event="replica_transport_started", key=asdict(task.key),
            job_id=task.spec.job_id, flow_id=task.spec.flow_id,
            task_info=asdict(task.task.info) if task.task.info else None))
        # The child uses its legacy local name; the Core keeps the physical ID.
        return await self.transports[endpoint].execute(task, "model")

    def cancel_pending(self, key):
        for child in self.transports.values():
            cancel = getattr(child, "cancel_pending", None)
            if cancel is not None:
                cancel(key)

    def abort_startup(self):
        failures = []
        for child in reversed(tuple(self.transports.values())):
            abort = getattr(child, "abort_startup", None)
            if abort is not None:
                try:
                    abort()
                except BaseException as error:
                    failures.append(error)
        if failures:
            raise RuntimeError("replica startup cleanup could not be confirmed") from failures[0]

    async def close(self):
        # The first Ray child may own the shared driver connection. Close it last.
        failures = []
        for child in reversed(tuple(self.transports.values())):
            try:
                await child.close()
            except BaseException as error:
                failures.append(error)
        if failures:
            raise RuntimeError("replica transport cleanup could not be confirmed") from failures[0]


def build_model_replica_execution(config, *, replicas, response_mode="completion", physical=None,
                                  before_request=None, max_tasks=None, max_active_requests=None,
                                  active_work=None, organize=None, **options):
    """Real assembly for Map gateways and complete-response NativeTaskSession consumers."""
    if type(replicas) is not ModelReplicaSet or config.model_id != replicas.model_id:
        raise ValueError("model replica directory differs from the fixed model")
    if any(options.get(name) is not None for name in (
            "execute", "policies", "transport_factory", "preparation_factory")):
        raise ValueError("replica execution owns routing and transport")
    for name in ("execute", "policies", "transport_factory", "preparation_factory"):
        options.pop(name, None)
    requests = replicas.max_active_requests if max_active_requests is None else max_active_requests
    work = replicas.max_active_work if active_work is None else active_work
    _positive(requests, "execution request capacity")
    _positive(work, "execution work capacity")
    if requests > replicas.max_active_requests or work > replicas.max_active_work:
        raise ValueError("execution capacity exceeds the declared global capacity")
    tasks = requests if max_tasks is None else max_tasks
    now = time.monotonic()
    topology = TopologySnapshot(tuple(EndpointSnapshot(r.endpoint_id, r.endpoint_url, replicas.role_id,
        ",".join(r.device_ids), True, 0, 0, None, now) for r in replicas.replicas), now)
    policies = SessionPolicies(StaticAdmissionController(requests), StaticReplicaRouter(replicas),
        topology, replicas.role_id, organize=organize or WorkWindowOrganizer(tasks, work))
    execution = build_fixed_model_execution(config, max_tasks=tasks, max_active_requests=requests,
        active_work=work, organize=None, policies=policies,
        transport_factory=partial(ReplicaTransport, directory=replicas, response_mode=response_mode,
                                  physical=physical, before_request=before_request), **options)

    cleanup_failure = None

    def close(timeout=5.0):
        nonlocal cleanup_failure
        if cleanup_failure is not None:
            raise RuntimeError("replica cleanup previously failed") from cleanup_failure
        if execution.engine.capacity.records or execution.engine.jobs.jobs:
            return False
        try:
            return execution.close(timeout)
        except Exception as error:
            cleanup_failure = error
            raise

    return replace(execution, close=close)


def replica_usage(execution):
    """Derive owner-thread observations from Core records, including unknown work."""
    result = {endpoint.endpoint_id: dict(active_requests=0, active_work=0, uncertain_requests=0)
              for endpoint in execution.engine.policies.topology.endpoints}
    for record in execution.engine.capacity.records.values():
        if record.compute:
            usage = result[record.endpoint]
            usage["active_requests"] += 1
            usage["active_work"] += record.task.estimated_work
            usage["uncertain_requests"] += record.phase == "UNCERTAIN"
    return result
