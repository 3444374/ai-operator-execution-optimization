"""Checks around the existing author backend; no task or stage scheduler."""

import hashlib
import json
import os
from pathlib import Path
import re
import socket

from integrations.imlane_records import owner
from integrations.imlane_records.method_environment import preflight_method, validate_author_build
from integrations.imlane_records.model_process import assigned_environment
from integrations.imlane_records.resource_limits import owner_limits, gpu_snapshot, gpu_exited
from src.baselines.common.private_artifacts import require_outside_git
from src.infrastructure import runner_lease


def read_json(path, *, expected_sha256=None):
    path = Path(path)
    require_outside_git(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 8 * 1024**2:
        raise ValueError("configuration needs a bounded regular file outside Git")
    with path.open("rb") as stream:
        payload = stream.read(8 * 1024**2 + 1)
    if len(payload) > 8 * 1024**2:
        raise ValueError("JSON input exceeds its byte allowance")
    if expected_sha256 is not None and hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise ValueError("source or owner context changed")
    def reject(value):
        raise ValueError("nonfinite JSON is unsupported: " + value)
    return json.loads(payload, parse_constant=reject)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_owner(path, *, expected_sha256=None):
    value = read_json(path, expected_sha256=expected_sha256)
    required = {"schema", "author_root", "method_python", "bridge_binary", "artifact_root",
                "preflight_report", "model_environment", "owner_resource_limits", "lease_sha256",
                "budget", "gpu_idle_max_mib"}
    if type(value) is not dict or set(value) != required or value["schema"] != "semloom.imlane_owner.v1":
        raise ValueError("an explicit IMLane owner context is required")
    for name in ("author_root", "method_python", "bridge_binary", "artifact_root", "preflight_report"):
        if type(value[name]) is not str or not Path(value[name]).is_absolute():
            raise ValueError("owner paths must be absolute")
        require_outside_git(Path(value[name]))
    if (type(value["lease_sha256"]) is not str or len(value["lease_sha256"]) != 64
            or any(c not in "0123456789abcdef" for c in value["lease_sha256"])):
        raise ValueError("owner context must bind the normal external lease SHA")
    budget = value["budget"]
    if type(budget) is not dict or set(budget) != {"path", "id", "limit", "unit_id"}:
        raise ValueError("owner context needs an existing persistent budget unit")
    from src.experiments.attempt_ledger import AttemptBudget
    AttemptBudget(budget["id"], budget["limit"])
    if (type(budget["unit_id"]) is not str or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", budget["unit_id"])
            or not Path(budget["path"]).is_absolute()):
        raise ValueError("invalid existing unit or budget path")
    require_outside_git(Path(budget["path"]))
    if type(value["owner_resource_limits"]) is not dict:
        raise ValueError("the owner must declare explicit resource allowances")
    owner_limits(value["owner_resource_limits"])
    assigned_environment(value["model_environment"])
    if type(value["gpu_idle_max_mib"]) is not int or value["gpu_idle_max_mib"] < 0:
        raise ValueError("an explicit nonnegative idle GPU allowance is required")
    return value


def check_external_lease(value):
    """Read the owner's normal lease; this worker never acquires or releases it."""
    import psutil

    path = Path(value["artifact_root"]) / runner_lease.HOST_SCOPE_DIR_NAME / runner_lease.LEASE_NAME
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024**2:
        raise RuntimeError("external host lease changed")
    with path.open("rb") as stream:
        payload = stream.read(1024**2 + 1)
    if len(payload) > 1024**2 or hashlib.sha256(payload).hexdigest() != value["lease_sha256"]:
        raise RuntimeError("external host lease changed")
    record = runner_lease._decode_lease(payload, path)
    if (record["config_fingerprint"] != runner_lease.HOST_SCOPE_FINGERPRINT
            or record["hostname"] != socket.gethostname()
            or not record["process_start_id"]
            or runner_lease._process_start_id(record["pid"]) != record["process_start_id"]
            or record["pid"] not in {os.getpid(), *(p.pid for p in psutil.Process().parents())}):
        raise RuntimeError("the live normal host lease must belong to this worker's owner ancestry")
    return dict(sha256=value["lease_sha256"], ownership="external owner", released_by_worker=False)


def preflight_owner(value, engine):
    check_external_lease(value)
    if read_json(value["preflight_report"]).get("status") != "ok":
        raise ValueError("a successful current runtime preflight report is required")
    limits = owner_limits(value["owner_resource_limits"])
    if not hasattr(os, "sched_getaffinity") or len(os.sched_getaffinity(0)) > limits["cpu_cores"]:
        raise ValueError("the owner must launch this child inside its declared CPU allowance")
    build = validate_author_build(value["author_root"], value["bridge_binary"])
    abi = preflight_method(value["method_python"], value["author_root"], Path(__file__).parents[2], chain=True)
    return dict(author=build, method_abi=abi, engine_environment=engine.validate_driver_environment(),
                external_lease=check_external_lease(value), owner_resource_limits=limits)


class AuthorRunGuard:
    """Observe actual author closures before the shared owner advances a phase."""

    def __init__(self, value, args):
        self.value, self.args = value, args
        self.seen, self.phase_gpu_exit = set(), []
        check_external_lease(value)
        self.before = gpu_snapshot(value["model_environment"]["CUDA_VISIBLE_DEVICES"])
        if self.before["compute_processes"] or self.before["memory_mib"] > value["gpu_idle_max_mib"]:
            raise RuntimeError("the owner's assigned GPU is not idle before this query")

    def _gpu(self):
        after = gpu_snapshot(self.value["model_environment"]["CUDA_VISIBLE_DEVICES"])
        confirmed = (gpu_exited(self.before, after)
                     and after["memory_mib"] <= self.value["gpu_idle_max_mib"])
        return dict(snapshot=after, gpu_exit_confirmed=confirmed)

    def __call__(self):
        check_external_lease(self.value)
        self.args.stop_budget.check()
        for scope in self.args.scopes:
            if scope.closed and scope.namespace not in self.seen:
                if owner.resource_state(scope)["resources_confirmed"] is not True:
                    raise RuntimeError("author resources remain unknown before the next phase")
                evidence = dict(namespace=scope.namespace, **self._gpu())
                self.phase_gpu_exit.append(evidence)
                if evidence["gpu_exit_confirmed"] is not True:
                    raise RuntimeError("GPU exit is unconfirmed; the next author phase must not start")
                self.seen.add(scope.namespace)
        return False

    def finish(self):
        evidence = self._gpu()
        lease = dict(external_lease_valid=True)
        try:
            check_external_lease(self.value)
        except BaseException as error:
            lease = dict(external_lease_valid=False, lease_error_type=type(error).__name__)
        return dict(gpu_before=self.before, phase_gpu_exit=list(self.phase_gpu_exit), **evidence, **lease,
                    external_host_lease_released_by_worker=False,
                    process_exit_scope="author scope receipts and outer production supervisor")
