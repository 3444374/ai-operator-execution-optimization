"""Pinned single-node native vLLM DP/TP launch configuration for any modality."""

from dataclasses import asdict, dataclass
import os
import re

from src.infrastructure.gpu_allocation import resolve_gpu_allocation


def validate_resource_identity(requested, resolved):
    """Validate a launch declaration without probing GPUs or claiming live binding."""
    requested_fields = {"gpu_count", "allowed_gpus", "tensor_parallel_size"}
    constants = dict(
        adaptation="deployment capacity only; no performance tuning",
        pipeline_parallel_size=1,
        model_scheduler_owner="vllm_native",
        load_balancing="vllm_internal",
        scope="single_node",
        version="0.25.1",
        identity_scope="requested rank mapping; live process/GPU binding requires replica inspection",
    )
    fields = {
        "requested_count",
        "requested_devices",
        "allocation_source",
        "allowed_gpu_uuids",
        "selected_gpu_uuids",
        "resolved_count",
        "data_parallel_size",
        "tensor_parallel_size",
        "rank_gpu_uuids",
        *constants,
    }
    if (
        type(requested) is not dict
        or set(requested) != requested_fields
        or type(resolved) is not dict
        or set(resolved) != fields
    ):
        raise ValueError("vLLM resource identity fields are invalid")

    def count(value):
        if type(value) is not int or not 1 <= value <= 256:
            raise ValueError("vLLM resource identity count must be an integer from 1 to 256")
        return value

    def devices(value, *, uuids=False):
        pattern = r"GPU-[A-Za-z0-9-]+" if uuids else r"(?:[0-9]+|GPU-[A-Za-z0-9-]+)"
        if (
            type(value) not in (list, tuple)
            or not 1 <= len(value) <= 256
            or any(type(item) is not str or not re.fullmatch(pattern, item) for item in value)
            or len(set(value)) != len(value)
        ):
            raise ValueError("vLLM resource identity devices are invalid")
        return tuple(value)

    wanted = requested["gpu_count"]
    if wanted != "auto":
        count(wanted)
    if (
        type(resolved["requested_count"]) is not type(wanted)
        or resolved["requested_count"] != wanted
    ):
        raise ValueError("vLLM requested GPU count differs from its declaration")
    n, tp, dp = (
        count(resolved[name])
        for name in ("resolved_count", "tensor_parallel_size", "data_parallel_size")
    )
    if count(requested["tensor_parallel_size"]) != tp or dp * tp != n:
        raise ValueError("vLLM resource identity topology is inconsistent")
    for name, expected in constants.items():
        if type(resolved[name]) is not type(expected) or resolved[name] != expected:
            raise ValueError("vLLM resource identity differs in " + name)
    declared = devices(resolved["requested_devices"])
    allowed = devices(resolved["allowed_gpu_uuids"], uuids=True)
    selected = devices(resolved["selected_gpu_uuids"], uuids=True)
    if (
        len(selected) != n
        or selected != allowed[:n]
        or n != (len(allowed) if wanted == "auto" else wanted)
    ):
        raise ValueError("vLLM resource identity selection is inconsistent")
    source = resolved["allocation_source"]
    if source not in ("allowed_gpus", "GPU_IDS", "CUDA_VISIBLE_DEVICES"):
        raise ValueError("vLLM resource identity allocation source is invalid")
    explicit = requested["allowed_gpus"]
    if explicit is not None:
        if type(explicit) is str:
            explicit = explicit.split(",")
        if source != "allowed_gpus" or devices(explicit) != declared:
            raise ValueError("vLLM resource identity explicit allocation differs")
    elif source == "allowed_gpus":
        raise ValueError("vLLM resource identity explicit allocation is absent")
    if source == "CUDA_VISIBLE_DEVICES" and any(not value.startswith("GPU-") for value in declared):
        raise ValueError("vLLM resource identity CUDA ordinals are ambiguous")
    if len(allowed) > len(declared):
        raise ValueError("vLLM resource identity exceeds its declared allocation")
    if all(value.startswith("GPU-") for value in declared):
        if tuple(value for value in declared if value in allowed) != allowed:
            raise ValueError("vLLM resource identity UUID allocation order differs")
    ranks = resolved["rank_gpu_uuids"]
    if type(ranks) not in (list, tuple) or [devices(rank, uuids=True) for rank in ranks] != [
        selected[i : i + tp] for i in range(0, n, tp)
    ]:
        raise ValueError("vLLM resource identity rank mapping differs")


def verify_resource_argv(argv, resolved):
    """Match the declaration to live canonical topology flags, without GPU inference."""
    expected = {
        "--tensor-parallel-size": str(resolved["tensor_parallel_size"]),
        "--pipeline-parallel-size": "1",
        "--data-parallel-size": str(resolved["data_parallel_size"]),
        "--data-parallel-backend": "mp",
        "--distributed-executor-backend": "mp",
    }
    blocked = (
        "--tensor-parallel",
        "--pipeline-parallel",
        "--data-parallel",
        "--distributed-executor",
        "--enable-expert-parallel",
        "--enable-elastic-ep",
        "--headless",
        "--nnodes",
        "--node-rank",
        "--decode-context-parallel",
        "--prefill-context-parallel",
        "--device-ids",
        "--config",
        "-tp",
        "-pp",
        "-dp",
        "-dcp",
        "-pcp",
        "-ep",
        "-n",
        "-r",
    )
    seen = set()
    for index, argument in enumerate(argv):
        flag, separator, inline = argument.partition("=")
        if not flag.replace("_", "-").startswith(blocked):
            continue
        if flag not in expected or flag in seen:
            raise ValueError("live vLLM resource topology has duplicate or unsupported overrides")
        actual = inline if separator else (argv[index + 1] if index + 1 < len(argv) else None)
        if actual != expected[flag]:
            raise ValueError("live vLLM resource topology differs in " + flag)
        seen.add(flag)
    if seen != set(expected):
        raise ValueError("live vLLM resource topology flags are incomplete")


@dataclass(frozen=True)
class VllmResources:
    gpu_count: int | str = "auto"
    allowed_gpus: tuple[str, ...] | None = None
    tensor_parallel_size: int = 1

    def resolve(self, server_args, *, environ=None, **probe_options):
        env = os.environ if environ is None else environ
        if type(self.tensor_parallel_size) is not int or not 1 <= self.tensor_parallel_size <= 256:
            raise ValueError("vLLM tensor parallel size must be an integer from 1 to 256")
        blocked = (
            "--tensor-parallel",
            "--pipeline-parallel",
            "--data-parallel",
            "--distributed-executor",
            "--enable-expert-parallel",
            "--enable-elastic-ep",
            "--headless",
            "--nnodes",
            "--node-rank",
            "--decode-context-parallel",
            "--prefill-context-parallel",
            "--device-ids",
            "--config",
        )
        aliases = ("-tp", "-pp", "-dp", "-dcp", "-pcp", "-ep", "-n", "-r")
        if any(
            arg.split("=", 1)[0].replace("_", "-").startswith(blocked + aliases)
            for arg in server_args
        ):
            raise ValueError(
                "resource config owns native vLLM topology; remove parallel/backend/headless overrides from server arguments"
            )
        if any(key.startswith("VLLM_DP_") for key in env):
            raise ValueError(
                "native local DP resource config does not accept VLLM_DP environment overrides"
            )
        allocation = resolve_gpu_allocation(
            self.gpu_count, self.allowed_gpus, environ=env, **probe_options
        )
        if allocation.count % self.tensor_parallel_size:
            raise ValueError("allocated GPU count must be divisible by vLLM tensor parallel size")
        dp = allocation.count // self.tensor_parallel_size
        argv = [
            *server_args,
            "--tensor-parallel-size",
            str(self.tensor_parallel_size),
            "--pipeline-parallel-size",
            "1",
            "--data-parallel-size",
            str(dp),
            "--data-parallel-backend",
            "mp",
            "--distributed-executor-backend",
            "mp",
        ]
        report = dict(
            allocation.report(),
            data_parallel_size=dp,
            tensor_parallel_size=self.tensor_parallel_size,
            pipeline_parallel_size=1,
            model_scheduler_owner="vllm_native",
            load_balancing="vllm_internal",
            scope="single_node",
            version="0.25.1",
            rank_gpu_uuids=[
                allocation.selected_gpu_uuids[i : i + self.tensor_parallel_size]
                for i in range(0, allocation.count, self.tensor_parallel_size)
            ],
            identity_scope="requested rank mapping; live process/GPU binding requires replica inspection",
        )
        return allocation, argv, dict(resources_requested=asdict(self), resources_resolved=report)


def verify_parsed_launch(parsed, resolved, port):
    """Check the vendor parser's effective settings before sealing the sidecar."""
    expected = dict(
        data_parallel_size=resolved["data_parallel_size"],
        tensor_parallel_size=resolved["tensor_parallel_size"],
        pipeline_parallel_size=1,
        data_parallel_backend="mp",
        distributed_executor_backend="mp",
        nnodes=1,
        node_rank=0,
        headless=False,
        data_parallel_rank=None,
        data_parallel_start_rank=None,
        data_parallel_size_local=None,
        data_parallel_hybrid_lb=False,
        data_parallel_external_lb=False,
        data_parallel_multi_port_external_lb=False,
        enable_expert_parallel=False,
        enable_elastic_ep=False,
        device_ids=None,
        decode_context_parallel_size=1,
        prefill_context_parallel_size=1,
        port=port,
        uds=None,
        grpc=False,
    )
    if any(
        not hasattr(parsed, name) or getattr(parsed, name) != value
        for name, value in expected.items()
    ):
        raise ValueError(
            "effective vLLM parser configuration differs from the local native DP allocation/endpoint"
        )
