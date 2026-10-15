"""Pinned single-node native vLLM DP/TP launch configuration for any modality."""
from dataclasses import asdict, dataclass
import os

from src.infrastructure.gpu_allocation import resolve_gpu_allocation


@dataclass(frozen=True)
class VllmResources:
    gpu_count: int | str = "auto"
    allowed_gpus: tuple[str, ...] | None = None
    tensor_parallel_size: int = 1

    def resolve(self, server_args, *, environ=None, **probe_options):
        env = os.environ if environ is None else environ
        if type(self.tensor_parallel_size) is not int or not 1 <= self.tensor_parallel_size <= 256:
            raise ValueError("vLLM tensor parallel size must be an integer from 1 to 256")
        blocked = ("--tensor-parallel", "--pipeline-parallel", "--data-parallel", "--distributed-executor",
                   "--enable-expert-parallel", "--enable-elastic-ep", "--headless", "--nnodes", "--node-rank",
                   "--decode-context-parallel", "--prefill-context-parallel", "--device-ids", "--config")
        aliases = ("-tp", "-pp", "-dp", "-dcp", "-pcp", "-ep", "-n", "-r")
        if any(arg.split("=", 1)[0].replace("_", "-").startswith(blocked + aliases) for arg in server_args):
            raise ValueError("resource config owns native vLLM topology; remove parallel/backend/headless overrides from server arguments")
        if any(key.startswith("VLLM_DP_") for key in env):
            raise ValueError("native local DP resource config does not accept VLLM_DP environment overrides")
        allocation = resolve_gpu_allocation(self.gpu_count, self.allowed_gpus, environ=env, **probe_options)
        if allocation.count % self.tensor_parallel_size:
            raise ValueError("allocated GPU count must be divisible by vLLM tensor parallel size")
        dp = allocation.count // self.tensor_parallel_size
        argv = [*server_args, "--tensor-parallel-size", str(self.tensor_parallel_size),
                "--pipeline-parallel-size", "1", "--data-parallel-size", str(dp),
                "--data-parallel-backend", "mp", "--distributed-executor-backend", "mp"]
        report = dict(allocation.report(), data_parallel_size=dp,
                      tensor_parallel_size=self.tensor_parallel_size, pipeline_parallel_size=1,
                      model_scheduler_owner="vllm_native", load_balancing="vllm_internal",
                      scope="single_node", version="0.25.1",
                      rank_gpu_uuids=[allocation.selected_gpu_uuids[i:i + self.tensor_parallel_size]
                                      for i in range(0, allocation.count, self.tensor_parallel_size)],
                      identity_scope="requested rank mapping; live process/GPU binding requires replica inspection")
        return allocation, argv, dict(resources_requested=asdict(self), resources_resolved=report)


def verify_parsed_launch(parsed, resolved, port):
    """Check the vendor parser's effective settings before sealing the sidecar."""
    expected = dict(data_parallel_size=resolved["data_parallel_size"],
        tensor_parallel_size=resolved["tensor_parallel_size"], pipeline_parallel_size=1,
        data_parallel_backend="mp", distributed_executor_backend="mp", nnodes=1, node_rank=0,
        headless=False, data_parallel_rank=None, data_parallel_start_rank=None,
        data_parallel_size_local=None, data_parallel_hybrid_lb=False,
        data_parallel_external_lb=False, data_parallel_multi_port_external_lb=False,
        enable_expert_parallel=False, enable_elastic_ep=False, device_ids=None,
        decode_context_parallel_size=1, prefill_context_parallel_size=1,
        port=port, uds=None, grpc=False)
    if any(not hasattr(parsed, name) or getattr(parsed, name) != value for name, value in expected.items()):
        raise ValueError("effective vLLM parser configuration differs from the local native DP allocation/endpoint")
