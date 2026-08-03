"""Same-request baseline adapters and artifact contracts."""

from .text.ceilings import (
    VllmBenchConfig,
    build_vllm_bench_command,
    write_vllm_custom_dataset,
)
from .text.controls import BoundedHttpConfig, run_bounded_http
from .common.contracts import BaselineRequestResult, ChatRequest, ManifestMetadata
from .common.manifests import (
    assign_endpoint_shards,
    read_manifest,
    write_manifest,
)
from .text.products import (
    OceanBaseConfig,
    SqlStatement,
    build_ai_complete_sql,
    build_register_model_sql,
    run_oceanbase_ai_complete,
)
from .text.orchestration.postgres_manifest import load_postgres_requests
from .common.provenance import AdapterProvenance, adapter_provenance
from .common.results import (
    summarize_group_service_counters,
    summarize_results,
    validate_results,
)
from .text.frameworks import (
    DaftPromptConfig,
    RayDataHttpConfig,
    run_daft_prompt,
    run_ray_data_http,
)

__all__ = [
    "AdapterProvenance",
    "BaselineRequestResult",
    "BoundedHttpConfig",
    "ChatRequest",
    "DaftPromptConfig",
    "ManifestMetadata",
    "OceanBaseConfig",
    "RayDataHttpConfig",
    "SqlStatement",
    "VllmBenchConfig",
    "assign_endpoint_shards",
    "adapter_provenance",
    "build_ai_complete_sql",
    "build_register_model_sql",
    "build_vllm_bench_command",
    "read_manifest",
    "run_bounded_http",
    "run_daft_prompt",
    "run_oceanbase_ai_complete",
    "run_ray_data_http",
    "load_postgres_requests",
    "summarize_group_service_counters",
    "summarize_results",
    "validate_results",
    "write_vllm_custom_dataset",
    "write_manifest",
]
