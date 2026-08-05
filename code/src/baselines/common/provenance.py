"""Classify text comparison arms and fail closed on native-baseline claims."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal


ComparisonRole = Literal[
    "service_ceiling",
    "direct_client_control",
    "framework_native_baseline",
    "database_product_native_baseline",
]


@dataclass(frozen=True)
class AdapterProvenance:
    """Auditable ownership metadata for one text comparison adapter."""

    comparison_role: ComparisonRole
    implementation_provenance: str
    scheduler_owner: str
    custom_scheduling_code: bool
    formal_baseline_eligible: bool
    formal_control_eligible: bool
    upstream_source: str
    qualification_gate: str

    def summary_fields(self) -> dict[str, object]:
        """Return stable fields written into every run summary."""

        return asdict(self)


_ADAPTER_PROVENANCE = {
    "vllm_bench": AdapterProvenance(
        comparison_role="service_ceiling",
        implementation_provenance="vendor_cli",
        scheduler_owner="vllm_bench_and_vllm_server",
        custom_scheduling_code=False,
        formal_baseline_eligible=False,
        formal_control_eligible=True,
        upstream_source="https://docs.vllm.ai/en/stable/cli/bench/serve/",
        qualification_gate="same_manifest_model_protocol_and_endpoints",
    ),
    "bounded_http": AdapterProvenance(
        comparison_role="direct_client_control",
        implementation_provenance="project_bounded_async_client",
        scheduler_owner="project_asyncio_control",
        custom_scheduling_code=True,
        formal_baseline_eligible=False,
        formal_control_eligible=True,
        upstream_source="local_project_control",
        qualification_gate="same_manifest_model_protocol_and_endpoints",
    ),
    "bounded_completions": AdapterProvenance(
        comparison_role="direct_client_control",
        implementation_provenance="project_batched_completions_client",
        scheduler_owner="project_asyncio_control",
        custom_scheduling_code=True,
        formal_baseline_eligible=False,
        formal_control_eligible=True,
        upstream_source="local_project_control",
        qualification_gate="completions_track_only",
    ),
    "daft_native": AdapterProvenance(
        comparison_role="framework_native_baseline",
        implementation_provenance="vendor_native_api_graph",
        scheduler_owner="daft_native_runner",
        custom_scheduling_code=False,
        formal_baseline_eligible=True,
        formal_control_eligible=True,
        upstream_source="https://docs.daft.ai/en/stable/ai-functions/prompt/",
        qualification_gate="builtin_daft_functions_prompt_only",
    ),
    "daft_ray": AdapterProvenance(
        comparison_role="framework_native_baseline",
        implementation_provenance="vendor_native_api_graph",
        scheduler_owner="daft_ray_runner",
        custom_scheduling_code=False,
        formal_baseline_eligible=True,
        formal_control_eligible=True,
        upstream_source="https://docs.daft.ai/en/stable/ai-functions/prompt/",
        qualification_gate="builtin_daft_functions_prompt_only",
    ),
    "ray_data_http": AdapterProvenance(
        comparison_role="framework_native_baseline",
        implementation_provenance="vendor_native_api_graph",
        scheduler_owner="ray_data",
        custom_scheduling_code=False,
        formal_baseline_eligible=True,
        formal_control_eligible=True,
        upstream_source=(
            "https://docs.ray.io/en/latest/data/api/doc/"
            "ray.data.llm.HttpRequestProcessorConfig.html"
        ),
        qualification_gate="official_http_processor_without_project_credit",
    ),
    "oceanbase": AdapterProvenance(
        comparison_role="database_product_native_baseline",
        implementation_provenance="vendor_builtin_sql_ai_function",
        scheduler_owner="oceanbase",
        custom_scheduling_code=False,
        formal_baseline_eligible=True,
        formal_control_eligible=True,
        upstream_source=(
            "https://en.oceanbase.com/docs/common-oceanbase-database-"
            "10000000003678975"
        ),
        qualification_gate="oceanbase_ai_function_capability_and_same_endpoint",
    ),
    "duckdb_ai": AdapterProvenance(
        comparison_role="database_product_native_baseline",
        implementation_provenance="duckdb_community_ai_extension",
        scheduler_owner="duckdb_sql_executor_and_ai_community_extension",
        custom_scheduling_code=False,
        formal_baseline_eligible=True,
        formal_control_eligible=True,
        upstream_source=(
            "https://duckdb.org/community_extensions/extensions/ai.html"
        ),
        qualification_gate=(
            "bounded_output_zero_error_extension_version_and_same_endpoint"
        ),
    ),
}


def adapter_provenance(adapter: str) -> AdapterProvenance:
    """Return registered provenance, rejecting unclassified adapters."""

    try:
        provenance = _ADAPTER_PROVENANCE[adapter]
    except KeyError as exc:
        raise ValueError(
            f"adapter {adapter!r} has no provenance classification"
        ) from exc
    if provenance.formal_baseline_eligible and provenance.custom_scheduling_code:
        raise ValueError(
            f"adapter {adapter!r} cannot be both native and project-scheduled"
        )
    return provenance


def registered_adapters() -> tuple[str, ...]:
    """Return adapters in deterministic registration order."""

    return tuple(_ADAPTER_PROVENANCE)
