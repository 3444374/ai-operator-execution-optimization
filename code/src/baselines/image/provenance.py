"""Auditable provenance and formal-use rules for image execution arms."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ImageArmProvenance:
    """Describe who owns scheduling and whether an arm is a formal baseline."""

    role: str
    implementation_provenance: str
    scheduler_owner: str
    custom_scheduling_code: bool
    formal_baseline_eligible: bool
    upstream_source: str


IMAGE_ARM_PROVENANCE = {
    "daft_builtin_embed": ImageArmProvenance(
        role="framework_native_baseline",
        implementation_provenance="vendor_builtin_ai_function",
        scheduler_owner="daft",
        custom_scheduling_code=False,
        formal_baseline_eligible=True,
        upstream_source="https://docs.daft.ai/en/stable/api/functions/embed_image/",
    ),
    "ray_data_staged": ImageArmProvenance(
        role="framework_native_api_baseline",
        implementation_provenance="official_api_with_workload_udfs",
        scheduler_owner="ray_data",
        custom_scheduling_code=False,
        formal_baseline_eligible=True,
        upstream_source="https://docs.ray.io/en/latest/data/batch_inference.html",
    ),
    "daft_native": ImageArmProvenance(
        role="diagnostic_reference",
        implementation_provenance="project_authored_daft_udf",
        scheduler_owner="daft",
        custom_scheduling_code=False,
        formal_baseline_eligible=False,
        upstream_source="https://docs.daft.ai/en/stable/custom-code/gpu/",
    ),
    "daft_ray": ImageArmProvenance(
        role="diagnostic_reference",
        implementation_provenance="project_authored_daft_udf",
        scheduler_owner="daft",
        custom_scheduling_code=False,
        formal_baseline_eligible=False,
        upstream_source="https://docs.daft.ai/en/stable/custom-code/gpu/",
    ),
    "daft_staged": ImageArmProvenance(
        role="diagnostic_reference",
        implementation_provenance="project_authored_daft_cpu_and_gpu_udfs",
        scheduler_owner="daft",
        custom_scheduling_code=False,
        formal_baseline_eligible=False,
        upstream_source=(
            "https://help.aliyun.com/en/polardb/polardb-for-postgresql/"
            "heterogeneous-operator-scheduling"
        ),
    ),
    "project_ray": ImageArmProvenance(
        role="project_method",
        implementation_provenance="project_implementation",
        scheduler_owner="project_ray",
        custom_scheduling_code=True,
        formal_baseline_eligible=False,
        upstream_source="local_project",
    ),
}


def image_arm_provenance(arm: str) -> ImageArmProvenance:
    """Return fail-closed provenance metadata for one registered image arm."""
    try:
        return IMAGE_ARM_PROVENANCE[arm]
    except KeyError as error:
        raise ValueError(f"image arm has no provenance contract: {arm}") from error


def require_formal_arm_allowed(
    arm: str,
    *,
    phase: str,
    allow_non_native_diagnostic: bool,
) -> None:
    """Reject project-authored reference UDFs masquerading as formal baselines."""
    provenance = image_arm_provenance(arm)
    if phase != "formal" or provenance.role != "diagnostic_reference":
        return
    if not allow_non_native_diagnostic:
        raise ValueError(
            f"{arm} is a project-authored diagnostic reference, not a native formal "
            "baseline; pass --allow-non-native-diagnostic only for explicitly labelled "
            "mechanism diagnostics"
        )
