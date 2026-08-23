"""Typed, side-effect-free contracts for the SAOR DB-E2E matrix."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class JobManifestIdentity:
    job_id: str
    path: str
    rows: int
    sha256: str


@dataclass(frozen=True)
class JobReleaseEpoch:
    """External Job release, independent of executor request-arrival replay."""

    job_id: str
    release_time_s: float


@dataclass(frozen=True)
class JobObservationIdentity:
    """Freeze the external role, entitlement, and request SLO for one Job."""

    job_id: str
    role: str
    weight: float
    request_slo_s: float
    job_jct_slo_s: float | None = None


@dataclass(frozen=True)
class MfuContract:
    status: str
    gpu_peak_tflops_per_gpu: float
    precision: str
    reason: str


@dataclass(frozen=True)
class MatchedArm:
    arm_id: str
    kind: str
    scheduler_owner: str
    output_root: str
    manifest_path: str
    manifest_sha256: str
    job_manifests: tuple[JobManifestIdentity, ...]
    endpoint_ids: tuple[str, ...]
    service_signature: tuple[tuple[str, object], ...]
    protocol: str
    output_cap: int
    job_release_schedule: tuple[JobReleaseEpoch, ...]
    arrival_replay_capability: str
    job_internal_arrival_contract: str
    performance_writeback_mode: str
    unsupported_request_tails: tuple[tuple[str, object], ...]
    source: tuple[tuple[str, object], ...]
    organizer: str
    calibration_path: str
    calibration_sha256: str
    mfu_contract: MfuContract
    project_contract: tuple[tuple[str, object], ...] = ()
    raw_field_names: tuple[str, ...] = ()

    def project_value(self, name: str) -> object | None:
        return dict(self.project_contract).get(name)

    @property
    def arrival_offsets_s(self) -> tuple[float, ...]:
        """Compatibility view for executor configs whose field is named offsets."""

        return tuple(item.release_time_s for item in self.job_release_schedule)


@dataclass(frozen=True)
class MatchedSystemConfig:
    seed: int
    warmup_repeats: int
    formal_repeats: int
    matrix_output_root: str
    endpoint_urls: tuple[str, ...]
    gpu_formal_locally_authorized: bool
    matched_manifest_status: str
    service_identity: tuple[tuple[str, object], ...]
    arms: tuple[MatchedArm, ...]
    job_observation_contracts: tuple[JobObservationIdentity, ...] = ()
    observation_gateway_request_timeout_s: float = 600.0


@dataclass(frozen=True)
class ScheduledMatchedCell:
    phase: str
    repeat: int
    order_index: int
    arm_id: str
    report_blocks: tuple[str, ...]
