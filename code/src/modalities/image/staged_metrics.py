"""Timing, byte counts and result assembly for staged image execution."""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from ...scheduling.runtime.stage_broker import StageBrokerSnapshot
from .contracts import ImageEmbeddingResult
from .execution import EmbeddingAudit, ExecutionResult


@dataclass
class StagedImageMetrics:
    first_output_s: float | None = None
    batch_completion_wall_s: list[float] = field(default_factory=list)
    batch_actor_service_s: list[float] = field(default_factory=list)
    batch_unattributed_wait_s: list[float] = field(default_factory=list)
    batch_preprocess_s: list[float] = field(default_factory=list)
    batch_host_copy_s: list[float] = field(default_factory=list)
    batch_h2d_s: list[float] = field(default_factory=list)
    batch_forward_s: list[float] = field(default_factory=list)
    batch_d2h_s: list[float] = field(default_factory=list)
    batch_source_next_s: list[float] = field(default_factory=list)
    batch_driver_materialize_s: list[float] = field(default_factory=list)
    batch_submit_s: list[float] = field(default_factory=list)
    batch_prepare_queue_s: list[float] = field(default_factory=list)
    batch_ready_residence_s: list[float] = field(default_factory=list)
    encoded_bytes: int = 0
    input_tensor_bytes: int = 0
    device_input_bytes: int = 0
    output_bytes: int = 0
    active_blocks_peak: int = 0
    encoded_bytes_peak: int = 0
    ready_bytes_peak: int = 0
    prepare_inflight_peak: int = 0
    model_inflight_peak: int = 0

    def record_peaks(self, snapshot: StageBrokerSnapshot) -> None:
        self.active_blocks_peak = max(self.active_blocks_peak, snapshot.active_blocks)
        self.encoded_bytes_peak = max(self.encoded_bytes_peak, snapshot.encoded_held_bytes)
        self.ready_bytes_peak = max(self.ready_bytes_peak, snapshot.ready_held_bytes)
        self.prepare_inflight_peak = max(self.prepare_inflight_peak, snapshot.prepare_inflight)
        self.model_inflight_peak = max(self.model_inflight_peak, snapshot.model_inflight)

    def record_model_result(self, result: ImageEmbeddingResult, completion_wall_s: float) -> None:
        self.batch_completion_wall_s.append(completion_wall_s)
        self.batch_actor_service_s.append(result.service_s)
        telemetry = result.telemetry
        self.batch_preprocess_s.append(telemetry.preprocess_s)
        self.batch_unattributed_wait_s.append(
            max(0.0, completion_wall_s - telemetry.preprocess_s - result.service_s)
        )
        self.batch_host_copy_s.append(telemetry.host_copy_s)
        if telemetry.h2d_s > 0:
            self.batch_h2d_s.append(telemetry.h2d_s)
        if telemetry.forward_s > 0:
            self.batch_forward_s.append(telemetry.forward_s)
        if telemetry.d2h_s > 0:
            self.batch_d2h_s.append(telemetry.d2h_s)
        self.encoded_bytes += telemetry.encoded_bytes
        self.input_tensor_bytes += telemetry.input_tensor_bytes
        self.device_input_bytes += telemetry.device_input_bytes
        self.output_bytes += telemetry.output_bytes

    def finish(self, *, total_s: float, submitted_batches: int,
               audit: EmbeddingAudit, ready_work_limit: float) -> ExecutionResult:
        engine_stats = json.dumps(
            {
                "broker": "bounded_stage_broker_v1",
                "encoded_bytes_peak": self.encoded_bytes_peak,
                "execution_mode": "hse_static",
                "model_inflight_peak": self.model_inflight_peak,
                "prepare_inflight_peak": self.prepare_inflight_peak,
                "ready_bytes_peak": self.ready_bytes_peak,
                "ready_work_limit": ready_work_limit,
            },
            sort_keys=True,
        )
        return ExecutionResult(
            total_s=total_s,
            first_output_s=self.first_output_s or total_s,
            audit=audit.finish(),
            batch_completion_wall_s=tuple(self.batch_completion_wall_s),
            batch_actor_service_s=tuple(self.batch_actor_service_s),
            batch_unattributed_wait_s=tuple(self.batch_unattributed_wait_s),
            batch_preprocess_s=tuple(self.batch_preprocess_s),
            batch_host_copy_s=tuple(self.batch_host_copy_s),
            batch_h2d_s=tuple(self.batch_h2d_s),
            batch_forward_s=tuple(self.batch_forward_s),
            batch_d2h_s=tuple(self.batch_d2h_s),
            batch_source_next_s=tuple(self.batch_source_next_s),
            batch_driver_materialize_s=tuple(self.batch_driver_materialize_s),
            batch_submit_s=tuple(self.batch_submit_s),
            batch_prepare_queue_s=tuple(self.batch_prepare_queue_s),
            batch_ready_residence_s=tuple(self.batch_ready_residence_s),
            encoded_bytes=self.encoded_bytes,
            input_tensor_bytes=self.input_tensor_bytes,
            device_input_bytes=self.device_input_bytes,
            output_bytes=self.output_bytes,
            submitted_batches=submitted_batches,
            pending_batches_peak=self.active_blocks_peak,
            encoded_bytes_peak=self.encoded_bytes_peak,
            ready_bytes_peak=self.ready_bytes_peak,
            prepare_inflight_peak=self.prepare_inflight_peak,
            model_inflight_peak=self.model_inflight_peak,
            execution_mode="hse_static",
            engine_stats=engine_stats,
        )
