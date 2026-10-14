"""Explicit CLIP input reuse without caching or shortening model outputs."""

from __future__ import annotations

import time
from dataclasses import dataclass, replace

import numpy as np

from ...data.materializers.prepared_assets import PreparedAssetKey
from .clip import ClipTensorActor
from .input_residency import DeviceInputCache, InputTransferCost, prepared_tensor_key


class InputCompletionUnknown(RuntimeError):
    """A retained input still has potentially running device consumers."""


@dataclass(frozen=True)
class InputReuseObservation:
    key: PreparedAssetKey
    transfer: InputTransferCost
    identity_s: float
    identity_copy_bytes: int
    host_ownership_copy_bytes: int
    host_copy_s: float
    h2d_s: float | None
    settle_s: float | None = None


class ReusableClipTensorActor(ClipTensorActor):
    """Optional whole-batch input reuse on one serial CLIP actor.

    The normal worker builder continues to use ClipTensorActor. A borrowed pool
    can explicitly wrap this class; input_cache_bytes=0 avoids hashing and keeps
    the original conversion. The cache budget covers only retained logical
    tensor bytes, and defaults to at most eight entries. Oversized inputs use
    the original path. No extra model consumers or retry are introduced.
    """

    def __init__(self, model_revision: str, *, input_cache_bytes: int = 0,
                 input_cache_entries: int = 8, **kwargs):
        self._inputs = DeviceInputCache(
            budget_bytes=input_cache_bytes, max_entries=input_cache_entries,
            location=kwargs.get("device", "cuda"))
        self._cache_enabled = input_cache_bytes > 0
        self._input_element_bytes = {"float16": 2, "float32": 4, "bfloat16": 2}.get(
            kwargs.get("dtype", "float16"))
        self._active_input = None
        self._last_input_observation = None
        super().__init__(model_revision, **kwargs)

    def input_residency_snapshot(self):
        """Small immutable metadata; never return resident tensor payloads."""
        return self._inputs.snapshot()

    def input_transfer_cost(self, key: PreparedAssetKey, *, host_bytes: int,
                            device_bytes: int) -> InputTransferCost:
        return self._inputs.transfer_cost(key, host_bytes=host_bytes, device_bytes=device_bytes)

    def input_reuse_observation(self) -> InputReuseObservation | None:
        return self._last_input_observation

    def clear_input_residency(self) -> None:
        self._inputs.clear()

    def synchronize(self) -> None:
        super().synchronize()
        self._inputs.confirm_synchronized()

    def embed(self, batch):
        if not self._cache_enabled:
            return super().embed(batch)
        started = time.perf_counter()
        snapshot = self._inputs.snapshot()
        if snapshot.unknown_references or snapshot.reserved_bytes:
            raise InputCompletionUnknown("confirm this actor's device completion before reuse")
        succeeded = False
        self._last_input_observation = None
        try:
            result = super().embed(batch)
            succeeded = True
        finally:
            pending = self._active_input is not None or bool(self._inputs.snapshot().reserved_bytes)
            settle_started = time.perf_counter()
            try:
                self._finish_input(succeeded=succeeded)
            finally:
                if self._last_input_observation is not None:
                    self._last_input_observation = replace(
                        self._last_input_observation,
                        settle_s=time.perf_counter() - settle_started if pending else None)
        return replace(result, service_s=time.perf_counter() - started)

    def _device_input(self, payload: np.ndarray):
        if not self._cache_enabled:
            return super()._device_input(payload)
        started = time.perf_counter()
        identity_copy_bytes = 0 if payload.flags.c_contiguous else int(payload.nbytes)
        key = prepared_tensor_key(
            payload, processor_revision=self.semantics.processor_revision,
            device_dtype=str(self._dtype))
        identity_s = time.perf_counter() - started
        device_bytes = int(payload.size * self._input_element_bytes)
        measurements = []
        host_device = self._device.startswith("cpu")

        def load():
            # CPU as_tensor can alias a writable NumPy array. Retained inputs
            # must remain owned even if the caller subsequently mutates it.
            copy_started = time.perf_counter()
            owned = payload.copy() if host_device else payload
            copy_s = time.perf_counter() - copy_started if host_device else 0.0
            tensor, host_copy_s, h2d_s = super(ReusableClipTensorActor, self)._device_input(owned)
            measurements.append((host_copy_s + copy_s, h2d_s))
            return tensor

        lease = self._inputs.acquire(key, logical_bytes=device_bytes, load=load)
        if lease is None:
            pixel_values, host_copy_s, h2d_s = super()._device_input(payload)
            hit = False
            host_copy_bytes = 0 if payload.flags.writeable else int(payload.nbytes)
        else:
            self._active_input = lease
            pixel_values, hit = lease.payload, lease.cache_hit
            host_copy_s, h2d_s = measurements[0] if measurements else (0.0, 0.0)
            host_copy_bytes = (int(payload.nbytes)
                               if not hit and (host_device or not payload.flags.writeable)
                               else 0)
        cost = InputTransferCost(hit, 0 if hit else int(payload.nbytes),
                                 0 if hit else device_bytes)
        self._last_input_observation = InputReuseObservation(
            key, cost, identity_s, identity_copy_bytes, host_copy_bytes, host_copy_s,
            h2d_s if self._detailed_stage_timing and self._device.startswith("cuda") else None)
        return pixel_values, host_copy_s, h2d_s

    def _finish_input(self, *, succeeded: bool):
        lease = self._active_input
        if lease is None and not self._inputs.snapshot().reserved_bytes:
            return
        try:
            super().synchronize()
        except BaseException as error:
            if lease is not None:
                self._inputs.mark_unknown(lease)
            self._active_input = None
            if not isinstance(error, Exception):
                raise
            if succeeded:
                raise InputCompletionUnknown("retained input device completion was not confirmed") from error
            # Preserve the original forward/conversion error and its owner.
        else:
            if lease is not None:
                self._inputs.release(lease, retain=succeeded)
            self._inputs.confirm_synchronized()
            self._active_input = None
