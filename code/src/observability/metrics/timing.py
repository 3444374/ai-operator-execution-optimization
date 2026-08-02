"""Stage timing and low-overhead periodic sampling."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass


@dataclass
class StageTimer:
    name: str
    start_s: float
    elapsed_s: float = 0.0

    @classmethod
    def start(cls, name: str) -> "StageTimer":
        return cls(name=name, start_s=time.perf_counter())

    def stop(self) -> float:
        self.elapsed_s = time.perf_counter() - self.start_s
        return self.elapsed_s

class PeriodicSampler:
    """Collect timestamped resource snapshots without blocking the run loop."""

    def __init__(
        self,
        sample: Callable[[], dict[str, object]],
        *,
        interval_s: float = 0.25,
    ) -> None:
        if interval_s <= 0:
            raise ValueError("interval_s must be positive")
        self._sample = sample
        self._interval_s = interval_s
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._samples: list[dict[str, object]] = []
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    @property
    def samples(self) -> tuple[dict[str, object], ...]:
        with self._lock:
            return tuple(dict(item) for item in self._samples)

    @property
    def is_running(self) -> bool:
        return self._thread.is_alive()

    def close(self) -> None:
        self._stop.set()
        self._thread.join()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                values = self._sample()
            except Exception as exc:
                values = {"sample_status": f"unavailable:{type(exc).__name__}"}
            with self._lock:
                self._samples.append(
                    {
                        "sample_index": len(self._samples),
                        "sample_epoch_s": time.time(),
                        **values,
                    }
                )
            self._stop.wait(self._interval_s)
