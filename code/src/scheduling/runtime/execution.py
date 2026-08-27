"""Engine-independent physical execution facade for synchronous scheduling."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field

from ..core.models import PayloadEnvelope, TopologySnapshot
from ..core.scheduler import (
    AdmissionPolicy,
    EndpointRouter,
    PoolRouter,
    SchedulerConfig,
    SchedulerResult,
    SubmissionAdapter,
    SynchronousScheduler,
)


@dataclass(frozen=True)
class SynchronousExecutionEngine:
    """Compose policies and an engine adapter behind one execution interface."""

    admission: AdmissionPolicy
    router: EndpointRouter
    adapter: SubmissionAdapter
    pool_id: str
    config: SchedulerConfig = field(default_factory=SchedulerConfig)
    pool_router: PoolRouter | None = None
    epoch_clock: Callable[[], float] = time.time

    def execute(
        self,
        envelopes: Iterable[PayloadEnvelope],
        topology: TopologySnapshot,
    ) -> SchedulerResult:
        scheduler = SynchronousScheduler.from_config(
            admission=self.admission,
            router=self.router,
            adapter=self.adapter,
            pool_id=self.pool_id,
            config=self.config,
            pool_router=self.pool_router,
            epoch_clock=self.epoch_clock,
        )
        return scheduler.run(envelopes, topology)
