"""Group an accepted window by capability/locality and bounded complete-task work."""

from dataclasses import dataclass

from ..core.session_contract import TaskKey
from ..core.session_policy import TaskCandidate
from .service_quantum import slice_service_quanta


@dataclass(frozen=True)
class WorkWindowOrganizer:
    max_members: int
    target_work: int | None
    shortest_first: bool = False
    candidate_window_rows: int | None = None

    def __post_init__(self):
        values = [self.max_members]
        values.extend(v for v in (self.target_work, self.candidate_window_rows) if v is not None)
        if any(type(v) is not int or v <= 0 for v in values):
            raise ValueError("organization limits must be positive integers")
        if type(self.shortest_first) is not bool:
            raise ValueError("shortest_first must be boolean")

    def __call__(self, window: tuple[TaskCandidate, ...]) -> tuple[TaskKey, ...]:
        if not window:
            return ()
        if self.candidate_window_rows is not None:
            window = window[:self.candidate_window_rows]
        if any(candidate.task.info is None or candidate.spec is None for candidate in window):
            raise ValueError("work organization requires typed task information")

        def compatible(candidate):
            info, spec = candidate.task.info, candidate.spec
            return (
                spec.operator,
                spec.capability,
                info.stage_id,
                info.work.primary_stage,
                info.work.primary.unit,
                info.work.calibration_signature,
                info.work.locality_key,
            )

        # Anchor to the oldest accepted task; local sorting must not starve its group.
        anchor = compatible(window[0])
        group = [candidate for candidate in window if compatible(candidate) == anchor]
        if self.shortest_first:
            group.sort(key=lambda candidate: candidate.task.estimated_work)
        group = group[: self.max_members]
        if self.target_work is None:
            return tuple(candidate.key for candidate in group)
        quantum = slice_service_quanta(
            [candidate.task.estimated_work for candidate in group], self.target_work
        )[0]
        # This is a work grouping, not permission to concatenate prompts into one request.
        return tuple(candidate.key for candidate in group[quantum.start : quantum.stop])
