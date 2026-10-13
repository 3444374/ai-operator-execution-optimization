"""Optional Job opportunity controls over the existing read-only work view."""

from ..core.session_jobs import round_robin_flow


class RemainingWorkPriority:
    """Select the smallest known estimate, optionally compensating bypassed Jobs.

    This does not preempt accepted requests or claim SRPT optimality. Unknown
    Jobs receive RR opportunities when all totals are unknown; with known peers
    they can receive the same estimated-opportunity compensation as the candidate.
    """

    def __init__(self, *, work_unit, bypass_work_limit=None):
        if type(work_unit) is not str or not work_unit:
            raise ValueError('a work unit is required')
        if bypass_work_limit is not None and (
                type(bypass_work_limit) is not int or bypass_work_limit <= 0):
            raise ValueError('bypass work must be a positive integer or None')
        self.work_unit, self.bypass_work_limit = work_unit, bypass_work_limit
        self._bypassed = {}

    def _preferred(self, ready):
        known = tuple(j for j in ready if j.work.remaining_work is not None
                      and j.work.remaining_work > 0)
        shortest = min(j.work.remaining_work for j in known)
        return tuple(j for j in known if j.work.remaining_work == shortest)

    def __call__(self, ready, history):
        # Only continuously eligible identities accrue compensation. Failed
        # submissions also consume selections; service time is never observed.
        self._bypassed = {j.job_id: self._bypassed.get(j.job_id, 0) for j in ready}
        if (any(j.work is None or j.work.work_unit != self.work_unit for j in ready)
                or not any(j.work.remaining_work is not None and j.work.remaining_work > 0
                           for j in ready)):
            self._bypassed.clear()
            return round_robin_flow(ready, history)
        forced = tuple(j for j in ready if self.bypass_work_limit is not None
                       and self._bypassed[j.job_id] >= self.bypass_work_limit)
        if forced:
            worst = max(self._bypassed[j.job_id] for j in forced)
            choices = tuple(j for j in forced if self._bypassed[j.job_id] == worst)
        else:
            choices = self._preferred(ready)
        choice = round_robin_flow(choices, history)
        if self.bypass_work_limit is not None:
            selected = next(j for j in ready if j.job_id == choice.job_id)
            for job in ready:
                self._bypassed[job.job_id] = (0 if job.job_id == choice.job_id else
                    self._bypassed[job.job_id] + selected.work.dispatch_work)
        return choice


class BoundedShortJobBurst(RemainingWorkPriority):
    """Prefer small sealed Jobs; compensate at an estimated selection-work threshold.

    This is a differentiated-service experiment, not token-service fairness or a
    wall-time guarantee. One selection can cross the threshold. With no small
    known Job, the preference is RR rather than remaining-work priority.
    """

    def __init__(self, *, work_unit, short_work_limit, bypass_work_limit):
        if any(type(v) is not int or v <= 0 for v in (short_work_limit, bypass_work_limit)):
            raise ValueError('work limits must be positive integers')
        super().__init__(work_unit=work_unit, bypass_work_limit=bypass_work_limit)
        self.short_work_limit = short_work_limit

    def _preferred(self, ready):
        small = tuple(j for j in ready if j.work.remaining_work is not None
                      and 0 < j.work.remaining_work <= self.short_work_limit)
        if not small:
            return ready
        shortest = min(j.work.remaining_work for j in small)
        return tuple(j for j in small if j.work.remaining_work == shortest)
