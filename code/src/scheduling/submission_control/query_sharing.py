"""Optional finite completion bursts over the existing Job opportunity selector."""

from ..core.session_jobs import round_robin_flow


class BoundedShortJobBurst:
    """Prefer small sealed Jobs; compensate at an estimated selection-work threshold.

    This is a differentiated-service experiment, not token-service fairness or a
    wall-time guarantee. Counters cover continuously eligible Jobs only. Failed
    submissions also consume opportunities; no accepted request is preempted.
    One selection can cross the threshold; it is not an absolute work-wait bound.
    """

    def __init__(self, *, work_unit, short_work_limit, bypass_work_limit):
        if type(work_unit) is not str or not work_unit:
            raise ValueError('a work unit is required')
        if any(type(v) is not int or v <= 0 for v in (short_work_limit, bypass_work_limit)):
            raise ValueError('work limits must be positive integers')
        self.work_unit = work_unit
        self.short_work_limit, self.bypass_work_limit = short_work_limit, bypass_work_limit
        self._bypassed = {}

    def __call__(self, ready, history):
        # Unknown or incomparable units use the existing simple policy. The map
        # never outlives eligible identities and is bounded by Engine max_jobs.
        self._bypassed = {j.job_id: self._bypassed.get(j.job_id, 0) for j in ready}
        if any(j.work is None or j.work.work_unit != self.work_unit for j in ready):
            self._bypassed.clear()
            return round_robin_flow(ready, history)
        forced = tuple(j for j in ready if self._bypassed[j.job_id] >= self.bypass_work_limit)
        small = tuple(j for j in ready if j.work.remaining_work is not None
                      and 0 < j.work.remaining_work <= self.short_work_limit)
        if forced:
            worst = max(self._bypassed[j.job_id] for j in forced)
            choices = tuple(j for j in forced if self._bypassed[j.job_id] == worst)
        elif small:
            shortest = min(j.work.remaining_work for j in small)
            choices = tuple(j for j in small if j.work.remaining_work == shortest)
        else:
            choices = ready
        choice = round_robin_flow(choices, history)
        selected = next(j for j in ready if j.job_id == choice.job_id)
        for job in ready:
            self._bypassed[job.job_id] = (0 if job.job_id == choice.job_id
                                         else self._bypassed[job.job_id] + selected.work.dispatch_work)
        return choice
