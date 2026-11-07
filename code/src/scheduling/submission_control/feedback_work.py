"""Shared work observations and rate comparisons for feedback policies."""
from __future__ import annotations

from dataclasses import dataclass, field
import math

from ...planning.work import StageWork


@dataclass(frozen=True)
class SupplyState:
    """Control-thread demand at the end of the same native measurement window.

    Counts describe actual prepared/admitted work, not potential future model
    calls. StageWork retains declared units, not calibrated hardware cost.
    """
    observed_at_s: float
    ready_items: int
    prepared_total: int
    submitted_total: int
    active_batches: int
    upstream_pending: bool
    exhausted: bool
    capacity_blocked: str | None = None
    ready_work: tuple[StageWork, ...] = ()
    local_capacity: dict = field(default_factory=dict)
    source_exhausted: bool = False
    active_requests: int | None = None

    def __post_init__(self):
        if not math.isfinite(self.observed_at_s) or self.observed_at_s < 0:
            raise ValueError("supply timestamp must be finite and non-negative")
        for name in ("ready_items", "prepared_total", "submitted_total", "active_batches"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 0:
                raise ValueError(name + " must be a non-negative integer")
        if self.active_requests is not None and (type(self.active_requests) is not int or self.active_requests < 0):
            raise ValueError('active_requests must be a non-negative integer or unknown')
        if any(type(v) is not bool for v in (self.upstream_pending, self.exhausted, self.source_exhausted)):
            raise TypeError("supply state flags must be boolean")


def _local_pair_evidence(reference, candidate, margin):
    """Match work direction before reading rate, never fit token-cost weights.

    Larger logical input/uncached input/output are assumed not cheaper locally.
    Means omit distribution and execution-state effects; this is operational
    evidence from original work, not a causal estimate or a confidence interval.
    """
    names = ("input", "computed", "output")
    reference_ranges=[(w['completed_begin'],w['completed_end']) for w in reference['windows'] if 'completed_begin' in w]
    candidate_ranges=[(w['completed_begin'],w['completed_end']) for w in candidate['windows'] if 'completed_begin' in w]
    if any(a[0]<b[1] and b[0]<a[1] for a in reference_ranges for b in candidate_ranges):
        return dict(outcome=None,not_lighter_pairs=[],not_heavier_pairs=[],support_pairs=0,
                    lower=None,upper=None,rule='overlapping completed prefixes are not independent evidence')
    def pairs(direction):
        used, used_candidate, result = [], [], []
        span=lambda w,i:(w.get('completed_begin',i),w.get('completed_end',i+1))
        overlaps=lambda a,b:a[0]<b[1] and b[0]<a[1]
        for j, b in enumerate(candidate["windows"]):
            b_span=span(b,j)
            if any(overlaps(b_span,old) for old in used_candidate):continue
            eligible=[i for i,a in enumerate(reference['windows'])
                      if not any(overlaps(span(a,i),old) for old in used)
                      and not ('completed_begin' in a and 'completed_begin' in b
                               and overlaps(span(a,i),b_span))
                      and all(direction*(b[n]-a[n]) >= 0 for n in names)]
            if eligible:
                # Work-only matching: nearest logical input, uncached input,
                # then output; chronological ties. Never pick by faster rate.
                i=min(eligible,key=lambda i:tuple(abs(b[n]-reference['windows'][i][n]) for n in names))
                a=reference['windows'][i]
                used.append(span(a,i));used_candidate.append(b_span)
                result.append(dict(reference=i,candidate=j,gain=b["rate"]/a["rate"]-1,
                    reference_phase=reference["control_phase_id"],candidate_phase=candidate["control_phase_id"],
                    reference_prefix=span(a,i),candidate_prefix=b_span,
                    equal_work=all(a[n]==b[n] for n in names)))
        return result
    heavier, lighter = pairs(1), pairs(-1)
    gain = (len(heavier)>=2 and min(p['gain'] for p in heavier)>margin)
    loss = (len(lighter)>=2 and max(p['gain'] for p in lighter)<-margin)
    opposing_loss = any(p['gain'] < -margin for p in lighter)
    opposing_gain = any(p['gain'] > margin for p in heavier)
    equal = [p for p in heavier if p['equal_work']]
    outcome = ('gain' if gain and not opposing_loss else
               'regression' if loss and not opposing_gain else
               'no_gain' if len(equal)>=2 and all(abs(p['gain'])<=margin for p in equal)
                   and not opposing_gain and not opposing_loss else None)
    evidence = heavier if outcome=='gain' else lighter if outcome=='regression' else equal
    return dict(outcome=outcome,not_lighter_pairs=heavier,not_heavier_pairs=lighter,
        support_pairs=max(len(heavier),len(lighter)),
        lower=None if not evidence else min(p['gain'] for p in evidence),
        upper=None if not evidence else max(p['gain'] for p in evidence),
        rule='disjoint work-direction matches, work-only nearest descriptors and chronological ties; no rate-based selection or token normalization')
