"""Fixed measured stage costs and observations for the existing ready policies.

Costs are native API elapsed time, including its queueing; not GPU-only time or
uncached-token work. Calibration is separate from the compared requests.
"""
from dataclasses import asdict
import hashlib
import json
import math
from pathlib import Path
import statistics

from src.infrastructure.private_artifacts import require_outside_git
from src.scheduling.organization.ready_work import ReadyCost, WorkProgressPriority


STAGES = ('map0', 'map1', 'keep')
MODES = {'ready-shortest': 'shortest_work', 'ready-longest': 'longest_path',
         'ready-progress': 'progress_per_work'}


def signature(value):
    return {key: value[key] for key in ('public_git_commit', 'input_sha256',
                                      'engine', 'generation', 'resources')}


def calibrate(case, *, source_sha256):
    native = case['close']['model_owner']['model_ready']['native_execution_profile']
    if (case['status'] != 'completed' or case['actual_calls'] != 48 or
            case['close']['status'] != 'closed' or not case['close']['query_completed'] or
            native.get('batch_invariant_requested') is not True or
            native.get('batch_invariant_active') is not True):
        raise ValueError('completed calibration and observed native numerical profile required')
    costs = {}
    for stage in STAGES:
        values = [c['model_elapsed_s'] for c in case['calls'] if c['algorithm_stage'] == stage]
        if len(values) != 16 or any(not math.isfinite(v) or v <= 0 for v in values):
            raise ValueError('each stage needs 16 positive measured native API intervals')
        costs[stage] = max(1, round(statistics.median(values) * 1e9))
    return dict(schema='semloom.ready_work_calibration.v1', signature=signature(case),
                source_case_sha256=source_sha256, service_ns=costs,
                cost_scope='median native API interval including queueing; not exclusive GPU time',
                remaining_path='sum of subsequent stage medians; no future outputs', max_wait_s=1.0)


def load_profile(config):
    if config['suite'] == 'ready-work-calibration':
        if 'ready_work_profile' in config:
            raise ValueError('calibration must not consume a prior cost profile')
        return None
    spec = config['ready_work_profile']
    path = Path(spec['path']); require_outside_git(path)
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != spec['sha256']:
        raise ValueError('ready cost profile changed')
    profile = json.loads(raw)
    from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec
    from src.semantic_methods.lotus.records import RecordsResources
    expected = dict(public_git_commit=config['public_git_commit'], input_sha256=config['input']['sha256'],
        engine=asdict(EngineSpec.from_dict(config['engine'])),
        generation=asdict(GenerationSpec.from_dict(config['generation'])),
        resources=asdict(RecordsResources(**config['resources'])))
    expected = json.loads(json.dumps(expected))
    if (profile.get('schema') != 'semloom.ready_work_calibration.v1' or
            profile.get('signature') != expected or set(profile.get('service_ns', {})) != set(STAGES) or
            any(type(v) is not int or v <= 0 for v in profile['service_ns'].values()) or
            profile.get('max_wait_s') != 1.0 or not profile.get('source_case_sha256')):
        raise ValueError('ready cost profile signature or measured costs differ')
    return profile


class ObservedReadySelector:
    def __init__(self, case_id, profile):
        self.case_id, self.profile, self.choices = case_id, profile, []
        if case_id != 'ready-fifo' and (case_id not in MODES or profile is None):
            raise ValueError('known comparator and frozen calibration required')
        self.policy = None if case_id == 'ready-fifo' else WorkProgressPriority(
            self.estimate, mode=MODES[case_id], max_wait_s=profile['max_wait_s'])

    def estimate(self, candidate):
        if candidate.profile_name not in STAGES or candidate.work_unit != 'model_calls':
            return None
        stage = STAGES.index(candidate.profile_name)
        costs = self.profile['service_ns']
        return ReadyCost(costs[candidate.profile_name], sum(costs[s] for s in STAGES[stage+1:]),
                         'calibration:' + self.profile['source_case_sha256'])

    def __call__(self, candidates, now_s):
        selected, reason = ((candidates[0].sequence, 'fifo') if self.policy is None
                            else self.policy(candidates, now_s))
        chosen = next(c for c in candidates if c.sequence == selected)
        self.choices.append(dict(at=now_s, candidates=[asdict(c) for c in candidates],
            selected=asdict(chosen), reason=reason, differs_from_fifo=selected != candidates[0].sequence,
            accepted=False))
        return selected, reason

    def observe_event(self, event):
        if event.action != 'Core_offer_accepted' or not self.choices:
            return
        choice = self.choices[-1]
        selected = choice['selected']
        if (selected['call_id'] != event.row.call_id or selected['stage_index'] != event.stage_index
                or choice['reason'] != event.reason):
            raise ValueError('accepted Core offer does not match the last observed ready choice')
        choice['accepted'] = True

    def report(self):
        return dict(case_id=self.case_id, profile=self.profile, choices=self.choices,
                    accepted_choices=sum(c['accepted'] for c in self.choices),
                    contended_accepted=sum(c['accepted'] and len(c['candidates']) > 1 for c in self.choices),
                    changed_accepted=sum(c['accepted'] and c['differs_from_fifo'] for c in self.choices))
