"""Offline supply and paired-throughput diagnostics for declared M1 points.

Ranges describe observed repeats, not confidence intervals or GPU saturation.
All numerical decision tolerances come from the run's predeclared policy.
"""
import math
from statistics import median


def validate_policy(policy):
    required = {'epsilon', 'max_relative_spread', 'min_repeats',
                'supply_level', 'min_supply_fraction', 'middle_fraction'}
    if set(policy) != required:
        raise ValueError('complete explicit selection policy required')
    for key in required - {'min_repeats'}:
        value = policy[key]
        if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value < 1:
            raise ValueError('selection fractions must be finite and between zero and one')
    if type(policy['min_repeats']) is not int or not 3 <= policy['min_repeats'] <= 31:
        raise ValueError('at least three paired measurement repeats required')


def supply_observation(events, *, start_ns, end_ns, capacity, policy):
    """Integrate actual HTTP occupancy, retaining startup and drain in full area."""
    validate_policy(policy)
    if not 0 <= start_ns < end_ns or type(capacity) is not int or capacity < 1:
        raise ValueError('invalid supply observation interval or capacity')
    kinds = {'http_started': 1, 'core_http_started': 1,
             'http_finished': -1, 'core_http_finished': -1}
    timeline = sorted((e for e in events if e['event'] in kinds), key=lambda e: e['monotonic_ns'])
    margin = (end_ns - start_ns) * (1 - policy['middle_fraction']) / 2
    left, right = start_ns + margin, end_ns - margin
    active = peak = area = middle_area = supplied = 0
    previous = start_ns
    for event in timeline:
        now = event['monotonic_ns']
        if type(now) is not int or not previous <= now <= end_ns:
            raise ValueError('HTTP event outside complete query interval')
        overlap = max(0, min(now, right) - max(previous, left))
        area += active * (now - previous)
        middle_area += active * overlap
        if active >= math.ceil(capacity * policy['supply_level']):
            supplied += overlap
        active += kinds[event['event']]
        if not 0 <= active <= capacity:
            raise ValueError('HTTP occupancy exceeds declared capacity or loses a start')
        peak = max(peak, active)
        previous = now
    if active or not timeline:
        raise ValueError('missing or unfinished HTTP responsibilities')
    return dict(peak=peak, active_request_seconds=area / 1e9,
                middle_mean_active=middle_area / (right - left),
                middle_supplied_fraction=supplied / (right - left),
                middle_start_ns=left, middle_end_ns=right,
                scope='actual client HTTP supply, not model-running occupancy')


def _rates(records, policy):
    if len(records) < policy['min_repeats']:
        raise ValueError('not enough independent paired measurement slots')
    result = {}
    for row in records:
        repeat = row['repeat']
        if type(repeat) is not int or repeat < 1 or repeat in result:
            raise ValueError('duplicate or invalid paired repeat identity')
        count, seconds = row['completed_rows'], row['jct_seconds']
        if (row['status'] != 'passed' or type(count) is not int or count < 1
                or count != row['expected_rows'] or row['actual_posts'] != count
                or row['quality']['invalid'] != 0
                or type(seconds) not in (int, float) or not math.isfinite(seconds) or seconds <= 0):
            raise ValueError('selection requires complete valid runs; failed runs cannot be dropped')
        supplied = row['supply']['middle_supplied_fraction']
        if type(supplied) not in (int, float) or not math.isfinite(supplied) or not 0 <= supplied <= 1:
            raise ValueError('missing or invalid sustained supply observation')
        result[repeat] = count / seconds
    return result


def paired_comparison(candidate, reference, *, policy):
    """Conservative observed paired range; does not assert statistical equivalence."""
    validate_policy(policy)
    first, second = _rates(candidate, policy), _rates(reference, policy)
    if first.keys() != second.keys():
        raise ValueError('paired repeat sets differ')
    for a, b in zip(sorted(candidate, key=lambda r: r['repeat']), sorted(reference, key=lambda r: r['repeat'])):
        if a['expected_rows'] != b['expected_rows'] or a['manifest_sha256'] != b['manifest_sha256']:
            raise ValueError('paired workload identities differ')
    ratios = [first[key] / second[key] for key in sorted(first)]
    floor = 1 - policy['epsilon']
    status = ('within_declared_loss_in_all_pairs' if min(ratios) >= floor else
              'throughput_loss_in_all_pairs' if max(ratios) < floor else 'inconclusive_repeat_variation')
    differences = []
    for a, b in zip(sorted(candidate, key=lambda r: r['repeat']), sorted(reference, key=lambda r: r['repeat'])):
        if 'output_values' in a and 'output_values' in b:
            if a['output_values'].keys() != b['output_values'].keys():
                raise ValueError('paired output identities differ')
            differences.append(sum(a['output_values'][k] != b['output_values'][k] for k in a['output_values']))
    return dict(status=status, paired_output_difference_counts=differences or None,
                paired_rate_ratios=ratios, median_rate_ratio=median(ratios),
                observed_ratio_range=[min(ratios), max(ratios)],
                uncertainty='observed paired range only; no confidence or equivalence claim')


def select_capacity(samples, *, capacities, policy):
    """Identify a provisional platform candidate only on reachable supplied points."""
    validate_policy(policy)
    if (len(capacities) < 3 or list(capacities) != sorted(set(capacities))
            or any(type(c) is not int or not 1 <= c <= 256 for c in capacities)
            or set(samples) != set(capacities)):
        raise ValueError('at least three distinct predeclared capacities and all results required')
    rates = {c: _rates(samples[c], policy) for c in capacities}
    repeat_sets = [set(v) for v in rates.values()]
    if any(v != repeat_sets[0] for v in repeat_sets):
        raise ValueError('capacity repeat sets differ')
    identities = {(r['expected_rows'], r['manifest_sha256']) for rows in samples.values() for r in rows}
    if len(identities) != 1:
        raise ValueError('capacity scan mixes input identities')
    medians = {c: median(v.values()) for c, v in rates.items()}
    spread = {c: (max(v.values()) - min(v.values())) / medians[c] for c, v in rates.items()}
    supplied = {c: all(r['supply']['middle_supplied_fraction'] >= policy['min_supply_fraction']
                       for r in samples[c]) for c in capacities}
    best = max(capacities, key=lambda c: medians[c])
    candidates = [c for c in capacities if medians[c] >= (1 - policy['epsilon']) * medians[best]]
    comparisons = {c: paired_comparison(samples[c], samples[best], policy=policy) for c in capacities}
    top = paired_comparison(samples[capacities[-1]], samples[capacities[-2]], policy=policy)
    result = dict(selected=None, observed_best=best, median_rates=medians,
                  relative_spread=spread, sustained_supply=supplied, comparisons=comparisons,
                  top_pair=top, saturation_proven=False, independent_evaluation_required=True)
    if not all(supplied.values()):
        status = 'inconclusive_supply'
    elif any(v > policy['max_relative_spread'] for v in spread.values()):
        status = 'inconclusive_repeat_variation'
    elif top['observed_ratio_range'][0] > 1 / (1 - policy['epsilon']):
        status = 'no_platform_observed'
    elif top['observed_ratio_range'][1] > 1 / (1 - policy['epsilon']):
        status = 'inconclusive_repeat_variation'
    else:
        eligible = [c for c in candidates if comparisons[c]['status'] == 'within_declared_loss_in_all_pairs']
        if len(eligible) < 2:
            status = 'inconclusive_repeat_variation'
        else:
            status = 'platform_candidate' if best != capacities[0] else 'lower_supply_range_unresolved'
            if status == 'platform_candidate':
                result['selected'] = min(eligible)
    return dict(result, status=status)
