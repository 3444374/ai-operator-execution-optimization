"""Call and query observations for separately identified native adapter runs."""
from collections import defaultdict
import math

from src.observability.metrics.statistics import percentile


def sample_distribution(values, *, unit):
    samples = list(values)
    if any(type(v) not in (int, float) or not math.isfinite(v) or v < 0 for v in samples):
        raise ValueError('latency samples must be finite nonnegative numbers')
    count = len(samples)
    result = dict(unit=unit, count=count, samples=samples,
                  quantile_method='nearest rank', independent_population_tail_estimate=False)
    if not count:
        return dict(result, status='unavailable', reason='no completed observations',
                    minimum=None, maximum=None, p99=None, p99_99=None)
    for label, probability in [('p99', 99), ('p99_99', 99.99)]:
        rank = math.ceil(probability / 100 * count)
        result[label] = percentile(samples, probability)
        result[label + '_rank'] = rank
        result[label + '_is_sample_maximum'] = rank == count
    return dict(result, status='observed', minimum=min(samples), maximum=max(samples))


def _event_time(event):
    value = event.get('monotonic_ns')
    if type(value) is not int or value < 1:
        raise ValueError('a positive monotonic observation is required')
    return value


def summarize_calls(events, expected_calls):
    """Use method readiness and original-caller receipts, never proxy arrivals.

    Missing stages remain unavailable. Model inference time is not inferred
    from an HTTP interval. Each full call remains one statistical observation.
    """
    def identity(value):
        return value.get('row_id'),value.get('stage_id'),value.get('call_id')
    expected = {identity(c): c for c in expected_calls}
    if len(expected) != len(expected_calls):
        raise ValueError('duplicate expected call identity')
    observed = defaultdict(dict)
    names = ('task_ready', 'executor_submit', 'http_started', 'http_body_read',
             'worker_enter', 'worker_response', 'caller_response', 'result_release')
    for event in events:
        name = event.get('event')
        if name not in names:
            continue
        key = identity(event)
        if key not in expected:
            raise ValueError('unexpected model call observation')
        call = expected[key]
        if (event.get('row_id'), event.get('stage_id')) != (call['row_id'], call['stage_id']):
            raise ValueError('model call observation changed row or stage identity')
        if name in observed[key]:
            raise ValueError('duplicate ' + name + ' observation')
        if name == 'task_ready' and event.get('request_values_sha256') != call['request_values_sha256']:
            raise ValueError('method readiness differs from the declared call')
        _event_time(event)
        observed[key][name] = event

    samples, requests = [], []
    successors = defaultdict(list)
    stage_samples = defaultdict(list)
    for key, call in expected.items():
        call_id=call['call_id']
        found = observed[key]
        if 'task_ready' not in found or 'caller_response' not in found:
            raise ValueError('a completed run lacks method readiness or caller receipt')
        start, end = found['task_ready'], found['caller_response']
        domains = {e.get('clock_domain') for e in found.values()}
        if None in domains or len(domains) != 1:
            raise ValueError('call observations do not share a verified clock domain')
        if _event_time(end) < _event_time(start):
            raise ValueError('caller receipt precedes legal task readiness')
        response_hash = end.get('response_bytes_sha256')
        if not isinstance(response_hash, str) or len(response_hash) != 64:
            raise ValueError('caller receipt lacks the complete response digest')
        duration = (_event_time(end) - _event_time(start)) / 1e9
        samples.append(duration)
        row = dict(call_id=call_id, row_id=call['row_id'], stage_id=call['stage_id'],
                   model_role=call.get('model_role', 'main'), request_e2e_seconds=duration,
                   task_ready_ns=_event_time(start), caller_response_ns=_event_time(end),
                   request_values_sha256=call['request_values_sha256'],
                   response_bytes_sha256=response_hash, clock_domain=start['clock_domain'])
        if 'response_representation' in call:
            row['response_representation']=call['response_representation']
        spans = [('ready_to_submit', 'task_ready', 'executor_submit'),
                 ('submit_to_http', 'executor_submit', 'http_started'),
                 ('http_roundtrip', 'http_started', 'http_body_read'),
                 ('worker_prepare_and_http', 'worker_enter', 'worker_response'),
                 ('worker_return_to_caller', 'worker_response', 'caller_response'),
                 ('return_to_caller', 'http_body_read', 'caller_response'),
                 ('result_retention', 'caller_response', 'result_release')]
        for label, first, last in spans:
            if first not in found or last not in found:
                row[label + '_seconds'] = None
                continue
            left, right = _event_time(found[first]), _event_time(found[last])
            if right < left:
                raise ValueError(label + ' observations are reversed')
            row[label + '_seconds'] = (right - left) / 1e9
            stage_samples[label].append(row[label + '_seconds'])
        if 'http_body_read' in found and found['http_body_read'].get('response_bytes_sha256') != response_hash:
            raise ValueError('the caller did not receive the observed complete response')
        if 'stage_ordinal' in call:
            successors[call['row_id']].append((call['stage_ordinal'], row))
        requests.append(row)

    waiting = []
    for row_id, values in successors.items():
        values.sort(key=lambda item: item[0])
        if len({ordinal for ordinal, _ in values}) != len(values):
            raise ValueError('duplicate row stage ordinal')
        for (previous, predecessor), (current, successor) in zip(values, values[1:]):
            if current != previous + 1:
                raise ValueError('missing sequential method stage')
            if successor['clock_domain'] != predecessor['clock_domain']:
                raise ValueError('successor and predecessor clocks differ')
            elapsed = successor['task_ready_ns'] - predecessor['caller_response_ns']
            if elapsed < 0:
                raise ValueError('successor became ready before its predecessor response')
            waiting.append(dict(row_id=row_id, predecessor=predecessor['stage_id'],
                                successor=successor['stage_id'],
                                predecessor_response_to_successor_ready_seconds=elapsed / 1e9))
    return dict(statistical_unit='complete model call',
                request_e2e=sample_distribution(samples, unit='seconds'), calls=requests,
                stages={name:sample_distribution(stage_samples[name], unit='seconds') for name in (
                    'ready_to_submit', 'submit_to_http', 'http_roundtrip',
                    'worker_prepare_and_http', 'worker_return_to_caller',
                    'return_to_caller', 'result_retention')},
                successor_waits=waiting,
                model_inference_seconds=dict(status='unavailable',
                    reason='complete HTTP responses do not expose per-call model execution times'),
                queue_scope='ready-to-submit and submit-to-HTTP include preparation and scheduling; no pure queue claim')


def summarize_queries(records):
    full, ready = [], []
    for value in records:
        if value.get('status') != 'passed':
            raise ValueError('failed runs must be retained separately from successful query samples')
        full.append(value['full_query_seconds'])
        ready.append(value['execution']['ready_query_seconds'])
    return dict(statistical_unit='complete query',
                full_application=sample_distribution(full, unit='seconds'),
                ready_query=sample_distribution(ready, unit='seconds'))


def compare_methods(first, second):
    """Compare occurrence identities and method-selected complete call values."""
    def index(calls):
        result = {}
        for call in calls:
            identity = (call['row_id'], call['stage_id'], call['call_id'])
            if identity in result:
                raise ValueError('duplicate method call occurrence')
            result[identity] = (call['model_role'], call['request_values_sha256'])
        return result
    left, right = index(first), index(second)
    if left != right:
        raise ValueError('paired executors ran different method call content or roles')
    return dict(method_calls_equal=True, logical_calls=len(left),
                equality_scope='row, stage, call identity, model role and complete request values',
                quality_equivalence_established=False)


def executor_phase_observations(events):
    """Report measured preparation spans without adding overlapping durations."""
    spans = defaultdict(lambda: defaultdict(list))
    failures = defaultdict(int)
    payload_spans = defaultdict(list)
    payload_failures = defaultdict(int)
    for event in events:
        if event.get('event') == 'payload_stage':
            value=event.get('elapsed_ns')
            if type(value) is not int or value < 0:
                raise ValueError('invalid measured payload stage duration')
            payload_spans[event['stage']].append(value/1e9)
            payload_failures[event['stage']] += event.get('status') != 'completed'
            continue
        if event.get('event') != 'ray_work':
            continue
        stage = event['stage']
        failures[stage] += event.get('status') != 'completed'
        for name in ('queue_ns','work_ns','resume_ns','elapsed_ns'):
            value = event.get(name)
            if value is not None:
                if type(value) is not int or value < 0:
                    raise ValueError('invalid measured executor phase duration')
                spans[stage][name.removesuffix('_ns')].append(value/1e9)
    return dict(payload_generation={stage:dict(duration=sample_distribution(values,unit='seconds'),
                    failed_spans=payload_failures[stage]) for stage,values in payload_spans.items()},
                representation_and_transfer={stage:dict(
                    **{name:sample_distribution(values,unit='seconds') for name,values in durations.items()},
                    failed_spans=failures[stage]) for stage,durations in spans.items()},
                organization=dict(status='unavailable',reason='selection events have no separate start/end probe'),
                aggregation_scope='individual measured spans; nested or parallel durations are not deducted from query time')
