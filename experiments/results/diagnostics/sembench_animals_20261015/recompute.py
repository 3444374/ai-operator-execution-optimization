"""Recompute this finite qualification from retained input, run and source archives.

Requires Pillow, already present in the qualified driver; makes no model/network call.
"""
import argparse
import base64
from collections import Counter
import csv
import hashlib
import io
import json
from pathlib import Path
import re
import statistics
import tarfile

from PIL import Image


ARMS = ('lotus-native', 'daft', 'ray-data')
HERE = Path(__file__).resolve().parent


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()


def members(path):
    with tarfile.open(path, 'r|gz') as archive:
        for item in archive:
            if item.isfile():
                yield item.name.removeprefix('./'), archive.extractfile(item).read()


def identity(raw):
    with Image.open(io.BytesIO(raw)) as image:
        rgb = image.convert('RGB')
        return (rgb.width, rgb.height, sha(rgb.tobytes()))


def distribution(values):
    return dict(min=min(values), median=statistics.median(values), max=max(values), total=sum(values))


def recompute(raw_archive, input_archive, source_archive, *, run_id='model-02', arms=ARMS):
    source = json.loads((HERE/'source.json').read_text())
    execution = source if run_id == 'model-02' else source['capacity_retry']
    with (HERE/'input-selection.csv').open(newline='') as stream:
        selection = list(csv.DictReader(stream))
    rows = {r['row_id']: r for r in selection}
    pixels = {(int(r['width']), int(r['height']), r['rgb_sha256']): r['row_id'] for r in selection}
    assert len(rows) == len(pixels) == 64
    archive_receipts = {}
    for key, path, expected in (
        ('raw', raw_archive, execution['raw_bundle']),
        ('inputs', input_archive, source['input_bundle']),
        ('execution_source', source_archive, execution['execution_source_bundle']),
    ):
        raw = path.read_bytes()
        assert len(raw) == expected['bytes'] and sha(raw) == expected['sha256'], key
        archive_receipts[key] = dict(bytes=len(raw), sha256=sha(raw))
    inputs = dict(members(input_archive))
    manifest = json.loads(inputs['manifest.json'])
    assert manifest['sha256'] == sha(canonical({k: v for k, v in manifest.items() if k != 'sha256'}))
    assert manifest['sha256'] == source['manifest_content_digest']
    for position, row in enumerate(selection):
        raw = inputs[f'images/{position}.jpg']
        assert sha(raw) == row['sha256'] == row['author_v6_sha256']
        assert len(raw) == int(row['bytes']) and identity(raw) in pixels
        frozen = manifest['rows'][position]
        assert frozen['row_id'] == row['row_id'] and frozen['Species'] == row['Species']
        assert frozen['image']['rgb_sha256'] == row['rgb_sha256']
    del inputs
    small, requests, hashes = {}, {a: {} for a in arms}, {}
    for name, raw in members(raw_archive):
        hashes[name] = sha(raw)
        if name.startswith(run_id+'/') and name.endswith('/requests.jsonl'):
            arm = name.split('/')[1]
            for line in raw.splitlines():
                call = json.loads(line)
                body = call['body']
                wire = json.dumps(body, ensure_ascii=False, separators=(',', ':')).encode()
                assert len(wire) == call['bytes'] and sha(wire) == call['body_sha256']
                assert {k: v for k, v in body.items() if k != 'messages'} == dict(
                    model='Qwen2-VL-2B-Instruct', logprobs=False, max_completion_tokens=8192,
                    temperature=0, top_p=1)
                messages = body['messages']
                assert [m['role'] for m in messages] == ['system', 'user']
                parts = messages[1]['content']
                assert [p['type'] for p in parts] == ['text', 'text', 'image_url', 'text']
                assert [p['text'] for p in parts if p['type'] == 'text'] == [
                    'Context:\n', '[Image]: \n', '\n\nClaim: The image Image contains a zebra.']
                url = parts[2]['image_url']['url']
                assert url.startswith('data:image/png;base64,')
                png = base64.b64decode(url.split(',', 1)[1], validate=True)
                row_id = pixels[identity(png)]
                assert row_id not in [r['row_id'] for r in requests[arm].values()]
                requests[arm][call['body_sha256']] = dict(
                    row_id=row_id, attempt=call['attempt'], request_bytes=call['bytes'],
                    body_sha256=call['body_sha256'], messages_sha256=sha(canonical(messages)),
                    png_sha256=sha(png))
        elif name.endswith(('.json', '.jsonl')):
            small[name] = raw
    code = dict(members(source_archive))
    source_identity = json.loads(small[execution.get('source_identity_file', 'source-identity.json')])
    assert source_identity['commit'] == execution['execution_source_commit']
    for name, expected in source_identity['files'].items():
        assert sha(code[name]) == expected, name
    del code
    result = dict(schema='semloom.sembench_animals_audit.v1', run_id=run_id,
                  archives=archive_receipts, source_commit=source_identity['commit'],
                  verified_source_files=len(source_identity['files']), rows=64, arms=[])
    observations = []
    body_sets = []
    for arm in arms:
        prefix = f'{run_id}/{arm}/'
        summary = json.loads(small[prefix+'summary.json'])
        decisions = json.loads(small[prefix+'filter-decisions.json'])
        assert [p[0] for p in decisions] == list(rows)
        decisions = dict(decisions)
        assert all(type(v) is bool for v in decisions.values())
        response_rows = [json.loads(line) for line in small[prefix+'responses.jsonl'].splitlines()]
        trace = [json.loads(line) for line in small[prefix+'http-trace.jsonl'].splitlines()]
        ledger = [json.loads(line) for line in small[prefix+'attempts.jsonl'].splitlines()]
        calls = requests[arm]
        assert len(calls) == len(response_rows) == len(trace) == len(ledger)-1 == 64
        assert {r['request_sha256'] for r in response_rows} == set(calls)
        assert {r['request_body_sha256'] for r in trace} == set(calls)
        assert {r['request_sha256'] for r in ledger[1:]} == set(calls)
        assert sorted(r['attempt'] for r in calls.values()) == list(range(1, 65))
        confusion = dict(true_positive=0, false_positive=0, true_negative=0, false_negative=0)
        for row_id, kept in decisions.items():
            truth = 'ZEBRA' in rows[row_id]['Species']
            confusion[('true_' if truth == kept else 'false_') + ('positive' if kept else 'negative')] += 1
        assert summary['classification_audit'] == dict(**confusion, evaluated_rows=64)
        prediction = sum(decisions.values())
        truth = sum('ZEBRA' in r['Species'] for r in rows.values())
        error = abs(prediction-truth)/truth
        assert summary['quality']['original_metric'] == dict(relative_error=error,
            absolute_error=float(abs(prediction-truth)), mean_absolute_percentage_error=100*error)
        assert summary['quality']['ground_truth'] == truth == 3
        assert summary['quality']['predicted'] == prediction
        assert json.loads(small[prefix+'original-result.json'])['data'] == [[prediction]]
        for response in response_rows:
            assert response['status'] == 200
            body = json.loads(base64.b64decode(response['body_base64'], validate=True))
            assert body['model'] == 'Qwen2-VL-2B-Instruct'
            choice = body['choices'][0]
            assert choice['finish_reason'] == 'stop'
            answer = re.fullmatch(r'(?:Answer:\s*)?(True|False)', choice['message']['content'].strip(), re.I)
            assert answer and (answer[1].lower() == 'true') == decisions[calls[response['request_sha256']]['row_id']]
            calls[response['request_sha256']].update(body['usage'])
        events, latencies = [], []
        for event in trace:
            assert event['status'] == 'completed' and event['client_status'] == event['upstream_status'] == 200
            assert event['forwarded'] and event['retry_count'] == 0 and not event['errors']
            assert event['forwarded_body_sha256'] == event['request_body_sha256']
            start, end = event['received_monotonic_ns'], event['request_terminal_monotonic_ns']
            assert end >= start
            latency = (end-start)/1e9
            latencies.append(latency)
            events += [(event['upstream_dispatch_started_monotonic_ns'], 1),
                       (event['upstream_response_body_read_completed_monotonic_ns'], -1)]
            call = calls[event['request_body_sha256']]
            assert event['actual_prompt_tokens'] == call['prompt_tokens']
            assert event['actual_output_tokens'] == call['completion_tokens']
            observations.append(dict(run_id=run_id, arm=arm, **{k: call[k] for k in (
                'row_id', 'attempt', 'request_bytes', 'body_sha256', 'messages_sha256', 'png_sha256',
                'prompt_tokens', 'completion_tokens', 'total_tokens')},
                kept=decisions[call['row_id']], truth='ZEBRA' in rows[call['row_id']]['Species'],
                handler_to_terminal_seconds=latency, http_status=200))
        active = peak = 0
        for _, delta in sorted(events):
            active += delta
            peak = max(peak, active)
        assert active == 0
        resource_passed = peak <= 4
        if 'resource_gate' in summary:
            assert summary['resource_gate']['passed'] == resource_passed
            assert summary['resource_gate']['observed_upstream_peak'] == peak
        assert summary['status'] == 'completed' and summary['model_calls'] == summary['http_requests'] == 64
        assert summary['warmup_model_calls'] == summary['preparation_model_calls'] == summary['retries'] == 0
        assert summary['cache'] is False and summary['observation_mode'] == 'performance'
        usage = {k: sum(r[k] for r in calls.values()) for k in ('prompt_tokens', 'completion_tokens', 'total_tokens')}
        assert all(summary['lm_stats']['physical_usage'][k] == v for k, v in usage.items())
        sizes = [r['request_bytes'] for r in calls.values()]
        body_sets.append(Counter(r['messages_sha256'] for r in calls.values()))
        result['arms'].append(dict(arm=arm, status='completed', resource_allowance_passed=resource_passed,
            qualification_status='passed' if resource_passed else 'failed: observed HTTP peak exceeds four slots',
            query_count=1, model_calls=64,
            full_query_seconds=summary['full_query_seconds'], application_seconds=summary['application_seconds'],
            predicted=prediction, truth=truth, original_relative_error=error, **confusion,
            request_bytes=distribution(sizes), requests_over_core_1mib=sum(n > 1048576 for n in sizes),
            usage=usage, handler_to_terminal_seconds=distribution(latencies),
            peak_observed_upstream_inflight=peak, versions=summary['versions'],
            evidence_sha256={key: hashes[prefix+key] for key in (
                'summary.json', 'attempts.jsonl', 'requests.jsonl', 'responses.jsonl',
                'http-trace.jsonl', 'filter-decisions.json', 'original-result.json')}))
    assert all(c == body_sets[0] for c in body_sets)
    controller = json.loads(small[run_id+'/controller-result.json'])
    release = json.loads(small[run_id+'/lease-release.json'])
    assert controller['model_revision'] == source['model']['revision']
    assert controller['source_commit'] == source_identity['commit']
    assert controller['api_warmup_requests'] == 0 and controller['healthy_query_total_deadline'] is None
    if run_id == 'model-03':
        assert controller['previous_model_calls'] == 192 and controller['cumulative_model_call_limit'] == 256
    assert controller['status'] == 'completed' and controller['service_exit'] == 0
    assert not controller['remaining_owned_pids'] and release['released'] and not release['stale_recovery']
    result.update(messages_identical_across_arms=True, original_pixels_verified=True, total_model_calls=64*len(arms),
        all_retained_arms_pass_resource_allowance=all(r['resource_allowance_passed'] for r in result['arms']),
        service_startup_seconds=controller['service_startup_seconds'], service_exit=0, lease_released=True,
        query_tail_quantiles=dict(status='unavailable', reason='one complete query per arm'),
        monetary_cost=dict(status='unavailable', reason='no local model pricing; LM numeric zero is not a measured cost'))
    return result, observations


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--raw-archive', type=Path, required=True)
    parser.add_argument('--input-archive', type=Path, required=True)
    parser.add_argument('--source-archive', type=Path, required=True)
    parser.add_argument('--retry-archive', type=Path)
    parser.add_argument('--retry-source-archive', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    first, observations = recompute(args.raw_archive, args.input_archive, args.source_archive)
    runs = [first]
    if args.retry_archive or args.retry_source_archive:
        if not (args.retry_archive and args.retry_source_archive):
            parser.error('both retry archives are required')
        retry, extra = recompute(args.retry_archive, args.input_archive, args.retry_source_archive,
                                 run_id='model-03', arms=('daft',))
        assert Counter(r['messages_sha256'] for r in extra) == Counter(
            r['messages_sha256'] for r in observations if r['arm'] == 'lotus-native')
        assert retry['arms'][0]['resource_allowance_passed']
        runs.append(retry)
        observations.extend(extra)
    result = dict(schema='semloom.sembench_animals_results.v2', runs=runs,
                  total_model_calls=sum(r['total_model_calls'] for r in runs))
    selected=[]
    for arm in ARMS:
        chosen=runs[-1] if len(runs)>1 and arm=='daft' else first
        metric=next(r for r in chosen['arms'] if r['arm']==arm)
        selected.append(dict(run_id=chosen['run_id'],arm=arm,
                             resource_allowance_passed=metric['resource_allowance_passed']))
    result['selected_qualification_arms']=selected
    result['selected_all_pass_resource_allowance']=all(r['resource_allowance_passed'] for r in selected)
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output/'results.json').write_text(json.dumps(result, indent=2, allow_nan=False)+'\n')
    observations.sort(key=lambda r: (r['run_id'], ARMS.index(r['arm']), int(r['row_id'].split('-')[-1])))
    with (args.output/'requests.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(observations[0]), lineterminator='\n')
        writer.writeheader()
        writer.writerows(observations)
    print(json.dumps(dict(total_model_calls=result['total_model_calls'],
        resource_allowance_passed=[r['all_retained_arms_pass_resource_allowance'] for r in runs])))
