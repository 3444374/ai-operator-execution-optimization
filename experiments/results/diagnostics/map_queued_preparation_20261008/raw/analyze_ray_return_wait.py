"""Recompute all real Ray fixture associations and return segments from raw observations."""
import argparse
import gzip
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import statistics


def digest(value):
    return hashlib.sha256(value).hexdigest()


def distribution(values):
    values = sorted(values)
    return dict(count=len(values), mean_ms=sum(values)/len(values)/1e6,
        median_ms=statistics.median(values)/1e6,
        p95_ms=values[math.ceil(.95*len(values))-1]/1e6, sum_seconds=sum(values)/1e9)


def analyze(root, ledger_root=None):
    suite = json.loads((root/'summary.json').read_text())
    assert suite['status']=='passed' and suite['issued_calls']==suite['actor_call_limit']==16448
    assert suite['http_requests']==suite['model_requests']==suite['pg_queries']==suite['gpu_requested']==0
    assert suite['cluster_connection_closed'] and len(suite['runs'])==20
    cases = []
    for run in suite['runs']:
        name, rows = run['run_id'], run['rows']
        directory = root/name
        result = json.loads((directory/'result.json').read_text())
        assert result['status']=='passed' and result['exactly_once']
        assert result['issued_calls']==result['accounted_calls']==rows
        assert result['actor_disposed'] and result['transport_drained'] and not result['cleanup_errors']
        with gzip.open(directory/'events.jsonl.gz', 'rt') as stream:
            events = [json.loads(line) for line in stream]
        def keyed(kind, key='key'):
            values = [e for e in events if e['event']==kind]
            data = {e[key]['sequence'] if key=='key' else e['sequence']:e for e in values}
            assert len(data)==len(values)==rows and set(data)==set(range(rows))
            return data
        received, guards = keyed('ray_http_completed'), keyed('remote_request_guard')
        counted = keyed('fixture_accounted', key='sequence')
        submitted, terminal = keyed('submitted'), keyed('terminal')
        future = {v['sequence']:v['future_ready_ns'] for v in result['future_ready']}
        workers = {v['sequence']:v for v in result['actor_calls']}
        assert len(future)==len(workers)==len(result['future_ready'])==len(result['actor_calls'])==rows
        assert set(future)==set(workers)==set(range(rows))
        assert sorted(e['attempt'] for e in counted.values())==list(range(1, rows+1))
        before, after, total, active_spans, durable = [], [], [], [], []
        for i in range(rows):
            request = f'fixture-input:{i}:'.encode()+b'x'*128
            output = f'fixture-output:{i}'.encode()
            assert workers[i]['request_sha256']==result['request_digest_by_sequence'][str(i)]==digest(request)
            assert result['result_digest_by_sequence'][str(i)]==digest(output)
            assert guards[i]['status']=='completed'
            assert counted[i]['monotonic_ns']<=received[i]['rpc_started_ns']
            started, ended = workers[i]['started_ns'], workers[i]['ended_ns']
            receipt, available = received[i], future[i]
            assert receipt['shared_clock'] and receipt['worker_started_ns']==started and receipt['worker_ended_ns']==ended
            assert submitted[i]['monotonic_ns']<=receipt['rpc_started_ns']<=started<=ended<=available<=receipt['received_ns']<=terminal[i]['monotonic_ns']
            before.append(available-ended)
            after.append(receipt['received_ns']-available)
            total.append(receipt['received_ns']-ended)
            active_spans.extend(((started, 1), (ended, -1)))
            durable.append((counted[i]['attempt'], digest(request)))
        active=peak=0
        for _, change in sorted(active_spans):
            active+=change
            assert active>=0
            peak=max(peak, active)
        assert active==0 and peak<=64 and peak==result['worker_active_peak']
        for event in events:
            usage=event.get('usage')
            if usage:
                assert 0<=usage['active_requests']<=64 and 0<=usage['active_work']<=64 and 0<=usage['held_tasks']<=128
            if 'object_bytes' in event:
                assert 0<=event['object_bytes']<=2**22
        assert sum(before)+sum(after)==sum(total)
        calculated={name:distribution(values) for name, values in (
            ('worker_end_to_callback', before), ('callback_to_receive', after), ('worker_end_to_receive', total))}
        guard_parts={name:distribution([g[name] for g in guards.values()]) for name in
            ('reserve_ns', 'hash_ns', 'request_observe_ns')}
        if result['guard_kind']=='threaded':
            guard_parts.update({name:distribution([g[name] for g in guards.values()]) for name in
                ('reserve_queue_ns', 'reserve_resume_ns')})
        prep=[e for e in events if e['event']=='ray_preparation_completed']
        work=[e for e in events if e['event']=='ray_work']
        assert result['prepared_blocks']==len(prep)
        assert result['object_batches']==sum(e['event']=='ray_block_put' for e in events)
        for metric, values in calculated.items():
            for field, value in values.items():
                assert math.isclose(result[metric][field], value, rel_tol=1e-12, abs_tol=1e-12)
        if ledger_root is not None:
            path=(ledger_root/name/'ledger.sqlite').absolute()
            with sqlite3.connect(path.as_uri()+'?mode=ro', uri=True) as connection:
                saved=connection.execute('SELECT sequence,request_sha256 FROM shared_requests ORDER BY sequence').fetchall()
                closed=connection.execute('SELECT unit_id FROM closed_shared_units').fetchall()
                quota=connection.execute('SELECT limit_count,allocated FROM budget').fetchone()
            assert saved==sorted(durable) and closed==[('case',)] and quota==(rows, rows)
        seconds=(result['query_completed_ns']-result['query_started_ns'])/1e9
        assert seconds==result['query_seconds']
        cases.append(dict(run_id=name, phase=run['phase'], repeat=run['repeat'], rows=rows,
            path=result['path'], guard_kind=result['guard_kind'], query_seconds=seconds,
            first_result_seconds=result['first_result_seconds'], **calculated,
            guard=result['guard'], prepared_blocks=result['prepared_blocks'], object_batches=result['object_batches'],
            guard_parts=guard_parts,
            core_active_mean=sum(terminal[i]['monotonic_ns']-submitted[i]['monotonic_ns'] for i in range(rows))/1e9/seconds,
            preparation_payload_sum_seconds=sum(e['payload_ns'] for e in prep)/1e9 if prep else None,
            preparation_put_sum_seconds=sum(e['put_ns'] for e in prep)/1e9 if prep else None,
            transport_work_sum_seconds={stage:sum(e['work_ns'] for e in work if e['stage']==stage and e['work_ns'] is not None)/1e9
                for stage in ('payload_next', 'object_put', 'payload_close')},
            object_peak_bytes=result['object_peak_bytes'], actor_active_peak=peak,
            actor_active_mean=sum(w['ended_ns']-w['started_ns'] for w in workers.values())/1e9/seconds,
            events_sha256=digest((directory/'events.jsonl.gz').read_bytes()),
            result_sha256=digest((directory/'result.json').read_bytes())))
    aggregates={}
    for path in ('immediate', 'coalesced'):
        for guard in ('sync', 'threaded'):
            measured=[c for c in cases if c['phase']=='measurement' and c['path']==path and c['guard_kind']==guard]
            assert [c['repeat'] for c in measured]==[1, 2, 3]
            values=dict(query_seconds=statistics.median(c['query_seconds'] for c in measured),
                first_result_seconds=statistics.median(c['first_result_seconds'] for c in measured),
                prepared_blocks=statistics.median(c['prepared_blocks'] for c in measured),
                object_batches=statistics.median(c['object_batches'] for c in measured),
                core_active_mean=statistics.median(c['core_active_mean'] for c in measured),
                actor_active_mean=statistics.median(c['actor_active_mean'] for c in measured))
            for metric in ('worker_end_to_callback', 'callback_to_receive', 'worker_end_to_receive', 'guard'):
                values[metric+'_mean_ms']=statistics.median(c[metric]['mean_ms'] for c in measured)
            for metric in measured[0]['guard_parts']:
                values[metric+'_mean_ms']=statistics.median(c['guard_parts'][metric]['mean_ms'] for c in measured)
            aggregates[path+'-'+guard]=values
    return dict(status='passed', scope='real single-node Ray/Daft and durable accounting; synthetic sleep service; no PG/HTTP/model/GPU work',
        analyzer_sha256=digest(Path(__file__).read_bytes()),
        verified_cases=len(cases), verified_actor_calls=sum(c['rows'] for c in cases),
        private_sqlite_verified=ledger_root is not None, cases=cases, medians=aggregates,
        limitations=['Python callback observation includes GIL/scheduling, not physical data arrival',
                     'Core consumption time is not SQL or model E2E', 'small deterministic text payload, single node and Job'],
        http_requests=0, model_requests=0, pg_queries=0)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--ledger-root', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args=parser.parse_args()
    result=analyze(args.evidence, args.ledger_root)
    args.output.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(dict(status=result['status'], verified_cases=result['verified_cases'],
        verified_actor_calls=result['verified_actor_calls'], private_sqlite_verified=result['private_sqlite_verified'],
        medians=result['medians']), indent=2))
