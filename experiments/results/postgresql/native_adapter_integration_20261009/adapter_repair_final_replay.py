"""Verify the final vendor repairs, compiled regressions and resident inputs."""
import base64
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path

root=Path(__file__).parent
identity=json.loads((root/'adapter-repair-final-verification.json').read_text())
archive=root/identity['public_archive']['path']
assert hashlib.sha256(archive.read_bytes()).hexdigest()==identity['public_archive']['sha256']
members={}
with gzip.open(archive,'rt') as stream:
    for line in stream:
        record=json.loads(line);name=record['path']
        assert not Path(name).is_absolute() and '..' not in Path(name).parts and name not in members
        data=base64.b64decode(record['data_base64'],validate=True)
        assert hashlib.sha256(data).hexdigest()==record['sha256']
        members[name]=(data,record['original_sha256'])
assert len(members)==identity['public_archive']['members']
def value(name):return json.loads(members[name][0])
def lines(name):return [json.loads(line) for line in members[name][0].splitlines()]
base='adapter-repair-fixture03/'
owner=value(base+'owner-result.json');assert owner['status']=='passed' and owner['real_model_posts']==0
posts=0
for run in owner['runs']:
    arm=run['arm'];assert run['exit_code']==0 and not run['timed_out']
    prefix=base+arm+'/'
    inputs=value(prefix+'fixture-inputs.json');requests=lines(prefix+'fixture-requests.jsonl')
    assert [len(rows) for rows in inputs]==[8,8,128] and len(requests)==144
    owners=[];offset=0
    for ordinal,rows in enumerate(inputs):
        query=prefix+'supplier-query-'+str(ordinal)+'/'
        summary=value(query+'summary.json');timing=value(query+'persistent-query.json')
        assert summary['status']=='passed' and summary['actual_posts']==len(rows)
        assert timing['query_summary_sha256']==members[query+'summary.json'][1]
        assert timing['t_release_ns']<=timing['t_submit_ns']<timing['t_eof_ns']
        assert Counter(tuple(v['row']) for v in lines(query+'q0/results.jsonl'))==Counter((v['row_id'],'ok') for v in rows)
        traces=lines(query+'http-trace.jsonl');assert len(traces)==len(rows)
        assert all(v['status']=='completed' and v['retry_count']==0 and
            v['request_body_sha256']==v['forwarded_body_sha256'] for v in traces)
        expected=Counter(v['text'] for v in rows);observed=Counter()
        for request in requests[offset:offset+len(rows)]:
            matches=[text for text in expected if text in json.dumps(request)]
            assert len(matches)==1;observed.update(matches)
        assert observed==expected;offset+=len(rows);owners.append(timing['persistent_lifecycle'])
        if arm.startswith('lotus-'):
            events=lines(query+'method-events.jsonl')
            returned=[v for v in events if v['event']=='lotus_batch_return'];assert len(returned)==1
            assert returned[0]['response_count']==len(rows)
            assert timing['t_submit_ns']<=returned[0]['monotonic_ns']<=timing['t_eof_ns']
    for key in ('owner_id','execution_id','lm_id','duckdb_connection_id','sema_pid','ray_session_id'):
        assert len({v[key] for v in owners})==1,(arm,key)
    assert value(prefix+'group/group-summary.json')['status']=='passed'
    posts+=144
endpoint=value(base+'sema-native-direct-endpoint/endpoint-verification.json')
assert endpoint['endpoint_changed'] and endpoint['same_author_pid'] and endpoint['process_closed']
posts+=endpoint['fixture_posts']
assert posts==owner['fixture_posts']==identity['fixture_posts']==1154
source=value(base+'source-verification.json')
assert source['status']=='passed' and source['source_commit']==identity['source_commit']
assert source['files_verified']==identity['source_files']
for run in identity['regressions']:
    log=members[base+run['label']+'.txt'][0].decode()
    assert run['exit_code']==0 and 'Ran '+str(run['total'])+' tests' in log
    assert '\nOK' in log
    if run['label']=='duckdb-actual-regression':assert run['passed']==run['total']==20 and run['skipped']==0
for run in owner['runs']:
    group=value(base+run['arm']+'/group/group-summary.json')
    assert group['cleanup_errors']=={} and not group['poisoned']
    state=group['owners'][run['arm']]
    assert state['completed_queries']==3 and state['result_cache'] is False
    if state['core_usage'] is not None:
        assert all(v==0 for v in state['core_usage'].values()) and state['core_jobs']==0
cleanup_cases={'code/tests/semantic_methods/test_lotus_cleanup.py': ['test_input_error_survives_close_failure', 'test_decode_error_survives_release_and_close_failures', 'test_observation_error_keeps_previous_full_response_and_closes_all_deliveries', 'test_sdk_error_keeps_full_response_and_survives_cleanup_failures', 'test_release_failure_without_execution_error_propagates_and_close_runs', 'test_first_cleanup_error_survives_later_close_failure', 'test_close_failure_after_success_propagates', 'test_response_limit_error_keeps_full_response_and_survives_cleanup_failures', 'test_normal_success_retains_full_response_sdk_usage_and_returns_core_resources', 'test_cleanup_diagnostics_reset_for_next_batch', 'test_cleanup_error_in_callers_except_block_is_not_suppressed'], 'code/tests/execution_provider/test_sema_service_cleanup.py': ['test_runner_failure_still_closes_executor_client_and_loop', 'test_multiple_close_failures_keep_all_stages_and_first_exception', 'test_query_first_error_survives_thread_timeout_and_keeps_failure_artifact', 'test_thread_timeout_without_query_error_is_reported', 'test_trace_failures_are_all_recorded_without_replacing_query_error', 'test_cancel_failure_keeps_query_error_and_continues_trace_close', 'test_cancel_failure_without_query_error_propagates_after_trace_close', 'test_http_error_arriving_during_join_remains_primary_with_cleanup_failure', 'test_http_error_arriving_during_join_without_cleanup_failure_is_reported', 'test_actual_http_runner_failure_drains_core_on_io_thread', 'test_actual_http_keeps_query_error_when_runner_cleanup_fails', 'test_startup_failure_survives_executor_close_failure', 'test_site_failure_before_close_waits_for_http_writer_before_core_close', 'test_persistent_site_stop_failure_retains_unconfirmed_listener']}
cleanup_log=members[base+'integrated-regression.txt'][0].decode()
for names in cleanup_cases.values():
    for name in names:
        assert any(line.startswith(name+' (') and line.endswith(' ... ok') for line in cleanup_log.splitlines()),name
processes=value(base+'final-process-cleanup.json')
assert processes['owned_ray_processes_remaining']==[] and processes['real_model_posts']==0
print(str(len(members))+' final-source members verified; compiled regressions and 8 arms / 26 resident queries / 1154 fixture POST; first-error checks, owner reuse and returned Core resources; zero model POST')
