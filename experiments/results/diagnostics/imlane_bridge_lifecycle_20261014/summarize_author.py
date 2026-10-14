"""Reduce retained private software records; no HTTP, author startup or model."""
import argparse
import base64
from collections import Counter
import hashlib
import json
from pathlib import Path


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def summarize(directory):
    inputs = {}
    records = {}
    for name in ('cpython01-complete.json', 'ray01-complete.json',
                 'build-and-preflight.json', 'final-check02.json', 'delta01-check.stdout'):
        raw = (directory / name).read_bytes()
        inputs[name] = dict(bytes=len(raw), sha256=digest(raw))
        records[name] = json.loads(raw)
    build = records['build-and-preflight.json']['build']['record']
    assert all(records['build-and-preflight.json'][name]['record']['status'] == 'ok'
               for name in ('author', 'lotus'))
    final = records['final-check02.json']
    delta = records['delta01-check.stdout']
    assert build['returncode'] == 0 and final['author_asset_sha_unchanged']
    assert final['source_checks_passed'] and not final['owned_survivors']
    assert not final['host_lease_exists'] and not final['gpu_compute_apps']
    cells, observations, cleanup = [], [], []
    sdk_environment = None
    author_environment = None
    comparisons = dict(requests=0, consumed_responses=0, consumed_response_bytes=0)
    identities = set()
    cases = ('healthy', 'transport', 'timeout', 'consumer', 'cancel', 'crash', 'parser')
    for runtime in ('cpython', 'ray'):
        report = records[runtime + '01-complete.json']['summary']
        assert report['passed'] and report['candidate_commit'] == delta['candidate_commit']
        assert report['source_manifest_sha256'] == delta['source_lock_sha256']
        assert report['bridge_sha256'] == build['binary_sha256']
        assert report['wall_limit_s'] == 0 and not report['resource_errors']
        if author_environment is None:
            author_environment = report['versions']
        assert report['versions'] == author_environment
        assert len(report['cleanup']) == len(cases) * 2
        for index, cell in enumerate(report['cells']):
            reuse, sdk, parser = cell['reuse_bridge'], cell['sdk'], cell['parser']
            assert cell['passed'] and sdk['passed'] and parser['passed']
            batches = cell['healthy']['batches']
            expected = [1, 0, 0] if reuse else [1, 1, 1]
            assert [b['bridge_stats']['lifecycle']['context_creations'] for b in batches] == expected
            assert [b['bridge_stats']['lifecycle']['registrations'] for b in batches] == expected
            assert [b['bridge_stats']['lifecycle']['output_copies'] for b in batches] == [4, 4, 4]
            assert cell['healthy']['output_lifecycle'] and cell['healthy']['controlled_reordering']
            assert sdk['fixture']['actual_posts'] == 28 and parser['fixture']['actual_posts'] == 4
            assert [b['rows'] for b in sdk['batches']] == [4, 12, 6, 6]
            assert parser['first_error']['expected'] and parser['unknown']['new_owner_rejected']
            assert parser['unknown']['first_marker_unchanged']
            assert parser['unknown']['remote_http_stopped'] == 'unavailable'
            for fact in (sdk, parser):
                requests = {(r['batch'], r['call_id']): r for r in fact['requests']}
                receipts = fact['fixture']['receipts']
                assert len(requests) == len(receipts)
                # Original SDK headers do not carry the adapter's internal call ID.
                key = lambda r: (r['payload_sha256'], r['payload_bytes'])
                assert Counter(map(key, requests.values())) == Counter(map(key, receipts))
                for receipt in receipts:
                    choices = [v for v in requests.values() if key(v) == key(receipt)]
                    assert choices
                    request = choices[0]
                    assert request['payload_sha256'] == receipt['payload_sha256']
                    assert request['payload_bytes'] == receipt['payload_bytes']
                    for name in {k.lower() for k, _v in request['headers']}:
                        assert [v for k, v in request['headers'] if k.lower() == name] == [
                            v for k, v in receipt['headers'] if k.lower() == name]
                    comparisons['requests'] += 1
                for response in fact['responses']:
                    full = response
                    raw = base64.b64decode(full['body_base64'], validate=True)
                    assert digest(raw) == full['body_sha256'] and len(raw) == full['body_bytes']
                    matches = [r for r in fact['fixture']['responses']
                               if r['body_sha256'] == full['body_sha256'] and r['body_bytes'] == len(raw)]
                    assert matches
                    receipt = matches[0]
                    assert all(full[k] == receipt[k] for k in
                               ('body_sha256', 'body_bytes', 'status_code', 'headers', 'http_version'))
                    comparisons['consumed_responses'] += 1
                    comparisons['consumed_response_bytes'] += len(raw)
            assert all(r['complete_response_bytes_unchanged'] for r in sdk['output_lifecycle'])
            environment = dict(versions=sdk['module_versions'], source_sha256=sdk['sdk_source_hashes'])
            if sdk_environment is None:
                sdk_environment = environment
            assert environment == sdk_environment
            cell_id = runtime + ('-reuse' if reuse else '-default')
            cells.append(dict(id=cell_id, passed=True, root_posts=cell['root_posts'],
                root_reserved=cell['root_reserved'], sdk_posts=28, parser_posts=4,
                actual_posts=cell['actual_posts'], post_limit=cell['post_limit'],
                healthy_root_rows=[4, 4, 4], context_creations=expected, registrations=expected,
                same_healthy_bridge_pid=(len({b['bridge_pid'] for b in batches}) == 1 if reuse else None),
                independent_batch_ids=(len({b['batch_id'] for b in batches}) == 3 if reuse else None),
                output_lifecycle_verified=True, sdk_rows=[4, 12, 6, 6],
                sdk_context_creations=[b['close']['bridge_stats']['lifecycle']['context_creations'] for b in sdk['batches']],
                query_results=sdk['query_results'], author_workers=len(cell['workers']['workers']),
                author_traced_healthy_calls=cell['workers']['rows'], faults=[r['case'] for r in cell['cases']],
                parser_consumed_responses=len(parser['responses']), parser_first_error_preserved=True,
                declared_coordinator_executors=batches[0]['actual_coordinator_executors'],
                native_settings=batches[0]['native_settings'],
                native_settings_are_physical_executor_count=batches[0]['native_settings_are_physical_executor_count']))
            observations.append(dict(cell=cell_id,
                root_batches=[dict(stats=b['bridge_stats'], python_timing_ns=b['timing_ns']) for b in batches],
                sdk_batches=[dict(label=b['label'], rows=b['rows'], stats=b['close']['bridge_stats']) for b in sdk['batches']],
                faults=[dict(case=r['case'], passed=r['passed'], clean=r['close']['clean'],
                    bridge_stopped=r['close']['bridge_stopped'], remote_http_stopped=r['close']['remote_http_stopped'],
                    marker_sha256=r['marker_sha256'], new_owner_rejected=r['new_owner_rejected'],
                    first_error_type=None if r['first_error'] is None else r['first_error']['type']) for r in cell['cases']]))
            for case, row in zip(cases, report['cleanup'][index * 7:(index + 1) * 7]):
                assert not row['errors'] and not row['owned_survivors'] and not row['remaining_shm']
                assert row['owned_threads_stopped'] and not row['forced_identities']
                assert row['passed'] or row.get('expected_fault_owner_cleanup_verified')
                identities.update((p['pid'], p['start_ticks']) for p in row['owned_process_identities'])
                cleanup.append(dict(cell=cell_id, case=case, original_passed=row['passed'],
                    expected_fault_owner_cleanup_verified=row.get('expected_fault_owner_cleanup_verified', False),
                    forced_process_count=len(row['forced_identities']),
                    shm_fallback_members=len(row['forced_shared_memory']),
                    shm_bytes_before_fallback=sum(row['shared_memory_before_fallback'].values()),
                    owned_survivors=0, remaining_shm_bytes=0, threads_stopped=True,
                    rss_peak_bytes=row['rss_peak_bytes'], shm_peak_bytes=row['shared_memory_peak_bytes']))
    assert len(identities) == final['owned_identity_count']
    summary = dict(schema='semloom.imlane.bridge_author_checks.v1', date='2026-10-14',
        approved_base_commit=delta['approved_base_commit'], execution_commit=delta['candidate_commit'],
        author_commit=build['upstream_commit'], source_file_count=final['source_file_count'],
        source_manifest_sha256=delta['source_lock_sha256'], source_delta=delta['changed'],
        bridge=dict(source_sha256=build['source_sha256'], binary_sha256=build['binary_sha256'], compile_returncode=0),
        preflight=dict(author='ok', lotus='ok'), input_records=inputs,
        author_environment=author_environment, sdk_environment=sdk_environment,
        scope='actual author CPython/Ray and pinned original SDK, finite synthetic localhost software fixture',
        healthy_total_limit_s=0, host_cpu_quota_cores=16, assigned_cpu_slots=8,
        rss_limit_bytes=8 * 1024**3, namespace_shm_limit_bytes=2 * 1024**3,
        cells=cells, total_posts=sum(c['actual_posts'] for c in cells), sdk_byte_audit=comparisons,
        cleanup_scopes=len(cleanup), original_clean_scopes=sum(r['original_passed'] for r in cleanup),
        expected_fault_shm_cleanup_scopes=sum(bool(r['shm_fallback_members']) for r in cleanup),
        owned_identities_exit_verified=len(identities), lease_released=True, resource_errors=[],
        models=0, gpu=0, retries=0,
        failure_history=[dict(stage='control transport', outcome='macOS PTY truncated initial command; no child or output; terminal fixed before one retry'),
                         dict(stage='record reduction', outcome='field name mistakes in read-only summaries corrected; original stderr retained privately; no matrix rerun')],
        pending=['full original 2000-row workload/CLI', 'PG integration of the new session',
                 'async UDF combined with the new session', 'model quality and matched performance'],
        timing_rule='overlapping inclusive stages are not summed as JCT; software program wall is not a performance comparison')
    return summary, dict(schema='semloom.imlane.bridge_author_observations.v1', batches=observations, cleanup=cleanup)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    summary, observations = summarize(args.input_dir)
    for name, value in (('author-summary.json', summary), ('author-observations.json', observations)):
        (args.output_dir / name).write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')
    print(json.dumps(dict(cells=len(summary['cells']), posts=summary['total_posts'], cleanup_scopes=summary['cleanup_scopes'])))
