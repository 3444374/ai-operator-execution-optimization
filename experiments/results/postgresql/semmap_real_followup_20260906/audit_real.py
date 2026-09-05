"""Read-only allowlisted audit; original request/output text never leaves the host."""
import argparse
from collections import Counter
import hashlib
import importlib.util
import json
from pathlib import Path

from src.baselines.common.redact import redact_text


def digest(path):
    with path.open('rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()


def load(path):
    return json.loads(path.read_text())


def audit_run(root, trace_audit):
    summary = load(root/'summary.json')
    result = {key: summary.get(key) for key in ('status', 'scope', 'runtime_commit', 'initial_attempts',
        'final_attempts', 'required_phases', 'error_type', 'error_code', 'quality_evaluated', 'performance_evaluated')}
    result['phases'] = {}
    processes = set()
    for phase, value in summary['phases'].items():
        path = root/phase
        item = {key: value[key] for key in ('measurement_status', 'policy_status', 'safe')}
        item['failure_metrics'] = [v['metric'] for v in value['failures']]
        item['problem_count'] = len(value['problems'])
        for window in ('baseline', 'operation', 'cleanup'):
            item[window] = trace_audit(path/window/'process_samples.jsonl.gz')
            processes.update(tuple(v['baseline_identity']) for v in item[window]['roles'].values())
        outcome = load(path/'operation/operation_outcome.json')
        item['sqlstate'] = outcome.get('operation_error', {}).get('sqlstate')
        item['sampling_error_count'] = len(outcome.get('sampling_errors', []))
        events = load(path/'http-evidence.json')
        requests = [event for event in events if event['event'] == 'request']
        completions = [event for event in events if event['event'] == 'completion']
        item['http_events'] = dict(Counter(event['event'] for event in events))
        item['completion_models_match'] = all(event['response_model_id'] == 'qwen2.5-7b' for event in completions)
        item['finish_reasons'] = [event['finish_reason'] for event in completions]
        item['request_attempts'] = [event['attempt'] for event in requests]
        item['request_input_bytes'] = [len(event['body']['messages'][1]['content'].encode()) for event in requests]
        item['usage'] = [{'prompt_tokens': event['prompt_tokens'], 'output_tokens': event['output_tokens']} for event in completions]
        item['output_sha256'] = [hashlib.sha256(event['raw_output'].encode()).hexdigest() for event in completions]
        sql_result = path/'sql-result.json'
        if sql_result.exists():
            rows = load(sql_result)
            nonnull = [row for row in rows if row[1] is not None]
            item['sql_row_ids'] = [row[0] for row in rows]
            item['sql_null_rows'] = sum(row[1] is None for row in rows)
            item['null_output_preserved'] = all(row[2] is None for row in rows if row[1] is None)
            item['sql_bytes_match_raw_completion'] = len(nonnull) == len(completions) and all(
                row[2].encode() == event['raw_output'].encode() for row, event in zip(nonnull, completions))
            item['request_input_matches_sql_row'] = len(nonnull) == len(requests) and all(
                row[1] == event['body']['messages'][1]['content'] for row, event in zip(nonnull, requests))
        item['service_ended_queue'] = load(path/'service-ended-queue.json')
        item['socket_peak_from_report'] = value['diagnostics']['peak']['peak']['provider_uds_session_fd_peak_delta_combined']
        result['phases'][phase] = item
    checks = []
    for path in root.rglob('SHA256SUMS.json'):
        entries = load(path)
        checks.append({'path': str(path.relative_to(root)), 'sha256': digest(path), 'entries': len(entries),
                       'mismatches': sum(not (path.parent/name).is_file() or digest(path.parent/name) != expected
                                         for name, expected in entries.items())})
    result['hash_checks'] = checks
    result['original_selected_file_sha256'] = {str(path.relative_to(root)): digest(path) for path in root.rglob('*')
        if path.is_file() and path.relative_to(root).parts[0] not in ('data', 'socket')}
    result['same_processes_still_active'] = []
    for pid, start in sorted(processes):
        try:
            stat = Path('/proc', str(pid), 'stat').read_text()
            fields = stat[stat.rfind(')')+2:].split()
            if int(fields[19]) == start and fields[0] != 'Z':
                result['same_processes_still_active'].append([pid, start])
        except FileNotFoundError:
            pass
    result['postmaster_pid_file_exists'] = (root/'data/postmaster.pid').exists()
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--trace-auditor', type=Path, required=True)
    parser.add_argument('--artifact-root', type=Path, required=True)
    parser.add_argument('--ledger', type=Path, required=True)
    parser.add_argument('--readback-probe', type=Path, required=True)
    parser.add_argument('roots', nargs='+', type=Path)
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location('trace_auditor', args.trace_auditor)
    trace = importlib.util.module_from_spec(spec); spec.loader.exec_module(trace)
    ledger = args.ledger.read_bytes(); lines = ledger.splitlines(keepends=True)
    cleanup = load(args.artifact_root/'service-cleanup.json')
    result = {'producer': 'read_only_allowlisted_real_audit', 'raw_payload_exported': False,
        'runs': {root.name: audit_run(root, trace.trace_audit) for root in args.roots},
        'ledger': {'attempts': len(lines)-1, 'limit': 32, 'sha256': hashlib.sha256(ledger).hexdigest(),
                   'original_25_prefix_sha256': hashlib.sha256(b''.join(lines[:26])).hexdigest()},
        'readback_probe': load(args.readback_probe),
        'ledger_failure_regression': load(args.artifact_root/'ledger-failure-regression-v3.json'),
        'service_cleanup': {key: cleanup[key] for key in ('service_identity_unchanged_before_stop', 'active_owned_pids_after_stop',
            'port_listening_after_stop', 'gpu_memory_after_mb', 'ledger_final_attempts')},
        'model_files': load(args.artifact_root/'model-file-hashes.json'),
        'official_model_identity': load(args.artifact_root/'semmap-model-upstream-identity.json'),
        'service_verified': load(args.artifact_root/'service-verified.json'),
        'artifact_file_sha256': {path.name: digest(path) for path in args.artifact_root.iterdir() if path.is_file()},
        'experiment_identities': [load(args.artifact_root/name) for name in ('experiment-commit-v2.json', 'experiment-commit-v3.json')]}
    print(redact_text(json.dumps(result, indent=2, sort_keys=True)))
