"""Matched local Core/transport control using synthetic tables and service."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import platform
import sqlite3
import subprocess
import time

from src.baselines.common.private_artifacts import new_private_directory, write_private_json
from src.baselines.common.redact import redact_json_values
from .map_observation_probe import run_case


def run_probe(output):
    output = Path(output).absolute()
    new_private_directory(output)
    repo = Path(__file__).resolve().parents[3]
    sources = ('mapped_core_probe.py', 'map_observation_probe.py', 'mapped_request_budget.py',
               'cell_budget.py', 'shared_request_budget.py', 'choice_gateway_observer.py', 'buffered_events.py')
    summary = dict(schema='semloom.mapped_core_probe.v1', status='running',
        scope='local_core_transport_with_synthetic_vendor_and_service',
        reference_commit=subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True).strip(),
        source_sha256={name: hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest() for name in sources},
        started_utc=datetime.now(timezone.utc).isoformat(), python=platform.python_version(),
        sqlite=sqlite3.sqlite_version, platform=platform.system(), machine=platform.machine(),
        synthetic_call_limit=8224, max_seconds=300, http_requests=0, model_requests=0, pg_queries=0, runs=[])
    write_private_json(output / 'manifest.json', summary)
    began = time.monotonic()
    try:
        for phase, repeat in (('check', -1), ('warmup', 0), ('measurement', 1),
                              ('measurement', 2), ('measurement', 3)):
            modes = ('durable', 'mapped') if repeat <= 0 or repeat % 2 else ('mapped', 'durable')
            for mode in modes:
                if time.monotonic() - began >= 240:
                    raise TimeoutError('Core diagnosis overall deadline')
                identity = f'{phase}-{repeat}-{mode}'
                summary['active_run'] = identity
                result = run_case(output / identity, mode, 'buffered', rows=16 if phase == 'check' else 1024)
                summary['runs'].append(dict(run_id=identity, phase=phase, repeat=repeat, **result))
                print(json.dumps({key: result[key] for key in ('accounting', 'query_seconds',
                    'accounting_setup_seconds', 'preparation_windows', 'core_active_mean',
                    'synthetic_worker_active_mean')}), flush=True)
        if sum(r['synthetic_calls'] for r in summary['runs']) != summary['synthetic_call_limit']:
            raise AssertionError('Core suite call total differs')
        summary['status'] = 'passed'
    except BaseException as error:
        summary.update(status='failed', error_type=type(error).__name__)
        failed = output / summary.get('active_run', 'not-started') / 'result.json'
        if failed.is_file():
            summary['failed_run'] = json.loads(failed.read_text())
        raise
    finally:
        summary.update(completed_utc=datetime.now(timezone.utc).isoformat(), suite_seconds=time.monotonic() - began)
        write_private_json(output / 'summary.json', redact_json_values(summary))
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    run_probe(parser.parse_args(argv).output)


if __name__ == '__main__':
    main()
