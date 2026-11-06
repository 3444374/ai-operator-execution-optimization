"""One native Daft Q3 worker and result-ready-based process supervision.

The caller provides the host lease, permanent attempt reservation, environment
preflight and final GPU/SHM observation. This entry never installs or retries.
"""

import argparse
import math
from pathlib import Path
import time

from src.infrastructure.private_artifacts import (
    new_private_directory, publish_private_json, read_json, write_private_json,
)
from src.infrastructure.redact import redact_text


def _save_artifact(path, value, *, primary=None, publish=False):
    try:
        (publish_private_json if publish else write_private_json)(path, value)
    except Exception as error:
        if primary is None:
            raise
        primary.add_note('native artifact ' + path.name + ' write failed: ' + redact_text(str(error)))
        return False
    return True


def configuration(value):
    fields = {'schema', 'reviews_csv', 'model', 'sampling_params', 'engine_kwargs',
              'provider_options', 'query_timeout_s', 'cleanup_s', 'stop_grace_s'}
    if type(value) is not dict or set(value) != fields or value['schema'] != 'semloom.native_daft_q3.v1':
        raise ValueError('an explicit native Daft Q3 configuration is required')
    for name in ('query_timeout_s', 'cleanup_s', 'stop_grace_s'):
        number = value[name]
        if type(number) not in (int, float) or not math.isfinite(number) or number <= 0:
            raise ValueError(name + ' must be finite and positive')
    if value['stop_grace_s'] >= value['cleanup_s']:
        raise ValueError('forced process stop must fit inside the cleanup allowance')
    for name in ('sampling_params', 'engine_kwargs', 'provider_options'):
        if type(value[name]) is not dict:
            raise ValueError(name + ' must be an explicit object')
    return value


class QueryDeadline:
    """One cleanup allowance, including graceful close and forced process stop."""
    def __init__(self, ready_path, *, query_s, cleanup_s, stop_grace_s, monitor=None, clock=time.monotonic):
        self.path = Path(ready_path)
        self.clock, self.monitor = clock, monitor
        self.started = clock()
        self.query_deadline = self.started + query_s
        self.cleanup_s, self.stop_grace_s = cleanup_s, stop_grace_s
        self.ready = None

    def __call__(self, identities):
        if self.monitor is not None:
            self.monitor(identities)
        if self.ready is None and self.path.exists():
            value = read_json(self.path)
            if value['schema'] != 'semloom.native_daft_q3.result_ready.v1':
                raise ValueError('unexpected native result-ready artifact')
            self.ready = value['result_ready_monotonic_ns'] / 1e9
        now = self.clock()
        if self.ready is not None and self.ready > self.query_deadline:
            raise TimeoutError('native Daft result exceeded its complete query allowance')
        if self.ready is None and now >= self.query_deadline:
            raise TimeoutError('native Daft query exceeded its complete query allowance')
        if self.ready is not None and now >= self.ready + self.cleanup_s - self.stop_grace_s:
            raise TimeoutError('native Daft post-result close exceeded its graceful allowance')

    def report(self):
        return dict(started_monotonic=self.started, query_deadline_monotonic=self.query_deadline,
            result_ready_monotonic=self.ready, cleanup_s=self.cleanup_s,
            forced_stop_reserved_s=self.stop_grace_s,
            cleanup_deadline_monotonic=None if self.ready is None else self.ready + self.cleanup_s)


def run_worker(config, directory, *, execute_query=None, tokenizer=None):
    started = time.monotonic_ns()
    config = configuration(config)
    directory = Path(directory)
    if not directory.exists():
        new_private_directory(directory)
    from src.baselines.text.sembench_movie_native import load_q3_source

    source = load_q3_source(config['reviews_csv'])
    ready_path = directory / 'result-ready.json'
    if ready_path.exists():
        raise FileExistsError('native query output must not reuse an earlier result')

    def ready(result):
        timestamp = result['metadata']['result_ready_monotonic_ns']
        publish_private_json(ready_path, dict(schema='semloom.native_daft_q3.result_ready.v1',
            result_ready_monotonic_ns=timestamp, worker_started_monotonic_ns=started,
            cold_result_ready_seconds=(timestamp - started) / 1e9,
            source=source['source'], result=result))

    report = dict(schema='semloom.native_daft_q3.worker.v1', status='failed',
        source=source['source'], warmup=0, whole_query_retries=0, performance_claim=False,
        model_ready_query_seconds=None, physical_gpu_exit_confirmed=None,
        result_artifact='result-ready.json', worker_started_monotonic_ns=started)
    primary = None
    try:
        if execute_query is None:
            from src.baselines.text.frameworks.daft_sembench_native import execute_q3
            execute_query = execute_q3
        if tokenizer is None:
            from transformers import AutoTokenizer
            tokenizer = AutoTokenizer.from_pretrained(config['model'], local_files_only=True,
                                                       trust_remote_code=False)
        result = execute_query(source['rows'], model=config['model'], tokenizer=tokenizer,
            sampling_params=config['sampling_params'], engine_kwargs=config['engine_kwargs'],
            provider_options=config['provider_options'], on_result_ready=ready,
            close_timeout_s=config['cleanup_s'] - config['stop_grace_s'])
        report.update(status='completed', model_close_receipt=result['metadata']['model_close_receipt'])
    except BaseException as error:
        primary = error
        partial = getattr(error, 'native_q3_report', None)
        report.update(error_type=type(error).__name__, error=redact_text(str(error)),
            model_close_receipt=getattr(error, 'model_close_report', None))
        if partial is not None:
            if report['model_close_receipt'] is None:
                report['model_close_receipt'] = partial['metadata']['model_close_receipt']
            if not ready_path.exists():
                # Materialized predictions can survive a later count or READY
                # publication failure. They do not establish a ready result.
                saved = _save_artifact(directory / 'failed-result.json', dict(
                    schema='semloom.native_daft_q3.failed_result.v1', source=source['source'],
                    result=partial, error_type=type(error).__name__), primary=error, publish=True)
                report['result_artifact'] = 'failed-result.json' if saved else None
        raise
    finally:
        report.update(worker_end_monotonic_ns=time.monotonic_ns(), result_ready_recorded=ready_path.exists())
        _save_artifact(directory / 'worker.json', report, primary=primary)
    return report


def supervised_run(config_path, directory, *, model_python, monitor=None):
    from src.experiments.lotus_records_supervisor import supervise

    config = configuration(read_json(config_path))
    directory = Path(directory)
    new_private_directory(directory)
    deadline = QueryDeadline(directory / 'result-ready.json', query_s=config['query_timeout_s'],
        cleanup_s=config['cleanup_s'], stop_grace_s=config['stop_grace_s'], monitor=monitor)
    command = [str(model_python), '-u', '-B', '-m', 'src.experiments.native_daft_query', 'worker',
               '--config', str(config_path), '--output-dir', str(directory)]
    primary = None
    try:
        return supervise(command, directory / 'supervision',
            timeout_s=config['query_timeout_s'] + config['cleanup_s'],
            grace_s=config['stop_grace_s'], natural_exit_grace_s=config['stop_grace_s'] / 2,
            monitor=deadline)
    except BaseException as error:
        primary = error
        raise
    finally:
        _save_artifact(directory / 'deadline.json', deadline.report(), primary=primary)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('worker',))
    parser.add_argument('--config', required=True)
    parser.add_argument('--output-dir', required=True)
    args = parser.parse_args(argv)
    run_worker(read_json(args.config), args.output_dir)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
