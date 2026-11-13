"""Prepare and audit a no-model Q3 through the author's DuckDB and DBEnd.

The outer controller owns the host lease, resource supervisor and final GPU/SHM
checks. This CPU entry never loads a model or substitutes a records execution.
"""

import argparse
import csv
import hashlib
import io
import math
import os
from pathlib import Path
import signal
import threading
import time

from src.baselines.text.sembench_movie_native import (
    describe_q3_source, evaluate_q3, load_q3_source, q3_messages, read_q3_source,
)
from src.infrastructure.private_artifacts import (
    content_digest, file_identity, new_private_directory, open_private_text,
    read_json, write_private_json,
)
from src.infrastructure.redact import redact_text


def _write_input_rows(stream, rows):
    writer = csv.DictWriter(stream, fieldnames=('occurrence_id', 'id', 'reviewId', 'reviewText'))
    writer.writeheader()
    writer.writerows(rows)


def _export_source(source, directory):
    directory = Path(directory)
    new_private_directory(directory)
    with open_private_text(directory / 'input.csv', newline='') as stream:
        _write_input_rows(stream, source['rows'])


def materialize_input(reviews_csv, directory, *, provider_mode):
    """Read and export the real gold-free input, without audit-only hashing."""
    if provider_mode not in {'model', 'software_ipc'}:
        raise ValueError('declare model or software_ipc input explicitly')
    source = read_q3_source(reviews_csv)
    directory = Path(directory)
    _export_source(source, directory)
    return dict(raw_bytes=source['raw_bytes'], rows=source['rows'],
        original_csv=str(Path(reviews_csv).resolve()), provider_mode=provider_mode)


def publish_input_packet(directory, snapshot):
    """Publish identity after execution, using the exact materialized snapshot."""
    directory = Path(directory)
    expected = io.StringIO(newline='')
    _write_input_rows(expected, snapshot['rows'])
    data = expected.getvalue().encode('utf-8')
    input_identity = dict(sha256=hashlib.sha256(data).hexdigest(), bytes=len(data))
    if file_identity(directory / 'input.csv') != input_identity:
        raise ValueError('native IMLane input.csv changed after materialization')
    packet = dict(schema='semloom.imlane.duckdb.q3.input.v1', provider_mode=snapshot['provider_mode'],
        original_csv=snapshot['original_csv'], source=describe_q3_source(snapshot),
        input=input_identity)
    write_private_json(directory / 'packet.json', packet)
    return packet


def prepare_input(reviews_csv, directory, *, provider_mode):
    """Keep the immediate input-plus-packet contract for existing CPU callers."""
    snapshot = materialize_input(reviews_csv, directory, provider_mode=provider_mode)
    return publish_input_packet(directory, snapshot)


def prepare_fixture(reviews_csv, directory):
    """Export all source rows; gold determines only explicit software replies."""
    source = load_q3_source(reviews_csv)
    directory = Path(directory)
    _export_source(source, directory)
    replies = {}
    for row, labeled in zip(source['rows'], source['labeled'], strict=True):
        if row['id'] != 'taken_3':
            continue
        if labeled['scoreSentiment'] not in ('POSITIVE', 'NEGATIVE'):
            raise ValueError('CPU fixture requires the original positive/negative gold labels')
        message = content_digest(q3_messages(row['reviewText']))
        reply = 'Answer: True' if labeled['scoreSentiment'] == 'POSITIVE' else 'Answer: False'
        if message in replies and replies[message] != reply:
            raise ValueError('identical messages have conflicting gold; use a different CPU fixture')
        replies[message] = reply
    write_private_json(directory / 'replies.json', replies)
    packet = dict(schema='semloom.imlane.duckdb.q3.fixture.v1', provider_mode='software_fixture',
        original_csv=str(Path(reviews_csv).resolve()), source=source['source'],
        input=file_identity(directory / 'input.csv'), model_requests=0)
    write_private_json(directory / 'packet.json', packet)
    return packet


def software_reply(messages_batch):
    """The author loads this CPU-only batch callable; no inference is performed."""
    replies = read_json(os.environ['SEMLOOM_IMLANE_Q3_REPLIES'])
    return [replies[content_digest(messages)] for messages in messages_batch]


def audit_result(packet_dir, result_path, receipt_dir, checkout, *, provider_mode='software_fixture'):
    """Evaluate the saved scalar and the same inference receipts, never the graph."""
    packet_dir = Path(packet_dir)
    packet = read_json(packet_dir / 'packet.json')
    schema = ('semloom.imlane.duckdb.q3.fixture.v1' if provider_mode == 'software_fixture'
              else 'semloom.imlane.duckdb.q3.input.v1')
    if (provider_mode not in {'software_fixture', 'software_ipc', 'model'}
            or packet['schema'] != schema or packet['provider_mode'] != provider_mode):
        raise ValueError('an explicit matching native IMLane input packet is required')
    source = load_q3_source(packet['original_csv'])
    if source['source'] != packet['source'] or file_identity(packet_dir / 'input.csv') != packet['input']:
        raise ValueError('native IMLane source changed after preparation')
    result = read_json(result_path)
    if result['status'] != 'query_returned' or result['native_ai_query_returns'] != 1:
        raise ValueError('native IMLane did not return the single Q3 result')
    if (result['source_rows'] != len(source['rows'])
            or result['eligible_rows'] != source['source']['eligible_rows']):
        raise ValueError('native SQL source/predicate differs from the prepared full cohort')
    predictions, calls = [], []
    for path in sorted(Path(receipt_dir).glob('q3-batch-*.json')):
        # These are complete, exclusively published owned artifacts. Their
        # actual retained bytes, rather than the generic config-file limit,
        # determine the read boundary; outer process RSS remains supervised.
        receipt = read_json(path, max_bytes=path.stat().st_size)
        if (receipt['schema'] != 'semloom.imlane.duckdb.q3.batch.v1'
                or receipt['provider_mode'] != provider_mode or receipt['status'] != 'succeeded'):
            raise ValueError('native IMLane has a failed or mismatched batch receipt')
        count = len(receipt['predictions'])
        batch_calls = receipt['provider_calls']
        if (receipt['logical_rows'] != count or len(receipt['sources']) != count
                or len(batch_calls) != 1 or batch_calls[0]['logical_rows'] != count
                or batch_calls[0]['status'] != 'succeeded' or batch_calls[0]['returned'] is not True):
            raise ValueError('native IMLane batch invocation remains incomplete or unknown')
        predictions.extend(receipt['predictions'])
        calls.extend(batch_calls)
    expected = {row['occurrence_id']: row for row in source['rows']}
    for row in predictions:
        original = expected[row['occurrence_id']]
        if row['messages_sha256'] != content_digest(q3_messages(original['reviewText'])):
            raise ValueError('native IMLane changed a Q3 message')
    quality = evaluate_q3(source, predictions,
        positive_review_cnt=result['positive_review_cnt'], checkout=checkout)
    return dict(quality=quality, predictions=predictions, provider_calls=calls,
        source=source['source'], native_result=result,
        model_requests=None if provider_mode == 'model' else 0,
        physical_model_attempts=None, provider_mode=provider_mode,
        performance_claim=False, qualification=(
            'model responses through author database path; external physical cleanup separate'
            if provider_mode == 'model' else 'CPU software replies through author database path'))


def run_fixture(args):
    from integrations.imlane_duckdb.runtime import DuckDBScope
    from integrations.imlane_records import owner

    for name in ('packet', 'output', 'author_root', 'driver'):
        setattr(args, name, Path(getattr(args, name)).resolve())
    # The venv's invocation path selects its site packages; keep that symlink.
    args.method_python = Path(args.method_python).absolute()
    packet = read_json(args.packet / 'packet.json')
    if packet['provider_mode'] != 'software_fixture':
        raise ValueError('this entry runs only a declared zero-model CPU fixture')
    new_private_directory(args.output)
    receipts = args.output / 'receipts'
    receipts.mkdir(mode=0o700)
    config = args.output / 'udf.json'
    write_private_json(config, dict(provider_path=str(Path(__file__).resolve()),
        provider_function='software_reply', provider_mode='software_fixture', receipt_dir=str(receipts)))
    args.scopes = []
    args.stop_budget = owner.StopBudget(threading.Event())
    previous_signals = {number: signal.signal(number, args.stop_budget.signal)
                        for number in (signal.SIGTERM, signal.SIGINT)}
    args.workload_environment = dict(SEMLOOM_IMLANE_Q3_CONFIG=str(config),
        SEMLOOM_IMLANE_Q3_REPLIES=str((args.packet / 'replies.json').resolve()))
    report = dict(schema='semloom.imlane.duckdb.q3.cpu_run.v1', status='failed', cleanup=[],
        model_requests=0, performance_claim=False, kind=args.kind)
    started = time.monotonic()
    primary = None
    try:
        kernel = Path(__file__).resolve().parents[2] / 'integrations/imlane_duckdb/q3_udf.py'
        with DuckDBScope(args, report, kernel=kernel) as scope:
            command = [str(args.driver), '--input', str(args.packet / 'input.csv'),
                '--module', str(kernel), '--function', 'infer_q3', '--kind', args.kind,
                '--output', str(args.output / 'query.json')]
            if args.threads is not None:
                command += ['--threads', str(args.threads)]
            process, writer = owner.spawn(command, scope.env, args.output / 'query.log')
            scope.children.append((process, writer))
            scope.track_root(process)
            remaining = max(0., args.query_seconds - (time.monotonic() - started))
            code = process.wait(timeout=remaining)
            writer.join(1)
            if code:
                raise RuntimeError('native author DuckDB Q3 failed; preserve query log and receipts')
            scope.check()
        args.stop_budget.check()
        report.update(status='passed', driver_exit=0,
            process_and_namespace_cleanup=owner.resource_state(scope),
            query_result='query.json', receipt_directory='receipts')
    except BaseException as error:
        primary = error
        report.update(error_type=type(error).__name__, error=redact_text(str(error)))
        raise
    finally:
        for number, handler in previous_signals.items():
            signal.signal(number, handler)
        report['elapsed_seconds'] = time.monotonic() - started
        try:
            write_private_json(args.output / 'worker.json', report)
        except Exception as error:
            if primary is None:
                raise
            primary.add_note('native IMLane CPU report failed: ' + redact_text(str(error)))
    return report


def _positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError('a positive integer is required')
    return number


def _positive_seconds(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError('finite positive seconds are required')
    return number


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='action', required=True)
    prepare = commands.add_parser('prepare-fixture')
    prepare.add_argument('--reviews-csv', type=Path, required=True)
    prepare.add_argument('--output', type=Path, required=True)
    worker = commands.add_parser('worker')
    for name in ('packet', 'output', 'author-root', 'method-python', 'driver'):
        worker.add_argument('--' + name, type=Path, required=True)
    worker.add_argument('--kind', choices=('process', 'thread'), required=True)
    worker.add_argument('--executor-count', type=_positive_int)
    worker.add_argument('--threads', type=_positive_int)
    worker.add_argument('--query-seconds', type=_positive_seconds, default=120)
    args = parser.parse_args(argv)
    if args.action == 'prepare-fixture':
        prepare_fixture(args.reviews_csv, args.output)
    else:
        run_fixture(args)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
