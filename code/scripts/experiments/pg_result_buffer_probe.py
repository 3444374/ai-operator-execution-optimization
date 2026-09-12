"""Diagnose ordinary PG result buffering with bounded, model-free streaming SQL.

Run only against an authorized isolated PG18.3 after runtime preflight. The scalar
temporary function sleeps once per row, then records server time. It does not
materialize a PL/pgSQL set. Client timing uses its own monotonic clock; server and
client absolute timestamps are never subtracted. No SemMap or model is involved.
"""
import argparse
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import time

from src.baselines.common.private_artifacts import new_private_directory, write_private_json
from src.baselines.common.redact import redact_text


ROWS = 128
PAYLOAD_BYTES = (8, 128, 128, 8)
STATEMENT_TIMEOUT_MS = 15000
CREATE_MARKER = """
CREATE FUNCTION pg_temp.semloom_buffer_mark() RETURNS text
LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    PERFORM pg_sleep(0.005);
    RETURN (extract(epoch FROM clock_timestamp())::numeric(20,6))::text;
END $$
"""
QUERY = """SELECT i::text, repeat('x', %s), pg_temp.semloom_buffer_mark()
           FROM generate_series(0, 127) AS g(i)"""


def specification():
    return dict(kind='ordinary_pg_send_buffer_diagnostic', model_posts=0, rows=ROWS,
                payload_bytes=list(PAYLOAD_BYTES), sleep_per_row_s=.005,
                statement_timeout_ms=STATEMENT_TIMEOUT_MS, query=QUERY,
                marker_function=CREATE_MARKER, expected_server_version_num=180003,
                scope='scalar expression completion to client receive; no SemMap node timing',
                source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest())


def run_case(connection, root, index, payload_bytes):
    records = []
    started = time.monotonic_ns()
    try:
        with connection.cursor() as cursor:
            for row in cursor.stream(QUERY, (payload_bytes,), size=1):
                received = time.monotonic_ns()
                records.append(dict(row=list(row), received_ns=received))
        ended = time.monotonic_ns()
        if len(records) != ROWS or any(
                record['row'][0] != str(i) or record['row'][1] != 'x'*payload_bytes
                for i, record in enumerate(records)):
            raise ValueError('streamed row identity or payload mismatch')
        server = [Decimal(r['row'][2]) for r in records]
        if any(b <= a for a, b in zip(server, server[1:])):
            raise ValueError('server marker did not advance per row')
        # Text-format DataRow: type/length/count = 7 bytes, then 4 + value bytes
        # per non-null column. RowDescription and command messages are additional.
        wire_bytes = sum(7 + sum(4 + len(value.encode()) for value in r['row']) for r in records)
        return dict(index=index, payload_bytes=payload_bytes, rows=ROWS,
                    data_row_bytes=wire_bytes, server_production_span_s=float(server[-1]-server[0]),
                    first_client_row_s=(records[0]['received_ns']-started)/1e9,
                    client_receive_span_s=(records[-1]['received_ns']-records[0]['received_ns'])/1e9,
                    jct_s=(ended-started)/1e9)
    finally:
        write_private_json(root/f'case-{index}-rows.json', dict(
            started_ns=started, stopped_ns=time.monotonic_ns(), records=records))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--describe', action='store_true', help='print specification without connecting')
    parser.add_argument('--dsn-env', default='SEMLOOM_TEST_PG_DSN')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if args.describe:
        print(json.dumps(specification(), indent=2))
        return 0
    if args.output is None or not os.environ.get(args.dsn_env):
        parser.error('provide a fresh --output and a populated --dsn-env')
    root = args.output
    new_private_directory(root)
    write_private_json(root/'specification.json', specification())
    summary = dict(status='failed', cases=[], actual_model_posts=0,
                   server_cleanup='temporary objects end with the owned connection')
    try:
        import psycopg
        with psycopg.connect(os.environ[args.dsn_env], autocommit=True, connect_timeout=5,
                             application_name='semloom-buffer-probe') as connection:
            if connection.info.server_version != 180003:
                raise ValueError('diagnostic requires the declared PG18.3 server')
            summary.update(server_version_num=connection.info.server_version,
                           psycopg_version=psycopg.__version__)
            connection.execute("SELECT set_config('statement_timeout', %s, false)",
                               (str(STATEMENT_TIMEOUT_MS),))
            # Establish this connection's private temporary schema.
            connection.execute('CREATE TEMP TABLE semloom_buffer_probe_scope (unused boolean)')
            connection.execute(CREATE_MARKER)
            summary['plan'] = connection.execute('EXPLAIN (FORMAT JSON) '+QUERY, (8,)).fetchone()[0]
            for index, payload_bytes in enumerate(PAYLOAD_BYTES):
                summary['cases'].append(run_case(connection, root, index, payload_bytes))
                write_private_json(root/'summary.json', summary)
        summary['status'] = 'completed_diagnostic'
        summary['connection_closed'] = True
    except Exception as error:
        summary['error'] = dict(type=type(error).__name__, detail=redact_text(str(error)))
    finally:
        write_private_json(root/'summary.json', summary)
    print(json.dumps(dict(status=summary['status'], actual_model_posts=0)))
    return 0 if summary['status'] == 'completed_diagnostic' else 1


if __name__ == '__main__':
    raise SystemExit(main())
