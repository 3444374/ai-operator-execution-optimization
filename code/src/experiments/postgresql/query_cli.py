"""Prepare, install or execute a bounded PG-source benchmark query.

Run requires a pre-existing authorized budget and fixed model configuration.
It supervises a fresh worker; raw artifacts and DSNs stay outside Git.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import sys

from src.baselines.common.private_artifacts import new_private_directory,write_private_json
from src.baselines.text.sembench_movie import verify_checkout,SEMBENCH_COMMIT
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from .query_config import QueryConfig
from .query_inputs import QueryInputs
from .query_workloads import prepare,movie_csv_examples,squad_examples,file_identity,load_manifest,read_prepared


def parser():
    result=argparse.ArgumentParser(description=__doc__)
    commands=result.add_subparsers(dest='command',required=True)
    movie=commands.add_parser('prepare-movie')
    movie.add_argument('--csv',type=Path,required=True)
    movie.add_argument('--sembench-checkout',type=Path,required=True)
    movie.add_argument('--provenance',type=Path,required=True,help='source version, generator and sampling identity JSON')
    squad=commands.add_parser('prepare-squad')
    squad.add_argument('--source',type=Path,required=True)
    squad.add_argument('--selection',type=Path,required=True)
    squad.add_argument('--split',choices=('tuning','evaluation'),required=True)
    for item in (movie,squad):
        item.add_argument('--max-rows',type=int,required=True)
        item.add_argument('--output',type=Path,required=True)
    install=commands.add_parser('install')
    install.add_argument('--manifest',type=Path,required=True)
    install.add_argument('--table',required=True)
    install.add_argument('--dsn-env',default='SEMLOOM_QUERY_DSN')
    install.add_argument('--output',type=Path,required=True)
    run=commands.add_parser('run')
    for name in ('config','manifest','model','budget','output'):
        run.add_argument('--'+name,type=Path,required=True)
    run.add_argument('--budget-id',required=True)
    run.add_argument('--max-attempts',type=int,required=True)
    run.add_argument('--dsn-env',default='SEMLOOM_QUERY_DSN')
    for name in ('pg-log','sembench-checkout','tokenizer','ray-temp-root'):
        run.add_argument('--'+name,type=Path)
    run.add_argument('--worker',action='store_true',help=argparse.SUPPRESS)
    return result


def dsn(args):
    value=os.environ.get(args.dsn_env)
    if not value:raise ValueError('required DSN environment variable is unset')
    return value


def main(argv=None):
    args=parser().parse_args(argv)
    if args.command=='prepare-movie':
        verify_checkout(args.sembench_checkout)
        provenance=json.loads(args.provenance.read_text())
        provenance.update(sembench_commit=SEMBENCH_COMMIT,csv=file_identity(args.csv))
        prepare(args.output,'movie',movie_csv_examples(args.csv),provenance,max_rows=args.max_rows)
    elif args.command=='prepare-squad':
        selection=json.loads(args.selection.read_text())
        prepare(args.output,'squad',squad_examples(args.source,selection,args.split,args.max_rows),
                dict(source=file_identity(args.source),selection=file_identity(args.selection),split=args.split),
                max_rows=args.max_rows)
    elif args.command=='install':
        import psycopg
        from .query_tables import install_input_table
        manifest=load_manifest(args.manifest)
        inputs=QueryInputs(manifest['kind'],args.table,manifest['rows'],manifest['max_source_bytes'],manifest['max_input_bytes'])
        new_private_directory(args.output)
        with psycopg.connect(dsn(args),autocommit=True) as connection:
            report=install_input_table(connection,inputs,read_prepared(args.manifest,'raw.jsonl'))
        write_private_json(args.output/'installation.json',dict(report,manifest_sha256=manifest['sha256']))
    else:
        config=QueryConfig(**json.loads(args.config.read_text()))
        budget=AttemptBudget(args.budget_id,args.max_attempts)
        ledger=CellBudgetLedger(args.budget,budget)
        connection_dsn=dsn(args)
        if args.worker:
            def cancelled(*_):raise TimeoutError('query supervisor requested cancellation')
            signal.signal(signal.SIGTERM,cancelled)
            from .query_runner import run_query
            run_query(config,manifest_path=args.manifest,model_path=args.model,budget_path=args.budget,
                budget=budget,root=args.output/'unit',dsn=connection_dsn,pg_log=args.pg_log,
                checkout=args.sembench_checkout,tokenizer_path=args.tokenizer,ray_temp_root=args.ray_temp_root)
        else:
            from .query_supervisor import supervise
            new_private_directory(args.output)
            command=[sys.executable,'-m',__package__+'.query_cli',*(sys.argv[1:] if argv is None else argv),'--worker']
            supervise(command,args.output,lambda:ledger.close_shared_unit(config.unit_id),
                      query_timeout_s=config.query_timeout_s)
    return 0


if __name__=='__main__':
    raise SystemExit(main())
