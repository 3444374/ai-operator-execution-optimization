#!/usr/bin/env python3
"""Prepare capacity inputs, initialize an explicit budget, or execute one Map cell."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.baselines.common.private_artifacts import write_private_json, new_private_directory
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    prepare = sub.add_parser('prepare')
    prepare.add_argument('--source', type=Path, required=True)
    prepare.add_argument('--tokenizer', type=Path, required=True)
    prepare.add_argument('--model-revision', required=True)
    prepare.add_argument('--context-limit', type=int, required=True)
    prepare.add_argument('--rows-per-split', type=int, required=True)
    prepare.add_argument('--seed', type=int, required=True)
    prepare.add_argument('--output', type=Path, required=True)
    budget = sub.add_parser('budget')
    budget.add_argument('--output', type=Path, required=True)
    budget.add_argument('--budget-id', required=True)
    budget.add_argument('--requests', type=int, required=True)
    budget.add_argument('--seconds', type=int, required=True)
    cell = sub.add_parser('cell')
    for key in ('config', 'manifest', 'fixed-model', 'budget-file', 'output'):
        cell.add_argument('--' + key, type=Path, required=True)
    cell.add_argument('--budget-id', required=True)
    cell.add_argument('--requests', type=int, required=True)
    cell.add_argument('--pg-dsn-env', default='MAP_CAPACITY_PG_DSN')
    cell.add_argument('--pg-log', type=Path)
    args = parser.parse_args(argv)
    if args.command == 'prepare':
        from transformers import AutoTokenizer, __version__
        from scripts.data.import_squad_workload import parse_squad_dev, _validate_dev_sha256, _validate_dev_count
        from src.baselines.text.squad_capacity import prepare_capacity_samples
        raw = args.source.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        _validate_dev_sha256(digest)
        examples = parse_squad_dev(json.loads(raw))
        _validate_dev_count(len(examples))
        tokenizer = AutoTokenizer.from_pretrained(args.tokenizer, local_files_only=True)
        identity = dict(revision=args.model_revision, transformers=__version__,
                        chat_template_sha256=hashlib.sha256(tokenizer.chat_template.encode()).hexdigest(),
                        files={p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in args.tokenizer.iterdir()
                               if p.is_file() and p.suffix in ('.json', '.jinja')})
        result = prepare_capacity_samples(examples, digest, tokenizer, rows_per_split=args.rows_per_split,
                    seed=args.seed, context_limit=args.context_limit, tokenizer_identity=identity)
        new_private_directory(args.output)
        for name, manifest in result.pop('manifests').items():
            write_private_json(args.output / (name + '.json'), manifest)
        write_private_json(args.output / 'profile.json', result)
    elif args.command == 'budget':
        if not 1 <= args.seconds <= 86400:
            raise ValueError('positive duration of at most one day required')
        ledger = CellBudgetLedger.create(args.output, AttemptBudget(args.budget_id, args.requests),
                                         deadline_utc=time.time()+args.seconds)
        print(json.dumps(ledger.snapshot()))
    else:
        from contextlib import nullcontext
        from src.experiments.postgresql.map_capacity_runner import CellConfig, run_cell
        config = CellConfig(**json.loads(args.config.read_text()))
        connection = nullcontext(None)
        if config.arm == 'pg':
            import psycopg
            connection = psycopg.connect(os.environ[args.pg_dsn_env], autocommit=True)
        with connection as conn:
            result = run_cell(config, manifest=json.loads(args.manifest.read_text()), fixed_model_file=args.fixed_model,
                budget_file=args.budget_file, budget=AttemptBudget(args.budget_id, args.requests),
                root=args.output, connection=conn, pg_log=args.pg_log)
        print(json.dumps(dict(status=result['status'], actual_requests=result['actual_requests'])))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
