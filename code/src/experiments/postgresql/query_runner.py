"""Run one predeclared query on an installed immutable PG source and fixed model.

The caller owns model startup and an existing campaign budget. A failed unit is
never retried or refunded. Evaluation and source-file reads follow execution.
"""
import asyncio
from dataclasses import asdict
from pathlib import Path
import os
import time
from types import SimpleNamespace

from src.baselines.common.private_artifacts import new_private_directory,write_private_json
from src.baselines.text.squad_map import INSTRUCTION
from src.baselines.text.sembench_movie import MOVIE_MAP_INSTRUCTION,MOVIE_FILTER_INSTRUCTION,verify_checkout
from src.execution_provider.semantic_map import SemanticMapPlan
from src.execution_provider.wire import v3
from src.execution_provider.adapters.model_config import load_fixed_model_config
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.process_sampling import ProcessSampler
from .query_inputs import QueryInputs
from .query_workloads import load_manifest
from .query_execution import run_pg,run_direct,run_lotus,run_ray
from .query_evaluation import evaluate
from .cell_evidence import CellErrors,collect_cell_evidence


def run_query(config, *, manifest_path, model_path, budget_path, budget, root, dsn,
              pg_log=None, checkout=None, tokenizer_path=None, ray_temp_root=None):
    manifest_path,model_path,root=Path(manifest_path),Path(model_path),Path(root)
    manifest=load_manifest(manifest_path)
    if config.task!='map' and manifest['kind']!='movie':
        raise ValueError('Movie query requires a Movie source')
    inputs=QueryInputs(manifest['kind'],config.table,manifest['rows'],manifest['max_source_bytes'],
                       manifest['max_input_bytes'],config.movie_id)
    model=load_fixed_model_config(model_path)
    if config.task=='map':
        plan=SemanticMapPlan(INSTRUCTION if inputs.kind=='squad' else MOVIE_MAP_INSTRUCTION,
                             model.model_id,64 if inputs.kind=='squad' else 128)
        semantic_digest=plan.digest
    else:
        plan=v3.SemanticFilterPlan(MOVIE_FILTER_INSTRUCTION,model.model_id)
        semantic_digest=v3.semantic_spec_digest(plan)
        if checkout is None:
            raise ValueError('original Movie evaluation needs a pinned SemBench checkout')
        verify_checkout(checkout)
    if config.arm=='pg' and pg_log is None:
        raise ValueError('PG execution requires its server log for independent row bindings')
    new_private_directory(root)
    errors=CellErrors()
    ledger=None
    reserved=False
    summary=dict(status='failed',config=asdict(config),manifest_sha256=manifest['sha256'],
                 semantic_reference_sha256=semantic_digest,started_ns=time.monotonic_ns(),
                 prompt_parser_owner='LOTUS1.2.4' if config.arm=='lotus' else 'PostgreSQL SemanticPlanSpec',
                 measurement='PG source query; durable shared POST accounting; evaluation after execution',
                 performance_qualified=False)
    try:
        write_private_json(root/'query.json',summary)
        ledger=CellBudgetLedger(budget_path,budget)
        ledger.reserve_unit(config.unit_id,config.max_posts or manifest['rows'])
        reserved=True
        write_private_json(root/'unit-reserved.json',dict(unit_id=config.unit_id))
        with errors.capture('execution'):
            if config.arm=='pg-source-direct':
                execution,resources=asyncio.run(run_direct(config,inputs,plan,dsn,model,ledger,root,errors))
            elif config.arm=='ray-data':
                execution,resources=run_ray(config,inputs,plan,dsn,model,ledger,root,errors,ray_temp_root)
            else:
                import psycopg
                with psycopg.connect(dsn,autocommit=True) as connection:
                    if config.arm=='pg':
                        execution,resources=run_pg(config,inputs,plan,connection,Path(pg_log),model_path,ledger,root,errors)
                    else:
                        execution,resources=run_lotus(config,inputs,connection,model,ledger,root,errors,checkout,tokenizer_path)
        errors.raise_if_failed()
        summary.update(execution=execution,resources=resources,t_execution_cleanup_ns=time.monotonic_ns())
        with ProcessSampler(root/'evaluation-rss.jsonl',{'evaluator':os.getpid()}) as sampler:
            sampler.phase='evaluation'
            with errors.capture('evaluation'):
                summary['evaluation']=evaluate(config,inputs,plan,manifest_path,root,checkout)
        summary['evaluation_resources']=sampler.summary()
        summary['status']='passed'
    except BaseException as failure:
        errors.record('unit',failure)
    finally:
        if reserved:
            errors.attempt('budget_close',lambda:ledger.close_shared_unit(config.unit_id))
        summary['evidence']=errors.attempt('evidence',lambda:collect_cell_evidence(root,SimpleNamespace(queries=1)))
        summary['errors']=errors.details
        if errors.first is not None:
            summary['status']='failed'
        summary['ended_ns']=time.monotonic_ns()
        errors.attempt('summary_write',lambda:write_private_json(root/'summary.json',summary))
    errors.raise_if_failed()
    return summary
