"""One bounded source-information query, using the existing direct client/recorder.

All methods share a disk result sink restoring source order. Global metadata is
paid inside invocation-to-EOF; an explicitly named reuse arm reports its producer.
This is a PG-source diagnostic, not an ai_semantic.map carrier experiment.
"""
import asyncio
from contextlib import asynccontextmanager, closing
import hashlib
import json
import os
from pathlib import Path
import time

from src.baselines.common.private_artifacts import new_private_directory, write_private_json, content_digest
from src.baselines.text.sembench_movie import MOVIE_MAP_INSTRUCTION
from src.execution_provider.adapters.model_config import load_fixed_model_config, MAX_MODEL_RESPONSE_BYTES
from src.execution_provider.adapters.map_organization import tokenizer_fingerprint
from src.execution_provider.semantic_map import SemanticMapPlan, MAX_OUTPUT_BYTES
from src.experiments.attempt_ledger import observe_async_http_posts
from src.experiments.buffered_events import BufferedEvents
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.process_sampling import ProcessSampler
from src.experiments.request_identity import request_identity
from .cell_evidence import CellErrors, collect_cell_evidence
from .map_direct import DirectMap
from .map_query_recording import record_async_execution, query_failure_scope
from .m2_source import InformationSource, SourceLimits, MODES, open_store
from .query_config import QueryConfig
from .query_inputs import QueryInputs
from .query_evaluation import evaluate, read_events
from .query_workloads import load_manifest, read_prepared
from .m1_measurement import output_identity, token_usage, retained_resource_areas


async def _execute(config, inputs, plan, model, connection, claimed, root, tokenizer, identity, errors):
    limits = SourceLimits(**config['source_limits'])
    deadline = time.monotonic()+config['query_timeout_s']
    source = None
    direct = None
    with BufferedEvents(root/'events.jsonl') as events:
        def record(event):
            events.record(dict(event, monotonic_ns=time.monotonic_ns()))
        def request(attempt, payload):
            values=json.loads(payload)
            record(dict(event='request',attempt=attempt,key=request_identity.get(),body=values,
                        request_values_sha256=content_digest(values),request_bytes_sha256=hashlib.sha256(payload).hexdigest()))
        with closing(open_store(root/'results.sqlite',limits.result_disk_bytes)) as sink:
            sink.execute('CREATE TABLE results(row_id TEXT PRIMARY KEY, position INTEGER UNIQUE, value TEXT)')
            def source_event(event):
                record(event)
                sink.execute('INSERT INTO results(row_id,position) VALUES(?,?)',(event['row_id'],event['position']))
            source = InformationSource(connection,inputs,plan,tokenizer,mode=config['mode'],limits=limits,
                root=root,identity=identity,deadline=deadline,emit=source_event,reuse=config.get('reuse_metadata'))
            direct = DirectMap(model,config['concurrency'],plan,record)
            @asynccontextmanager
            async def stream():
                async def rows():
                    with query_failure_scope(), connection.transaction():
                        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                        record(dict(event='source_snapshot',value=connection.execute('SELECT pg_current_snapshot()::text').fetchone()[0]))
                        with closing(source.rows()) as candidates:
                            async with direct.rows(candidates,1) as results:
                                async for row_id,value in results:
                                    source.check_time()
                                    if value not in ('POSITIVE','NEGATIVE'):
                                        raise ValueError('invalid classification; stopping source-information query')
                                    if len(value.encode()) > MAX_OUTPUT_BYTES:
                                        raise ValueError('result exceeds semantic output size')
                                    if sink.execute('UPDATE results SET value=? WHERE row_id=? AND value IS NULL',
                                                    (value,row_id)).rowcount != 1:
                                        raise ValueError('duplicate or unbound model result')
                        sink.commit()
                        if sink.execute('SELECT count(*),count(value) FROM results').fetchone() != (inputs.max_rows,inputs.max_rows):
                            raise ValueError('source-information result set is incomplete')
                        for row in sink.execute('SELECT row_id,value FROM results ORDER BY position'):
                            source.check_time()
                            yield row
                iterator=rows()
                try:
                    yield iterator
                finally:
                    await iterator.aclose()
            try:
                with observe_async_http_posts(claimed,request), ProcessSampler(root/'query-rss.jsonl',
                        {'consumer_direct':os.getpid(),'pg_backend':connection.info.backend_pid}) as sampler:
                    with errors.capture('execution'):
                        result=await record_async_execution(root/'q0',stream,max_rows=inputs.max_rows,
                            max_result_bytes=inputs.max_rows*70000,flush_rows=64,query_timeout_s=config['query_timeout_s'])
            finally:
                if direct is not None:
                    errors.attempt('source_summary',lambda:write_private_json(root/'source.json',source.metrics))
                    try:
                        await direct.close()
                    except BaseException as error:
                        errors.record('transport_close',error)
    errors.raise_if_failed()
    return result,dict(processes=sampler.summary(),source=source.metrics,
        result_disk_bytes=(root/'results.sqlite').stat().st_size,
        storage_scope='candidate payload plus at most C pending requests; fixed SQLite page caps; journal/cache overhead separately bounded by finite rows')


def run_information_query(config, *, manifest_path, model_path, budget_path, budget, root, dsn, tokenizer=None):
    started=time.monotonic_ns();root=Path(root).absolute()
    manifest=load_manifest(manifest_path)
    if manifest['kind'] != 'movie' or config['mode'] not in MODES:
        raise ValueError('source-information runner requires a declared Movie mode')
    limits=SourceLimits(**config['source_limits'])
    query=QueryConfig(config['unit_id'],'pg-source-direct','map',config['table'],
        concurrency=config['concurrency'],query_timeout_s=config['query_timeout_s'])
    inputs=QueryInputs('movie',query.table,manifest['rows'],manifest['max_source_bytes'],manifest['max_input_bytes'])
    if limits.candidate_rows*(inputs.max_source_bytes+inputs.max_input_bytes)>limits.payload_bytes:
        raise ValueError('candidate byte budget does not cover declared worst-case rows')
    if limits.result_disk_bytes < inputs.max_rows*(MAX_OUTPUT_BYTES+inputs.max_source_bytes+128):
        raise ValueError('result disk budget does not cover every response and row identity')
    if tokenizer_fingerprint(config['tokenizer_path']) != config['tokenizer_sha256']:
        raise ValueError('tokenizer identity changed')
    new_private_directory(root)
    errors=CellErrors();ledger=None;reserved=False
    summary=dict(status='failed',config=config,manifest_sha256=manifest['sha256'],
        query_preparation_started_ns=started,comparison_role='direct_client_control',
        scheduler_owner='DirectMap bounded client + model service; experimental source ordering',
        pg_semantic_carrier=False,performance_qualified=False,
        client_result_reserve_bytes=query.concurrency*MAX_MODEL_RESPONSE_BYTES,
        max_payload_row_references=limits.candidate_rows+query.concurrency+1)
    try:
        model=load_fixed_model_config(model_path)
        plan=SemanticMapPlan(MOVIE_MAP_INSTRUCTION,model.model_id,128)
        if tokenizer is None:
            from transformers import AutoTokenizer
            tokenizer=AutoTokenizer.from_pretrained(config['tokenizer_path'],local_files_only=True,trust_remote_code=False)
        identity=dict(manifest_sha256=manifest['sha256'],semantic_digest=plan.digest,
                      tokenizer_sha256=config['tokenizer_sha256'],context_tokens=limits.context_tokens)
        ledger=CellBudgetLedger(budget_path,budget);ledger.reserve_unit(query.unit_id,manifest['rows']);reserved=True
        claimed=ledger.claim_shared_unit(query.unit_id)
        write_private_json(root/'unit-reserved.json',dict(unit_id=query.unit_id))
        import psycopg
        with psycopg.connect(dsn,autocommit=True) as connection:
            connection.execute('SELECT set_config(%s,%s,false)',('statement_timeout',str(int(query.query_timeout_s*1000))))
            execution,resources=asyncio.run(_execute(config,inputs,plan,model,connection,claimed,root,tokenizer,identity,errors))
        summary.update(execution=execution,resources=resources,t_execution_cleanup_ns=time.monotonic_ns())
        summary['evaluation']=evaluate(query,inputs,plan,manifest_path,root,None)
        expected=[row[1] for row in read_prepared(manifest_path,'raw.jsonl')]
        values=[json.loads(line)['row'][0] for line in (root/'q0/results.jsonl').read_text().splitlines()]
        if expected != values:
            raise ValueError('source order was not restored at final delivery')
        summary['output_order_verified']=True
        summary['output_values']=output_identity(root/'q0/results.jsonl')
        events=read_events(root/'events.jsonl')
        summary['token_usage']=token_usage(events,inputs.max_rows)
        summary['resource_areas']=retained_resource_areas(events,root/'query-rss.jsonl')
        summary['jct_seconds']=(execution['t_query_terminal_ns']-started)/1e9
        summary['effective_rows_per_second']=inputs.max_rows/summary['jct_seconds']
        if summary['evaluation']['actual_posts'] != inputs.max_rows or summary['evaluation']['quality']['invalid']:
            raise ValueError('source-information POST count or quality format differs')
        summary['status']='passed'
    except BaseException as error:
        errors.record('query',error)
    finally:
        if reserved:errors.attempt('budget_close',lambda:ledger.close_shared_unit(query.unit_id))
        summary['errors']=errors.details
        summary['evidence']=errors.attempt('evidence',lambda:collect_cell_evidence(root,type('Cell',(),{'queries':1})()))
        if errors.first is not None:summary['status']='failed'
        errors.attempt('summary',lambda:write_private_json(root/'summary.json',summary))
    errors.raise_if_failed()
    return summary
