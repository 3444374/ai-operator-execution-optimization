"""Query recording and observation around the Daft-owned native SQL/HTTP graph."""
import os
from pathlib import Path

from src.baselines.common.private_artifacts import open_private_text
from src.experiments.native_http_observer import NativeSessionFactory
from src.experiments.process_sampling import ProcessSampler
from .map_query_recording import record_execution


def run_daft(config, inputs, plan, dsn, model, ledger, root, errors):
    # A fresh supervised query process sets these before loading Daft.
    import sys
    if 'daft' in sys.modules:
        raise RuntimeError('native Daft query requires a fresh process')
    import psycopg
    import daft
    from src.baselines.text.frameworks.daft_pg_http import open_rows
    shared = ledger.claim_shared_unit(config.unit_id)
    event_root = root/'worker-events'; event_root.mkdir()
    factory = NativeSessionFactory(shared,str(event_root),model.endpoint_url,model.timeout_ms/1000)

    def connect():
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool
        def raw_connection():
            return psycopg.connect(dsn,options='-c default_transaction_read_only=on -c statement_timeout='+str(int(config.query_timeout_s*1000)))
        return create_engine('postgresql+psycopg://',creator=raw_connection,poolclass=NullPool).connect()

    try:
        with ProcessSampler(root/'query-rss.jsonl',{'consumer_daft':os.getpid()},include_children=True) as sampler:
            headers = {'Authorization':'Bearer '+model.bearer_token} if model.bearer_token else None
            with errors.capture('query'):
                result = record_execution(root/'q0',lambda:open_rows(inputs,plan,connect,factory,
                    concurrency=config.concurrency,partitions=config.daft_read_partitions,num_threads=config.daft_num_threads,headers=headers),
                    max_rows=inputs.max_rows,max_result_bytes=inputs.max_rows*70000,
                    flush_rows=64,query_timeout_s=config.query_timeout_s,
                    cancel_query=lambda:ledger.close_shared_unit(config.unit_id))
    finally:
        def collect():
            with open_private_text(root/'events.jsonl') as out:
                for path in sorted(event_root.glob('*.jsonl')):
                    with path.open() as source:
                        for line in source:out.write(line)
        errors.attempt('worker_event_collection',collect)
    return result,dict(processes=sampler.summary(),daft_version=daft.__version__,
        scheduler_owner='Daft Native',kernel='matched one-row async batch HTTP UDF; not built-in prompt',
        daft_num_threads=config.daft_num_threads,requested_reader_partitions=config.daft_read_partitions,
        http_capacity=config.concurrency,native_batch_size=1,native_async_batches=max(1,config.concurrency-1),
        client_lifecycle='one client per row invocation',
        physical_cpu_isolation=False,pg_reader_rss='unavailable',
        source_retention='native SQL reader partition materialization')
