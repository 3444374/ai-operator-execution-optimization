"""Execute one declared database query arm; caller owns source and model service."""
import os
import hashlib
import json
from pathlib import Path
import sys
import time

from src.baselines.common.private_artifacts import write_private_json, open_private_text, content_digest
from src.experiments.attempt_ledger import observe_async_http_posts
from src.experiments.buffered_events import BufferedEvents
from src.experiments.native_http_observer import NativeSessionFactory, observe_native_httpx
from src.experiments.process_sampling import ProcessSampler
from .map_query_recording import record_pg_query, record_async_execution, record_execution
from .pg_source_direct import PgSourceDirectMap
from .query_tables import map_statement
from .movie_queries import movie_statement
from .runtime_helpers import owned_child_process, wait_for_path
from src.experiments.request_identity import request_identity


def run_pg(config, inputs, plan, connection, pg_log, model_path, ledger, root, errors):
    socket = root/'g.sock'
    if len(str(socket).encode())>100:
        raise ValueError('PG query requires a short private output directory')
    command=[sys.executable,'-m','src.experiments.choice_gateway_observer',
        '--events',str(root/'public-events.jsonl'),'--private-events',str(root/'events.jsonl'),
        '--session-events',str(root/'sessions.jsonl'),'--observer-summary',str(root/'observer.json'),
        '--event-content','full','--event-write-mode','buffered','--cell-budget',str(ledger.path),
        '--shared-unit-budget','--unit-id',config.unit_id,'--budget-id',ledger.budget.budget_id,
        '--max-attempts',str(ledger.budget.limit),'--','--socket',str(socket),
        '--fixed-model-config',str(model_path),'--incremental-map','--max-active-jobs','1',
        '--max-active-requests',str(config.concurrency),'--max-held-tasks',str(config.window),
        '--input-buffer-bytes',str(config.input_bytes),'--result-buffer-bytes',str(config.result_bytes)]
    if config.organization_config is not None:
        from src.execution_provider.adapters.map_organization import MapOrganizationConfig
        path = Path(config.organization_config)
        if path.stat().st_size > 16384:
            raise ValueError('organization configuration is too large')
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != config.organization_sha256:
            raise ValueError('organization configuration identity differs')
        organization = MapOrganizationConfig(**json.loads(content))
        if organization.model_id != plan.model_id or organization.window_rows > config.window:
            raise ValueError('organization differs from query model/window')
        write_private_json(root/'organization.json', json.loads(content))
        command.extend(('--organization-config',str(root/'organization.json')))
    settings={'semloom_pg.gateway_socket':str(socket),
        'semloom_pg.provider_execution_profile':'incremental-map' if config.task=='map' else 'query-job',
        'semloom_pg.provider_window_tasks':str(config.window),'semloom_pg.provider_window_bytes':str(config.pg_window_bytes),
        'semloom_pg.enable_total_window_budget':'on' if config.pg_total_budget else 'off',
        'semloom_pg.provider_staging_bytes':str(config.pg_staging_bytes),
        'semloom_pg.test_window_memory':'on' if config.pg_total_budget else 'off',
        'semloom_pg.enable_predicate_prefetch':'on','semloom_pg.enable_filter_count':'on',
        'semloom_pg.test_map_binding_id_column':'row_id' if config.task=='map' else '',
        'semloom_pg.test_filter_binding_id_column':'row_id' if config.task!='map' else '',
        'statement_timeout':str(int(config.query_timeout_s*1000))}
    for key,value in settings.items():
        connection.execute('SELECT set_config(%s,%s,false)',(key,value))
    statement=(map_statement(inputs,plan) if config.task=='map' else
               movie_statement(inputs,int(config.task[-1]),plan.model_id))
    from psycopg import sql
    explain=connection.execute(sql.SQL('EXPLAIN (FORMAT JSON) ')+statement).fetchone()[0]
    write_private_json(root/'plan.json',explain)
    write_private_json(root/'pg-backend.json',dict(backend_pid=connection.info.backend_pid))
    write_private_json(root/'gateway-command.json',command)
    env=dict(os.environ,PYTHONPATH=str(Path(__file__).resolve().parents[3])+os.pathsep+os.environ.get('PYTHONPATH',''))
    with owned_child_process(command,root,'gateway',env,None) as gateway:
        wait_for_path(socket,gateway)
        with ProcessSampler(root/'query-rss.jsonl',{'consumer':os.getpid(),'gateway':gateway.pid,
                                                  'pg_backend':connection.info.backend_pid}) as sampler:
            offset=pg_log.stat().st_size
            try:
                with errors.capture('query'):
                    result=record_pg_query(connection,statement,root/'q0',max_rows=inputs.max_rows,
                        max_result_bytes=inputs.max_rows*70000,flush_rows=64,query_timeout_s=config.query_timeout_s)
            finally:
                def capture():
                    with pg_log.open('rb') as source,open_private_text(root/'q0-producer.log') as out:
                        source.seek(offset)
                        for line in source:out.write(line.decode())
                errors.attempt('producer_capture',capture)
    if gateway.returncode!=0 or socket.exists():
        raise ValueError('query gateway did not shut down cleanly')
    return result,dict(processes=sampler.summary(),gateway_exit=gateway.returncode)


async def run_direct(config, inputs, plan, dsn, model, ledger, root, errors):
    import psycopg
    shared=ledger.claim_shared_unit(config.unit_id)
    async with await psycopg.AsyncConnection.connect(dsn,autocommit=True) as connection:
        await connection.execute('SELECT set_config(%s,%s,false)',('statement_timeout',str(int(config.query_timeout_s*1000))))
        with BufferedEvents(root/'events.jsonl') as events:
            def record(event):
                events.record(dict(event,monotonic_ns=time.monotonic_ns()))
            def request(attempt,payload):
                values=json.loads(payload)
                record(dict(event='request',attempt=attempt,key=request_identity.get(),body=values,request_values_sha256=content_digest(values),
                            request_bytes_sha256=hashlib.sha256(payload).hexdigest()))
            with observe_async_http_posts(shared,request):
                direct=PgSourceDirectMap(model,config.concurrency,plan,record)
                try:
                    with ProcessSampler(root/'query-rss.jsonl',{'consumer_direct':os.getpid(),
                                                              'pg_backend':connection.info.backend_pid}) as sampler:
                        with errors.capture('query'):
                            result=await record_async_execution(root/'q0',
                                lambda:direct.query(connection,inputs,1,result_order=config.result_order),
                                max_rows=inputs.max_rows,max_result_bytes=inputs.max_rows*70000,
                                flush_rows=64,query_timeout_s=config.query_timeout_s)
                finally:
                    await direct.close()
    return result,dict(processes=sampler.summary(),source=direct.source_metrics)


def run_lotus(config, inputs, connection, model, ledger, root, errors, checkout, tokenizer_path):
    from src.baselines.text.frameworks.lotus_pg import configure_lm,open_rows
    lm=configure_lm(model,config.concurrency,tokenizer_path=tokenizer_path)
    shared=ledger.claim_shared_unit(config.unit_id)
    connection.execute('SELECT set_config(%s,%s,false)',('statement_timeout',str(int(config.query_timeout_s*1000))))
    def decisions(rows):
        write_private_json(root/'native-decisions.json',rows)
    def prompts(rows):
        write_private_json(root/'native-prompts.json',rows)
    with BufferedEvents(root/'events.jsonl') as events,observe_native_httpx(shared,events.record,model.endpoint_url,
                                                                           timeout_s=config.query_timeout_s):
        with ProcessSampler(root/'query-rss.jsonl',{'consumer_lotus':os.getpid(),'pg_backend':connection.info.backend_pid}) as sampler:
            with errors.capture('query'):
                result=record_execution(root/'q0',lambda:open_rows(connection,inputs,checkout,int(config.task[-1]),decisions,prompts),
                    max_rows=inputs.max_rows,max_result_bytes=inputs.max_rows*70000,flush_rows=64,
                    query_timeout_s=config.query_timeout_s,cancel_query=lambda:connection.cancel_safe(timeout=1))
    return result,dict(processes=sampler.summary(),source_retention='native full DataFrame, bounded by declared input rows',
                       cache_hits=lm.stats.cache_hits,native_filter_default_on_parse_failure=True)


def run_ray(config, inputs, plan, dsn, model, ledger, root, errors, ray_temp_root):
    # Ray reads this native async-batch setting at module import, including in
    # workers. Actor max_concurrency alone does not limit batches within a task.
    os.environ['RAY_DATA_DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY']='1'
    import psycopg
    import ray
    from ray.data._internal.planner.plan_udf_map_op import DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY
    if DEFAULT_ASYNC_BATCH_UDF_MAX_CONCURRENCY != 1:
        raise RuntimeError('Ray query requires a fresh process with native async batch concurrency 1')
    from src.baselines.text.frameworks.ray_data_pg_http import open_rows,RaySqlHttpConfig
    if ray.is_initialized():
        raise RuntimeError('query runner must own an isolated Ray runtime')
    if ray_temp_root is None or len(str(ray_temp_root).encode())>40 or Path(ray_temp_root).exists():
        raise ValueError('Ray query needs a new short private runtime directory')
    shared=ledger.claim_shared_unit(config.unit_id)
    event_root=root/'worker-events';event_root.mkdir()
    factory=NativeSessionFactory(shared,str(event_root),model.endpoint_url,model.timeout_ms/1000)
    reader_root=root/'reader-processes';reader_root.mkdir()
    def connect():
        import uuid
        import psutil
        connection=psycopg.connect(dsn,options='-c default_transaction_read_only=on -c statement_timeout='+str(int(config.query_timeout_s*1000)))
        try:
            path=reader_root/(uuid.uuid4().hex+'.pending')
            pid=connection.info.backend_pid
            process=psutil.Process(pid)
            times=process.cpu_times()
            write_private_json(path,dict(pid=pid,created_at=process.create_time(),
                initial_rss_bytes=process.memory_info().rss,initial_cpu_seconds=times.user+times.system))
            path.rename(path.with_suffix('.json'))
        except BaseException:
            connection.close()
            raise
        return connection
    def reader_pids():
        return {'pg_reader:'+str(value['pid']):(value['pid'],value['created_at'])
                for path in reader_root.glob('*.json') for value in [json.loads(path.read_text())]}
    ray.init(address='local',num_cpus=config.ray_num_cpus,num_gpus=0,include_dashboard=False,
             _node_ip_address='127.0.0.1',object_store_memory=config.ray_object_store_bytes,_temp_dir=str(ray_temp_root))
    try:
        with ProcessSampler(root/'query-rss.jsonl',{'consumer_ray':os.getpid()},
                            include_children=True,pid_provider=reader_pids) as sampler:
            headers={'Authorization':'Bearer '+model.bearer_token} if model.bearer_token else None
            def record_stats(value):
                with open_private_text(root/'ray-stats.txt') as out:out.write(value)
            with errors.capture('query'):
                result=record_execution(root/'q0',lambda:open_rows(inputs,plan,
                    RaySqlHttpConfig(config.ray_read_blocks,config.ray_read_concurrency,config.ray_actors,config.ray_batch_rows),
                    connect,factory,headers=headers,record_stats=record_stats),max_rows=inputs.max_rows,max_result_bytes=inputs.max_rows*70000,
                    flush_rows=64,query_timeout_s=config.query_timeout_s,cancel_query=ray.shutdown)
    finally:
        ray.shutdown()
        def collect():
            with open_private_text(root/'events.jsonl') as out:
                for path in sorted(event_root.glob('*.jsonl')):
                    with path.open() as source:
                        for line in source:out.write(line)
        errors.attempt('worker_event_collection',collect)
    return result,dict(processes=sampler.summary(),ray_runtime_connected=ray.is_initialized(),
        source_retention='native SQL reader materializes each actual shard',
        requested_reader_blocks=config.ray_read_blocks,reader_concurrency=config.ray_read_concurrency,
        ray_logical_cpus=config.ray_num_cpus,ray_actors=config.ray_actors,
        http_capacity=config.concurrency,object_store_allocation_bytes=config.ray_object_store_bytes,
        reader_connection_snapshots=[json.loads(path.read_text()) for path in sorted(reader_root.glob('*.json'))],
        physical_cpu_isolation=False,object_store_used_bytes=None,
        native_async_batches_per_actor=1,ray_version=ray.__version__)
