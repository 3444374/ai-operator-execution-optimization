"""Original LOTUS Movie Q1, all 2000 prepared calls, one author DBEnd query."""
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import sys
import time

CODE=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(CODE))
from src.baselines.common.redact import redact_text
from src.baselines.common.private_artifacts import write_private_json
from src.baselines.text.frameworks.native_calls import NativeCallExecutor,_call_wire
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import load_fixed_model_config
from src.experiments.postgresql.imlane_query import load_imlane_call_configs
from src.experiments.postgresql.sembench_queries import MovieDataset,MovieQuery,MovieQueryResult
from src.experiments.postgresql.sembench_query_cli import load_config
from src.semantic_methods.lotus.batch import lotus_executor
from src.semantic_methods.lotus.native import LotusNativeBatchExecutor


def publish_prepared(root,prepared):
    temporary=root/'prepared-ready.tmp';write_private_json(temporary,prepared)
    temporary.replace(root/'prepared-ready.json')


def main():
    config_path,spec_path,endpoint,directory=sys.argv[1:]
    root=Path(directory);specification=json.loads(Path(spec_path).read_text());expected=specification['expected']
    mode=json.loads(Path(config_path).read_text())['runtime']
    config=load_imlane_call_configs({mode:config_path})[mode]
    adapter=None
    facts=dict(passed=False,runtime=config.runtime,models=0,gpu=0)
    def save():
        (root/'client-result.json').write_text(json.dumps(facts,indent=2)+'\n')
    try:
        import lotus
        from lotus.models import LM
        from tokenizers import Tokenizer
        for key,sha_key in (('raw','raw_sha256'),('csv','csv_sha256')):
            assert hashlib.sha256(Path(specification[key]).read_bytes()).hexdigest()==expected[sha_key]
        arm='lotus-method-native-imlane'+('-ray' if mode=='ray' else '')
        dataset=MovieDataset(Path(specification['checkout']),Path(specification['manifest']),Path(specification['csv']))
        data_preflight=dataset.preflight();rows=dataset.load_rows();query=MovieQuery(dataset,1)
        run_config=dict(schema='semloom.sembench_lotus_queries.v1',dataset=dict(checkout_env='ASYNC_Q1_CHECKOUT',
            manifest_env='ASYNC_Q1_MANIFEST',csv_env='ASYNC_Q1_CSV',manifest_content_digest=dataset.expected_manifest_digest),
            arms=[arm],queries=[1],warmups=0,repeats=1,max_attempts=2000,
            model_config_env='ASYNC_Q1_MODEL',output_env='ASYNC_Q1_OUTPUT',native_options={'concurrency':8},
            query_timeout_s=120,owner_timeout_s=300,imlane_config_envs={mode:'ASYNC_Q1_CALL'},lotus_observation_mode='qualification')
        run_path=root/'sembench-config.json';write_private_json(run_path,run_config)
        assert load_config(run_path)==run_config
        assert query.check_executor_shape(arm,rows,imlane_config=config)==2000
        model_path=root/'model-config.json';os.environ['ASYNC_Q1_TOKEN']='local-fixture'
        write_private_json(model_path,dict(endpoint_url=endpoint+'/v1/chat/completions',model_id=expected['model_id'],
            timeout_ms=5000,bearer_token_env='ASYNC_Q1_TOKEN'))
        model=load_fixed_model_config(model_path)
        adapter=NativeCallExecutor('imlane',NativeGraphOptions(**run_config['native_options']),imlane_config=config)
        delegate=adapter._imlane
        assert adapter.max_batch_rows==delegate.max_batch_rows==config.max_batch_rows==2000
        facts['public_entry']=dict(entry='MovieQueryResult.execute',config_loader='sembench_query_cli.load_config',
            arm=arm,movie_shape_rows=2000,factory='NativeCallExecutor',delegate=type(delegate).__name__,
            bridge_delegate=type(delegate.delegate).__name__,configured_max_batch_rows=config.max_batch_rows,
            effective_max_batch_rows=adapter.max_batch_rows,bridge_sha256=delegate.delegate.config['bridge_sha256'],
            udf_sha256=delegate.delegate.config['udf_sha256'],data_preflight=data_preflight,
            cli_process_owner_proxy_evaluator='not_run; public query API exercised directly')
        lm=LM('openai/'+model.model_id,api_base=endpoint+'/v1',api_key='local-fixture',
            temperature=0,top_p=1,max_tokens=8,max_batch_size=8,stop=['\n'],num_retries=0,max_retries=0,
            timeout=5,tokenizer=Tokenizer.from_file(specification['tokenizer']))
        prepared={};delivered=[];responses=[]
        class CapturedBridge:
            max_batch_rows=adapter.max_batch_rows;max_batch_bytes=adapter.max_batch_bytes
            last_close_report=None;last_cleanup_errors=()
            def iter_batch(self,calls,**options):
                assert len(calls)==2000 and len({c.call_id for c in calls})==2000
                original=[_call_wire(replace(c,endpoint='http://127.0.0.1:9/v1/chat/completions')).encode() for c in calls]
                prepared.update(rows=len(calls),payload_total_bytes=sum(len(c.payload) for c in calls),
                    max_payload_bytes=max(len(c.payload) for c in calls),
                    payload_sequence_sha256=hashlib.sha256(b''.join(c.payload for c in calls)).hexdigest(),
                    original_wire_sequence_sha256=hashlib.sha256(b'\n'.join(original)+b'\n').hexdigest())
                assert prepared['payload_sequence_sha256']==expected['payload_sequence_sha256']
                assert prepared['original_wire_sequence_sha256']==expected['wire_sequence_sha256']
                assert prepared['payload_total_bytes']==expected['payload_total_bytes']
                (root/'prepared.jsonl').write_text('\n'.join(_call_wire(c) for c in calls)+'\n')
                publish_prepared(root,prepared);facts['prepared']=prepared;save()
                until=time.monotonic()+30
                while not (root/'dispatch-approved.json').is_file():
                    if time.monotonic()>=until:raise TimeoutError('Q1 pre-dispatch software allowance was not issued')
                    time.sleep(.02)
                try:yield from adapter.iter_batch(calls,**options)
                finally:self.last_close_report=adapter.last_close_report
        bridge=CapturedBridge()
        def response(index,full):
            if not delivered:time.sleep(.2)
            assert full.status_code==200
            delivered.append(index)
            responses.append(dict(status_code=full.status_code,headers=full.headers,http_version=full.http_version,
                                  body_sha256=hashlib.sha256(full.body).hexdigest()))
        selected=LotusNativeBatchExecutor(bridge,model,query_id='q1-offline',operator_id='filter',
            max_batch_rows=2000,on_response=response)
        result=MovieQueryResult(query,rows,root)
        with lotus.settings.context(lm=lm,enable_cache=False),lotus_executor(lm,selected):
            pairs=list(result.execute())
        assert len(delivered)==2000 and set(delivered)==set(range(2000)) and len(pairs)==len(result.result)==5
        assert len(result.decisions)==len(result.prompts)==2000
        close=adapter.last_close_report
        assert close['clean'] and close['bridge_stats']['submitted']==close['bridge_stats']['delivered']==2000
        facts['public_entry']['coordinator_identity']=delegate._facts
        facts.update(passed=True,original_query_result_rows=len(pairs),complete_responses=len(delivered),
            distinct_occurrences=len(set(delivered)),close=close,responses=responses,
            original_parser='LOTUS 1.2.4 sem_filter',slow_consumer_seconds=.2)
        save()
    except BaseException as error:
        facts.update(error_type=type(error).__name__,error=redact_text(str(error)));save();raise
    finally:
        if adapter is not None:adapter.close()


if __name__=='__main__':main()
