"""Real native SQL/Arrow/Unix IPC gate with saved replies and no model imports.

The outer server coordinator must hold its existing host lease/resource guard.
Only the ModelService subprocess is replaced with the software engine fixture;
the native driver, UDF, actual SQL, source, full sink, ACK and audits remain real.
"""
import argparse
import csv
from pathlib import Path
import sys
from unittest.mock import patch

from integrations.imlane_duckdb import model_service, model_provider
from src.experiments import native_imlane_filters as binding
from src.infrastructure.private_artifacts import file_identity, new_private_directory, read_json, write_private_json
from tests.baselines.text.replay_imlane_movie_filters import reference, bridge_q3_reference


def prepare_fixture(config, original_run):
    query, method, prepared, requests, tokens, rows = binding.prepare_query(config)
    frozen = (bridge_q3_reference(original_run, config) if config['query_id'] == '3'
              else reference(original_run, config['query_id']))
    assert [r['messages'] for r in requests] == frozen['messages']
    assert [(r['title'],r['text']) for r in rows] == frozen['labels_and_texts']
    return dict(rows=[dict(r,token_ids=t,generated_text=text)
                for r,t,text in zip(rows,tokens,frozen['texts'],strict=True)],
                original_reference=frozen['evidence'], models=0, software_fixture=True)


def run(args):
    if sys.platform != 'linux':
        raise RuntimeError('native Filter DSO/SQL gate requires Linux; local parser tests are not SQL qualification')
    config, _generation = binding.configuration(read_json(args.config))
    if config['schema'] != binding.MOVIE_SCHEMA:
        raise ValueError('this gate requires one explicitly bound original Movie Filter query')
    expected = config['max_model_calls']
    # Fail stale native driver before any owner or software service allocation.
    binding.verified_build(args.build_record)
    root = args.output.resolve(); new_private_directory(root)
    config['model_service'] = dict(model_python=sys.executable,model=config['tokenizer'],
        served_model_name='imlane-zero-model-movie',dtype='bfloat16',engine_options={})
    fixture = prepare_fixture(config,args.original_run)
    fixture_path = root/'fixture.json';write_private_json(fixture_path,fixture)
    args.output = root/'worker'
    real_spawn, real_config = model_service.spawn, model_provider.configuration
    original_text = binding.open_private_text

    def software_spawn(command, environment, log):
        assert command[1:3] == ['-m','integrations.imlane_duckdb.model_ipc_worker']
        actual = [sys.executable,'-B','-m','tests.baselines.text.imlane_filter_software_service',
            '--fixture',str(fixture_path),'--receipts',str(args.output/'model-calls'),
            '--config',command[command.index('--config')+1]]
        write_private_json(root/'SOFTWARE-SPAWN.json',dict(requested=command,actual=actual,models=0))
        return real_spawn(actual,dict(environment,CUDA_VISIBLE_DEVICES=''),log)

    def software_provider(value):
        return real_config(dict(value,provider_mode='software_ipc'))

    # Negative cases alter only the native CSV after the original source has
    # been prepared. Native SQL must reject them before any provider request.
    from contextlib import contextmanager
    @contextmanager
    def input_writer(path, *a, **kw):
        with original_text(path,*a,**kw) as stream: yield stream
        if Path(path).name == 'input.csv' and args.case != 'complete':
            with Path(path).open(newline='') as stream: table=list(csv.reader(stream))
            if args.case == 'missing-row': table.pop()
            else: table[2][0] = table[1][0]
            with Path(path).open('w',newline='') as stream: csv.writer(stream).writerows(table)

    failure = None
    try:
        with patch.object(model_service,'spawn',side_effect=software_spawn), \
                patch.object(model_provider,'configuration',side_effect=software_provider), \
                patch.object(binding,'open_private_text',side_effect=input_writer):
            binding.run_worker(args,config)
    except BaseException as error:
        failure = error
    # These are freshly completed owned receipts, not untrusted config inputs.
    worker_path = args.output/'worker.json'
    software_path = args.output/'model-service/software-service-final.json'
    worker = read_json(worker_path, max_bytes=worker_path.stat().st_size)
    software = read_json(software_path, max_bytes=software_path.stat().st_size)
    if args.case == 'complete':
        if failure is not None: raise failure
        assert worker['status']=='completed' and software['software_generate']==expected
        assert worker['budget']['consumed_requests']==expected
        assert worker['native_alive_after_sink'] and worker['sink_release']['outcome']=='complete'
        assert len(software['calls'])==len({c['occurrence_id'] for c in software['calls']})==expected
    else:
        native = read_json(args.output/'query.json')
        assert failure is not None and worker['status']=='failed'
        assert native['error']=='native Filter requires every declared original occurrence exactly once'
        assert native['native_ai_query_attempts']==0
        assert software['software_generate']==worker['budget']['consumed_requests']==0
        assert not list((args.output/'model-calls').glob('*.started.json'))
    assert software['completed'] and not software['active_requests'] and not software['socket_exists']
    result=dict(status='passed',case=args.case,query_id=config['query_id'],expected_occurrences=expected,
        original_reply_reference=fixture['original_reference'],real_models=0,software_generate=software['software_generate'],
        native_DSO_SQL_executed=True,original_UDF_and_Arrow=True,real_Unix_IPC=True,
        full_original_sink_ACK_verified=args.case=='complete',source_identity=config['input_sha256'],
        build_record=file_identity(args.build_record),external_host_resource_verification_required=True)
    write_private_json(root/'CPU.json',result)
    print(result)
    return 0


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('config','original-run','output','build-record','author-root','method-python'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--case',choices=('complete','missing-row','duplicate-id'),default='complete')
    parser.add_argument('--kind',choices=('thread','process'),default='thread')
    parser.add_argument('--threads',type=int)
    parser.add_argument('--executor-count',type=int)
    return run(parser.parse_args(argv))


if __name__ == '__main__': raise SystemExit(main())
