"""Two original source tables and a frozen native Join CPU handoff."""
import csv
import json
from pathlib import Path

from .native_server import AUTHOR_SOURCE_COMMIT, physical_call_caps, query_task
from .movie_map_inputs import sha, source_binding

SCHEMA='semloom.kalypso_join_cpu_inputs.v1'


def prepare(cfg,spec,directory,operation):
    if (operation is None or operation.describe()['kind']!='join'
            or len(operation.records)!=spec['expected_calls']):
        raise ValueError('Original Join pairs differ from the complete native budget')
    directory=Path(directory);directory.mkdir(mode=0o700,parents=True,exist_ok=True)
    table=dict(query_id=spec['query_id'],instruction=operation.instruction)
    paths={}
    for side,frame in [('left',operation.frame),('right',operation.right_frame)]:
        suffix=':'+side
        columns=[column[:-len(suffix)] for column in operation.columns if column.endswith(suffix)]
        rows=[]
        for index,row in frame[columns].iterrows():
            cells={name:str(row[name]) for name in columns}
            data=next(iter(cells.values())) if len(cells)==1 else json.dumps(cells,ensure_ascii=False,sort_keys=True)
            rows.append(dict(data=data,fields={name+suffix:text for name,text in cells.items()}))
        table[side]=rows;path=directory/(side+'.csv');paths[side]=path
        with path.open('x',newline='') as stream:
            writer=csv.DictWriter(stream,fieldnames=['data']);writer.writeheader()
            writer.writerows(dict(data=row['data']) for row in rows)
    target=Path(cfg['map_prompt_root'])/(spec['query_id']+'.json');target.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    with target.open('x') as stream:json.dump(table,stream,ensure_ascii=False)
    return table,paths


def binding(cfg,unit):
    if (len(cfg['units'])!=1 or cfg['units'][0]!=unit or not unit['queries']
            or any(q['task']!='sembench-join' for q in unit['queries'])):
        raise ValueError('Join CPU preparation requires one dedicated native Join unit')
    physical_call_caps(unit)
    if cfg['source_commit']!=AUTHOR_SOURCE_COMMIT:raise ValueError('Join requires fixed original author05024')
    return dict(source=source_binding(cfg),author_source_commit=cfg['source_commit'],
        model=cfg['model'],model_revision=cfg['model_revision'],expected_template_sha256=cfg['expected_template_sha256'],
        source_data_sha256=cfg['source_data_sha256'],unit=unit)


def prepare_inputs(cfg,unit,output):
    from .sembench_inputs import make_query
    if cfg['execution_enabled'] or not cfg.get('CPU_fixture_only'):
        raise ValueError('Prepare native Join CPU inputs with execution disabled')
    bound=binding(cfg,unit);output=Path(output);output.mkdir(mode=0o700)
    local=dict(cfg,map_prompt_root=str(output/'prompts'));queries=[]
    for index,spec in enumerate(unit['queries']):
        qid=spec['query_id']
        if Path(qid).name!=qid or qid in ('.','..'):raise ValueError('Join query_id must be a filename-safe name')
        query=make_query(local,qid,spec,kind='join')
        directory=output/('query-'+str(index));table,paths=prepare(local,spec,directory,query.operation)
        source=directory/'source.json';source.write_text(json.dumps(query.describe(),ensure_ascii=False,indent=2)+'\n')
        paths.update(prompt=output/'prompts'/(qid+'.json'),source=source)
        queries.append(dict(query_id=qid,logical_occurrences=len(query.operation.records),
            files={role:dict(path=str(path.relative_to(output)),sha256=sha(path)) for role,path in paths.items()}))
    manifest=dict(schema=SCHEMA,binding=bound,queries=queries,model_calls=0)
    path=output/'CPU-INPUTS.json';path.write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n')
    return dict(status='prepared_original_Join_CPU_inputs_no_model',manifest=str(path.resolve()),sha256=sha(path),
        queries=len(queries),logical_occurrences=sum(item['logical_occurrences'] for item in queries))


def load_inputs(cfg):
    reference=cfg['native_cpu_inputs'];path=Path(reference['manifest']).resolve()
    if sha(path)!=reference['sha256']:raise ValueError('Frozen Join CPU manifest changed')
    manifest=json.loads(path.read_text())
    if (manifest['schema']!=SCHEMA or manifest['binding']!=binding(cfg,cfg['units'][0]) or manifest['model_calls']!=0):
        raise ValueError('Frozen Join CPU source/model/unit differs')
    expected=cfg['units'][0]['queries']
    if [q['query_id'] for q in manifest['queries']]!=[q['query_id'] for q in expected]:
        raise ValueError('Frozen Join CPU query sequence changed')
    loaded=[]
    for item,spec in zip(manifest['queries'],expected,strict=True):
        paths={}
        for role in ('left','right','prompt','source'):
            entry=item['files'][role];target=(path.parent/entry['path']).resolve()
            if not target.is_relative_to(path.parent) or sha(target)!=entry['sha256']:
                raise ValueError('Frozen Join CPU file changed: '+role)
            paths[role]=target
        table=json.loads(paths['prompt'].read_text());source=json.loads(paths['source'].read_text());original=query_task(spec)
        if (table['query_id']!=spec['query_id'] or len(table['left'])*len(table['right'])!=spec['expected_calls']
                or item['logical_occurrences']!=spec['expected_calls']
                or source['scenario']!=original['scenario'] or source['query']!='Q'+original['query_id']
                or source['query_id']!=spec['query_id'] or source['sembench_commit']!='c814e3807e72d4cf876b852b17e77f3cc94575c2'
                or source['operation']['kind']!='join' or source['operation']['instruction']!=table['instruction']
                or source['expected_model_calls']!=spec['expected_calls']):
            raise ValueError('Frozen Join CPU task, instruction or pair count changed')
        for side in ('left','right'):
            with paths[side].open(newline='') as stream:
                reader=csv.DictReader(stream)
                if reader.fieldnames!=['data'] or [r['data'] for r in reader]!=[r['data'] for r in table[side]]:
                    raise ValueError('Frozen Join side CSV differs from complete source')
        loaded.append(dict(spec=spec,table=table,paths=paths))
    return manifest,loaded
