"""Original single-operator source and field table for native Kalypso queries."""
import csv
from numbers import Integral
import json
from pathlib import Path

from src.baselines.text.sembench_text import SemBenchTextQuery
from src.serving.backends.vllm_protocol import GenerationSpec
from . import adapter as native
from .native_server import query_task


def make_query(cfg,qid,task='movie-q9',*,kind='map'):
    binding=query_task(dict(task=task) if isinstance(task,str) else task)
    hashes=cfg['source_data_sha256']
    if binding['scenario']=='movie':
        required={'Reviews.csv','Movies.csv'} if binding['query_id']=='10' else {'Reviews.csv'}
        if not required<=set(hashes) or set(hashes)-{'Reviews.csv','Movies.csv'}:
            raise ValueError('Native query requires the complete original source and evaluation file hashes')
    for name, expected in hashes.items():
        if native.sha(Path(cfg['data_dir'])/name) != expected:
            raise ValueError('Bound native source data changed: '+name)
    # These stage metadata fields do not set author first-request SamplingParams.
    query=SemBenchTextQuery(cfg['sembench'],cfg['data_dir'],**binding,execution_id=qid,
        model_name=cfg['model_name'],generation=GenerationSpec(max_tokens=8192 if kind=='map' else 8),
    )
    if not set(query.files)<=set(hashes):
        raise ValueError('Native query requires the complete original source and evaluation file hashes')
    if query.operation is None or query.operation.describe()['kind']!=kind:
        raise ValueError('The original query must use an ordinary text '+kind.title()+', not another semantic operator')
    return query


def prepare(cfg,spec,directory,operation,*,kind='map'):
    if (operation is None or operation.stage.kind != kind
            or len(operation.records) != spec['expected_calls']):
        raise ValueError('Original native selection differs from the bound complete input')
    frame = operation.frame
    records = operation.records
    values = []
    for i,r in enumerate(records):
        fields=dict(r.values)
        # Preserve the existing single-field author context. Multiple fields
        # keep their names and complete native per-cell text in one context.
        data=next(iter(fields.values())) if len(fields)==1 else json.dumps(fields,ensure_ascii=False,sort_keys=True)
        row=dict(idx=i,row_id=r.row_id,source_position=r.source_position,source_index=r.source_index,
                 fields=fields,data=data)
        if 'reviewText' in fields:
            row['reviewText']=fields['reviewText']
        if 'reviewId' in frame:
            review_id=frame.iloc[i]['reviewId']
            row['reviewId']=int(review_id) if isinstance(review_id,Integral) else review_id
        values.append(row)
    if len({v['row_id'] for v in values}) != len(values):
        raise ValueError('Duplicate occurrence identity')
    directory.mkdir(mode=0o700,parents=True,exist_ok=True)
    path = directory/'native-input.csv'
    with path.open('x',newline='') as out:
        w=csv.DictWriter(out,fieldnames=['data']);w.writeheader();w.writerows({'data':v['data']} for v in values)
    table=Path(cfg['map_prompt_root'])/(spec['query_id']+'.json');table.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    with table.open('x') as out:json.dump(dict(query_id=spec['query_id'],instruction=operation.stage.method.instruction,rows=values),out,ensure_ascii=False)
    return values,path
