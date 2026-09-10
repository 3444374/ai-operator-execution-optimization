"""Prepare separate raw-input, reference and identity files outside query time.

Query runners load the small identity file first. Payload and label files are
read for table installation or post-query evaluation, never as a hidden source
for a claimed PG-source execution.
"""
import csv
import hashlib
import json
from pathlib import Path

from src.baselines.common.private_artifacts import new_private_directory, open_private_text, write_private_json, content_digest
from src.baselines.text.squad_map import validate_manifest
from .query_inputs import QueryInputs


SCHEMA = 'semloom.pg_query_inputs.v2'
MAX_PREPARED_ROWS = 30000
MAX_RAW_FILE_BYTES = 256*1048576


def file_identity(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(1048576),b''):
            digest.update(block)
    return dict(sha256=digest.hexdigest(),bytes=Path(path).stat().st_size)


def prepare(root, kind, examples, provenance, *, max_rows, max_source_bytes=65536, max_input_bytes=65536):
    """examples yields (row_id, first_raw_column, second_raw_column, reference)."""
    if type(max_rows) is not int or not 1 <= max_rows <= MAX_PREPARED_ROWS:
        raise ValueError('unsupported preparation row bound')
    root = Path(root)
    new_private_directory(root)
    spec = QueryInputs(kind,'input_table',max_rows,max_source_bytes,max_input_bytes)
    seen = set()
    total_bytes = count = 0
    with open_private_text(root/'raw.jsonl') as raw, open_private_text(root/'references.jsonl') as refs:
        for position,example in enumerate(examples):
            identity,first,second,reference=example[:4]
            if position >= max_rows or identity in seen:
                raise ValueError('source exceeds row limit or repeats an identity')
            row = [position,identity,first,second]
            if kind=='movie':row.append(example[4] if len(example)==5 else identity)
            spec.convert(row)
            if kind == 'movie' and reference not in ('POSITIVE','NEGATIVE'):
                raise ValueError('movie reference is not a supported sentiment')
            if kind == 'squad' and (not isinstance(reference,list) or not reference or
                                   any(not isinstance(v,str) or not v for v in reference)):
                raise ValueError('SQuAD reference must contain answer strings')
            value = json.dumps(row,ensure_ascii=False,separators=(',',':'))+'\n'
            total_bytes += len(value.encode())
            if total_bytes > MAX_RAW_FILE_BYTES:
                raise ValueError('prepared source exceeds total raw byte bound')
            raw.write(value)
            refs.write(json.dumps(dict(row_id=identity,review_id=row[4] if kind=='movie' else None,movie_id=first if kind=='movie' else None,
                                       reference=reference),ensure_ascii=False)+'\n')
            seen.add(identity)
            count += 1
    if count == 0:
        raise ValueError('prepared input is empty')
    value = dict(schema=SCHEMA,kind=kind,rows=count,max_source_bytes=max_source_bytes,
                 max_input_bytes=max_input_bytes,provenance=provenance,
                 files={name:file_identity(root/name) for name in ('raw.jsonl','references.jsonl')})
    value['sha256'] = content_digest(value)
    write_private_json(root/'manifest.json',value)
    return value


def movie_csv_examples(path):
    with Path(path).open(newline='') as stream:
        reader = csv.DictReader(stream)
        if not {'reviewId','id','reviewText','scoreSentiment'} <= set(reader.fieldnames or ()):
            raise ValueError('Movie CSV lacks original SemBench fields')
        for position,row in enumerate(reader):
            yield 'movie-row-'+str(position),row['id'],row['reviewText'],row['scoreSentiment'],row['reviewId']


def squad_examples(source_path, selected_manifest, split, rows):
    """Rejoin selected IDs to original raw fields; never parse a formatted prompt."""
    validate_manifest(selected_manifest)
    selected = selected_manifest['splits'][split][:rows]
    if len(selected) != rows:
        raise ValueError('selection has too few rows')
    selected_ids = {r['source_example_id'] for r in selected}
    found = {}
    source = json.loads(Path(source_path).read_text())
    if source.get('version') != '1.1':
        raise ValueError('expected original SQuAD 1.1 data')
    for article in source['data']:
        for paragraph in article['paragraphs']:
            for question in paragraph['qas']:
                identity = question['id']
                if identity in selected_ids:
                    if identity in found:
                        raise ValueError('original SQuAD repeats a selected ID')
                    found[identity] = (paragraph['context'],question['question'],[a['text'] for a in question['answers']])
    spec = QueryInputs('squad','input_table',rows)
    for position,row in enumerate(selected):
        identity = row['source_example_id']
        if identity not in found:
            raise ValueError('selected question is absent from original SQuAD')
        context,question,answers = found[identity]
        converted = spec.convert((position,identity,context,question))
        if converted['input_text'] != row['input_text'] or answers != row['reference_answers']:
            raise ValueError('original raw fields differ from the qualified selection')
        yield identity,context,question,answers


def load_manifest(path):
    path = Path(path)
    if path.stat().st_size > 1048576:
        raise ValueError('source identity file exceeds its bound')
    value = json.loads(path.read_text())
    if value.get('schema') != SCHEMA or value.get('sha256') != content_digest({k:v for k,v in value.items() if k!='sha256'}):
        raise ValueError('source identity mismatch')
    if type(value['rows']) is not int or not 1 <= value['rows'] <= MAX_PREPARED_ROWS:
        raise ValueError('invalid source row count')
    QueryInputs(value['kind'],'input_table',value['rows'],value['max_source_bytes'],value['max_input_bytes'])
    return value


def read_prepared(path, name):
    path = Path(path)
    manifest = load_manifest(path)
    if name not in ('raw.jsonl','references.jsonl'):
        raise ValueError('unknown prepared file')
    target = path.parent/name
    if target.stat().st_size > MAX_RAW_FILE_BYTES or file_identity(target) != manifest['files'][name]:
        raise ValueError('prepared file identity differs')
    count = 0
    with target.open() as stream:
        for line in stream:
            count += 1
            if count > manifest['rows']:
                raise ValueError('prepared file has excess rows')
            yield json.loads(line)
    if count != manifest['rows']:
        raise ValueError('prepared file is incomplete')
