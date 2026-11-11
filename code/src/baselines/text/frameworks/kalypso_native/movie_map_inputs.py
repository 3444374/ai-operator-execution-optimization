"""Frozen Q9 input handoff from the SemBench driver to the native CPU Python."""
import csv
import hashlib
import json
import subprocess
from pathlib import Path

from .native_server import AUTHOR_SOURCE_COMMIT, physical_call_caps

SCHEMA = 'semloom.kalypso_q9_cpu_inputs.v1'
SOURCE_FILES = (
    'src/baselines/text/frameworks/kalypso_native/movie_map_inputs.py',
    'src/baselines/text/frameworks/kalypso_native/movie_map.py',
    'src/baselines/text/frameworks/kalypso_native/movie_map_cpu.py',
    'src/baselines/text/frameworks/kalypso_native/map_observation.py',
    'src/baselines/text/frameworks/kalypso_native/native_server.py',
    'src/baselines/text/frameworks/kalypso_native/cpu_native.py',
    'src/baselines/text/sembench_source.py',
    'src/baselines/text/sembench_text.py',
    'src/baselines/text/sembench_movie.py',
    'src/experiments/kalypso_query_lifecycle.py',
)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def source_binding(cfg):
    root = Path(cfg['baseline_source']).resolve() / 'code'
    if (root / SOURCE_FILES[0]).resolve() != Path(__file__).resolve():
        raise ValueError('CPU input handoff imported a different maintained source')
    actual = subprocess.check_output(['git', '-C', str(root.parent), 'rev-parse',
        'HEAD', 'HEAD^{tree}', 'HEAD:code'], text=True, timeout=10).split()
    if actual[:2] != [cfg['adapter_commit'], cfg['adapter_tree']]:
        raise ValueError('CPU input handoff source commit/tree differs from owner binding')
    dirty = subprocess.check_output(['git', '-C', str(root.parent), 'status', '--porcelain',
        '--untracked-files=no', '--', 'code'], text=True, timeout=10)
    if dirty:
        raise ValueError('CPU input handoff requires the clean owner-bound code tree')
    return dict(adapter_commit=actual[0], adapter_tree=actual[1], code_tree=actual[2],
        file_sha256={name: sha(root / name) for name in SOURCE_FILES},
        scope='verified clean Git checkout, owner-bound commit/tree and actual participating file hashes')



def binding(cfg, unit):
    if (len(cfg['units']) != 1 or not unit['queries'] or cfg['units'][0] != unit
            or any(q['task'] != 'movie-q9' for q in unit['queries'])):
        raise ValueError('Q9 CPU preparation requires one dedicated Map unit')
    physical_call_caps(unit)
    if cfg['source_commit'] != AUTHOR_SOURCE_COMMIT:
        raise ValueError('Q9 CPU input handoff requires original author05024')
    return dict(source=source_binding(cfg), author_source_commit=cfg['source_commit'],
        model=cfg['model'], model_revision=cfg['model_revision'],
        expected_template_sha256=cfg['expected_template_sha256'],
        source_data_sha256=cfg['source_data_sha256'], unit=unit)


def prepare_inputs(cfg, unit, output):
    """Driver-only step: execute original selection, without any native imports."""
    from .movie_map import make_query, prepare
    if cfg['execution_enabled'] or not cfg.get('CPU_fixture_only'):
        raise ValueError('Prepare CPU inputs with execution disabled and CPU_fixture_only=true')
    bound = binding(cfg, unit)
    output = Path(output)
    output.mkdir(mode=0o700)
    local = dict(cfg, map_prompt_root=str(output / 'prompts'))
    queries = []
    for index, spec in enumerate(unit['queries']):
        qid = spec['query_id']
        if Path(qid).name != qid or qid in ('.', '..'):
            raise ValueError('Q9 CPU query identity must be a filename-safe name')
        query = make_query(local, qid)
        directory = output / ('query-' + str(index))
        rows, csv_path = prepare(local, spec, directory, query.operation)
        description = query.describe()
        description['policy'] = 'original Q9 Map selection; no model calls in CPU preparation'
        description['native_retry_policy'] = 'author length retry remains at most once at8192'
        source_path = directory / 'source.json'
        source_path.write_text(json.dumps(description, ensure_ascii=False, indent=2) + '\n')
        prompt_path = output / 'prompts' / (qid + '.json')
        queries.append(dict(query_id=qid, logical_occurrences=len(rows),
            files={role: dict(path=str(path.relative_to(output)), sha256=sha(path))
                   for role, path in dict(csv=csv_path, prompt=prompt_path, source=source_path).items()}))
    manifest = dict(schema=SCHEMA, binding=bound, queries=queries, model_calls=0,
        preparation='original SemBench selection in driver Python; author Python consumes only frozen files')
    path = output / 'CPU-INPUTS.json'
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n')
    return dict(status='prepared_original_Q9_CPU_inputs_no_model', manifest=str(path.resolve()),
                sha256=sha(path), queries=len(queries), logical_occurrences=sum(q['logical_occurrences'] for q in queries))


def load_inputs(cfg):
    """Author-side validation uses only stdlib and the existing native boundary."""
    reference = cfg['native_cpu_inputs']
    path = Path(reference['manifest']).resolve()
    if sha(path) != reference['sha256']:
        raise ValueError('Frozen Q9 CPU input manifest changed')
    manifest = json.loads(path.read_text())
    if (manifest['schema'] != SCHEMA or manifest['binding'] != binding(cfg, cfg['units'][0])
            or manifest['model_calls'] != 0):
        raise ValueError('Frozen Q9 CPU source/model/unit binding differs')
    queries = manifest['queries']
    expected = cfg['units'][0]['queries']
    if [q['query_id'] for q in queries] != [q['query_id'] for q in expected]:
        raise ValueError('Frozen Q9 CPU query sequence differs')
    loaded = []
    for item, spec in zip(queries, expected, strict=True):
        paths = {}
        for role in ('csv', 'prompt', 'source'):
            entry = item['files'][role]
            target = (path.parent / entry['path']).resolve()
            if not target.is_relative_to(path.parent) or sha(target) != entry['sha256']:
                raise ValueError('Frozen Q9 CPU input file changed: ' + role)
            paths[role] = target
        description = json.loads(paths['source'].read_text())
        table = json.loads(paths['prompt'].read_text())
        if (description['scenario'] != 'movie' or description['query'] != 'Q9'
                or description['ranking'] != 'map' or description['query_id'] != spec['query_id']
                or description['sembench_commit'] != 'c814e3807e72d4cf876b852b17e77f3cc94575c2'
                or description['expected_model_calls'] != 256
                or description['operation']['stage']['method']['instruction'] != table['instruction']):
            raise ValueError('Frozen Q9 CPU input is not the original Map task/instruction')
        rows = table['rows']
        if (table['query_id'] != spec['query_id'] or item['logical_occurrences'] != 256 or len(rows) != 256
                or any(type(r['idx']) is not int or type(r['source_position']) is not int for r in rows)
                or [r['idx'] for r in rows] != list(range(256))
                or [r['source_position'] for r in rows] != list(range(256))
                or [r['row_id'] for r in rows] != [spec['query_id'] + '-row-' + str(i) for i in range(256)]):
            raise ValueError('Frozen Q9 CPU occurrence identities differ')
        with paths['csv'].open(newline='') as stream:
            reader = csv.DictReader(stream)
            if reader.fieldnames != ['data'] or [r['data'] for r in reader] != [r['reviewText'] for r in rows]:
                raise ValueError('Frozen Q9 CPU CSV differs from complete occurrence table')
        loaded.append(dict(spec=spec, table=table, paths=paths))
    return manifest, loaded
