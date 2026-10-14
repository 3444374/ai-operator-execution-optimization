"""Replay fixed LOTUS Filter functions against private, existing HTTP evidence.

Uses Python's standard library. Supply the private parser source separately;
neither vendor source nor model responses are embedded in this script or output.
"""
import argparse
import ast
from collections import Counter
import hashlib
import json
from pathlib import Path
import tarfile
from types import SimpleNamespace


FUNCTION_NAMES = ('cot_postprocessor', 'deepseek_cot_postprocessor',
                  'get_cot_postprocessor', 'filter_postprocess')


def require(value, message):
    if not value:
        raise RuntimeError(message)


def digest(value):
    return hashlib.sha256(value).hexdigest()


def file_digest(path):
    value = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def request_digest(value):
    return digest(json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(',', ':'), allow_nan=False).encode())


class MemoryLogger:
    def __init__(self):
        self.fallback_events = 0

    def info(self, message):
        # Deliberately retain no model content or log text.
        self.fallback_events += 1


def load_functions(source_path, expected_sha):
    raw = source_path.read_bytes()
    require(digest(raw) == expected_sha, 'parser source SHA differs')
    original = ast.parse(raw)
    names = set(FUNCTION_NAMES)
    selected = [node for node in original.body
                if isinstance(node, ast.FunctionDef) and node.name in names
                or isinstance(node, ast.Assign) and
                any(isinstance(target, ast.Name) and target.id == 'COT_POSTPROCESSORS'
                    for target in node.targets)]
    require([node.name for node in selected if isinstance(node, ast.FunctionDef)] == list(FUNCTION_NAMES),
            'original parser function set/order differs')
    require(len(selected) == 5 and not any(isinstance(node, (ast.Import, ast.ImportFrom))
            for selected_node in selected for node in ast.walk(selected_node)),
            'selected parser dependency boundary differs')
    # Original function/assignment nodes are unchanged. Postponing annotations
    # avoids importing the vendor LM/output-container types solely for hints.
    future = ast.ImportFrom(module='__future__',
                            names=[ast.alias(name='annotations')], level=0)
    module = ast.Module(body=[future, *selected], type_ignores=[])
    ast.fix_missing_locations(module)
    logger = MemoryLogger()
    namespace = dict(json=json, lotus=SimpleNamespace(logger=logger),
                     SemanticFilterPostprocessOutput=SimpleNamespace)
    exec(compile(module, '<fixed-lotus-filter-functions>', 'exec'), namespace)
    return namespace, logger, dict(
        source_sha256=expected_sha, source_bytes=len(raw),
        functions=list(FUNCTION_NAMES), original_function_AST_unchanged=True,
        future_annotations_only=True,
        selected_AST_sha256=digest(ast.dump(ast.Module(body=selected, type_ignores=[]),
                                 include_attributes=False).encode()),
        SDK_imported=False, logger_and_output_container='memory-only stand-ins')


def read_json(archive, name):
    return json.loads(archive.extractfile(name).read())


def read_lines(archive, name):
    return [json.loads(line) for line in archive.extractfile(name).read().splitlines()]


def replay_query(archive, arm, query, model_id, namespace, logger):
    expected_count = 2000 if query == 1 else 120
    base = f'model-matched/{arm}/original-query-slot/output/a0-measurement-0-q{query}/'
    occurrences = read_json(archive, f'request-preparation/q{query}-occurrences.json')
    expected = {row['row_id']: row['values_sha256'] for row in occurrences}
    decisions = read_json(archive, base+'filter-decisions.json')
    protocols = read_lines(archive, base+'protocols.jsonl')
    sdk = [row for row in read_lines(archive, base+'complete-responses.jsonl')
           if 'response_values' in row]
    require(len(expected) == len(occurrences) == len(decisions) == len(protocols) == len(sdk)
            == expected_count, 'row/response count differs')
    require([row[0] for row in decisions] == list(expected)
            and all(type(row[1]) is bool for row in decisions), 'original decision row order/type differs')
    require(Counter(request_digest(row['request']) for row in protocols)
            == Counter(row['values_sha256'] for row in occurrences), 'complete request multiset differs')
    by_id = {row['response']['id']: row for row in protocols}
    require(len(by_id) == expected_count, 'raw HTTP response ID duplicated')
    by_row, response_ids = {}, set()
    for record in sdk:
        response = record['response_values']
        response_id, row_id = response['id'], record['row_id']
        require(row_id in expected and row_id not in by_row and response_id not in response_ids
                and response_id in by_id, 'SDK row/response association differs')
        raw = by_id[response_id]
        require(raw['http_status'] == 200 and response['model'] == raw['response']['model'] == model_id,
                'HTTP response status/model differs')
        require(request_digest(raw['request']) == expected[row_id], 'HTTP response belongs to another row')
        raw_content = raw['response']['choices'][0]['message']['content']
        require(type(raw_content) is str and
                response['choices'][0]['message']['content'] == raw_content,
                'HTTP and SDK content differ')
        by_row[row_id] = raw_content
        response_ids.add(response_id)
    answers = [by_row[row_id] for row_id in expected]
    # Matches LM.get_model_name for the fixed OpenAI-prefixed Qwen model.
    name = model_id.rsplit('/', 1)[-1].split(':', 1)[0].lower()
    require('qwen' in name, 'declared fixed Qwen model differs')
    model = SimpleNamespace(get_model_name=lambda: name)
    before = logger.fallback_events
    replayed = namespace['filter_postprocess'](answers, model, default=True,
                                              output_tokens=('True', 'False'))
    mismatches = [row_id for (row_id, actual), repeated in zip(decisions, replayed.outputs)
                  if actual != repeated or type(repeated) is not bool]
    require(len(replayed.outputs) == expected_count and replayed.raw_outputs == answers
            and len(replayed.explanations) == expected_count, 'original Filter result shape differs')
    require(not mismatches, 'offline original parser decisions differ')
    parsed_answers, _ = namespace['get_cot_postprocessor'](model)(answers)
    both = sum('true' in answer.lower() and 'false' in answer.lower()
               for answer in parsed_answers if isinstance(answer, str))
    return dict(arm=arm, query=query, rows=expected_count, replayed_rows=expected_count,
        mismatch_rows=mismatches, SDK_HTTP_row_hash_bound=True,
        original_parser_decisions_equal=True, default=True, output_tokens=['True', 'False'],
        model_name=name, selected_model_processor='cot_postprocessor',
        default_fallback_rows=logger.fallback_events-before, both_boolean_token_rows=both,
        input_content_vector_sha256=digest(json.dumps(answers, ensure_ascii=False,
                                           separators=(',', ':')).encode()),
        recorded_decisions_sha256=digest(json.dumps(decisions, separators=(',', ':')).encode()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive-a', type=Path, required=True)
    parser.add_argument('--archive-a-sha', required=True)
    parser.add_argument('--archive-b', type=Path, required=True)
    parser.add_argument('--archive-b-sha', required=True)
    parser.add_argument('--parser-source', type=Path, required=True)
    parser.add_argument('--parser-sha', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--preserve', type=Path, action='append', default=[])
    args = parser.parse_args()
    preserved = {str(index): file_digest(path) for index, path in enumerate(args.preserve)}
    namespace, logger, code = load_functions(args.parser_source, args.parser_sha)
    rows, hashes = [], {}
    model_ids = set()
    for family, path, expected_sha, arms in (
            ('A', args.archive_a, args.archive_a_sha, ('N-A', 'Daft-A')),
            ('B', args.archive_b, args.archive_b_sha, ('RayData-A', 'S-A'))):
        hashes[family] = file_digest(path)
        require(hashes[family] == expected_sha, 'private closed archive SHA differs')
        with tarfile.open(path, 'r:gz') as archive:
            model_id = read_json(archive, 'state.json')['model_id']
            model_ids.add(model_id)
            source = archive.extractfile('source/code/src/semantic_methods/lotus/sdk.py').read()
            tree = ast.parse(source)
            declared = next(ast.literal_eval(node.value) for node in tree.body
                if isinstance(node, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == 'SOURCE_HASHES' for t in node.targets))
            require(declared['sem_ops/postprocessors.py'] == args.parser_sha,
                    'parser SHA not the archived fixed source requirement')
            for arm in arms:
                for query in (2, 1):
                    rows.append(dict(warm_state_family=family,
                        **replay_query(archive, arm, query, model_id, namespace, logger)))
    require(len(model_ids) == 1 and sum(row['replayed_rows'] for row in rows) == 8480,
            'four-arm model/count differs')
    require(preserved == {str(index): file_digest(path) for index, path in enumerate(args.preserve)},
            'previous evidence bytes changed')
    output = dict(schema='independent.four_arm_original_parser_replay.v1', passed=True,
        parser=code, archive_sha256=hashes, model_id=next(iter(model_ids)),
        total_queries=len(rows), total_replayed_rows=sum(row['replayed_rows'] for row in rows),
        total_mismatches=sum(len(row['mismatch_rows']) for row in rows), queries=rows,
        default_fallback_rows=sum(row['default_fallback_rows'] for row in rows),
        SDK_modules_imported=False, network_IO=False, new_model_requests=0,
        previous_evidence_sha256_preserved=preserved,
        scope='Original Filter postprocessing algorithm on existing complete HTTP content after SDK/id/row/hash equality. No provider response decoding, model generation or full vendor SDK initialization was repeated.')
    # Exclusive output creation keeps earlier review/replay outputs intact.
    with args.output.open('x') as stream:
        stream.write(json.dumps(output, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps(dict(passed=True, queries=output['total_queries'],
        replayed_rows=output['total_replayed_rows'], mismatches=output['total_mismatches'],
        default_fallback_rows=output['default_fallback_rows'],
        vendor_SDK_imported=False, network_IO=False, new_model_requests=0)))


if __name__ == '__main__':
    main()
