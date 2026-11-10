#!/usr/bin/env python3
"""Profile original LOTUS text layouts and exact token-prefix subgroups on CPU."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.infrastructure.private_artifacts import new_private_directory, write_private_json
from src.experiments.request_layout_study import profile_layouts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('config', 'sembench-checkout', 'data-dir', 'tokenizer', 'output-dir'):
        parser.add_argument('--' + name, required=True)
    for name in ('max-rows', 'block-tokens', 'window-rows'):
        parser.add_argument('--' + name, type=int, required=True)
    args = parser.parse_args()
    if min(args.max_rows, args.block_tokens, args.window_rows) < 1:
        parser.error('positive row limits, block hypothesis and window required')
    import lotus
    from transformers import AutoTokenizer
    from src.experiments.sembench_text_query import configuration, query_from_config
    from src.semantic_methods.lotus.record_method import LotusRecordMethod
    from src.semantic_methods.lotus.source import CHAIN_FILES, source_report
    config, engine, generation, _ = configuration(args.config)
    query = query_from_config(config, args.sembench_checkout, args.data_dir, generation)
    operation = query.operation
    if operation is None or len(operation.records) > args.max_rows:
        raise ValueError('the complete initial operator must fit the explicit study size')
    if engine.chat_template is not None:
        raise ValueError('layout study requires the tokenizer-owned chat template')
    tokenizer = AutoTokenizer.from_pretrained(args.tokenizer, local_files_only=True)
    output = Path(args.output_dir)
    new_private_directory(output)
    with lotus.settings.context(enable_cache=False):
        result = profile_layouts(LotusRecordMethod(operation.stage), operation.records,
                                 tokenizer, query_id=query.query_id,
                                 block_tokens=args.block_tokens, window_rows=args.window_rows)
    result.update(query=query.describe(), source=source_report('layout-study', CHAIN_FILES))
    write_private_json(output / 'result.json', result)
    for arm in result['arms']:
        write_private_json(output / (arm['layout'] + '.requests.json'), arm['capture'])
    print(json.dumps(dict(model_calls=0, gpu=False, layouts=[dict(layout=a['layout'],
        requests=len(a['capture']['requests']), full_prompt_tokens=sum(r['prompt_tokens'] for r in a['capture']['requests']),
        prefix_tree_shared_tokens=sum(w['shared_prefix_tree_tokens'] for w in a['windows'])) for a in result['arms']])))


if __name__ == '__main__':
    main()
