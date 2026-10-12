"""Replay the original selection and optionally describe tuning input order, offline."""
from pathlib import Path
import argparse
import gzip
import hashlib
import json
import math
import random


def replay():
    root = Path(__file__).resolve().parent / 'raw'
    verification = json.loads((root / 'verification.json').read_text())
    profile_bytes = (root / 'native-token-profile.json.gz').read_bytes()
    assert len(profile_bytes) == verification['public_profile_bytes']
    assert hashlib.sha256(profile_bytes).hexdigest() == verification['public_profile_sha256']
    rows = json.loads(gzip.decompress(profile_bytes))
    expected = json.loads((root / 'profile-summary.json').read_text())
    assert len(rows) == expected['source_rows'] == 10570
    assert len({row['source_example_id'] for row in rows}) == len(rows)
    assert [row['source_position'] for row in rows] == list(range(len(rows)))
    groups = sorted({row['context_sha256'] for row in rows})
    random.Random(20261011).shuffle(groups)
    partitions = {key: ('tuning', 'evaluation')[i % 2] for i, key in enumerate(groups)}
    pools = {'tuning': [], 'evaluation': []}
    for row in rows:
        assert row['split'] == partitions[row['context_sha256']]
        assert type(row['input_tokens']) is int and row['input_tokens'] > 0
        assert row['fits_context'] == (row['input_tokens'] + 64 <= 4096)
        if row['fits_context']:
            pools[row['split']].append(row)
    assert {key: len(value) for key, value in pools.items()} == expected['eligible_rows']
    reconstructed = {}
    tuning = None
    for split, pool in pools.items():
        selected = sorted(random.Random(f'{20261011}:{split}:natural').sample(pool, 512),
                          key=lambda row: row['source_position'])
        if split == 'tuning':
            tuning = selected
        values = [row['input_tokens'] for row in selected]
        ordered = sorted(values)
        quantiles = {str(p): ordered[max(0, math.ceil(p / 100 * len(values)) - 1)]
                     for p in (0, 10, 50, 90, 99, 100)}
        mean = sum(values) / len(values)
        reconstructed[split] = dict(rows=len(values), input_token_quantiles=quantiles,
            mean_input_tokens=mean,
            coefficient_of_variation=(sum((value-mean)**2 for value in values)/len(values))**.5/mean,
            ratio_90_to_10=quantiles['90']/quantiles['10'],
            request_bytes=sum(row['request_bytes'] for row in selected))
    assert reconstructed == expected['splits']
    decision = 'eligible' if reconstructed['tuning']['ratio_90_to_10'] >= 2 else 'insufficient_heterogeneity'
    assert decision == expected['decision'] == 'insufficient_heterogeneity'
    assert expected['model_calls'] == 0
    assert json.loads((root / 'stop-record.json').read_text())['status'] == 'stopped_before_model'
    status = dict(status='passed', metadata_rows=len(rows), selection_rows=1024,
                  decision=decision, model_calls=0)
    return status, tuning, expected, verification


def candidate_order(tuning, expected, verification):
    """Describe two source chunks; never simulate arrival, completion, or service time."""
    cutoff = expected['splits']['tuning']['input_token_quantiles']['90']
    blocks = []
    for start in range(0, len(tuning), 256):
        window = tuning[start:start + 256]
        values = [row['input_tokens'] for row in window]
        order = sorted(range(len(window)), key=lambda index: values[index])
        rank = {index: position for position, index in enumerate(order)}
        tail = [index for index, value in enumerate(values) if value > cutoff]
        pairs = [(left, right) for index, left in enumerate(values) for right in values[index + 1:]]
        changed = [index for index in range(len(window)) if rank[index] != index]
        def order_digest(indices):
            positions = [window[i]['source_position'] for i in indices]
            return hashlib.sha256(json.dumps(positions, separators=(',', ':')).encode()).hexdigest()
        blocks.append(dict(
            source_slice=[start, start + len(window)], rows=len(window),
            distinct_lengths=len(set(values)), min_tokens=min(values), max_tokens=max(values),
            changed_positions=len(changed), non_tail_changed_positions=sum(values[i] <= cutoff for i in changed),
            absolute_rank_displacement_sum=sum(abs(rank[i] - i) for i in range(len(window))),
            tail_rows=len(tail), tail_moved_later=sum(rank[i] > i for i in tail),
            tail_moved_earlier=sum(rank[i] < i for i in tail), tail_unchanged=sum(rank[i] == i for i in tail),
            inverted_unequal_length_pairs=sum(left > right for left, right in pairs),
            unequal_length_pairs=sum(left != right for left, right in pairs),
            changed_group_members=len(set(order) ^ set(range(len(window)))),
            original_order_sha256=order_digest(range(len(window))),
            ascending_order_sha256=order_digest(order),
        ))
    counts = ('changed_positions', 'non_tail_changed_positions', 'absolute_rank_displacement_sum',
              'tail_rows', 'tail_moved_later', 'tail_moved_earlier', 'tail_unchanged',
              'inverted_unequal_length_pairs', 'unequal_length_pairs', 'changed_group_members')
    return dict(
        schema='semloom.squad.tuning_order_description.v1',
        analysis_id='squad-tuning-candidate-order-20261012',
        preparation_source=expected['preparation_source'],
        profile_sha256=verification['public_profile_sha256'],
        program_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        input_split='tuning', seed=20261011, selection='same natural sample, original source order',
        selected_rows=len(tuning), candidate_view='nonoverlapping source chunks, not observed Core candidates',
        chunk_rows=256, sort='stable ascending input_tokens',
        tail_definition=dict(input_tokens_greater_than=cutoff, basis='original tuning 90th percentile'),
        counts={key: sum(block[key] for block in blocks) for key in counts}, blocks=blocks,
        model_calls=0, queries_executed=0,
        simulated_service_times=False, formatter_and_tokenizer_rerun=False,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate-order', action='store_true', help='Describe only the original tuning sample order')
    args = parser.parse_args()
    status, tuning, expected, verification = replay()
    result = candidate_order(tuning, expected, verification) if args.candidate_order else status
    print(json.dumps(result, indent=2 if args.candidate_order else None))


if __name__ == '__main__':
    main()
