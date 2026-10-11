"""Replay native-length eligibility from all retained public metadata, without I/O services."""
from pathlib import Path
import gzip
import hashlib
import json
import math
import random


def main():
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
    for split, pool in pools.items():
        selected = sorted(random.Random(f'{20261011}:{split}:natural').sample(pool, 512),
                          key=lambda row: row['source_position'])
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
    print(json.dumps(dict(status='passed', metadata_rows=len(rows), selection_rows=1024,
                         decision=decision, model_calls=0)))


if __name__ == '__main__':
    main()
