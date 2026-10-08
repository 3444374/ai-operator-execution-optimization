"""Recount accepted but unborrowed rows from the earlier public archive."""

from collections import Counter
import hashlib
import json
from pathlib import Path
import tarfile


def analyze(repository):
    root = repository / 'experiments/results/postgresql/text_map_preparation_validation_20261004/raw'
    records = [json.loads(line) for line in (root/'repaired-storage-manifest.jsonl').read_text().splitlines()]
    results = []
    with tarfile.open(root/'repaired-evidence.tar.gz') as archive:
        for repeat in range(1, 4):
            record = next(r for r in records if r.get('path', '').endswith(
                f'prepared1024/measure{repeat}/events.jsonl'))
            data = archive.extractfile(record['storage']['member']).read()
            assert len(data) == record['bytes']
            assert hashlib.sha256(data).hexdigest() == record['sha256']
            events = sorted((json.loads(line) for line in data.splitlines()), key=lambda e:e['monotonic_ns'])
            accepted, borrowed, populations = set(), set(), []
            for event in events:
                key = event.get('key')
                if event['event'] == 'core_offer' and event.get('accepted_prefix_count', 0):
                    for offset in range(event['accepted_prefix_count']):
                        identity = (key['session_id'], key['sequence'] + offset)
                        assert identity not in accepted
                        accepted.add(identity)
                elif event['event'] == 'core_preparation_borrowed':
                    identity = (key['session_id'], key['sequence'])
                    assert identity in accepted and identity not in borrowed
                    populations.append(len(accepted-borrowed))
                    borrowed.add(identity)
            assert accepted == borrowed and len(borrowed) == 1024
            results.append(dict(repeat=repeat, event_sha256=record['sha256'], borrowed_rows=len(borrowed),
                candidate_population=dict(sorted(Counter(populations).items())),
                after_first_128=dict(sorted(Counter(populations[128:]).items()))))
    return dict(status='passed', source='public earlier query archive', verified_borrows=3072,
                new_model_requests=0, queries=results)


if __name__ == '__main__':
    print(json.dumps(analyze(Path.cwd()), indent=2))
