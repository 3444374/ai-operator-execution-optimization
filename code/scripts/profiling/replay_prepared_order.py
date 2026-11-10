#!/usr/bin/env python3
"""Replay FIFO and bounded candidate orders on an existing private request capture."""
import argparse
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.infrastructure.private_artifacts import write_private_json
from src.experiments.prepared_order_replay import replay


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--capture', required=True)
    parser.add_argument('--output', required=True)
    for name in ('candidate-limit', 'group-requests', 'prefix-token-limit', 'block-tokens'):
        parser.add_argument('--' + name, type=int, required=True)
    parser.add_argument('--repeats', type=int, default=3)
    args = parser.parse_args()
    path = Path(args.capture)
    raw = path.read_bytes()
    result = replay(json.loads(raw), candidate_limit=args.candidate_limit,
        group_requests=args.group_requests, prefix_token_limit=args.prefix_token_limit,
        block_tokens=args.block_tokens, repeats=args.repeats)
    result['capture_sha256'] = hashlib.sha256(raw).hexdigest()
    write_private_json(Path(args.output), result)
    print(json.dumps(dict(requests=result['requests'], model_calls=0,
                         metrics={a['mode']: a['within_group_shared_full_block_tokens'] for a in result['arms']})))


if __name__ == '__main__':
    main()
