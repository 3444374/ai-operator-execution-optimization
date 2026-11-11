"""Actual current-core import and full Arrow boundary, with frozen256 messages/tokens."""
import argparse
import hashlib
import json
import os
import tempfile
from pathlib import Path
import pyarrow as pa
from transformers import AutoTokenizer
from integrations.imlane_duckdb import q9_udf
from src.infrastructure.private_artifacts import content_digest


def main():
    p = Path(os.environ['SEMLOOM_Q9_FIXTURE'])
    parser = argparse.ArgumentParser()
    parser.add_argument('--tokenizer', required=True)
    args = parser.parse_args()
    reference = json.loads((p / 'token-reference.json').read_text())
    prepared = json.loads((p / 'udf-test-prepared.json').read_text())
    rows = prepared['rows']
    calls = prepared['calls']
    lock = json.loads((p / 'INPUT-IDENTITY.json').read_text())
    assert len(rows) == len(calls) == len(reference) == 256
    assert [content_digest(json.loads(c['messages_json'])) for c in calls] == lock['messages_sha256']
    assert [json.loads(c['messages_json']) for c in calls] == [r['messages'] for r in reference]
    tokenizer = AutoTokenizer.from_pretrained(args.tokenizer,
        local_files_only=True,
        trust_remote_code=False)
    for r in reference:
        assert tokenizer.apply_chat_template(r['messages'],
            tokenize=True,
            add_generation_prompt=True) == r['ids']
    with tempfile.TemporaryDirectory(prefix='q9-import-') as tmp:
        d = Path(tmp)
        messages = d / 'messages.json'
        envelope = {
            row['occurrence_id']: dict(row, messages=json.loads(call['messages_json']))
            for row, call in zip(rows, calls, strict=True)
        }
        messages.write_text(json.dumps(dict(rows=envelope)))
        provider = d / 'provider.py'
        provider.write_text(
            "import json\n"
            "from pathlib import Path\n\n"
            "def infer_batch(messages):\n"
            "    expected = json.loads(Path(__file__).with_name('expected.json').read_text())\n"
            "    assert messages == expected\n"
            "    return [str(i % 5 + 1) for i in range(len(messages))]\n"
        )
        (d / 'expected.json').write_text(json.dumps([r['messages'] for r in reference]))
        config = d / 'udf.json'
        config.write_text(json.dumps(dict(provider_path=str(provider),
            provider_function='infer_batch',
            provider_mode='software_fixture',
            receipt_dir=str(d),
            messages_path=str(messages),
            messages_sha256=hashlib.sha256(messages.read_bytes()).hexdigest())))
        os.environ['SEMLOOM_IMLANE_Q9_CONFIG'] = str(config)
        columns = ('occurrence_id', 'id', 'reviewId', 'reviewText')
        table = pa.table({
            f'c{i}': [row[column] for row in rows]
            for i, column in enumerate(columns)
        })
        result = q9_udf.infer_q9(table)
        assert result.column(0).type == pa.float64()
        assert result.column(0).to_pylist() == [float(i % 5 + 1) for i in range(256)]
        assert len({r['reviewId'] for r in rows}) == 128
        receipts = list(d.glob('q9-batch-*.json'))
        assert len(receipts) == 1
        receipt = json.loads(receipts[0].read_text())
        assert receipt['status'] == 'succeeded' and len(receipt['predictions']) == 256
        messages.write_text('{}')
        try:
            q9_udf.infer_q9(table)
        except ValueError as e:
            assert 'messages changed' in str(e)
        else:
            raise AssertionError('corrupted envelope accepted')
    evidence = dict(status='passed',
        actual_q9_import=True,
        actual_arrow_kernel=True,
        original_messages=256,
        exact_token_arrays=256,
        occurrences=256,
        business_ids=128,
        output='DOUBLE',
        receipt_predictions=256,
        corruption_rejected=True,
        models=0)
    print(evidence)

if __name__ == '__main__':
    main()
