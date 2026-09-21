"""Replay captured messages through local tokenizer files; send no model request."""
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
from transformers import AutoTokenizer

parser = argparse.ArgumentParser()
parser.add_argument('--query', type=Path, required=True)
parser.add_argument('--model', type=Path, required=True)
args = parser.parse_args()
record = json.loads(args.query.read_text())
messages = record['events'][0]['body']['messages']
tokenizer = AutoTokenizer.from_pretrained(args.model, local_files_only=True)
text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
tokens = tokenizer.apply_chat_template(messages, tokenize=True, add_generation_prompt=True)
print(json.dumps(dict(transformers=importlib.metadata.version('transformers'),
    tokenizer_class=type(tokenizer).__name__, result_type=type(tokens).__name__,
    python_len=len(tokens), tokenized=tokens if isinstance(tokens, list) else dict(tokens),
    encoded_text=tokenizer.encode(text, add_special_tokens=False),
    template=tokenizer.chat_template, rendered=text,
    reported_prompt_tokens=record['events'][1]['prompt_tokens']), indent=2))
