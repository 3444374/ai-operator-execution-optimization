"""Compare actual HTTP messages with service and model chat-template token IDs."""
import argparse
import hashlib
import json
from pathlib import Path
import urllib.request

from transformers import AutoTokenizer


def run(args):
    tokenizer = AutoTokenizer.from_pretrained(args.model, local_files_only=True)
    checked = []
    for record in map(json.loads, args.requests.read_text().splitlines()):
        body = record["body"]
        messages = body["messages"]
        rendered = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
        expected = tokenizer.apply_chat_template(messages, tokenize=True, add_generation_prompt=True, return_dict=False)
        request = urllib.request.Request(args.endpoint + "/tokenize", data=json.dumps({
            "model": body["model"], "messages": messages,
            "add_generation_prompt": True, "add_special_tokens": False,
        }).encode(), headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(request, timeout=30) as response:
            actual = json.load(response)
        assert actual["tokens"] == expected, "model/service template token mismatch"
        assert actual["count"] == record["prompt_tokens"], "completion/tokenize count mismatch"
        item = dict(case_id=record["case_id"], profile=record["profile"],
                    http_body_sha256=record["http_body_sha256"],
                    ids_equal=True, count=actual["count"], usage_equal=True,
                    rendered_sha256=hashlib.sha256(rendered.encode()).hexdigest(),
                    token_ids_sha256=hashlib.sha256(json.dumps(expected).encode()).hexdigest())
        if record["case_id"] != "failed-training-input":
            item.update(messages=messages, rendered=rendered, token_ids=expected)
        checked.append(item)
    output = dict(template_sha256=hashlib.sha256(tokenizer.chat_template.encode()).hexdigest(),
                  tokenizer_class=type(tokenizer).__name__, checks=checked)
    with args.output.open("x") as handle:
        json.dump(output, handle, ensure_ascii=False, indent=2)
    print(json.dumps(dict(template_checks=len(checked), all_equal=True)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("model", "requests", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--endpoint", required=True)
    run(parser.parse_args())
