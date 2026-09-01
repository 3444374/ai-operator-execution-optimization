"""Preserve the incomplete 7B comparison with a mismatched model default."""
import argparse
import json
from pathlib import Path
import shutil
import sys

from package_prompt import save, seal, sha

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--root", type=Path, required=True)
parser.add_argument("--repo", type=Path, required=True)
args = parser.parse_args()
sys.path.insert(0, str(args.repo / "code"))
from src.baselines.common.redact import redact_text
root = args.root
identity = json.loads((root / "service-identity.json").read_text())
assert not Path(f"/proc/{identity['sidecar']['pid']}").exists()
records = list(map(json.loads, (root / "responses.jsonl").read_text().splitlines()))
server_log = (root / "vllm/ep_8013.log").read_text()
save(root / "aborted.json", dict(reason="model-default repetition_penalty mismatch: 1.05 vs baseline 1.1",
    matched_comparison=False, stopped_before_completion=True, responses_preserved=len(records),
    server_chat_post_statuses=server_log.count("POST /v1/chat/completions"),
    qualification_verdict=None, production_changes=False, held_out_model_calls=0,
    owned_endpoint_stopped=True, raw_service_identity_sha256=sha(root/"service-identity.json")))
public = root / "public"
public.mkdir()
for name in ("responses.jsonl", "aborted.json", "plans.json", "cases.json", "schedule.json", "model-files.json",
             "parser-controls.json", "repro-verdict.json", "repro-template-audit.json",
             "prompt_qualification.py", "template_audit.py"):
    if (root/name).exists():
        shutil.copy2(root/name, public/name)
for name in ("archive_aborted.py", "package_prompt.py"):
    shutil.copy2(Path(__file__).with_name(name), public/name)
print(json.dumps(dict(public_files=seal(public), private_files=seal(root))))
