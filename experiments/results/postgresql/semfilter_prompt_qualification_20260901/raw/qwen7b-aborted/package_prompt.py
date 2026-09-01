"""Archive this stopped prompt diagnostic without publishing private inputs or paths."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import socket
import sys


def sha(path):
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8*1024*1024), b""):
            value.update(block)
    return value.hexdigest()


def save(path, value):
    with path.open("x") as handle:
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")


def seal(root):
    paths = sorted(p for p in root.rglob("*") if p.is_file() and p != root / "SHA256SUMS")
    with (root / "SHA256SUMS").open("x") as handle:
        for path in paths:
            handle.write(f"{sha(path)}  {path.relative_to(root)}\n")
    return len(paths)


def run(args):
    sys.path.insert(0, str(args.repo / "code"))
    from src.baselines.common.redact import redact_text
    root = args.root
    service = json.loads((root / "service-identity.json").read_text())
    assert not Path(f"/proc/{service['sidecar']['pid']}").exists(), "owned endpoint still running"
    with socket.socket() as probe:
        assert probe.connect_ex(("127.0.0.1", 8013)) != 0, "owned port still listening"
    summary = json.loads((root / "summary.json").read_text())
    responses = list(map(json.loads, (root / "responses.jsonl").read_text().splitlines()))
    assert len(responses) == summary["completion_requests"] == 119
    assert all(row["body_matches_request"] for row in responses)
    assert len({(r["profile"], r["case_id"], r["repeat"], r["phase"]) for r in responses}) == 119
    for name, count in (("repro-template-audit.json", 1), ("template-audit.json", 38)):
        audit = json.loads((root / name).read_text())
        assert len(audit["checks"]) == count
        assert all(row["ids_equal"] and row["usage_equal"] for row in audit["checks"])
    public = root / "public"
    public.mkdir()
    for name in ("plans.json", "cases.json", "schedule.json", "responses.jsonl", "summary.json",
                 "repro-verdict.json", "repro-template-audit.json", "template-audit.json",
                 "parser-controls.json", "model-files.json", "prompt_qualification.py", "template_audit.py"):
        shutil.copy2(root / name, public / name)
    shutil.copy2(Path(__file__), public / "package_prompt.py")
    argv = service["argv"]
    argv = ["<vllm-python>" if i == 0 else "<model-dir>" if value == argv[argv.index("--model")+1] else value
            for i,value in enumerate(argv)]
    save(public / "service.json", dict(argv=argv, vllm_version=service["sidecar"]["package_version"],
        cuda_visible_devices="0", raw_identity_sha256=sha(root / "service-identity.json"),
        identity_checked_before_after=True, model_files_checked_before_after=True,
        chat_template_override=False, local_endpoint_only=True))
    save(root / "cleanup.json", dict(owned_endpoint_stopped=True, owned_port_not_listening=True,
        database_started=False, scope="this experiment only; historical resources not modified"))
    shutil.copy2(root / "cleanup.json", public / "cleanup.json")
    tests = {}
    for suite in ("postgres", "gateway", "calibration"):
        path = root / f"python-{suite}.log"
        if path.exists():
            content = path.read_text()
            assert content.rstrip().endswith("OK")
            tests[suite] = int(re.search(r"Ran (\d+) tests", content).group(1))
            content = redact_text(content).replace(str(args.repo), "<repository>")
            (public / path.name).write_text("\n".join(line.rstrip() for line in content.splitlines())+"\n")
    save(public / "qualification.json", dict(source_commit=summary["source_commit"],
        python_tests=tests, preflight_status=json.loads((root/"preflight.json").read_text())["status"],
        postgresql_test_rerun=False, production_c_parser_controls=7, completion_requests=119,
        distinct_http_message_audits=38, tokenize_requests=39,
        production_changes=False, full_calibration_resumed=False,
        calibration_held_out_model_calls=0, independent_engineering_cases=18,
        expected_repeats_per_case=3, unlabelled_replay_cases=1))
    print(json.dumps(dict(public_files=seal(public), private_files=seal(root))))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--repo", type=Path, required=True)
    run(parser.parse_args())
