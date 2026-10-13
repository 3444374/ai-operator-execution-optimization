"""Own one finite two-replica model window, using existing replay and identity tools.

The coordinator supplies the timestamp of the first remote preparation action.
This script never installs packages, downloads assets, retries cells or resets a ledger.
"""

import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.request import urlopen

import psutil

CODE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CODE_ROOT))
from src.experiments.attempt_ledger import AttemptBudget
from src.experiments.cell_budget import CellBudgetLedger
from src.experiments.process_sampling import ProcessSampler
from src.experiments.replica_model_replay import JsonEvents
from src.execution_provider.adapters.model_replicas import ModelReplica, ModelReplicaSet


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--started-utc", type=float, required=True)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--source-sha256", required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--model-path", type=Path, required=True)
    parser.add_argument("--vllm-python", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.output
    root.mkdir(mode=0o700, exist_ok=False)
    repo = CODE_ROOT.parent
    if subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip() != args.expected_sha:
        raise ValueError("window source differs from the frozen SHA")
    deadline = args.started_utc + 1080
    budget_id = root.parent.name
    budget = CellBudgetLedger.create(root / "budget.sqlite", AttemptBudget(budget_id, 820), deadline_utc=deadline)
    owned, handles, leaders = {}, [], []
    report = dict(started_utc=args.started_utc, deadline_utc=deadline, source_sha=args.expected_sha,
                  budget_id=budget_id, loading_s=0.0, first_ready_utc=None, completed_topologies=[],
                  mfu="not_measured", memory_utilization_is_bandwidth_gbps=False)
    sampler = None
    health = JsonEvents(root / "health-get.jsonl")

    def save(name, value):
        (root / name).write_text(json.dumps(value, indent=2) + "\n")

    def remember():
        for process in leaders:
            try:
                parent = psutil.Process(process.pid)
                for p in [parent, *parent.children(recursive=True)]:
                    owned[(p.pid, p.create_time())] = p
            except psutil.NoSuchProcess:
                pass

    def live():
        result = []
        for (_, created), p in owned.items():
            try:
                if p.create_time() == created and p.is_running() and p.status() != psutil.STATUS_ZOMBIE:
                    result.append(p)
            except psutil.NoSuchProcess:
                pass
        return result

    def spawn(label, command, *, env=None):
        log = (root / (label + ".log")).open("x")
        handles.append(log)
        child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                 env=env, cwd=repo, start_new_session=True)
        leaders.append(child)
        remember()
        return child

    def run(label, command, *, cutoff=None):
        child = spawn(label, command)
        while child.poll() is None:
            remember()
            if time.time() >= min(deadline - 60, cutoff or deadline - 60):
                raise TimeoutError(label + " reached the window deadline")
            time.sleep(0.1)
        if child.returncode:
            raise RuntimeError(label + " failed; private log retained")

    failure = None
    try:
        preflight = [sys.executable, "code/scripts/environment/manage_environment.py", "--env-file",
            str(args.env_file), "check", "--groups", "core,text-qwen7b", "--json-out", str(root / "preflight.json")]
        run("preflight", preflight, cutoff=args.started_utc + 120)
        devices = subprocess.check_output(["nvidia-smi", "--query-gpu=uuid", "--format=csv,noheader"], text=True).splitlines()
        existing = subprocess.check_output(["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"], text=True).strip()
        if len(devices) != 2 or existing:
            raise ValueError("window requires two available GPUs and no existing compute processes")
        for port in (18180, 18181):
            import socket
            with socket.socket() as s:
                s.bind(("127.0.0.1", port))
        serving = ["--model", str(args.model_path), "--served-model-name", "Qwen2.5-7B-Instruct",
            "--host", "127.0.0.1", "--tensor-parallel-size", "1", "--dtype", "bfloat16",
            "--max-model-len", "2048", "--max-num-seqs", "16", "--max-num-batched-tokens", "4096",
            "--gpu-memory-utilization", "0.80", "--enforce-eager", "--no-enable-prefix-caching"]
        declaration = hashlib.sha256(json.dumps(serving).encode()).hexdigest()
        replicas = tuple(ModelReplica(label, f"http://127.0.0.1:{18180+i}/v1/chat/completions",
            "window-host", (devices[i],), declaration, 4, 4) for i, label in enumerate(("a", "b")))
        directory = ModelReplicaSet("Qwen2.5-7B-Instruct", replicas, 8, 8)
        save("replicas.json", asdict(directory))
        save("serving.json", dict(argv=serving, model_path=args.model_path.as_posix(),
            declaration_digest_is_weight_audit=False))
        spawn("gpu-samples", ["nvidia-smi", "--query-gpu=timestamp,index,uuid,utilization.gpu,utilization.memory,power.draw,memory.used,memory.total,clocks.sm,clocks.mem",
                               "--format=csv,noheader,nounits", "-l", "1"])
        sampler = ProcessSampler(root / "process-samples.jsonl", {"owner": os.getpid()}, interval=1,
                                 max_samples=1200, include_children=True)
        sampler.__enter__()
        for i, topology in enumerate(("single", "dual")):
            sampler.phase = topology + "-loading"
            begin = time.time()
            env = os.environ.copy()
            env.update(CUDA_VISIBLE_DEVICES=str(i), OMP_NUM_THREADS="8",
                HF_HOME=str(root / "cache/hf"), XDG_CACHE_HOME=str(root / "cache/xdg"),
                VLLM_CACHE_ROOT=str(root / "cache/vllm"), TORCHINDUCTOR_CACHE_DIR=str(root / "cache/torch"))
            model = spawn("api-" + replicas[i].endpoint_id, ["taskset", "-c", "20-27" if i == 0 else "28-35",
                str(args.vllm_python), "code/scripts/services/launch_vllm_with_identity.py", "--identity-output",
                str(root / ("identity-" + replicas[i].endpoint_id + ".json")), "--port", str(18180 + i),
                "--", *serving, "--port", str(18180 + i)], env=env)
            while True:
                remember()
                if model.poll() is not None:
                    raise RuntimeError("model startup failed; no restart")
                if report["loading_s"] + time.time() - begin >= 300 or time.time() >= deadline - 180:
                    raise TimeoutError("cumulative model loading limit reached")
                if report["first_ready_utc"] is not None and time.time() >= report["first_ready_utc"] + 600:
                    raise TimeoutError("first-ready activity limit reached while loading B")
                health_started = time.monotonic()
                try:
                    with urlopen(f"http://127.0.0.1:{18180+i}/health", timeout=1) as response:
                        health(dict(event="health_get", endpoint_id=replicas[i].endpoint_id,
                            status=response.status, elapsed_s=time.monotonic() - health_started))
                        if response.status == 200:
                            break
                except (OSError, TimeoutError) as error:
                    health(dict(event="health_get", endpoint_id=replicas[i].endpoint_id,
                        error_type=type(error).__name__, elapsed_s=time.monotonic() - health_started))
                time.sleep(0.5)
            report["loading_s"] += time.time() - begin
            if report["first_ready_utc"] is None:
                report["first_ready_utc"] = time.time()
            cutoff = min(args.started_utc + 900, report["first_ready_utc"] + 600)
            report["stop_new_post_utc"] = cutoff
            save("progress.json", report)
            chosen = replicas[:1] if i == 0 else replicas
            qualified = ModelReplicaSet(directory.model_id, chosen, 4 * len(chosen), 4 * len(chosen))
            save(topology + "-replicas.json", asdict(qualified))
            verify = [sys.executable, "code/scripts/services/verify_model_replicas.py", "--replica-config",
                str(root / (topology + "-replicas.json")), "--output", str(root / (topology + "-identity.json"))]
            for r in chosen:
                verify += ["--runtime-identity", r.endpoint_id + "=" + str(root / ("identity-" + r.endpoint_id + ".json"))]
            run(topology + "-verify", verify, cutoff=cutoff)
            sampler.phase = topology + "-replay"
            run(topology + "-replay", [sys.executable, "code/scripts/benchmarks/replica_model_replay.py",
                "--source", str(args.source), "--source-sha256", args.source_sha256,
                "--replicas", str(root / "replicas.json"), "--budget", str(root / "budget.sqlite"),
                "--budget-id", budget_id, "--topology", topology, "--stop-new-post-utc", str(cutoff),
                "--output", str(root / topology)], cutoff=cutoff + 35)
            report["completed_topologies"].append(topology)
            save("progress.json", report)
    except BaseException as error:
        failure = error
        report["error_type"] = type(error).__name__
    finally:
        remember()
        if sampler is not None:
            sampler.phase = "cleanup"
        # Only recorded PID/birth identities are terminated; service exit is not a returned response.
        for p in live():
            try:
                p.terminate()
            except psutil.NoSuchProcess:
                pass
        _, remaining = psutil.wait_procs(live(), timeout=min(15, max(0, deadline - time.time())))
        for p in remaining:
            try:
                if p.create_time() == next(created for (pid, created) in owned if pid == p.pid):
                    p.kill()
            except psutil.NoSuchProcess:
                pass
        psutil.wait_procs(remaining, timeout=10)
        for child in leaders:
            try:
                child.wait(timeout=1)
            except subprocess.TimeoutExpired:
                pass
        if sampler is not None:
            sampler.__exit__()
            report["process_sampling"] = sampler.summary()
        report.update(ended_utc=time.time(), remaining_owned_pids=[p.pid for p in live()], budget=budget.snapshot(),
            status="failed" if failure else "passed", model_response_cleanup_is_process_exit=False)
        report["gpu_compute_after"] = subprocess.check_output(["nvidia-smi", "--query-compute-apps=pid,process_name,used_gpu_memory",
                                                              "--format=csv,noheader"], text=True).splitlines()
        save("receipt.json", report)
        health.close()
        for handle in handles:
            handle.close()
    if failure:
        raise failure
    if report["remaining_owned_pids"] or report["gpu_compute_after"]:
        raise RuntimeError("window cleanup not independently clear")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
