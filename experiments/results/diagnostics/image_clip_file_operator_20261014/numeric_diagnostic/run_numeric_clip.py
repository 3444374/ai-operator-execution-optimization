#!/usr/bin/env python3
"""Private sequential CLI coordinator: native lease, logs and process exit evidence."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone


def utc():
    return datetime.now(timezone.utc).isoformat()


def write(path, value):
    temporary = path.with_suffix(path.suffix + ".pending")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def load_file(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def processes():
    result = {}
    for directory in Path("/proc").iterdir():
        if not directory.name.isdecimal():
            continue
        cwd = ""
        try:
            cwd = os.readlink(directory / "cwd")
        except OSError:
            pass
        try:
            fields = (directory / "stat").read_text().rsplit(")", 1)[1].split()
            item = {"pid": int(directory.name), "start_ticks": fields[19],
                    "ppid": int(fields[1]), "session": int(fields[3]),
                    "comm": (directory / "comm").read_text().strip()}
            item["cwd"] = cwd
            result[item["pid"]] = item
        except (OSError, IndexError, ValueError) as error:
            # Inaccessibility is not process exit. Only a disappeared PID can
            # leave a known birth identity absent from this snapshot.
            if isinstance(error, FileNotFoundError) and not directory.exists():
                continue
            result[int(directory.name)] = {"pid": int(directory.name), "cwd": cwd,
                "inspection_error": type(error).__name__}
    return result


def gpu_processes():
    result = subprocess.run(["nvidia-smi", "--query-compute-apps=pid,process_name,used_memory",
                             "--format=csv,noheader,nounits"], capture_output=True, text=True, timeout=10)
    if result.returncode:
        raise RuntimeError("nvidia-smi process check unavailable")
    return result.stdout.strip().splitlines()


def main():
    root = Path(sys.argv[1]).resolve()
    binding = json.loads((root / "execution-binding.json").read_text())
    commands = json.loads((root / "resolved-commands.json").read_text())
    source = Path(binding["source_root"])
    ray_tmp = Path(binding["ray_temp_root"])
    candidate = binding["candidate"]
    assert str(root) == binding["run_root"] and commands["candidate"] == candidate
    assert candidate == "45da0a5451bfcf785d98b9ab5336054bdbaf199f"
    bundle = json.loads((root / "source-package-manifest.json").read_text())
    assert bundle["candidate"] == candidate and bundle["privacy_findings"] == 0
    lease_source = source / "code/src/infrastructure/runner_lease.py"
    assert hashlib.sha256(lease_source.read_bytes()).hexdigest() == "637738aea07ffd1b8377ceb03ae9dd871769927e3f378c9d091d8b8af306d959"
    lease_module = load_file("clip_native_lease", lease_source)
    redactor = load_file("clip_native_redactor", source / "code/src/baselines/common/redact.py")
    for name in ("preparation", "qualification", "scale", "repeat", "diagnostic", "tmp"):
        (root / name).mkdir(exist_ok=True)
    env = dict(os.environ, **commands["env"])
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    tracked = {}
    current_session = [None]
    stop = threading.Event()

    def collect():
        snapshot = processes()
        for pid, item in snapshot.items():
            if pid == os.getpid():
                continue
            cwd = item["cwd"]
            if (item.get("session") == current_session[0] and current_session[0] is not None or
                    cwd == str(source) or cwd.startswith(str(source) + "/") or
                    cwd == str(ray_tmp) or cwd.startswith(str(ray_tmp) + "/")):
                tracked[(pid, item.get("start_ticks", ""))] = item
        return snapshot

    def monitor():
        while not stop.wait(0.25):
            collect()

    def survivors():
        snapshot = collect()
        result = []
        for (pid, ticks), item in list(tracked.items()):
            if pid not in snapshot:
                continue
            current = snapshot[pid]
            if "inspection_error" in current:
                result.append({**item, "inspection_error": current["inspection_error"]})
            elif not ticks or current["start_ticks"] == ticks:
                result.append(current)
        return result

    def confirm_exit():
        # Same bounded exit-observation period as the qualified CPU controller.
        # Expiry leaves completion unconfirmed; it is not a performance gate.
        end = time.monotonic() + 30
        while True:
            try:
                live = survivors()
                gpu = gpu_processes()
            except Exception as error:
                return {"checked_utc": utc(), "confirmed": False, "survivors": [],
                        "gpu_compute": None, "inspection_error": type(error).__name__}
            if any("inspection_error" in item for item in live):
                return {"checked_utc": utc(), "confirmed": False, "survivors": live,
                        "gpu_compute": gpu, "inspection_error": "owned_proc_visibility_unknown"}
            if not live and not gpu:
                return {"checked_utc": utc(), "confirmed": True, "survivors": [], "gpu_compute": []}
            if time.monotonic() >= end:
                return {"checked_utc": utc(), "confirmed": False, "survivors": live, "gpu_compute": gpu}
            time.sleep(0.25)

    status = {"candidate": candidate, "source_identity": "fixed Git blob hashes in source-package-manifest.json",
              "original_cli_git_commit": "unknown: archive source after public ref unavailable",
              "status": "preparation", "controller_pid": os.getpid(), "started_utc": utc(),
              "primary_cli_calls_planned": 3, "primary_model_images_planned": 963,
              "completed": [], "attempted": [], "host_lease_released": False,
              "optional_diagnostics_executed": False}
    try:
        lease = lease_module.acquire_runner_lease(
            root.parent / lease_module.HOST_SCOPE_DIR_NAME,
            config_fingerprint=lease_module.HOST_SCOPE_FINGERPRINT,
            repository_commit=candidate, recover_stale=False)
    except Exception as error:
        status["status"] = "waiting_host_lease"
        status["error"] = redactor.redact_text(f"{type(error).__name__}: {error}")
        status["GPU_model_started"] = False
        write(root / "status.json", status)
        return 2
    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    safe_release = False
    active_child = None

    def execute(item):
        nonlocal active_child
        for key in ("stdout", "stderr"):
            assert not Path(item[key]).exists(), item[key]
        started = utc()
        status["active_call"] = item["id"]
        status["active_call_started_utc"] = started
        write(root / "status.json", status)
        child_env = dict(env, **item.get("env_overrides", {}))
        child_env["SEMLOOM_CLIP_LEASE_OWNER_TOKEN"] = lease.owner.owner_token
        if item["id"] == "environment":
            child_env.pop("SEMLOOM_CLIP_TRACE_DIR", None)
        # The original environment contract requires its configured database URL;
        # it only checks presence for these groups and makes no database call.
        if item["id"] != "environment":
            child_env.update(DATABASE_URL="", PG_DSN="")
        active_child = subprocess.Popen(item["argv"], cwd=source,
            env=child_env, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, start_new_session=True)
        current_session[0] = active_child.pid
        snapshot = collect()
        identity = snapshot.get(active_child.pid, {"pid": active_child.pid,
                    "start_ticks": "", "inspection_error": "initial_proc_identity_unavailable"})
        tracked[(active_child.pid, identity.get("start_ticks", ""))] = identity
        status["active_child_identity"] = identity
        write(root / "status.json", status)
        stdout, stderr = active_child.communicate()
        code = active_child.returncode
        child_pid = active_child.pid
        active_child = None
        for key, text in (("stdout", stdout), ("stderr", stderr)):
            Path(item[key]).write_text(redactor.redact_text(text))
        exit_proof = confirm_exit()
        current_session[0] = None
        receipt = {"candidate": candidate, "argv": item["argv"], "id": item["id"],
                   "started_utc": started, "finished_utc": utc(), "child_pid": child_pid,
                   "exitcode": code, "physical_exit": exit_proof}
        receipt_path = Path(item.get("receipt", str(Path(item["stdout"]).with_suffix(".exit.json"))))
        write(receipt_path, receipt)
        status["attempted"].append(receipt)
        status.pop("active_call", None)
        status.pop("active_call_started_utc", None)
        status.pop("active_child_identity", None)
        write(root / "status.json", status)
        if code or not exit_proof["confirmed"]:
            raise RuntimeError(f"call {item['id']} exit={code}, physical_exit={exit_proof['confirmed']}")

    try:
        for name, expected in binding["checked_source_files"].items():
            assert hashlib.sha256((source / name).read_bytes()).hexdigest() == expected, name
        for item in bundle["files"]:
            assert hashlib.sha256((source / item["path"]).read_bytes()).hexdigest() == item["sha256"]
        for item in binding["diagnostic_helper_files"]:
            assert hashlib.sha256((root / item["path"]).read_bytes()).hexdigest() == item["sha256"]
        ray_tmp.mkdir(exist_ok=False)
        assert not gpu_processes(), "foreign GPU workload present; no model started"
        write(root / "status.json", status)
        for item in commands["preparation"]:
            execute(item)
        status["shared_preparation_completed_utc"] = utc()
        status["status"] = "numeric_diagnostic"
        for item in commands["runs"]:
            assert not gpu_processes(), "GPU boundary not clean before next CLI"
            assert not survivors(), "owned process boundary not clean before next CLI"
            status["status"] = item["group"]
            if "first_model_cli_started_utc" not in status:
                status["first_model_cli_started_utc"] = utc()
            execute(item)
            args = item["argv"]
            manifest = json.loads(Path(args[args.index("--out-manifest") + 1]).read_text())
            row = manifest["row"]
            assert row["rows"] == item["unique_images"] and row["output_rows"] == row["rows"]
            assert row["exactly_once"] is True and row["declared_total_cpus"] == 7
            assert row["embedding_output_contract_effective"] == "l2_normalized"
            assert row["detailed_stage_timing"] is False
            status["completed"].append({"id": item["id"], "group": item["group"],
                "arm": item["arm"], "rows": row["rows"], "operator_e2e_s": row["operator_e2e_s"],
                "manifest": args[args.index("--out-manifest") + 1]})
            write(root / "status.json", status)
        status["status"] = "complete"
    except BaseException as error:
        status["status"] = "failed_or_unconfirmed"
        status["error"] = redactor.redact_text(f"{type(error).__name__}: {error}")
        if active_child is not None and active_child.poll() is None:
            status["live_child_pid"] = active_child.pid
    finally:
        stop.set()
        thread.join()
        proof = confirm_exit()
        status["final_physical_exit"] = proof
        safe_release = proof["confirmed"] and (active_child is None or active_child.poll() is not None)
        write(root / "owned-processes.json", {"observed": list(tracked.values()), "exit_proof": proof})
        if safe_release:
            lease.release()
            status["host_lease_released"] = True
        status["finished_utc"] = utc()
        write(root / "status.json", status)
    return 0 if status["status"] == "complete" and safe_release else 1


if __name__ == "__main__":
    raise SystemExit(main())
