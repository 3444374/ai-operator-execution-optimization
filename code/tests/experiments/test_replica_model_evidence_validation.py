"""Offline numeric fixtures exercise qualification in normal and optimized Python."""

from collections import Counter
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[2] / "scripts/benchmarks/summarize_replica_model_replay.py"
ARMS = ("round_robin", "fixed_rate", "adaptive_route", "adaptive_supply")


def sha(value):
    return hashlib.sha256(value.encode()).hexdigest()


def write_json(root, rel, value):
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(value).encode()
    path.write_bytes(gzip.compress(raw) if rel.endswith(".gz") else raw)


def read_json(root, rel):
    raw = (root / rel).read_bytes()
    return json.loads(gzip.decompress(raw) if rel.endswith(".gz") else raw)


def update_manifest(root):
    paths = sorted(p for p in root.rglob("*") if p.is_file() and p.name != "metadata-manifest.json")
    write_json(root, "metadata-manifest.json", [dict(path=p.relative_to(root).as_posix(),
        size=p.stat().st_size, sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in paths])


def usage(held, active):
    return dict(held_tasks=held, input_bytes=held * 100, result_bytes=held * 1048576,
                active_requests=active, active_work=active)


def identity(endpoints):
    records = []
    for endpoint in endpoints:
        offset = ord(endpoint) - ord("a")
        records.append(dict(endpoint_id=endpoint, location_id="fixture-host", api_pid=100 + offset,
            process_start_time_ticks=str(1000 + offset), gpu_process_ids=[200 + offset],
            device_ids=["GPU-fixture-" + endpoint], service_version="fixture-version",
            serving_arguments_sha256=sha("same serving arguments"),
            listener_bindings=[dict(pid=100 + offset, process_start_time_ticks=str(1000 + offset),
                family="ipv4", address="127.0.0.1", port=18180 + offset, inode=str(2000 + offset))]))
    return dict(schema_version=2, replica_count=len(records), gpu_count=len(records),
                replica_set_sha256=sha("directory " + "".join(endpoints)), independent_devices=True, replicas=records)


def make_fixture(root):
    """The recorded 820-call shape, using no HTTP, server, model or original payload."""
    budget_id, deadline = "offline-evidence-fixture", 2000.0
    units, all_posts, endpoint_posts = [], [], Counter()
    first = 1
    for topology, endpoints, selected_k, calibrations in (("single", ("a",), 4, (1, 2, 4)),
                                                         ("dual", ("a", "b"), 8, (2, 4, 8))):
        specifications = [("protocol", "round_robin", 1, range(60, 62) if topology == "single" else range(62, 64))]
        specifications += [(f"cal-k{k}", "round_robin", k, range(24, 48)) for k in calibrations]
        specifications += [("warm-" + arm, arm, selected_k, range(48, 60)) for arm in ARMS]
        specifications += [(f"measure-{repeat}-{arm}", arm, selected_k, range(24))
                           for repeat in range(3) for arm in ARMS]
        for label, arm, k, sequences in specifications:
            cell_endpoints = (("a",) if topology == "single" else ("b",)) if label == "protocol" else endpoints
            count, unit = len(sequences), topology + "-" + label
            reservation = dict(unit_id=unit, first_attempt=first, requests=count, claimed=1)
            units.append(reservation)
            events, outcomes = [], []
            now = float(first)
            def event(name, **fields):
                nonlocal now
                now += 0.001
                events.append(dict(event=name, monotonic_s=now, utc_s=now + 1000, **fields))
            def snapshot(held, active=0, endpoint=None):
                event("core_supply_snapshot", queued=held-active, hard_dispatchable=held > active,
                    usage=usage(held, active), supply_limit=k,
                    endpoint_supply=None if arm == "round_robin" else {e: 4 for e in cell_endpoints},
                    endpoint_inflight={e: int(e == endpoint) if active else 0 for e in cell_endpoints})
            snapshot(count)
            for i, source_sequence in enumerate(sequences):
                endpoint = cell_endpoints[i % len(cell_endpoints)]
                endpoint_posts[endpoint] += 1
                key = dict(session_id=0, sequence=i)
                attempt = first + i
                payload_hash = sha("original request " + str(source_sequence))
                all_posts.append((unit, i + 1, payload_hash))
                event("route_decision", request_id=f"0:{i}", decision=dict(endpoint_id=endpoint, pool_id="model", reason="fixture"))
                event("submitted", key=key, usage=usage(count-i, 1))
                event("replica_transport_started", key=key, endpoint_id=endpoint)
                event("feedback_started", endpoint_id=endpoint)
                event("http_started", key=key, endpoint_id=endpoint)
                event("durable_post_reserved", attempt=attempt, payload_sha256=payload_hash)
                snapshot(count-i, active=1, endpoint=endpoint)
                event("http_finished", key=key, endpoint_id=endpoint)
                event("feedback_finished", endpoint_id=endpoint, work=1, successful=True)
                event("terminal", key=key, usage=usage(count-i, 0))
                outcomes.append(dict(source_sequence=source_sequence, source_row_id=f"fixture-row-{source_sequence}",
                    source_call_id=f"fixture-call-{source_sequence}", result_at_s=now,
                    body_sha256=sha(f"variable response ID {topology} {label} {source_sequence}"),
                    content_sha256=sha("content " + str(source_sequence)), finish_reason="stop",
                    usage=dict(prompt_tokens=100 + source_sequence, completion_tokens=4,
                               total_tokens=104 + source_sequence, prompt_tokens_details=None)))
                event("released", key=key, usage=usage(count-i-1, 0))
                snapshot(count-i-1)
            attempts = list(range(first, first + count))
            result = dict(topology=topology, label=label, arm=arm, requests=k, count=count,
                jct_s=count/k, assembly_s=0.001, process_cpu_s=0.1, policy_s=0.01, policy_calls=count*2,
                directory_sha256=sha("cell directory"), outcomes=outcomes, attempts=attempts, events=events)
            write_json(root, f"evidence-affinity/{topology}/{label}.json.gz", result)
            write_json(root, f"evidence-affinity/{topology}/{label}/cleanup.json",
                dict(clean=True, error=None, usage=usage(0, 0), outcomes=outcomes, reserved_attempts=attempts))
            first += count
        write_json(root, f"evidence-affinity/{topology}/group.json", dict(topology=topology,
            selected_k=selected_k, fixed_rates={e: 10.0 for e in endpoints}, source_sha256=sha("source subset"), completed_cells=20))
        path = root / f"evidence-affinity/{topology}/pressure.jsonl"
        path.write_text("\n".join(json.dumps(dict(event="service_metrics", endpoint_id=endpoint,
            elapsed_s=0.001, metrics={"vllm:num_requests_running": 0, "vllm:num_requests_waiting": 0,
            "vllm:kv_cache_usage_perc": 0}, monotonic_s=1, utc_s=1001)) for endpoint in endpoints) + "\n")
        write_json(root, f"evidence-affinity/{topology}-identity.json", identity(endpoints))
    budget = root / "evidence/budget.sqlite"
    budget.parent.mkdir(parents=True)
    with sqlite3.connect(budget) as db:
        db.executescript("""
            CREATE TABLE budget (schema TEXT, budget_id TEXT, limit_count INTEGER, allocated INTEGER, deadline REAL);
            CREATE TABLE units (unit_id TEXT, first_attempt INTEGER, requests INTEGER, claimed INTEGER);
            CREATE TABLE shared_units (unit_id TEXT);
            CREATE TABLE closed_shared_units (unit_id TEXT);
            CREATE TABLE shared_requests (unit_id TEXT, sequence INTEGER, request_sha256 TEXT);
        """)
        db.execute("INSERT INTO budget VALUES(?,?,?,?,?)", ("semloom.cell_budget.v1", budget_id, 820, 820, deadline))
        db.executemany("INSERT INTO units VALUES(?,?,?,?)", [(u["unit_id"],u["first_attempt"],u["requests"],1) for u in units])
        db.executemany("INSERT INTO shared_units VALUES(?)", [(u["unit_id"],) for u in units])
        db.executemany("INSERT INTO closed_shared_units VALUES(?)", [(u["unit_id"],) for u in units])
        db.executemany("INSERT INTO shared_requests VALUES(?,?,?)", all_posts)
    write_json(root, "evidence-affinity/receipt.json", dict(status="passed", remaining_owned_pids=[], gpu_compute_after=[],
        completed_topologies=["single", "dual"], source_sha="a"*40, budget_id=budget_id,
        started_utc=1000, ended_utc=1900, deadline_utc=deadline, loading_s=1,
        budget=dict(schema="semloom.cell_budget.v1", budget_id=budget_id, limit=820,
                    allocated_requests=820, deadline_utc=deadline, units=units)))
    write_json(root, "count-receipt.json", dict(server_success_posts=dict(endpoint_posts),
        ledger=dict(shared_requests=820, closed_units=40, open_shared_units=40)))
    write_json(root, "source-receipt.json", dict(subset_sha256=sha("source subset")))
    (root / "evidence-affinity/gpu-samples.log").write_text("\n".join(
        f"2030/01/01 08:00:00.000, {i}, GPU-fixture-{e}, 10, 10, 30, 1000, 24000, 2000, 10000"
        for i, e in enumerate(("a", "b"))) + "\n")
    update_manifest(root)


class ReplicaModelEvidenceValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="replica-evidence-validation-")
        cls.base = Path(cls.temp.name) / "base"
        cls.base.mkdir()
        make_fixture(cls.base)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def cli(self, metadata, output, optimized):
        env = dict(os.environ)
        env.pop("PYTHONOPTIMIZE", None)
        return subprocess.run([sys.executable, *(["-O"] if optimized else []), str(SCRIPT),
            "--metadata", str(metadata), "--output", str(output)], env=env, capture_output=True, text=True, timeout=20)

    def rejects(self, mutations):
        for name, mutate, rehash in mutations:
            with self.subTest(corruption=name), tempfile.TemporaryDirectory(prefix="replica-corrupt-evidence-") as tmp:
                root = Path(tmp) / "metadata"
                shutil.copytree(self.base, root)
                mutate(root)
                if rehash:
                    update_manifest(root)
                for optimized in (False, True):
                    with self.subTest(optimized=optimized):
                        out = Path(tmp) / ("optimized-output" if optimized else "normal-output")
                        result = self.cli(root, out, optimized)
                        self.assertNotEqual(result.returncode, 0, result.stdout)
                        self.assertIn("invalid model evidence", result.stderr)
                        self.assertFalse(out.exists())

    def test_valid_metadata_qualifies_in_both_modes_without_claiming_http_bytes_equal(self):
        for optimized in (False, True):
            with self.subTest(optimized=optimized), tempfile.TemporaryDirectory() as tmp:
                out = Path(tmp) / "output"
                result = self.cli(self.base, out, optimized)
                self.assertEqual(result.returncode, 0, result.stderr)
                summary = read_json(out, "summary.json")
                self.assertEqual(summary["model_posts"], 820)
                self.assertTrue(summary["canonical_outputs_equal"])
                self.assertEqual(summary["canonical_comparison_fields"], ["content_sha256", "finish_reason", "usage"])
                self.assertEqual(summary["http_response_bytes_equal"], "not_qualified")
                public = read_json(out, "raw.json.gz")
                self.assertNotIn("source_row_id", public["cells"][0]["outcomes"][0])
                self.assertNotIn("source_call_id", public["cells"][0]["outcomes"][0])

    def test_manifest_hash_size_coverage_and_paths_are_not_optimization_dependent(self):
        def manifest_change(root, field, value):
            manifest = read_json(root, "metadata-manifest.json")
            manifest[0][field] = value
            write_json(root, "metadata-manifest.json", manifest)
        def omitted(root):
            manifest = read_json(root, "metadata-manifest.json")
            write_json(root, "metadata-manifest.json", [m for m in manifest if m["path"] != "evidence/budget.sqlite"])
        self.rejects([
            ("wrong hash", lambda r: manifest_change(r, "sha256", "0"*64), False),
            ("wrong size", lambda r: manifest_change(r, "size", 0), False),
            ("unlisted budget", omitted, False),
            ("path escape", lambda r: manifest_change(r, "path", "../outside.json"), False),
        ])

    def test_budget_counts_units_hashes_and_server_splits_fail_in_both_modes(self):
        def sql(root, statement):
            with sqlite3.connect(root / "evidence/budget.sqlite") as db:
                db.execute(statement)
        def count_receipt(root, conserve):
            value = read_json(root, "count-receipt.json")
            value["server_success_posts"]["a"] -= 1
            if conserve:
                value["server_success_posts"]["b"] += 1
            write_json(root, "count-receipt.json", value)
        self.rejects([
            ("missing POST", lambda r: sql(r, "DELETE FROM shared_requests WHERE unit_id='single-protocol' AND sequence=1"), True),
            ("unclosed unit", lambda r: sql(r, "DELETE FROM closed_shared_units WHERE unit_id='single-protocol'"), True),
            ("wrong reservation", lambda r: sql(r, "UPDATE units SET requests=3 WHERE unit_id='single-protocol'"), True),
            ("changed POST hash", lambda r: sql(r, "UPDATE shared_requests SET request_sha256='"+'0'*64+"' WHERE unit_id='single-protocol' AND sequence=1"), True),
            ("server count", lambda r: count_receipt(r, False), True),
            ("per-endpoint server split", lambda r: count_receipt(r, True), True),
        ])

    def mutate_cell(self, root, mutate, topology="dual", label="measure-2-round_robin", cleanup=True):
        rel = f"evidence-affinity/{topology}/{label}.json.gz"
        value = read_json(root, rel)
        mutate(value)
        write_json(root, rel, value)
        if cleanup:
            rel = f"evidence-affinity/{topology}/{label}/cleanup.json"
            evidence = read_json(root, rel)
            evidence["outcomes"] = value["outcomes"]
            evidence["reserved_attempts"] = value["attempts"]
            write_json(root, rel, evidence)

    def test_source_counts_and_each_normalized_output_component_fail_in_both_modes(self):
        def change(value, field, replacement):
            value["outcomes"][0][field] = replacement
        def changed_usage(value):
            value["outcomes"][0]["usage"]["completion_tokens"] += 1
            value["outcomes"][0]["usage"]["total_tokens"] += 1
        self.rejects([
            ("cell count", lambda r: self.mutate_cell(r, lambda d: d.update(count=23)), True),
            ("duplicate occurrence", lambda r: self.mutate_cell(r, lambda d: change(d, "source_sequence", d["outcomes"][1]["source_sequence"])), True),
            ("attempt association", lambda r: self.mutate_cell(r, lambda d: d["attempts"].__setitem__(0, 1)), True),
            ("normalized content", lambda r: self.mutate_cell(r, lambda d: change(d, "content_sha256", "0"*64)), True),
            ("finish reason", lambda r: self.mutate_cell(r, lambda d: change(d, "finish_reason", "length")), True),
            ("usage", lambda r: self.mutate_cell(r, changed_usage), True),
            ("source identity", lambda r: self.mutate_cell(r, lambda d: change(d, "source_row_id", "different-fixture-row")), True),
            ("malformed content hash", lambda r: self.mutate_cell(r, lambda d: change(d, "content_sha256", "bad-hash")), True),
            ("non-finite timing", lambda r: self.mutate_cell(r, lambda d: d.update(jct_s=float("nan"))), True),
        ])

    def test_all_cell_phases_check_global_work_and_endpoint_capacity(self):
        def global_capacity(value):
            event = next(e for e in value["events"] if e["event"] == "submitted")
            event["usage"]["active_requests"] = event["usage"]["active_work"] = value["requests"] + 1
        def work_capacity(value):
            event = next(e for e in value["events"] if e["event"] == "submitted")
            event["usage"]["active_work"] = value["requests"] + 1
        def endpoint_capacity(value):
            event = next(e for e in value["events"] if e["event"] == "core_supply_snapshot")
            event["endpoint_inflight"][next(iter(event["endpoint_inflight"]))] = 5
        cases = []
        for label in ("protocol", "cal-k8", "warm-round_robin", "measure-2-round_robin"):
            cases.append((label + " global C", lambda r, label=label: self.mutate_cell(r, global_capacity, label=label), True))
            cases.append((label + " global W", lambda r, label=label: self.mutate_cell(r, work_capacity, label=label), True))
            cases.append((label + " endpoint C/W", lambda r, label=label: self.mutate_cell(r, endpoint_capacity, label=label), True))
        self.rejects(cases)

    def test_cleanup_and_complete_request_lifecycle_fail_in_both_modes(self):
        def cleanup_retained(root):
            rel = "evidence-affinity/dual/measure-2-round_robin/cleanup.json"
            value = read_json(root, rel)
            value["usage"]["held_tasks"] = 1
            write_json(root, rel, value)
        def missing_finish(value):
            index = next(i for i,e in enumerate(value["events"]) if e["event"] == "http_finished")
            del value["events"][index]
        self.rejects([
            ("retained cleanup", cleanup_retained, True),
            ("missing full response", lambda r: self.mutate_cell(r, missing_finish), True),
            ("unsupported policy timing", lambda r: self.mutate_cell(r, lambda d: d.update(policy_calls=0)), True),
        ])

    def test_structural_types_and_observation_support_fail_in_both_modes(self):
        def pressure(root, field, value):
            rel = "evidence-affinity/dual/pressure.jsonl"
            records = [json.loads(line) for line in (root/rel).read_text().splitlines()]
            records[0][field] = value
            (root/rel).write_text("\n".join(json.dumps(row) for row in records) + "\n")
        self.rejects([
            ("fractional request capacity", lambda r: self.mutate_cell(r, lambda d: d.update(requests=float(d["requests"]))), True),
            ("non-object outcome", lambda r: self.mutate_cell(r, lambda d: d["outcomes"].__setitem__(0, None)), True),
            ("boolean token usage", lambda r: self.mutate_cell(r, lambda d: d["outcomes"][0]["usage"].update(prompt_tokens=True)), True),
            ("unknown observation target", lambda r: pressure(r, "endpoint_id", "unqualified"), True),
            ("missing pressure support", lambda r: pressure(r, "metrics", {}), True),
        ])

    def test_independent_devices_need_gpu_process_and_listener_evidence(self):
        def modify(root, mutate):
            rel = "evidence-affinity/dual-identity.json"
            value = read_json(root, rel)
            mutate(value)
            write_json(root, rel, value)
        self.rejects([
            ("boolean without records", lambda r: modify(r, lambda d: d.update(replicas=[])), True),
            ("shared GPU", lambda r: modify(r, lambda d: d["replicas"][1].update(device_ids=d["replicas"][0]["device_ids"])), True),
            ("shared GPU process", lambda r: modify(r, lambda d: d["replicas"][1].update(gpu_process_ids=d["replicas"][0]["gpu_process_ids"])), True),
            ("missing GPU process", lambda r: modify(r, lambda d: d["replicas"][1].update(gpu_process_ids=[])), True),
            ("missing listener", lambda r: modify(r, lambda d: d["replicas"][1].update(listener_bindings=[])), True),
            ("missing process birth", lambda r: modify(r, lambda d: d["replicas"][1].update(process_start_time_ticks="")), True),
            ("different serving arguments", lambda r: modify(r, lambda d: d["replicas"][1].update(serving_arguments_sha256="0"*64)), True),
            ("unknown GPU sample", lambda r: (r/"evidence-affinity/gpu-samples.log").write_text(
                (r/"evidence-affinity/gpu-samples.log").read_text().replace("GPU-fixture-b", "GPU-fixture-other")), True),
        ])

    def test_backend_start_before_owner_observation_is_valid(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "metadata"
            shutil.copytree(self.base, root)
            def reorder(value):
                events = value["events"]
                index = next(i for i,e in enumerate(events) if e["event"] == "submitted")
                submitted = events.pop(index)
                index = next(i for i,e in enumerate(events) if e["event"] == "http_started")
                events.insert(index+1, submitted)
            self.mutate_cell(root, reorder, topology="single", label="cal-k1")
            update_manifest(root)
            for optimized in (False, True):
                out = Path(tmp) / ("optimized-output" if optimized else "normal-output")
                result = self.cli(root, out, optimized)
                self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
