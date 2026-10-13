"""Audit retained numeric model evidence and emit a payload-free public record."""

import argparse
from collections import Counter
import csv
import datetime
import gzip
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import statistics


ARMS = ("round_robin", "fixed_rate", "adaptive_route", "adaptive_supply")
USAGE_FIELDS = ("held_tasks", "input_bytes", "result_bytes", "active_requests", "active_work")


class EvidenceValidationError(ValueError):
    """Retained evidence failed a qualification requirement."""


def require(condition, message):
    if not condition:
        raise EvidenceValidationError(message)


def integer(value, label, minimum=0, maximum=None):
    require(type(value) is int and value >= minimum and (maximum is None or value <= maximum),
            label + " must be an integer within its qualified bounds")
    return value


def number(value, label, minimum=0):
    require(type(value) in (int, float) and math.isfinite(value) and value >= minimum,
            label + " must be a finite number within its qualified bounds")
    return value


def digest(value, label, length=64):
    require(type(value) is str and re.fullmatch("[0-9a-f]{" + str(length) + "}", value),
            label + " must be a hexadecimal digest")
    return value


def object_value(value, label):
    require(type(value) is dict, label + " must be an object")
    return value


def list_value(value, label):
    require(type(value) is list, label + " must be an array")
    return value


def reject_constant(value):
    raise EvidenceValidationError("non-finite JSON constant is not qualified")


class RetainedMetadata:
    """Read only bytes covered by the manifest; never silently add unlisted evidence."""
    def __init__(self, root):
        self.root = Path(root).resolve()
        manifest = json.loads((self.root / "metadata-manifest.json").read_bytes(), parse_constant=reject_constant)
        self.files = {}
        require(list_value(manifest, "manifest"), "manifest is empty")
        for item in manifest:
            item = object_value(item, "manifest entry")
            rel = item.get("path")
            require(type(rel) is str and rel and not Path(rel).is_absolute()
                    and all(part not in ("", ".", "..") for part in rel.split("/")),
                    "manifest path must stay within the metadata directory")
            path = self.root / rel
            require(path.resolve().is_relative_to(self.root) and path.is_file() and rel not in self.files,
                    "manifest path is missing, duplicated or outside the metadata directory")
            raw = path.read_bytes()
            require(len(raw) == integer(item.get("size"), "manifest size"), "manifest size mismatch: " + rel)
            require(hashlib.sha256(raw).hexdigest() == digest(item.get("sha256"), "manifest hash"),
                    "manifest hash mismatch: " + rel)
            self.files[rel] = raw

    def read(self, rel):
        require(rel in self.files, "required evidence is absent from the manifest: " + rel)
        return self.files[rel]

    def json(self, rel):
        raw = self.read(rel)
        return json.loads(gzip.decompress(raw) if rel.endswith(".gz") else raw, parse_constant=reject_constant)

    def lines(self, rel):
        return [json.loads(line, parse_constant=reject_constant) for line in self.read(rel).splitlines()]


def verify_devices(identity, endpoints):
    identity = object_value(identity, "replica identity")
    require(identity.get("schema_version") == 2 and identity.get("independent_devices") is True,
            "independent device identity schema is required")
    require(integer(identity.get("replica_count"), "replica count") == len(endpoints)
            and integer(identity.get("gpu_count"), "GPU count") == len(endpoints), "identity counts differ")
    digest(identity.get("replica_set_sha256"), "replica set digest")
    replicas = list_value(identity.get("replicas"), "replica identities")
    require(len(replicas) == len(endpoints), "replica identity evidence is incomplete")
    devices, processes, listeners, records, serving = set(), set(), set(), {}, set()
    for replica in replicas:
        replica = object_value(replica, "replica identity record")
        endpoint = replica.get("endpoint_id")
        require(endpoint in endpoints and endpoint not in records, "replica endpoint identity differs")
        api_pid = integer(replica.get("api_pid"), "model API pid", 1)
        ticks = replica.get("process_start_time_ticks")
        require(type(ticks) is str and ticks.isdecimal() and int(ticks) > 0, "model process birth evidence is missing")
        require(type(replica.get("location_id")) is str and replica["location_id"], "device location is missing")
        device_ids = list_value(replica.get("device_ids"), "device IDs")
        gpu_pids = list_value(replica.get("gpu_process_ids"), "GPU process IDs")
        require(len(device_ids) == 1 and type(device_ids[0]) is str and device_ids[0],
                "one observed GPU per replica is required")
        for pid in gpu_pids:
            integer(pid, "GPU process pid", 1)
        require(gpu_pids and len(set(gpu_pids)) == len(gpu_pids), "observed GPU processes are missing or duplicated")
        members = set(gpu_pids) | {api_pid}
        require(not devices.intersection(device_ids) and not processes.intersection(members),
                "replicas share a GPU or process owner")
        bindings = list_value(replica.get("listener_bindings"), "model listener evidence")
        require(bindings, "model listener evidence is missing")
        for binding in bindings:
            binding = object_value(binding, "model listener")
            require(integer(binding.get("pid"), "listener process pid", 1) in members and type(binding.get("process_start_time_ticks")) is str
                    and binding["process_start_time_ticks"].isdecimal() and int(binding["process_start_time_ticks"]) > 0,
                    "listener process evidence differs")
            if binding["pid"] == api_pid:
                require(binding["process_start_time_ticks"] == ticks, "listener has a different model process birth")
            family, address = binding.get("family"), binding.get("address")
            require((family == "ipv4" and address in ("127.0.0.1", "0.0.0.0"))
                    or (family == "ipv6" and address in ("::1", "::")), "model listener address evidence differs")
            port = integer(binding.get("port"), "model listener port", 1, 65535)
            inode = binding.get("inode")
            require(type(inode) is str and inode.isdecimal() and int(inode) > 0, "listener inode is missing")
            listener = (replica["location_id"], family, address, port)
            require(listener not in listeners, "replicas share a listener")
            listeners.add(listener)
        version = replica.get("service_version")
        require(type(version) is str and version, "service version is missing")
        serving.add((version, digest(replica.get("serving_arguments_sha256"), "serving arguments digest")))
        devices.update(device_ids)
        processes.update(members)
        records[endpoint] = replica
    require(set(records) == set(endpoints) and len(serving) == 1, "replica identity or serving equality differs")
    return records


def expected_cells(topology, selected_k):
    cells = {"protocol": ("round_robin", 1, range(60 if topology == "single" else 62,
                                                     62 if topology == "single" else 64))}
    for k in ((1, 2, 4) if topology == "single" else (2, 4, 8)):
        cells[f"cal-k{k}"] = ("round_robin", k, range(24, 48))
    for arm in ARMS:
        cells["warm-" + arm] = (arm, selected_k, range(48, 60))
        for repetition in range(3):
            cells[f"measure-{repetition}-{arm}"] = (arm, selected_k, range(24))
    return cells


def verify_budget(metadata, receipt, expected):
    rel = "evidence/budget.sqlite"
    raw = metadata.read(rel)
    with sqlite3.connect((metadata.root / rel).as_uri() + "?mode=ro", uri=True) as db:
        header = db.execute("SELECT schema,budget_id,limit_count,allocated,deadline FROM budget").fetchall()
        units = db.execute("SELECT unit_id,first_attempt,requests,claimed FROM units ORDER BY first_attempt").fetchall()
        shared = [r[0] for r in db.execute("SELECT unit_id FROM shared_units")]
        closed = [r[0] for r in db.execute("SELECT unit_id FROM closed_shared_units")]
        posts = db.execute("SELECT unit_id,sequence,request_sha256 FROM shared_requests").fetchall()
    require((metadata.root / rel).read_bytes() == raw, "budget changed while being audited")
    require(len(header) == 1 and header[0] == ("semloom.cell_budget.v1", receipt["budget_id"], 820, 820,
                                              receipt["deadline_utc"]), "budget header differs from the retained window")
    require(len(units) == 40 and {u[0] for u in units} == set(expected), "reserved units differ from the fixed matrix")
    require(len(shared) == len(closed) == 40 and set(shared) == set(closed) == set(expected),
            "shared units are unclosed, duplicated or do not match reservations")
    first, reservations, by_unit = 1, {}, {unit: {} for unit in expected}
    for unit, begin, count, claimed in units:
        require(type(begin) is int and begin == first and type(count) is int and count == expected[unit]
                and type(claimed) is int and claimed == 1,
                "reservation range, count or claim differs")
        reservations[unit] = dict(unit_id=unit, first_attempt=begin, requests=count, claimed=claimed)
        first += count
    require(first == 821 and len(posts) == 820, "durable POST count differs")
    for unit, sequence, request_hash in posts:
        require(unit in by_unit, "POST belongs to an unreserved unit")
        integer(sequence, "durable POST sequence", 1, expected[unit])
        require(sequence not in by_unit[unit], "duplicate durable POST sequence")
        by_unit[unit][sequence] = digest(request_hash, "durable request hash")
    require(all(len(by_unit[unit]) == count for unit, count in expected.items()), "durable unit POST count differs")
    snapshot = object_value(receipt.get("budget"), "window budget snapshot")
    require(snapshot.get("schema") == header[0][0] and snapshot.get("budget_id") == header[0][1]
            and snapshot.get("limit") == 820 and snapshot.get("allocated_requests") == 820
            and snapshot.get("deadline_utc") == header[0][4]
            and snapshot.get("units") == list(reservations.values()), "window budget snapshot differs from the ledger")
    return reservations, by_unit


def canonical_outcome(outcome):
    outcome = object_value(outcome, "outcome")
    integer(outcome.get("source_sequence"), "source sequence", 0, 63)
    for field in ("source_row_id", "source_call_id", "finish_reason"):
        require(type(outcome.get(field)) is str and outcome[field], "outcome " + field + " is missing")
    digest(outcome.get("body_sha256"), "HTTP body digest")
    digest(outcome.get("content_sha256"), "content digest")
    number(outcome.get("result_at_s"), "result time")
    usage = object_value(outcome.get("usage"), "outcome usage")
    prompt = integer(usage.get("prompt_tokens"), "prompt tokens", 1)
    completion = integer(usage.get("completion_tokens"), "completion tokens", 0, 8)
    require(integer(usage.get("total_tokens"), "total tokens") == prompt + completion, "token usage does not conserve counts")
    return outcome["content_sha256"], outcome["finish_reason"], json.dumps(usage, sort_keys=True, allow_nan=False)


def verify_cell(result, cleanup, specification, endpoints, reservation, posts, reference):
    arm, k, sequences = specification
    count, label = len(sequences), result["label"]
    require(result.get("arm") == arm and integer(result.get("requests"), "cell request capacity", 1) == k
            and integer(result.get("count"), "cell count", 1) == count, "cell configuration or count differs: " + label)
    digest(result.get("directory_sha256"), "cell directory digest")
    for field in ("jct_s", "assembly_s", "process_cpu_s", "policy_s"):
        number(result.get(field), field)
    require(result["jct_s"] > 0 and integer(result.get("policy_calls"), "policy call count", 1), "cell timing support is missing")
    outcomes, attempts = list_value(result.get("outcomes"), "outcomes"), list_value(result.get("attempts"), "attempts")
    require(len(outcomes) == len(attempts) == count, "cell outcome or attempt count differs: " + label)
    expected_attempts = list(range(reservation["first_attempt"], reservation["first_attempt"] + count))
    require(all(type(a) is int for a in attempts) and sorted(attempts) == expected_attempts,
            "cell attempts differ from its durable reservation: " + label)
    require(sorted(integer(object_value(o, "outcome").get("source_sequence"), "source sequence", 0, 63) for o in outcomes) == list(sequences),
            "cell source occurrences differ: " + label)
    for outcome in outcomes:
        seq = outcome["source_sequence"]
        canonical = (canonical_outcome(outcome), outcome["source_row_id"], outcome["source_call_id"])
        require(seq not in reference or reference[seq] == canonical, "normalized output or source association differs for a repeated occurrence")
        reference[seq] = canonical
    cleanup = object_value(cleanup, "cell cleanup")
    require(cleanup.get("clean") is True and cleanup.get("error") is None
            and cleanup.get("outcomes") == outcomes and cleanup.get("reserved_attempts") == attempts,
            "cell cleanup evidence differs: " + label)
    usage = object_value(cleanup.get("usage"), "cleanup usage")
    require(all(type(usage.get(f)) is int and usage[f] == 0 for f in USAGE_FIELDS), "cell cleanup retains capacity: " + label)
    events = list_value(result.get("events"), "cell events")
    require(events, "cell events are missing")
    routed, active, terminal, released, starts, finishes, reserved = {}, {}, set(), set(), {}, {}, {}
    event_counts, endpoint_posts, snapshots = Counter(), Counter(), []
    feedback_starts, feedback_finishes, sessions = Counter(), Counter(), set()
    for event in events:
        event = object_value(event, "event")
        name = event.get("event")
        require(type(name) is str, "event name is missing")
        event_counts[name] += 1
        number(event.get("monotonic_s"), "event monotonic time")
        number(event.get("utc_s"), "event UTC time")
        if name == "route_decision":
            decision = object_value(event.get("decision"), "route decision")
            request_id, endpoint = event.get("request_id"), decision.get("endpoint_id")
            require(type(request_id) is str and re.fullmatch(r"[0-9]+:[0-9]+", request_id)
                    and request_id not in routed and endpoint in endpoints, "route identity differs")
            routed[request_id] = endpoint
        if name in ("submitted", "terminal", "released", "replica_transport_started", "http_started", "http_finished"):
            key = object_value(event.get("key"), "event key")
            session_id = integer(key.get("session_id"), "session ID")
            sessions.add(session_id)
            request_id = str(session_id) + ":" + str(integer(key.get("sequence"), "request sequence", 0, count - 1))
            require(request_id in routed, "event has no preceding target decision")
            endpoint = routed[request_id]
            if name == "submitted":
                require(request_id not in active and request_id not in terminal, "request was submitted more than once")
                active[request_id] = endpoint
                require(len(active) <= k and max(Counter(active.values()).values()) <= 4, "event lifecycle exceeds global or endpoint capacity")
            elif name == "terminal":
                require(request_id in active and request_id in finishes, "request terminated without an active complete HTTP response")
                del active[request_id]
                terminal.add(request_id)
            elif name == "released":
                require(request_id in terminal and request_id not in released, "result release is duplicated or precedes termination")
                released.add(request_id)
            else:
                require(event.get("endpoint_id") == endpoint, "HTTP/transport target differs from Core route")
                if name == "http_started":
                    # The async backend can emit this before the owner's submitted
                    # observer. Capacity is checked on the authoritative Core edges.
                    require(request_id not in starts, "HTTP request started more than once")
                    starts[request_id] = event["monotonic_s"]
                    endpoint_posts[endpoint] += 1
                elif name == "http_finished":
                    require(request_id in starts and request_id not in finishes and event["monotonic_s"] >= starts[request_id],
                            "HTTP response is missing its unique start")
                    finishes[request_id] = event["monotonic_s"]
        if "usage" in event:
            usage = object_value(event["usage"], "event capacity usage")
            for field in USAGE_FIELDS:
                integer(usage.get(field), "event " + field)
            require(usage["active_requests"] == usage["active_work"] == len(active) <= k
                    and usage["held_tasks"] == count - len(released)
                    and usage["result_bytes"] <= count * 1048576, "event usage exceeds or differs from authoritative lifecycle")
        if name == "core_supply_snapshot":
            integer(event.get("queued"), "queued work", 0, count)
            require(type(event.get("hard_dispatchable")) is bool and "usage" in event, "Core readiness evidence is missing")
            inflight = object_value(event.get("endpoint_inflight"), "endpoint inflight")
            require(set(inflight) == set(endpoints), "endpoint capacity evidence is incomplete")
            require(all(integer(v, "endpoint inflight", 0, 4) == Counter(active.values())[e] for e, v in inflight.items()),
                    "endpoint snapshot differs from the authoritative lifecycle")
            limit = integer(event.get("supply_limit"), "supply limit", 1, k)
            require(arm == "adaptive_supply" or limit == k, "fixed supply point changed")
            if event.get("endpoint_supply") is not None:
                supply = object_value(event["endpoint_supply"], "endpoint supply")
                require(set(supply) == set(endpoints), "endpoint supply evidence is incomplete")
                for value in supply.values():
                    integer(value, "endpoint supply", 1, 4)
            snapshots.append(event)
        elif name == "durable_post_reserved":
            attempt = integer(event.get("attempt"), "reserved POST attempt", 1, 820)
            require(attempt in expected_attempts and attempt not in reserved, "durable POST reservation is duplicated or belongs to another cell")
            sequence = attempt - reservation["first_attempt"] + 1
            require(digest(event.get("payload_sha256"), "payload digest") == posts[sequence], "POST hash differs from the durable ledger")
            reserved[attempt] = event["payload_sha256"]
        elif name == "feedback_started":
            require(event.get("endpoint_id") in endpoints, "completion feedback endpoint differs")
            feedback_starts[event["endpoint_id"]] += 1
        elif name == "feedback_finished":
            require(event.get("successful") is True and event.get("work") == 1
                    and type(event.get("work")) is int and event.get("endpoint_id") in endpoints, "completion feedback differs from successful request-count work")
            feedback_finishes[event["endpoint_id"]] += 1
    for name in ("route_decision", "submitted", "terminal", "released", "replica_transport_started", "http_started", "http_finished",
                 "durable_post_reserved", "feedback_started", "feedback_finished"):
        require(event_counts[name] == count, "cell lifecycle event count differs: " + name)
    require(not active and len(sessions) == 1 and len(terminal) == len(released) == len(routed) == count and len(reserved) == count
            and feedback_starts == feedback_finishes == endpoint_posts,
            "cell lifecycle did not settle")
    require(snapshots and snapshots[0]["queued"] == count and snapshots[-1]["usage"]["held_tasks"] == 0,
            "initial ready work or final Core release evidence is missing")
    return endpoint_posts


def audit_metadata(root):
    metadata = RetainedMetadata(root)
    base = metadata.root / "evidence-affinity"
    receipt = object_value(metadata.json("evidence-affinity/receipt.json"), "window receipt")
    require(receipt.get("status") == "passed" and receipt.get("remaining_owned_pids") == [] and receipt.get("gpu_compute_after") == [],
            "window success and independent release evidence is missing")
    digest(receipt.get("source_sha"), "source commit", 40)
    for field in ("started_utc", "ended_utc", "deadline_utc", "loading_s"):
        number(receipt.get(field), "window " + field)
    require(receipt["started_utc"] <= receipt["ended_utc"] <= receipt["deadline_utc"], "window timestamps differ")
    require(receipt.get("completed_topologies") == ["single", "dual"], "window topology matrix is incomplete")
    source = object_value(metadata.json("source-receipt.json"), "source receipt")
    source_hash = digest(source.get("subset_sha256"), "source subset digest")
    groups, specifications, identities, expected = {}, {}, {}, {}
    for topology, endpoints in (("single", ("a",)), ("dual", ("a", "b"))):
        group = object_value(metadata.json(f"evidence-affinity/{topology}/group.json"), "topology group")
        require(group.get("topology") == topology and group.get("source_sha256") == source_hash
                and group.get("completed_cells") == 20, "group source or cell count differs")
        k = integer(group.get("selected_k"), "selected static K", 1, 4 * len(endpoints))
        require(k in ((1, 2, 4) if topology == "single" else (2, 4, 8)), "selected K was not calibrated")
        rates = object_value(group.get("fixed_rates"), "calibrated rates")
        require(set(rates) == set(endpoints), "calibrated rate coverage differs")
        require(all(number(v, "calibrated rate") > 0 for v in rates.values()), "calibrated rates must be positive")
        identities[topology] = verify_devices(metadata.json(f"evidence-affinity/{topology}-identity.json"), endpoints)
        specifications[topology] = expected_cells(topology, k)
        expected.update({topology + "-" + label: len(spec[2]) for label, spec in specifications[topology].items()})
        groups[topology] = group
    require(identities["single"]["a"] == identities["dual"]["a"], "the resident single-service identity changed before the dual group")
    reservations, posts = verify_budget(metadata, receipt, expected)
    count_receipt = object_value(metadata.json("count-receipt.json"), "count receipt")
    server_counts = object_value(count_receipt.get("server_success_posts"), "server POST counts")
    require(set(server_counts) == {"a", "b"} and sum(integer(v, "server POST count") for v in server_counts.values()) == 820,
            "server POST counts differ")
    # The historical field open_shared_units counts registrations, including closed ones.
    require(count_receipt.get("ledger") == dict(shared_requests=820, closed_units=40, open_shared_units=40),
            "count receipt differs from the retained ledger")
    # shared_units is a historical registration table. Its row count is not
    # the count of live units; closed_shared_units is the matching tombstone.
    cells, pressure, gpu, summary = [], {}, [], []
    reference, endpoint_posts = {}, Counter()
    for topology in ("single", "dual"):
        group, specs = groups[topology], specifications[topology]
        pressure[topology] = metadata.lines(f"evidence-affinity/{topology}/pressure.jsonl")
        require(pressure[topology], "service observation evidence is missing")
        observed = set()
        for sample in pressure[topology]:
            sample = object_value(sample, "service observation")
            require(sample.get("event") == "service_metrics" and sample.get("endpoint_id") in identities[topology],
                    "service observation target differs")
            observed.add(sample["endpoint_id"])
            for field in ("elapsed_s", "monotonic_s", "utc_s"):
                number(sample.get(field), "service observation " + field)
            metrics = object_value(sample.get("metrics"), "service metrics")
            for name, value in metrics.items():
                require(type(name) is str, "service metric name differs")
                number(value, "service metric value", minimum=-math.inf)
            for field in ("vllm:num_requests_running", "vllm:num_requests_waiting", "vllm:kv_cache_usage_perc"):
                number(metrics.get(field), "service pressure " + field)
            require(metrics["vllm:kv_cache_usage_perc"] <= 1, "KV pressure is outside physical bounds")
        require(observed == set(identities[topology]), "service observation endpoint coverage differs")
        expected_paths = {label + ".json.gz" for label in specs}
        require({p.name for p in (base / topology).glob("*.json.gz")} == expected_paths, "topology cell files differ from the fixed matrix")
        for path in sorted((base / topology).glob("*.json.gz")):
            label = path.name[:-len(".json.gz")]
            result = object_value(metadata.json(f"evidence-affinity/{topology}/{path.name}"), "cell result")
            require(result.get("label") == label and result.get("topology") == topology, "cell label or topology differs from its file")
            cleanup = metadata.json(f"evidence-affinity/{topology}/{label}/cleanup.json")
            unit = topology + "-" + label
            endpoints = identities[topology] if label != "protocol" else ("a",) if topology == "single" else ("b",)
            endpoint_posts.update(verify_cell(result, cleanup, specs[label], endpoints, reservations[unit], posts[unit], reference))
            events = []
            for original in result["events"]:
                event = {k: v for k, v in original.items() if k not in ("task_info", "job_id", "flow_id", "location_id")}
                events.append(event)
            # The source row IDs, call IDs, payload and full response stay in
            # private evidence. Anonymous sequence and output digests suffice.
            outcomes = [{k: v for k, v in o.items() if k not in ("source_row_id", "source_call_id")} for o in result["outcomes"]]
            cells.append(dict(result, events=events, outcomes=outcomes))
        for arm in ARMS:
            part = sorted((c for c in cells if c["topology"] == topology and c["label"].startswith("measure") and c["arm"] == arm), key=lambda c: c["label"])
            snapshots = [e for c in part for e in c["events"] if e["event"] == "core_supply_snapshot"]
            summary.append(dict(topology=topology, arm=arm, k=group["selected_k"],
                jct_s=[c["jct_s"] for c in part], median_jct_s=statistics.median(c["jct_s"] for c in part),
                policy_us_per_call=[c["policy_s"] / c["policy_calls"] * 1e6 for c in part],
                process_cpu_s=[c["process_cpu_s"] for c in part], assembly_s=[c["assembly_s"] for c in part],
                supply_limits=sorted(set(e["supply_limit"] for e in snapshots)),
                peak_owner_inflight=max(e["usage"]["active_requests"] for e in snapshots),
                replica_splits=[dict(Counter(e["endpoint_id"] for e in c["events"] if e["event"] == "replica_transport_started")) for c in part]))
    require(sum(c["count"] for c in cells) == 820 and len(cells) == 40 and len(reference) == 64
            and dict(endpoint_posts) == server_counts, "cell totals or per-endpoint POST counts differ")
    device_ids = {r["device_ids"][0] for r in identities["dual"].values()}
    gpu_devices = {}
    for row in csv.reader(metadata.read("evidence-affinity/gpu-samples.log").decode().splitlines()):
        row = [x.strip() for x in row]
        require(len(row) == 10 and row[2] in device_ids, "GPU sampling lacks the qualified device identity")
        stamp = datetime.datetime.strptime(row[0], "%Y/%m/%d %H:%M:%S.%f").replace(tzinfo=datetime.timezone(datetime.timedelta(hours=8))).timestamp()
        index = integer(int(row[1]), "GPU sample index", 0, 1)
        require(index not in gpu_devices or gpu_devices[index] == row[2], "GPU sample device mapping changed")
        require(not any(i != index and d == row[2] for i, d in gpu_devices.items()), "GPU sample device mapping is duplicated")
        gpu_devices[index] = row[2]
        values = [number(float(v), "GPU sample value") for v in row[3:]]
        require(values[0] <= 100 and values[1] <= 100 and values[3] <= values[4] and values[4] > 0,
                "GPU sample values are outside physical bounds")
        gpu.append(dict(utc_s=stamp, gpu=index, utilization_percent=float(row[3]), memory_activity_percent=float(row[4]),
            power_w=float(row[5]), memory_used_mib=float(row[6]), memory_total_mib=float(row[7]),
            sm_clock_mhz=float(row[8]), memory_clock_mhz=float(row[9])))
    require(set(gpu_devices.values()) == device_ids, "GPU samples do not cover the independently observed devices")
    # Content/finish/usage equality is a normalized comparison of retained hashes.
    # Full response bytes (including response IDs and timestamps) are not compared.
    data = dict(schema="semloom.replica_model_numeric_evidence.v1", evidence_type="model_request_subset_replay",
        source_sha=receipt["source_sha"], started_utc=receipt["started_utc"], ended_utc=receipt["ended_utc"],
        loading_s=receipt["loading_s"], model_posts=820, evaluation_posts=576, original_sql_queries=0,
        canonical_outputs_equal=True, canonical_comparison_fields=["content_sha256", "finish_reason", "usage"],
        http_response_bytes_equal="not_qualified", unclosed_units=0, controller_default_changed=False,
        server_success_posts=count_receipt["server_success_posts"], mfu="not_measured", query_p99="not_qualified",
        summary=summary, cells=cells, pressure=pressure, gpu=gpu)
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metadata", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    try:
        data = audit_metadata(args.metadata)
    except (EvidenceValidationError, OSError, ValueError, KeyError, TypeError, sqlite3.Error) as error:
        parser.error("invalid model evidence: " + str(error))
    args.output.mkdir(exist_ok=False)
    with gzip.open(args.output / "raw.json.gz", "wt", encoding="utf-8") as f:
        json.dump(data, f, separators=(",", ":"))
    (args.output / "summary.json").write_text(json.dumps({k:v for k,v in data.items() if k not in ("cells", "pressure", "gpu")}, indent=2) + "\n")
    print(json.dumps(dict(audited_cells=len(data["cells"]), model_posts=820, canonical_rows=64, unclosed_units=0)))


if __name__ == "__main__":
    main()
