"""Evidence-qualified summaries of passive vLLM worker decision events."""
from collections import Counter, defaultdict
from .observer_events import read_observer_events


_EVENTS = {
    "iteration_launch_start", "iteration_launch_end", "native_schedule_metadata",
    "native_cascade_decision", "native_graph_decision", "native_decision_unknown",
}


def _counts(value):
    return (isinstance(value, dict) and all(type(k) is str and type(v) is int and v >= 0
                                          for k, v in value.items()))


def _count(value):
    return type(value) is int and value >= 0


def _vector(value):
    return isinstance(value, list) and all(_count(v) for v in value)


def _iteration(key, events):
    by_event = defaultdict(list)
    for event in events:
        by_event[event["event"]].append(event)
    issues = []

    def one(name, required=True):
        rows = by_event[name]
        if len(rows) == 1:
            return rows[0]
        if required or rows:
            issues.append(("missing:" if not rows else "duplicate:") + name)
        return {}

    launch = one("iteration_launch_start")
    end = one("iteration_launch_end")
    metadata = one("native_schedule_metadata")
    graph = one("native_graph_decision")
    cascade = one("native_cascade_decision", required=False)
    if by_event["native_decision_unknown"]:
        issues.append("observer_extraction_failed")
    if end.get("status") != "returned":
        issues.append("native_execute_not_confirmed_returned")
    members = graph.get("scheduled_tokens")
    if (not _counts(members) or not members or not _counts(launch.get("scheduled_tokens"))
            or members != launch.get("scheduled_tokens")):
        issues.append("actual_members_unavailable_or_differ_from_schedule")
    raw, padded, num_reqs = graph.get("num_tokens"), graph.get("padded_tokens"), graph.get("num_reqs")
    if (not _count(raw) or not _count(padded) or not _count(num_reqs) or not _counts(members)
            or raw != sum(members.values()) or num_reqs != len(members) or padded < raw
            or not _count(metadata.get("total_scheduled_tokens")) or metadata.get("total_scheduled_tokens") != raw):
        issues.append("token_or_member_counts_inconsistent")
    mode = graph.get("cudagraph_mode")
    if mode not in ("NONE", "PIECEWISE", "FULL"):
        issues.append("graph_mode_unavailable")
    blocks = metadata.get("common_prefix_blocks")
    if not _vector(blocks):
        issues.append("prefix_blocks_unavailable")
    selected = graph.get("use_cascade_attn")
    if type(selected) is not bool:
        issues.append("cascade_selection_unavailable")
    if cascade:
        lengths = cascade.get("cascade_prefix_lengths")
        valid_lengths = (isinstance(lengths, list) and all(_vector(v) for v in lengths)
                         and any(v > 0 for group in lengths for v in group))
        if (cascade.get("scheduled_tokens") != members or cascade.get("common_prefix_blocks") != blocks
                or cascade.get("cascade_selected") is not selected
                or selected and not valid_lengths
                or selected is False and lengths is not None):
            issues.append("cascade_evidence_inconsistent")
    elif selected is True:
        issues.append("cascade_result_missing")
    elif selected is False and metadata.get("cascade_enabled") is True and metadata.get("microbatching_enabled") is False:
        issues.append("expected_cascade_call_missing")
    encoder_counts = metadata.get("encoder_input_counts")
    if (not _counts(encoder_counts) or not _count(graph.get("num_encoder_reqs"))
            or graph.get("num_encoder_reqs") != len(encoder_counts)):
        issues.append("encoder_input_counts_unavailable")
    return dict(
        run_id=key[0], observer_session_id=key[1], pid=key[2], observer_execute_id=key[3],
        status="unknown" if issues else "observed", reasons=issues,
        actual_members=members, common_prefix_blocks=blocks,
        computed_tokens=cascade.get("computed_tokens"),
        prompt_token_counts=graph.get('prompt_token_counts'), computed_before=graph.get('computed_before'),
        new_request_computed_tokens=metadata.get('new_request_computed_tokens'),
        cascade_selected=selected, cascade_call_observed=bool(cascade),
        cascade_prefix_lengths=cascade.get("cascade_prefix_lengths"),
        cudagraph_mode=mode, raw_tokens=raw, padded_tokens=padded,
        padding_tokens=padded - raw if _count(raw) and _count(padded) and padded >= raw else None,
        encoder_input_counts=metadata.get("encoder_input_counts"),
        force_eager=graph.get("force_eager"), should_ubatch=graph.get("should_ubatch"),
        evidence=[e.get("_evidence") for e in events if e.get("_evidence")],
    )


def diagnose(events, *, capture_issues=None):
    """Never infer execution time, cache hits, or a bottleneck from decisions."""
    groups = defaultdict(list)
    issues = ["sink_receipts_not_checked"] if capture_issues is None else list(capture_issues)
    for event in events:
        if event.get("event") not in _EVENTS:
            continue
        key = (event.get("run_id"), event.get("observer_session_id"),
               event.get("pid"), event.get("observer_execute_id"))
        if (not all(type(v) is str and v for v in key[:2])
                or not all(type(v) is int and v > 0 for v in key[2:])):
            issues.append("event_identity_missing")
            continue
        groups[key].append(event)
    rows = [_iteration(key, value) for key, value in sorted(groups.items())]
    observed = [row for row in rows if row["status"] == "observed"]
    findings = []
    shared_not_selected = [row for row in observed if any(row["common_prefix_blocks"]) and not row["cascade_selected"]]
    padded_rows = [row for row in observed if row["padding_tokens"] > 0]
    if shared_not_selected:
        findings.append(dict(
            phenomenon="shared_blocks_without_cascade", iterations=len(shared_not_selected),
            interpretation="Shared blocks were reported; cascade was not selected. This is not a cache-miss count or proof of avoidable work.",
            next_intervention="Keep full requests and capacity fixed; compare the current order with prefix grouping and measure actual attention kernels, waiting and complete-query time.",
        ))
    if padded_rows:
        findings.append(dict(
            phenomenon="graph_padding", iterations=len(padded_rows),
            interpretation="The native batch descriptor includes padding slots; they are not measured GPU work or elapsed time.",
            next_intervention="Keep requests and capacity fixed; vary native graph capture sizes, then compare kernel time, launch overhead and complete-query time.",
        ))
    raw_total = sum(row["raw_tokens"] for row in observed)
    padding_total = sum(row["padding_tokens"] for row in observed)
    return dict(
        schema="semloom.native_gpu_decisions.v1", source_type="observer_event_analysis",
        capture_status="incomplete" if issues else "closed_without_recorded_loss",
        capture_issues=sorted(set(issues)),
        interpretation_scope="Only observed iterations; even a clean sink does not prove all model workers were captured. GPU completion and semantic occurrence linkage require separate evidence.",
        iterations=rows,
        summary=dict(iterations=len(rows), locally_consistent_iterations=len(observed),
                     unknown_iterations=len(rows) - len(observed),
                     graph_modes=dict(Counter(row["cudagraph_mode"] for row in observed)),
                     cascade_selected_iterations=sum(row["cascade_selected"] for row in observed),
                     shared_blocks_without_cascade=len(shared_not_selected),
                     raw_token_slots=raw_total, padding_token_slots=padding_total,
                     padding_over_raw_ratio=padding_total / raw_total if raw_total else None),
        findings=findings,
        unresolved=[
            dict(question="supply_starvation", status="unknown", required="Legal ready work, admission and engine queue events on aligned clocks, GPU idle intervals, and a supply intervention."),
            dict(question="compute_or_bandwidth_limited", status="unknown", required="Actual kernel durations, device counters and arithmetic/DRAM work; utilization alone is insufficient."),
            dict(question="cache_hit_savings", status="unknown", required="Request/block identity, cache hits and recomputed tokens, plus a matched cache-policy intervention."),
            dict(question="query_speedup", status="unknown", required="Matched complete queries, quality, resources, observer off/on overhead and paired repetitions."),
        ],
    )


def analyze_directory(directory, *, max_events=1_000_000):
    """Read one private observer events directory; retain file/line evidence."""
    events, issues, sources = read_observer_events(directory, max_events=max_events, event_names=_EVENTS)
    if not events:
        issues.append("no_native_decision_events")
    report = diagnose(events, capture_issues=issues)
    report["sources"] = sources
    return report
