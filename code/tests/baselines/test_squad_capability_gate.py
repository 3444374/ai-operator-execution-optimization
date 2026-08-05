from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

CODE_ROOT = next(
    parent
    for parent in Path(__file__).resolve().parents
    if (parent / "src").is_dir()
)
if str(CODE_ROOT) not in sys.path:
    sys.path.insert(0, str(CODE_ROOT))

_GATE_PATH = CODE_ROOT / "scripts" / "baselines" / "squad_capability_gate.py"
_spec = importlib.util.spec_from_file_location("squad_capability_gate", _GATE_PATH)
gate = importlib.util.module_from_spec(_spec)
sys.modules["squad_capability_gate"] = gate
_spec.loader.exec_module(gate)

_IMPORTER_PATH = CODE_ROOT / "scripts" / "data" / "import_squad_workload.py"
_ispec = importlib.util.spec_from_file_location("import_squad_workload", _IMPORTER_PATH)
importer = importlib.util.module_from_spec(_ispec)
sys.modules["import_squad_workload"] = importer
_ispec.loader.exec_module(importer)


def _row(example_id: str, answers: list[str]) -> dict:
    return {
        "doc_id": abs(hash(example_id)) % (10**12),
        "text": "prompt " + example_id,
        "source_example_id": example_id,
        "answers": answers,
    }


def _fixture() -> list[dict]:
    # 30 short (1 word) + 40 medium (2 words) + 30 long (5 words) = 100
    rows = []
    for i in range(30):
        rows.append(_row(f"short{i:02d}", ["yes"]))
    for i in range(40):
        rows.append(_row(f"medium{i:02d}", ["two words"]))
    for i in range(30):
        rows.append(_row(f"long{i:02d}", ["one two three four five"]))
    return rows


class AnswerBucketTests(unittest.TestCase):
    def test_single_answer_buckets(self) -> None:
        self.assertEqual(gate._answer_bucket(["yes"]), "short")
        self.assertEqual(gate._answer_bucket(["the answer"]), "medium")
        self.assertEqual(gate._answer_bucket(["one two three four five"]), "long")

    def test_multi_answer_uses_max_not_first(self) -> None:
        # codex #6: bucket must not depend on answer array order.
        self.assertEqual(gate._answer_bucket(["yes", "a longer answer here"]), "medium")
        self.assertEqual(gate._answer_bucket(["a longer answer here", "yes"]), "medium")
        self.assertEqual(
            gate._answer_bucket(["x", "one two three four five words"]),
            "long",
        )

    def test_empty_answers_is_short(self) -> None:
        self.assertEqual(gate._answer_bucket([]), "short")


class LargestRemainderTests(unittest.TestCase):
    def test_sum_equals_target(self) -> None:
        sizes = [30, 40, 30]
        alloc = gate.largest_remainder_allocation(sizes, 10)
        self.assertEqual(sum(alloc), 10)
        self.assertEqual(len(alloc), 3)

    def test_proportional(self) -> None:
        # 30:40:30 of 100 -> 10 target -> 3:4:3
        self.assertEqual(gate.largest_remainder_allocation([30, 40, 30], 10), [3, 4, 3])

    def test_target_smaller_than_bucket_count(self) -> None:
        # codex #7: target < number of buckets must still sum to target.
        alloc = gate.largest_remainder_allocation([10, 10, 10], 2)
        self.assertEqual(sum(alloc), 2)
        self.assertLessEqual(max(alloc), 1)

    def test_round_sum_exceeds_target_handled(self) -> None:
        # A case where naive max(1, round(...)) would overshoot: 3 buckets,
        # target 2, each round(2*1/3)=1 -> naive sum 3 > 2. Largest-remainder
        # floors then distributes, so sum == 2.
        alloc = gate.largest_remainder_allocation([1, 1, 1], 2)
        self.assertEqual(sum(alloc), 2)

    def test_target_exceeds_total_capped_at_sizes(self) -> None:
        alloc = gate.largest_remainder_allocation([2, 3], 100)
        self.assertEqual(alloc, [2, 3])


class StratifiedSampleTests(unittest.TestCase):
    def test_deterministic_and_exact_count(self) -> None:
        rows = _fixture()
        first = gate.stratified_sample(rows, 10)
        second = gate.stratified_sample(rows, 10)
        self.assertEqual(len(first), 10)
        self.assertEqual(
            [r["source_example_id"] for r in first],
            [r["source_example_id"] for r in second],
        )
        ids = [r["source_example_id"] for r in first]
        self.assertEqual(len(set(ids)), 10)

    def test_target_exceeds_total_returns_all(self) -> None:
        rows = _fixture()[:5]
        sampled = gate.stratified_sample(rows, 10)
        self.assertEqual(len(sampled), 5)

    def test_stratification_covers_all_buckets(self) -> None:
        rows = _fixture()
        sampled = gate.stratified_sample(rows, 30)
        buckets_present = {gate._answer_bucket(r["answers"]) for r in sampled}
        self.assertEqual(buckets_present, {"short", "medium", "long"})

    def test_input_order_invariance(self) -> None:
        # codex #7: shuffling input must not change the selected IDs.
        rows = _fixture()
        forward = gate.stratified_sample(rows, 25)
        reversed_rows = list(reversed(rows))
        backward = gate.stratified_sample(reversed_rows, 25)
        self.assertEqual(
            [r["source_example_id"] for r in forward],
            [r["source_example_id"] for r in backward],
        )


class StructuredContentHashTests(unittest.TestCase):
    def test_stable(self) -> None:
        rows = _fixture()[:12]
        self.assertEqual(
            gate._structured_content_hash(rows),
            gate._structured_content_hash(rows),
        )

    def test_input_order_invariant(self) -> None:
        rows = _fixture()[:12]
        self.assertEqual(
            gate._structured_content_hash(rows),
            gate._structured_content_hash(list(reversed(rows))),
        )

    def test_id_change_changes_hash(self) -> None:
        # codex #8: a different selection (different source id) must hash
        # differently; bare-ID concat can collide, structured JSON must not.
        rows_a = _fixture()[:5]
        rows_b = [dict(r, source_example_id=r["source_example_id"] + "x") for r in rows_a]
        self.assertNotEqual(
            gate._structured_content_hash(rows_a),
            gate._structured_content_hash(rows_b),
        )

    def test_prompt_change_changes_hash(self) -> None:
        rows_a = _fixture()[:5]
        rows_b = [dict(r, text=r["text"] + " changed") for r in rows_a]
        self.assertNotEqual(
            gate._structured_content_hash(rows_a),
            gate._structured_content_hash(rows_b),
        )

    def test_content_hash_matches_importer(self) -> None:
        # Pin the gate's structured hash to the importer's canonical
        # compute_content_hash so the gate's workload hash is directly
        # comparable to the archived provenance content_hash.
        rows_dict = [
            _row(f"id{i:03d}", [f"answer {i}"] if i % 2 else [f"a{i}", f"answer number {i}"])
            for i in range(6)
        ]
        importer_rows = [
            importer.SquadRow(
                source_example_id=r["source_example_id"],
                context="ctx",
                question="q?",
                reference_answers=tuple(r["answers"]),
                prompt=r["text"],
            )
            for r in rows_dict
        ]
        self.assertEqual(
            gate._structured_content_hash(rows_dict),
            importer.compute_content_hash(importer_rows),
        )


class WorkloadIntegrityTests(unittest.TestCase):
    def _good_rows(self, count: int) -> list[dict]:
        rows = [_row(f"id{i:03d}", ["ans"]) for i in range(count)]
        return rows

    def test_good_workload_passes(self) -> None:
        rows = self._good_rows(5)
        h = gate._structured_content_hash(rows)
        ok, problems = gate._validate_workload_integrity(rows, 5, h)
        self.assertTrue(ok, problems)

    def test_wrong_count_fails(self) -> None:
        rows = self._good_rows(5)
        h = gate._structured_content_hash(rows)
        ok, problems = gate._validate_workload_integrity(rows, 6, h)
        self.assertFalse(ok)
        self.assertTrue(any("row_count" in p for p in problems))

    def test_duplicate_source_id_fails(self) -> None:
        rows = self._good_rows(5)
        rows[1] = dict(rows[0])  # duplicate source_example_id
        h = gate._structured_content_hash(rows)
        ok, problems = gate._validate_workload_integrity(rows, 5, h)
        self.assertFalse(ok)

    def test_duplicate_doc_id_fails(self) -> None:
        rows = self._good_rows(5)
        rows[1] = dict(rows[1], doc_id=rows[0]["doc_id"])  # duplicate doc_id
        h = gate._structured_content_hash(rows)
        ok, problems = gate._validate_workload_integrity(rows, 5, h)
        self.assertFalse(ok)
        self.assertTrue(any("doc_id" in p for p in problems))

    def test_blank_reference_string_fails(self) -> None:
        # answers=['']  (non-empty list, blank element) must now be rejected.
        rows = self._good_rows(5)
        rows[2] = dict(rows[2], answers=[""])
        h = gate._structured_content_hash(rows)
        ok, problems = gate._validate_workload_integrity(rows, 5, h)
        self.assertFalse(ok)
        self.assertTrue(any("reference_answers" in p for p in problems))

    def test_empty_reference_fails(self) -> None:
        rows = self._good_rows(5)
        rows[2] = dict(rows[2], answers=[])
        h = gate._structured_content_hash(rows)
        ok, problems = gate._validate_workload_integrity(rows, 5, h)
        self.assertFalse(ok)
        self.assertTrue(any("reference_answers" in p for p in problems))

    def test_hash_mismatch_fails(self) -> None:
        rows = self._good_rows(5)
        ok, problems = gate._validate_workload_integrity(rows, 5, "0" * 64)
        self.assertFalse(ok)
        self.assertTrue(any("content_hash" in p for p in problems))


class AttributionTests(unittest.TestCase):
    def test_idle_exclusive_match_attributable(self) -> None:
        before = {"vllm:num_requests_running": 0.0, "vllm:num_requests_waiting": 0.0,
                  "vllm:request_success_total": 100.0}
        after = {**before, "vllm:request_success_total": 356.0}
        summary, ok = gate._assess_attribution(before, after, requests_sent=256)
        self.assertTrue(ok)
        self.assertEqual(summary["request_success_delta"], 256)

    def test_not_idle_before_unavailable(self) -> None:
        before = {"vllm:num_requests_running": 5.0, "vllm:num_requests_waiting": 0.0,
                  "vllm:request_success_total": 100.0}
        after = {**before, "vllm:request_success_total": 356.0}
        _, ok = gate._assess_attribution(before, after, requests_sent=256)
        self.assertFalse(ok)

    def test_concurrent_traffic_unavailable(self) -> None:
        before = {"vllm:num_requests_running": 0.0, "vllm:num_requests_waiting": 0.0,
                  "vllm:request_success_total": 100.0}
        # delta 300 != 256 -> extra concurrent traffic
        after = {**before, "vllm:request_success_total": 400.0}
        summary, ok = gate._assess_attribution(before, after, requests_sent=256)
        self.assertFalse(ok)

    def test_counter_reset_unavailable(self) -> None:
        before = {"vllm:num_requests_running": 0.0, "vllm:num_requests_waiting": 0.0,
                  "vllm:request_success_total": 500.0,
                  "vllm:generation_tokens_total": 9999.0}
        after = {"vllm:num_requests_running": 0.0, "vllm:num_requests_waiting": 0.0,
                 "vllm:request_success_total": 756.0,
                 "vllm:generation_tokens_total": 10.0}  # reset
        _, ok = gate._assess_attribution(before, after, requests_sent=256)
        self.assertFalse(ok)

    def test_empty_scrape_unavailable(self) -> None:
        after = {"vllm:num_requests_running": 0.0, "vllm:num_requests_waiting": 0.0,
                 "vllm:request_success_total": 256.0}
        _, ok = gate._assess_attribution({}, after, requests_sent=256)
        self.assertFalse(ok)

    def test_gauge_missing_treated_as_not_idle(self) -> None:
        # Non-empty scrape but missing the running/waiting gauges: must NOT be
        # silently treated as idle.
        partial = {"vllm:request_success_total": 100.0}
        ok, reason = gate._endpoint_idle(partial)
        self.assertFalse(ok)
        self.assertEqual(reason, "gauge_missing")
        self.assertEqual(gate._scrape_status(partial), "gauge_missing")
        self.assertEqual(gate._scrape_status({}), "empty")
        full = {"vllm:num_requests_running": 0.0, "vllm:num_requests_waiting": 0.0}
        self.assertEqual(gate._scrape_status(full), "ok")


class RedactionWiringTests(unittest.TestCase):
    def test_command_field_redaction_strips_password_and_api_key(self) -> None:
        # Pins that the function the gate stores in report.json["command"]
        # strips the DB-URL password and masks --api-key. Regression guard
        # against a revert to raw list(sys.argv).
        from src.baselines.common.redact import redact_argument_list
        argv = [
            "python", "squad_capability_gate.py",
            "--database-url", "postgresql://postgres:postgres@localhost:5432/ai_operator",
            "--api-key", "hf_real_token",
            "--endpoint-url", "http://127.0.0.1:8000/v1/chat/completions",
        ]
        redacted = redact_argument_list(argv)
        joined = " ".join(redacted)
        self.assertNotIn("postgres:postgres@", joined)
        self.assertNotIn("hf_real_token", joined)
        self.assertIn("postgres:***@", joined)
        self.assertIn("***", joined)


if __name__ == "__main__":
    unittest.main()
