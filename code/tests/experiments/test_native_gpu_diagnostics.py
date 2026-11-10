import json
from pathlib import Path
import tempfile
import unittest

from src.experiments.native_gpu_diagnostics import analyze_directory, diagnose


def events(session="session"):
    common = dict(run_id="run", observer_session_id=session, pid=9, observer_execute_id=1)
    values = [
        dict(event="iteration_launch_start", scheduled_tokens={"a": 2, "b": 3}),
        dict(event="iteration_launch_end", status="returned"),
        dict(event="native_schedule_metadata", common_prefix_blocks=[2], total_scheduled_tokens=5,
             encoder_input_counts={}, cascade_enabled=True, microbatching_enabled=False),
        dict(event="native_cascade_decision", scheduled_tokens={"a": 2, "b": 3},
             common_prefix_blocks=[2], cascade_selected=False, cascade_prefix_lengths=None),
        dict(event="native_graph_decision", scheduled_tokens={"a": 2, "b": 3}, num_tokens=5,
             num_reqs=2, padded_tokens=8, cudagraph_mode="FULL", use_cascade_attn=False, num_encoder_reqs=0),
    ]
    return [dict(common, sequence=i, **value) for i, value in enumerate(values, 1)]


class DiagnosticsTests(unittest.TestCase):
    def test_selected_cascade_keeps_actual_lengths_and_computed_tokens(self):
        values = events()
        values[3].update(cascade_selected=True, cascade_prefix_lengths=[[16, 0]], computed_tokens={"a": 32, "b": 16})
        values[4].update(use_cascade_attn=True, cudagraph_mode="PIECEWISE")
        report = diagnose(values)
        self.assertEqual(report["summary"]["cascade_selected_iterations"], 1)
        self.assertEqual(report["summary"]["shared_blocks_without_cascade"], 0)
        self.assertEqual(report["iterations"][0]["computed_tokens"], {"a": 32, "b": 16})

    def test_observed_phenomena_do_not_claim_savings_or_bottleneck(self):
        value = diagnose(events())
        self.assertEqual(value["summary"]["locally_consistent_iterations"], 1)
        self.assertEqual(value["summary"]["padding_over_raw_ratio"], 3 / 5)
        self.assertEqual({v["phenomenon"] for v in value["findings"]},
                         {"shared_blocks_without_cascade", "graph_padding"})
        self.assertTrue(all(v["status"] == "unknown" for v in value["unresolved"]))

    def test_no_cross_session_or_pid_join(self):
        first, second = events("one"), events("two")
        result = diagnose(first[:3] + second[3:])
        self.assertEqual(result["summary"]["iterations"], 2)
        self.assertEqual(result["summary"]["locally_consistent_iterations"], 0)

    def test_missing_duplicate_failed_or_inconsistent_events_are_unknown(self):
        candidates = []
        candidates.append(events()[:-1])
        candidates.append(events() + [events()[-1]])
        for index, fields in [(1, {"status": "error"}), (2, {"common_prefix_blocks": None}),
                              (3, {"cascade_selected": True}), (4, {"num_tokens": 6}),
                              (4, {"scheduled_tokens": {"different": 5}})]:
            value = events(); value[index].update(fields); candidates.append(value)
        for value in candidates:
            with self.subTest(value=value):
                report = diagnose(value)
                self.assertEqual(report["summary"]["locally_consistent_iterations"], 0)
                self.assertEqual(report["findings"], [])

    def test_disabled_cascade_is_distinct_from_missing_expected_call(self):
        values = events(); del values[3]
        self.assertEqual(diagnose(values)["summary"]["unknown_iterations"], 1)
        values[2]["cascade_enabled"] = False
        report = diagnose(values)
        self.assertEqual(report["summary"]["locally_consistent_iterations"], 1)
        self.assertFalse(report["iterations"][0]["cascade_call_observed"])

    def test_closed_sink_evidence_and_losses_and_file_bound(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            path = root / "events-9.jsonl"
            path.write_text("".join(json.dumps(v) + "\n" for v in events()))
            self.assertEqual(analyze_directory(root)["capture_status"], "incomplete")
            status = dict(closed=True, flush_confirmed=True, events_written=5, events_attempted=5,
                          last_sequence=5, dropped=0, errors=0,
                          feedback_buffer=dict(drain_complete=True, closed=True, remaining=0,
                                               dropped=0, errors=0, enqueued=3, drained=3, attempted=3))
            footer = root / "events-9.status.json"
            footer.write_text(json.dumps(status))
            value = analyze_directory(root)
            self.assertEqual(value["capture_status"], "closed_without_recorded_loss")
            self.assertEqual(value["iterations"][0]["evidence"][0], dict(file=path.name, line=1))
            self.assertEqual(len(value["sources"][0]["sha256"]), 64)
            for altered in (dict(status, dropped=1), dict(status, flush_confirmed=False),
                            dict(status, feedback_buffer=dict(status["feedback_buffer"], errors=1))):
                footer.write_text(json.dumps(altered))
                self.assertEqual(analyze_directory(root)["capture_status"], "incomplete")
            with self.assertRaises(ValueError): analyze_directory(root, max_events=2)
            path.write_text(path.read_text() + "{broken\n")
            self.assertIn("events-9.jsonl:invalid_event", analyze_directory(root)["capture_issues"])

    def test_no_files_cannot_be_complete(self):
        with tempfile.TemporaryDirectory() as name:
            self.assertEqual(analyze_directory(name)["capture_status"], "incomplete")
            (Path(name)/"events-11.status.json").write_text("{}")
            self.assertIn("events-11.status.json:event_file_missing", analyze_directory(name)["capture_issues"])


if __name__ == "__main__":
    unittest.main()
