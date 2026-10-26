"""Result-contract migration checks; all calls/results are finite CPU fixtures."""

import ast
import asyncio
import base64
from copy import deepcopy
import importlib
import json
from pathlib import Path
import pickle
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from src.semantic_methods.lotus import record_results as results
from src.semantic_methods.lotus.record_chain import MapMethod, RecordOccurrence, StageSpec, stage_identity
from src.semantic_methods.lotus.records import FilterMethod
from src.serving.backends.vllm_engine import GenerationSpec


CODE = Path(__file__).resolve().parents[2]
LEGACY = "src.baselines.text.frameworks.lotus_chain_results"
CANONICAL = "src.semantic_methods.lotus.record_results"


def fixture():
    chain = SimpleNamespace(query_id="fixture-query", stages=(
        StageSpec("screen", FilterMethod("fixture-model", "Keep {text}",
                  resolved_input_columns=("text",)), GenerationSpec()),
        StageSpec("summarize", MapMethod("fixture-model", "Summarize {text}"), GenerationSpec()),
    ))
    records = [RecordOccurrence("row-" + str(i), i, "repeated-label", {"text": "text-" + str(i)})
               for i in range(3)]
    return records, chain


def envelope(records, chain, batch_id="fixture-batch", *, decisions=None, parents=None):
    calls, rows = [], []
    for record in records:
        keep = True if decisions is None else decisions[record.row_id]
        first = dict(stage_identity(chain.query_id, record, "screen"), status="succeeded",
                     parent_call_ids=list((parents or {}).get((record.row_id, record.source_position), ())),
                     decision=keep, text=str(keep), prompt_tokens=3, completion_tokens=1, total_tokens=4)
        calls.append(first)
        if keep:
            calls.append(dict(stage_identity(chain.query_id, record, "summarize"), status="succeeded",
                              parent_call_ids=[first["call_id"]], text="summary",
                              prompt_tokens=4, completion_tokens=2, total_tokens=6))
            row = record.describe()
            row["values"]["summary"] = "summary"
            rows.append(row)
    return dict(batch_id=batch_id, inputs=[results._source_binding(r) for r in records],
                calls=calls, stage_stats={}, columns=["text", "summary"], records=rows)


class ResultMigrationTests(unittest.TestCase):
    def test_all_legacy_exports_are_the_same_objects(self):
        old = importlib.import_module(LEGACY)
        for name, value in vars(old).items():
            if not name.startswith("__"):
                with self.subTest(name=name):
                    self.assertIs(value, getattr(results, name))

    def test_fresh_import_orders_preserve_identity_without_core_or_model_imports(self):
        program = '''
import importlib, importlib.abc, sys
sys.path.insert(0, sys.argv[1])
class Guard(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, *args):
        forbidden = ('src.scheduling', 'src.execution_provider', 'src.experiments',
                     'ray', 'daft', 'vllm', 'torch', 'lotus')
        if any(fullname == p or fullname.startswith(p + '.') for p in forbidden):
            raise AssertionError('unexpected dependency: ' + fullname)
sys.meta_path.insert(0, Guard())
first = importlib.import_module(sys.argv[2])
if sys.argv[2] == 'src.semantic_methods.lotus.record_results':
    assert not any(n == 'src.baselines' or n.startswith('src.baselines.') for n in sys.modules)
second = importlib.import_module(sys.argv[3])
assert first.ChainBatchResults is second.ChainBatchResults
assert first.ChainExecutionError is second.ChainExecutionError
'''
        for order in ((CANONICAL, LEGACY), (LEGACY, CANONICAL)):
            with self.subTest(order=order):
                run = subprocess.run([sys.executable, "-I", "-B", "-c", program,
                                      str(CODE), *order], capture_output=True, text=True, timeout=15)
                self.assertEqual(run.returncode, 0, run.stderr)

    def test_old_pickle_globals_and_new_state_are_readable_in_fresh_process(self):
        # Protocol 0 GLOBAL is the name lookup used by old class/function pickles.
        for name in ("ChainBatchResults", "ChainExecutionError", "record_row"):
            value = pickle.loads(("c" + LEGACY + "\n" + name + "\n.").encode())
            self.assertIs(value, getattr(results, name))
        records, chain = fixture()
        audit = results.ChainBatchResults(records, chain)
        audit.accept(envelope(records, chain))
        payload = base64.b64encode(pickle.dumps(audit)).decode()
        program = '''
import base64, importlib, json, pickle, sys
sys.path.insert(0, sys.argv[1])
audit = pickle.loads(base64.b64decode(sys.argv[2]))
new = importlib.import_module('src.semantic_methods.lotus.record_results')
old = importlib.import_module('src.baselines.text.frameworks.lotus_chain_results')
assert type(audit) is new.ChainBatchResults is old.ChainBatchResults
audit.verify_success(source_exhausted=True)
print(json.dumps({'calls': len(audit.calls), 'rows': len(audit.pending)}))
'''
        run = subprocess.run([sys.executable, "-I", "-B", "-c", program, str(CODE), payload],
                             capture_output=True, text=True, timeout=15)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual(json.loads(run.stdout), {"calls": 6, "rows": 3})

    def test_native_consumers_inherit_the_shared_class(self):
        from src.baselines.text.frameworks import daft_lotus_chain, lotus_chain_batch, ray_data_lotus_chain
        self.assertTrue(issubclass(daft_lotus_chain._DaftChainRows, results.ChainBatchResults))
        self.assertTrue(issubclass(ray_data_lotus_chain._RayChainRows, results.ChainBatchResults))
        self.assertIs(lotus_chain_batch._source_binding, results._source_binding)

    def test_moved_module_and_native_consumers_do_not_import_core(self):
        paths = [Path(results.__file__), *[
            CODE / "src/baselines/text/frameworks" / (name + ".py")
            for name in ("lotus_chain_results", "lotus_chain_batch", "ray_data_lotus_chain", "daft_lotus_chain")]]
        for path in paths:
            for node in ast.walk(ast.parse(path.read_text())):
                names = ([node.module or ""] if isinstance(node, ast.ImportFrom)
                         else [v.name for v in node.names] if isinstance(node, ast.Import) else [])
                for name in names:
                    with self.subTest(path=path.name, dependency=name):
                        self.assertFalse(name.startswith(("src.scheduling", "src.execution_provider", "src.semloom")))


class ResultContractTests(unittest.TestCase):
    def setUp(self):
        self.records, self.chain = fixture()

    def audit(self, records=None, **kwargs):
        return results.ChainBatchResults(self.records if records is None else records, self.chain, **kwargs)

    def test_filter_map_keeps_occurrences_usage_and_declared_output_order(self):
        audit = self.audit()
        decisions = {row.row_id: row.source_position != 1 for row in self.records}
        value = envelope(self.records, self.chain, decisions=decisions)
        value["calls"].reverse()
        value["records"].reverse()
        audit.accept(value)
        audit.verify_success(source_exhausted=True)
        self.assertEqual([audit.pop().row_id, audit.pop().row_id], ["row-2", "row-0"])
        self.assertEqual(len(audit.calls), 5)
        self.assertEqual(sum(c["total_tokens"] for c in audit.calls), 24)

    def test_out_of_order_batches_and_tail_preserve_distinct_repeated_labels(self):
        audit = self.audit()
        for rows, tag in [(self.records[2:], "tail"), (self.records[:2], "prefix")]:
            audit.accept(envelope(rows, self.chain, tag))
        audit.verify_success(source_exhausted=True)
        self.assertEqual([audit.pop().source_position for _ in range(3)], [2, 0, 1])
        self.assertEqual(len(audit.calls), 6)

    def test_empty_source_requires_no_calls_but_still_requires_explicit_eof(self):
        audit = self.audit([])
        audit.verify_success(source_exhausted=True)
        with self.assertRaisesRegex(ValueError, "source exhaustion"):
            audit.verify_success(source_exhausted=False)

    def test_missing_occurrence_and_duplicate_batch_are_not_success(self):
        audit = self.audit()
        value = envelope(self.records[:1], self.chain)
        audit.accept(value)
        with self.assertRaisesRegex(ValueError, "omitted"):
            audit.verify_coverage()
        with self.assertRaisesRegex(ValueError, "duplicate native batch"):
            audit.accept(value)

    def test_changed_input_or_call_identity_and_duplicate_call_are_rejected(self):
        def change_input(v): v["inputs"][0]["record_sha256"] = "changed"
        def change_call(v): v["calls"][0]["call_id"] = "changed"
        def duplicate_call(v): v["calls"].append(deepcopy(v["calls"][0]))
        for mutate in (change_input, change_call, duplicate_call):
            with self.subTest(mutation=mutate.__name__):
                value = envelope(self.records, self.chain)
                mutate(value)
                with self.assertRaises(ValueError):
                    self.audit().accept(value)

    def test_missing_stage_bad_parent_and_changed_final_occurrence_are_rejected(self):
        def missing_stage(v): v["calls"].pop()
        def bad_parent(v): v["calls"][1]["parent_call_ids"] = ["other-call"]
        def bad_final(v): v["records"][0]["source_index"] = "other-label"
        for mutate in (missing_stage, bad_parent, bad_final):
            with self.subTest(mutation=mutate.__name__):
                value = envelope(self.records, self.chain)
                mutate(value)
                audit = self.audit()
                with self.assertRaises(ValueError):
                    audit.accept(value)
                self.assertEqual(audit.calls, value["calls"])

    def test_failure_and_unknown_keep_known_calls_and_never_verify_success(self):
        for status in ("failed", "unknown"):
            with self.subTest(status=status):
                audit = self.audit()
                value = envelope(self.records, self.chain)
                value.update(error_type="FixtureFailure")
                value["calls"][-1]["status"] = status
                with self.assertRaises(results.ChainExecutionError) as caught:
                    audit.accept(value)
                self.assertEqual(caught.exception.chain_calls, value["calls"])
                with self.assertRaisesRegex(ValueError, "failed, unknown"):
                    audit.verify_success(source_exhausted=True)

    def test_raw_duplicates_or_foreign_responses_cannot_pass_success_gate(self):
        for duplicate in (True, False):
            with self.subTest(duplicate=duplicate):
                value = envelope(self.records, self.chain)
                raw = deepcopy(value["calls"][-1])
                value.update(raw_stage_id="summarize", raw_results=[raw, deepcopy(raw)] if duplicate else [{}])
                audit = self.audit()
                audit.accept(value)
                self.assertTrue(audit.unassociated_results)
                with self.assertRaisesRegex(ValueError, "protocol or cleanup"):
                    audit.verify_success(source_exhausted=True)

    def test_recovery_is_idempotent_and_conflicts_remain_visible(self):
        value = envelope(self.records, self.chain)
        audit = self.audit()
        audit.accept(value)
        audit.recover(value)
        self.assertEqual(len(audit.calls), 6)
        conflict = deepcopy(value)
        conflict["calls"][0]["total_tokens"] += 1
        with self.assertRaisesRegex(ValueError, "conflicting recovered"):
            audit.recover(conflict)
        with self.assertRaises(ValueError):
            audit.verify_success(source_exhausted=True)

    def test_cleanup_errors_and_seeded_parent_mismatch_remain_authoritative(self):
        parents = {(r.row_id, r.source_position): ("prior-" + r.row_id,) for r in self.records}
        audit = self.audit(initial_parents=parents)
        value = envelope(self.records, self.chain, parents=parents)
        value["calls"][0]["parent_call_ids"] = []
        with self.assertRaisesRegex(ValueError, "initial parent"):
            audit.accept(value)
        audit = self.audit()
        audit.accept(envelope(self.records, self.chain))
        audit.recover(dict(calls=[], cleanup_errors=["fixture-unconfirmed-close"]))
        self.assertEqual(audit.cleanup_errors, ("fixture-unconfirmed-close",))
        with self.assertRaises(ValueError):
            audit.verify_success(source_exhausted=True)

    def test_native_iterator_consumers_use_the_shared_audit(self):
        from src.baselines.text.frameworks.daft_lotus_chain import _DaftChainRows
        from src.baselines.text.frameworks.ray_data_lotus_chain import _RayChainRows
        value = envelope(self.records, self.chain)
        frame = SimpleNamespace(iter_rows=lambda: iter([{"batch_json": json.dumps(value)}]))
        for cls in (_DaftChainRows, _RayChainRows):
            with self.subTest(consumer=cls.__name__), tempfile.TemporaryDirectory() as control:
                # Graph execution and physical process shutdown are outside this result-contract test.
                with patch.object(cls, "close", return_value={"status": "closed"}):
                    rows = cls(frame, self.records, self.chain, time.monotonic() + 10, None, control)
                    try:
                        self.assertEqual([r.row_id for r in rows], [r.row_id for r in self.records])
                        self.assertEqual(len(rows.calls), 6)
                    finally:
                        rows._stop.set()
                        if rows._watchdog is not None:
                            rows._watchdog.join(timeout=1)

    def test_core_transport_applies_shared_validation_and_releases_only_known_input(self):
        from src.execution_provider.adapters.lotus_record_chain_core import _ChainTransport
        value = envelope(self.records, self.chain)
        task = SimpleNamespace(key=(1, 2), task=SimpleNamespace(payload=json.dumps([
            results.record_row(r) for r in self.records]).encode()))
        transport = object.__new__(_ChainTransport)
        transport._lock, transport._inputs, transport._outcomes = threading.Lock(), {}, {}
        transport.owner = SimpleNamespace(chain=self.chain, relation_barriers=False)
        transport._run = lambda _task: json.dumps(value).encode()
        payload = asyncio.run(transport._execute(task, None))
        self.assertEqual(json.loads(payload), value)
        self.assertEqual(transport._inputs, {})
        value["calls"][0]["call_id"] = "wrong-call"
        with self.assertRaisesRegex(ValueError, "evaluation identity"):
            asyncio.run(transport._execute(task, None))
        self.assertIn(task.key, transport._inputs)


if __name__ == "__main__":
    unittest.main()
