"""One semantic state pauses at Extract; existing owners still execute work."""

from copy import deepcopy
from dataclasses import replace
import json
import unittest

from src.semantic_methods.lotus import record_chain as chains
from src.semantic_methods.lotus.record_continuation import LotusChainProgram
from src.semantic_methods.lotus.record_relation import complete_extract_relation
from tests.semantic_methods import test_lotus_chain_composition as fixture


def relation_chain(pattern):
    original = fixture.chain_for(pattern)
    return chains.LotusRecordChain(original.stages, query_id=original.query_id,
                                  relation_barriers=True, _require_bound=True)


def reference(chain, records, infer, *, parents=None, columns=None):
    calls, stats, current = [], {}, list(records)
    for segment in chain.relation_segments():
        result = segment.run_batch(current, infer, initial_parents=parents, columns=columns,
                                  allow_nulls=parents is not None)
        calls.extend(result["calls"])
        stats.update(result["stage_stats"])
        current, columns = result["records"], result["columns"]
        if segment.stages[-1].kind == "extract":
            completed = complete_extract_relation(current, columns=columns,
                extract_stage=segment.stages[-1], query_id=chain.query_id,
                calls=calls, source_exhausted=True)
            current, columns, parents = list(completed.records), list(completed.columns), completed.parent_call_ids
    return dict(records=current, columns=columns, calls=calls, stage_stats=stats)


def finish(run, infer):
    for _ in range(2 * len(run.chain.stages) + 1):
        prepared = run.prepare()
        if prepared:
            run.complete(infer(prepared, generation=run.method.stage.generation))
        elif run.relation_pending:
            # This fixture owns the complete finite relation, including EOF.
            run.complete_relation(source_exhausted=True)
        else:
            return run.result()
    raise AssertionError("finite relation fixture did not finish")


class RelationHandoffTests(unittest.TestCase):
    def setUp(self):
        import lotus
        scope = lotus.settings.context(enable_cache=False)
        scope.__enter__()
        self.addCleanup(scope.__exit__, None, None, None)

    def test_compositions_match_existing_segment_execution(self):
        for pattern in ("E", "EF", "EM", "FEM", "MEEF", "MEFE", "EFM", "MFEFM"):
            with self.subTest(pattern=pattern):
                chain, rows = relation_chain(pattern), fixture.source_rows()
                parents = {(r.row_id, r.source_position): ("upstream/" + r.row_id,) for r in rows}
                infer, expected_infer = fixture.ResponseFixture(chain), fixture.ResponseFixture(chain)
                result = finish(chain.start_relation(rows, initial_parents=parents), infer)
                expected = reference(chain, rows, expected_infer, parents=parents)
                self.assertEqual(fixture.normalized(result), fixture.normalized(expected))
                self.assertEqual(infer.requests, expected_infer.requests)
                self.assertEqual(len({c["call_id"] for c in result["calls"]}), len(result["calls"]))

    def test_pause_requires_owner_EOF_and_cannot_publish_before_assembly(self):
        chain = relation_chain("EM")
        run = chain.start_relation(fixture.source_rows())
        infer = fixture.ResponseFixture(chain)
        run.complete(infer(run.prepare(), generation=run.method.stage.generation))
        self.assertTrue(run.relation_pending)
        before = deepcopy(run.snapshot())
        self.assertEqual(run.prepare(), [])
        with self.assertRaisesRegex(ValueError, "unfinished Extract relation"):
            run.result()
        with self.assertRaisesRegex(ValueError, "successful source EOF"):
            run.complete_relation(source_exhausted=False)
        self.assertEqual(before, run.snapshot())
        completed = run.complete_relation(source_exhausted=True)
        self.assertFalse(run.relation_pending)
        self.assertEqual([r.values["field_0"] for r in completed.records], ["value-1", "value-1", "value-2"])
        with self.assertRaisesRegex(ValueError, "no Extract relation"):
            run.complete_relation(source_exhausted=True)
        next_rows = run.prepare()
        self.assertEqual({r["algorithm_stage"] for r in next_rows}, {"step_1"})
        self.assertEqual(len(run.parents[("occ-0", 0)]), 2)
        self.assertNotIn("derived_1", completed.columns)

    def test_pending_relation_survives_json_state_handoff_without_reexecuting_Extract(self):
        chain = relation_chain("EME")
        run, infer = chain.start_relation(fixture.source_rows()), fixture.ResponseFixture(chain)
        run.complete(infer(run.prepare(), generation=run.method.stage.generation))
        frozen = json.loads(json.dumps(run.snapshot()))
        resumed = chains.ChainRun.restore(chain, frozen)
        self.assertTrue(resumed.relation_pending)
        self.assertEqual(resumed.prepare(), [])
        resumed.complete_relation(source_exhausted=True)
        result = finish(resumed, infer)
        self.assertEqual(len(result["calls"]), 9)
        self.assertEqual(len(infer.requests), 9)
        self.assertTrue(all(not r.index_updates for r in result["records"]))

    def test_empty_and_all_filtered_relations_still_cross_Extract_explicitly(self):
        for rows in ([], fixture.source_rows()):
            with self.subTest(rows=len(rows)):
                chain = relation_chain("FMEM")
                base = fixture.ResponseFixture(chain)
                def all_filtered(prepared, *, generation):
                    result = base(prepared, generation=generation)
                    if chain.stages[0].stage_id == prepared[0]["algorithm_stage"]:
                        for call in result:
                            call["text"] = "False"
                    return result
                run = chain.start_relation(rows)
                while run.prepare():
                    run.complete(all_filtered(run.prepared, generation=run.method.stage.generation))
                self.assertTrue(run.relation_pending)
                self.assertIn("derived_1", run.columns)
                self.assertNotIn("derived_3", run.columns)
                run.complete_relation(source_exhausted=True)
                result = finish(run, all_filtered)
                self.assertEqual(result["records"], [])
                self.assertIn("derived_3", result["columns"])
                self.assertEqual(len(result["calls"]), len(rows))

    def test_dynamic_keys_None_and_missing_follow_original_relation_assembly(self):
        chain = relation_chain("EM")
        class Dynamic(fixture.ResponseFixture):
            def one(self, row, generation):
                result = super().one(row, generation)
                if row["algorithm_stage"] == "step_0":
                    value = json.loads(result["text"])
                    if row["source_position"] == 0:
                        value["optional"] = "earlier"
                    elif row["source_position"] == 1:
                        value["optional"] = None
                    else:
                        value["late_key"] = "last"
                    result["text"] = json.dumps(value)
                return result
        rows = fixture.source_rows()
        run, infer = chain.start_relation(rows), Dynamic(chain)
        run.complete(infer(run.prepare(), generation=run.method.stage.generation))
        completed = run.complete_relation(source_exhausted=True)
        # Pinned LOTUS converts parsed JSON null to the text "None". A globally
        # present but absent key is filled with actual None by the assembler.
        self.assertEqual([r.values["optional"] for r in completed.records], ["None", "None", None])
        self.assertEqual([r.values["late_key"] for r in completed.records], [None, None, "last"])
        actual = finish(run, infer)
        self.assertEqual(fixture.normalized(actual), fixture.normalized(reference(chain, rows, Dynamic(chain))))

    def test_default_fused_FIFO_and_old_state_shape_remain_unchanged(self):
        chain = relation_chain("EM")
        with self.assertRaisesRegex(ValueError, "relation barrier"):
            chain.start_batch(fixture.source_rows())
        with self.assertRaisesRegex(ValueError, "relation barrier"):
            chain.run_batch(fixture.source_rows(), lambda *_a, **_kw: self.fail("inference must not start"))
        with self.assertRaisesRegex(ValueError, "Filter/Map"):
            LotusChainProgram(chain, max_result_bytes=65536)
        ordinary = fixture.chain_for("FM")
        with self.assertRaisesRegex(ValueError, "explicit relation_barriers"):
            ordinary.start_relation(fixture.source_rows())
        state = ordinary.start_batch(fixture.source_rows()).snapshot()
        self.assertNotIn("relation_execution", state)
        restored = chains.ChainRun.restore(ordinary, json.loads(json.dumps(state)))
        self.assertFalse(restored.relation_pending)
        self.assertEqual(state, restored.snapshot())

    def test_unknown_or_failed_Extract_cannot_create_a_successful_handoff(self):
        for status in ("failed", "unknown"):
            with self.subTest(status=status):
                chain = relation_chain("EM")
                run = chain.start_relation(fixture.source_rows(size=1))
                result = fixture.ResponseFixture(chain)(run.prepare(), generation=run.method.stage.generation)
                result[0].update(status=status, error_type="FixtureOutcome")
                with self.assertRaises(RuntimeError) as error:
                    run.complete(result)
                run.failure(error.exception)
                self.assertEqual(error.exception.chain_calls[0]["status"], status)
                self.assertEqual(error.exception.chain_calls[0]["total_tokens"], 14)
                for action in (run.prepare, run.result, lambda: run.complete_relation(source_exhausted=True)):
                    with self.assertRaisesRegex(ValueError, "has failed"):
                        action()

    def test_cancelled_relation_preserves_known_calls_and_cannot_resume_from_snapshot(self):
        chain = relation_chain("EM")
        run, infer = chain.start_relation(fixture.source_rows()), fixture.ResponseFixture(chain)
        run.complete(infer(run.prepare(), generation=run.method.stage.generation))
        calls = deepcopy(run.calls)
        cancelled = InterruptedError("fixture query cancel")
        run.failure(cancelled)
        self.assertEqual(run.calls, calls)
        self.assertEqual(cancelled.chain_calls, calls)
        restored = chains.ChainRun.restore(chain, json.loads(json.dumps(run.snapshot())))
        with self.assertRaisesRegex(ValueError, "has failed"):
            restored.complete_relation(source_exhausted=True)
        self.assertEqual(restored.calls, calls)

    def test_new_state_matches_independent_oracle_from_one_serialized_plan(self):
        from tests.semantic_methods import test_lotus_chain_composition_oracle as oracle
        plan, rows = oracle.relation_plan(), fixture.source_rows()
        wire = json.loads(json.dumps(plan.describe()))
        bound = chains.LotusRecordChain.from_dict(wire)
        observed, expected_replies = oracle.Replies(bound), oracle.Replies(plan)
        actual = finish(bound.start_relation(rows), observed)
        expected = oracle.relation_oracle(plan, rows, expected_replies)
        self.assertEqual(fixture.normalized(actual), fixture.normalized(expected))
        self.assertEqual(observed.requests, expected_replies.requests)
        self.assertEqual(wire, json.loads(json.dumps(bound.describe())))

    def test_unknown_later_Extract_preserves_all_prior_plan_receipts(self):
        from tests.semantic_methods import test_lotus_chain_composition_oracle as oracle
        plan, rows = oracle.relation_plan(), fixture.source_rows()
        replies, run = oracle.Replies(plan), plan.start_relation(rows)
        def incomplete(prepared, *, generation):
            values = replies(prepared, generation=generation)
            if prepared[0]["algorithm_stage"] == "step_2":
                values[0].update(status="unknown", error_type="FixtureUnknown")
            return values
        with self.assertRaisesRegex(RuntimeError, "unknown") as caught:
            finish(run, incomplete)
        run.failure(caught.exception)
        self.assertEqual(len(caught.exception.chain_calls), 9)
        self.assertEqual({c["algorithm_stage"] for c in caught.exception.chain_calls},
                         {"step_0", "step_1", "step_2"})
        self.assertEqual(sum(c["status"] == "unknown" for c in caught.exception.chain_calls), 1)
        self.assertFalse(any('step_3' in call for call in replies.requests))
        self.assertEqual(sum(c["total_tokens"] for c in caught.exception.chain_calls), 9 * 14)
        with self.assertRaisesRegex(ValueError, "has failed"):
            run.complete_relation(source_exhausted=True)

    def test_relation_source_order_and_occurrence_identity_are_explicit(self):
        chain, rows = relation_chain("EM"), fixture.source_rows()
        for values, reason in ((list(reversed(rows)), "increasing source_position"),
                ([rows[0], replace(rows[1], source_position=0)], "increasing source_position"),
                ([rows[0], replace(rows[1], row_id=rows[0].row_id)], "globally unique row_id")):
            with self.subTest(reason=reason), self.assertRaisesRegex(ValueError, reason):
                chain.start_relation(values)


if __name__ == "__main__":
    unittest.main()
