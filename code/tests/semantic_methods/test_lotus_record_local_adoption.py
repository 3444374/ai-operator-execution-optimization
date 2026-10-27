"""Adopt real row-continuation finals without rendering or interpreting twice."""

from copy import deepcopy
from dataclasses import replace
import json
import unittest
from unittest.mock import patch

from src.semantic_methods.lotus import record_chain as chains
from src.semantic_methods.lotus.record_continuation import LotusChainProgram
from src.semantic_methods.lotus.record_results import _source_binding
from src.serving.backends.vllm_engine import GenerationSpec
from tests.semantic_methods import test_lotus_chain_composition as fixture
from tests.semantic_methods import test_lotus_chain_relation_handoff as relation


def committed(run):
    return deepcopy(dict(position=run.position, records=[r.describe() for r in run.current],
        parents=run.parents, previous=run.previous_parents, columns=run.columns,
        calls=run.calls, stats=run.stats, prepared=run.prepared, pending=run.relation_pending))


def row_envelope(section, row, replies):
    """Run the actual continuation callbacks; only model responses are fixtures."""
    program = LotusChainProgram(section.chain, max_result_bytes=65536)
    step = program.start(program.input(row,
        parent_call_ids=section.initial_parents[(row.row_id, row.source_position)],
        columns=section.columns, allow_nulls=section.allow_nulls))
    while hasattr(step, "request"):
        request = json.loads(step.request.payload)
        call = replies.one(request["row"], GenerationSpec.from_dict(request["generation"]))
        step = program.resume(step.state, json.dumps(call).encode())
    return dict(json.loads(step.value), batch_id=row.row_id, inputs=[_source_binding(row)])


def core_envelopes(section, replies):
    """Use production Core/MethodDriver; complete the fake backend out of order."""
    driver, core, backend, pool = fixture.make_driver(section.chain)
    rows, cursor, eof, outputs = section.records, 0, False, []
    by_id = {r.row_id: r for r in rows}
    try:
        for _ in range(500):
            if cursor < len(rows):
                row = rows[cursor]
                if driver.offer_record(row,
                        parent_call_ids=section.initial_parents[(row.row_id, row.source_position)],
                        columns=section.columns, allow_nulls=section.allow_nulls):
                    cursor += 1
            if cursor == len(rows) and not eof:
                driver.end_input()
                eof = True
            driver.advance(driver.session.limits.held_tasks)
            if backend.pending:
                key = max(backend.pending, key=lambda k:
                    json.loads(backend.pending[k][1].task.payload)["row"]["source_position"])
                fixture.complete_one(backend, key, replies)
            for result in driver.results(4):
                output = json.loads(result.value)
                row = by_id[output["calls"][0]["row_id"]]
                outputs.append(dict(output, batch_id=row.row_id, inputs=[_source_binding(row)]))
                driver.release_result(result.row)
            assert core.capacity.usage().active_requests <= 2
            assert pool.used[0] <= 2
            if driver.finished:
                assert core.capacity.usage().held_tasks == 0 and pool.used == (0, 0)
                return outputs
        raise AssertionError("finite Core fixture did not finish")
    finally:
        driver.close()


def finish_sections(run, replies):
    """The test owns finite EOF; Extract still uses the existing relation path."""
    for _ in range(2 * len(run.chain.stages) + 1):
        if run.relation_pending:
            run.complete_relation(source_exhausted=True)
        elif run.position == len(run.chain.stages):
            return run.result()
        elif run.method.stage.kind in ("filter", "map"):
            section = run.delegate_record_local()
            for envelope in core_envelopes(section, replies):
                section.accept(envelope)
            run.adopt_record_local(section, source_exhausted=True)
        else:
            prepared = run.prepare()
            if prepared:
                run.complete(replies(prepared, generation=run.method.stage.generation))
    raise AssertionError("finite section fixture did not finish")


class RecordLocalAdoptionTests(unittest.TestCase):
    def setUp(self):
        import lotus
        scope = lotus.settings.context(enable_cache=False)
        scope.__enter__()
        self.addCleanup(scope.__exit__, None, None, None)

    def test_real_Core_finals_match_existing_full_plan_and_exact_requests(self):
        for pattern in ("FM", "MFFM", "FMEMF", "EMFM", "MFEFM", "FMEEFM"):
            with self.subTest(pattern=pattern):
                plan, rows = relation.relation_chain(pattern), fixture.source_rows(size=5)
                parents = {(r.row_id, r.source_position): ("prior/" + r.row_id,) for r in rows}
                observed, expected = fixture.ResponseFixture(plan), fixture.ResponseFixture(plan)
                actual = finish_sections(plan.start_relation(rows, initial_parents=parents), observed)
                reference = relation.reference(plan, rows, expected, parents=parents)
                self.assertEqual(fixture.normalized(actual), fixture.normalized(reference))
                self.assertEqual(observed.requests, expected.requests)
                self.assertEqual(len(actual["calls"]), len({c["call_id"] for c in actual["calls"]}))

    def test_same_method_objects_and_no_formatter_parser_or_model_replay(self):
        plan, rows = relation.relation_chain("FMEM"), fixture.source_rows()
        run = plan.start_relation(rows)
        with patch.object(chains.LotusRecordMethod, "__init__", side_effect=AssertionError("rebind")):
            section = run.delegate_record_local()
        self.assertTrue(all(a is b for a, b in zip(section.chain.methods, plan.methods[:2], strict=True)))
        replies = fixture.ResponseFixture(plan, all_true=True)
        for row in reversed(section.records):
            section.accept(row_envelope(section, row, replies))
        before = [deepcopy(chains.lm_report(m.lm)) for m in plan.methods]
        with patch.object(chains.LotusRecordMethod, "prepare_rows", side_effect=AssertionError("render")), \
                patch.object(chains.LotusRecordMethod, "interpret_results", side_effect=AssertionError("parse")):
            run.adopt_record_local(section, source_exhausted=True)
        self.assertEqual(before, [chains.lm_report(m.lm) for m in plan.methods])
        self.assertEqual(len(run.calls), 6)
        self.assertEqual(run.position, 2)
        self.assertEqual(run.stats["step_0"], dict(before[0], scope="worker_method_lifetime"))
        self.assertEqual(run.stats["step_1"], dict(before[1], scope="worker_method_lifetime"))
        self.assertEqual(sum(c["total_tokens"] for c in run.calls), 84)
        state = committed(run)
        with self.assertRaisesRegex(ValueError, "already adopted"):
            run.adopt_record_local(section, source_exhausted=True)
        with self.assertRaisesRegex(ValueError, "closed"):
            section.accept({})
        self.assertEqual(state, committed(run))

    def test_duplicate_payload_user_index_and_business_ids_are_not_occurrence_ids(self):
        plan = relation.relation_chain("MM")
        rows = fixture.source_rows(size=3)
        rows = [replace(r, values=deepcopy(rows[0].values), source_index=7) for r in rows]
        replies = fixture.ResponseFixture(plan)
        actual = finish_sections(plan.start_relation(rows), replies)
        self.assertEqual([r.row_id for r in actual["records"]], [r.row_id for r in rows])
        self.assertEqual([r.source_index for r in actual["records"]], [7, 7, 7])
        self.assertTrue(all(r.values["row_id"] == "business-value" for r in actual["records"]))
        self.assertTrue(all(r.values["call_id"] == "business-call" for r in actual["records"]))
        self.assertEqual(fixture.normalized(actual), fixture.normalized(
            relation.reference(plan, rows, fixture.ResponseFixture(plan))))

    def test_repeated_Map_may_overwrite_its_declared_source_column(self):
        base = fixture.chain_for("MM")
        stages = tuple(replace(s, method=replace(s.method, instruction="Rewrite {passage}",
            output_column="passage", resolved_input_columns=("passage",))) for s in base.stages)
        plan = chains.LotusRecordChain(stages, query_id="renamed-plan", relation_barriers=True)
        rows = fixture.source_rows(size=2)
        replies = fixture.ResponseFixture(plan)
        actual = finish_sections(plan.start_relation(rows), replies)
        self.assertEqual([r.values["passage"] for r in actual["records"]],
                         ["derived/step_1/occ-0", "derived/step_1/occ-1"])
        self.assertEqual(actual["columns"], list(rows[0].values))

    def test_generic_fields_query_names_and_sizes_use_the_same_adoption(self):
        for column, size in (("alternate_input", 1), ("内容", 8)):
            with self.subTest(column=column, size=size):
                base = fixture.chain_for("MFMF", column=column, query="unrelated-request-name")
                plan = chains.LotusRecordChain(base.stages, query_id=base.query_id,
                                              relation_barriers=True, _require_bound=True)
                rows = fixture.source_rows(column=column, size=size)
                observed, expected = fixture.ResponseFixture(plan), fixture.ResponseFixture(plan)
                actual = finish_sections(plan.start_relation(rows), observed)
                reference = relation.reference(plan, rows, expected)
                self.assertEqual(fixture.normalized(actual), fixture.normalized(reference))
                self.assertEqual(observed.requests, expected.requests)

    def test_Extract_group_parents_and_dynamic_missing_fields_seed_the_next_view(self):
        plan, rows = relation.relation_chain("EMFEM"), fixture.source_rows()
        class Dynamic(fixture.ResponseFixture):
            def one(self, row, generation):
                result = super().one(row, generation)
                if row["algorithm_stage"] == "step_0":
                    value = json.loads(result["text"])
                    if row["source_position"] == 0:
                        value["optional"] = "present"
                    result["text"] = json.dumps(value)
                return result
        run, replies = plan.start_relation(rows), Dynamic(plan, all_true=True)
        run.complete(replies(run.prepare(), generation=run.method.stage.generation))
        run.complete_relation(source_exhausted=True)
        section = run.delegate_record_local()
        self.assertEqual(section.initial_parents[("occ-0", 0)], section.initial_parents[("occ-1", 1)])
        self.assertEqual(len(section.initial_parents[("occ-0", 0)]), 2)
        self.assertIsNone(section.records[2].values["optional"])
        self.assertTrue(section.allow_nulls)
        for output in core_envelopes(section, replies):
            section.accept(output)
        run.adopt_record_local(section, source_exhausted=True)
        actual = finish_sections(run, replies)
        self.assertEqual(fixture.normalized(actual), fixture.normalized(
            relation.reference(plan, rows, Dynamic(plan, all_true=True))))

    def test_empty_and_all_filtered_sections_advance_only_with_successful_EOF(self):
        for rows in ([], fixture.source_rows()):
            with self.subTest(rows=len(rows)):
                plan, run = relation.relation_chain("FMEM"), None
                run = plan.start_relation(rows)
                class Reject(fixture.ResponseFixture):
                    def one(self, row, generation):
                        result = super().one(row, generation)
                        if row["algorithm_stage"] == "step_0":
                            result["text"] = "False"
                        return result
                replies = Reject(plan)
                section = run.delegate_record_local()
                for output in core_envelopes(section, replies):
                    section.accept(output)
                before = committed(run)
                with self.assertRaisesRegex(ValueError, "source exhaustion"):
                    run.adopt_record_local(section, source_exhausted=False)
                self.assertEqual(committed(run), before)
                run.adopt_record_local(section, source_exhausted=True)
                self.assertEqual(run.position, 2)
                self.assertIn("derived_1", run.columns)
                actual = finish_sections(run, replies)
                self.assertEqual(actual["records"], [])
                self.assertEqual(len(actual["calls"]), len(rows))
                self.assertIn("derived_3", actual["columns"])

    def test_role_switch_is_a_view_break_and_another_model_is_explicitly_rejected(self):
        base = fixture.chain_for("MMF")
        stages = tuple(replace(s, model_role="helper") if i else s for i, s in enumerate(base.stages))
        plan = chains.LotusRecordChain(stages, query_id=base.query_id, relation_barriers=True)
        run, replies = plan.start_relation(fixture.source_rows(size=1)), fixture.ResponseFixture(plan, all_true=True)
        section = run.delegate_record_local()
        self.assertEqual(len(section.chain.stages), 1)
        section.accept(row_envelope(section, section.records[0], replies))
        run.adopt_record_local(section, source_exhausted=True)
        section = run.delegate_record_local()
        self.assertEqual([s.model_role for s in section.chain.stages], ["helper", "helper"])
        changed = (base.stages[0], replace(base.stages[1], method=replace(base.stages[1].method,
            model_name="openai/other-model")), base.stages[2])
        multi = chains.LotusRecordChain(changed, query_id="different", relation_barriers=True)
        state = multi.start_relation(fixture.source_rows())
        with self.assertRaisesRegex(ValueError, "one model deployment"):
            state.delegate_record_local()
        self.assertEqual(state.position, 0)

    def test_one_active_path_and_Extract_pause_are_explicit(self):
        plan, rows = relation.relation_chain("FEM"), fixture.source_rows()
        run, replies = plan.start_relation(rows), fixture.ResponseFixture(plan)
        section = run.delegate_record_local()
        for action in (run.prepare, lambda: run.complete([]), run.delegate_record_local, run.result, run.snapshot):
            with self.subTest(action=action), self.assertRaises(ValueError):
                action()
        for row in section.records:
            section.accept(row_envelope(section, row, replies))
        run.adopt_record_local(section, source_exhausted=True)
        with self.assertRaisesRegex(ValueError, "not a record-local"):
            run.delegate_record_local()
        run.complete(replies(run.prepare(), generation=run.method.stage.generation))
        with self.assertRaisesRegex(ValueError, "already owns"):
            run.delegate_record_local()
        run.complete_relation(source_exhausted=True)
        self.assertEqual(len(run.delegate_record_local().chain.stages), 1)

    def test_missing_coverage_and_foreign_section_cannot_partially_commit(self):
        plan, rows = relation.relation_chain("FM"), fixture.source_rows(size=2)
        run, replies = plan.start_relation(rows), fixture.ResponseFixture(plan, all_true=True)
        section = run.delegate_record_local()
        section.accept(row_envelope(section, section.records[0], replies))
        before = committed(run)
        with self.assertRaisesRegex(ValueError, "omitted expected"):
            run.adopt_record_local(section, source_exhausted=True)
        other = plan.start_relation(rows).delegate_record_local()
        with self.assertRaisesRegex(ValueError, "foreign"):
            run.adopt_record_local(other, source_exhausted=True)
        self.assertEqual(committed(run), before)
        section.accept(row_envelope(section, section.records[1], replies))
        run.adopt_record_local(section, source_exhausted=True)
        self.assertEqual(len(run.calls), 4)

    def test_invalid_identity_parent_duplicate_and_fields_leave_query_uncommitted(self):
        def duplicate(envelope):
            envelope["calls"].append(deepcopy(envelope["calls"][0]))
        mutations = {
            "call identity": lambda e: e["calls"][0].update(call_id="foreign"),
            "parent": lambda e: e["calls"][0].update(parent_call_ids=["foreign"]),
            "source": lambda e: e["inputs"][0].update(record_sha256="wrong"),
            "duplicate": duplicate,
            "untouched field": lambda e: e["records"][0]["values"].update(call_id="corrupted"),
            "extra field": lambda e: e["records"][0]["values"].update(extra="corrupted"),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                plan = relation.relation_chain("FM")
                run = plan.start_relation(fixture.source_rows(size=1))
                section = run.delegate_record_local()
                envelope = row_envelope(section, section.records[0], fixture.ResponseFixture(plan, all_true=True))
                mutate(envelope)
                before = committed(run)
                try:
                    section.accept(envelope)
                except ValueError:
                    pass
                with self.assertRaises(ValueError):
                    run.adopt_record_local(section, source_exhausted=True)
                self.assertEqual(committed(run), before)

    def test_failed_or_unknown_segment_retains_full_prefix_but_never_advances(self):
        for status in ("failed", "unknown"):
            with self.subTest(status=status):
                plan, rows = relation.relation_chain("EMF"), fixture.source_rows(size=2)
                run, replies = plan.start_relation(rows), fixture.ResponseFixture(plan)
                run.complete(replies(run.prepare(), generation=run.method.stage.generation))
                run.complete_relation(source_exhausted=True)
                section = run.delegate_record_local()
                row = section.records[0]
                program = LotusChainProgram(section.chain, max_result_bytes=65536)
                step = program.start(program.input(row,
                    parent_call_ids=section.initial_parents[(row.row_id, row.source_position)],
                    columns=section.columns, allow_nulls=section.allow_nulls))
                request = json.loads(step.request.payload)
                call = replies.one(request["row"], GenerationSpec.from_dict(request["generation"]))
                call.update(status=status, error_type="FixtureOutcome")
                with self.assertRaises(RuntimeError) as caught:
                    program.resume(step.state, json.dumps(call).encode())
                before = committed(run)
                with self.assertRaises(RuntimeError):
                    section.accept(dict(batch_id=row.row_id, inputs=[_source_binding(row)],
                        calls=caught.exception.chain_calls, stage_stats=caught.exception.chain_stage_stats,
                        columns=caught.exception.chain_columns, records=[], error_type="FixtureOutcome"))
                # An independent row's already-produced final can still be retained.
                # Complete coverage never makes the failed section adoptable.
                section.accept(row_envelope(section, section.records[1], replies))
                section._results.verify_coverage()
                with self.assertRaises(ValueError):
                    run.adopt_record_local(section, source_exhausted=True)
                self.assertEqual(committed(run), before)
                run.failure(caught.exception)
                self.assertEqual(len(run.calls), 5)
                self.assertEqual(sum(c["status"] == status for c in run.calls), 1)
                self.assertEqual(sum(c["total_tokens"] for c in run.calls), 70)
                self.assertEqual(run.position, 1)
                self.assertEqual(len(replies.requests), 5)
                self.assertFalse(any('"step_2"' in call and '"occ-0"' in call for call in replies.requests))
                with self.assertRaisesRegex(ValueError, "has failed"):
                    run.prepare()

    def test_cancel_retains_accepted_facts_once_without_finishing_the_section(self):
        plan = relation.relation_chain("FM")
        run = plan.start_relation(fixture.source_rows(size=2))
        section = run.delegate_record_local()
        section.accept(row_envelope(section, section.records[0], fixture.ResponseFixture(plan, all_true=True)))
        before = committed(run)
        error = InterruptedError("fixture cancel")
        run.failure(error)
        self.assertEqual(run.position, before["position"])
        self.assertEqual([r.describe() for r in run.current], before["records"])
        self.assertEqual(len(run.calls), 2)
        run.failure(error)
        self.assertEqual(len(run.calls), 2)
        restored = chains.ChainRun.restore(plan, json.loads(json.dumps(run.snapshot())))
        for action in (run.prepare, run.result, lambda: run.adopt_record_local(section, source_exhausted=True),
                       restored.delegate_record_local):
            with self.assertRaisesRegex(ValueError, "has failed|live relation"):
                action()


if __name__ == "__main__":
    unittest.main()
