"""Existing chain combinations through LOTUS/Core; inference is a CPU fixture."""

from copy import deepcopy
from dataclasses import asdict, replace
from itertools import product
import json
import unittest
from unittest.mock import patch

from src.semantic_methods.budget import MethodBudgetPool, MethodCapacity, row_reservation
from src.semantic_methods.continuation import MethodLimits
from src.semantic_methods.lotus import record_chain as chains
from src.semantic_methods.lotus.record_continuation import LotusChainProgram
from src.semantic_methods.lotus.record_relation import complete_extract_relation
from src.semantic_methods.lotus.record_results import ChainBatchResults, _source_binding
from src.execution_provider.adapters.lotus_record_chain_fifo import LotusChainFIFO
from src.scheduling.core.session_contract import SessionSpec, TaskProfile
from src.serving.backends.vllm_engine import GenerationSpec, IDENTITY_FIELDS
from tests.scheduling.test_incremental_session import setup


def chain_for(pattern, *, module=chains, column="passage", query="opaque-flow", role="main"):
    stages, field = [], column
    for i, kind in enumerate(pattern):
        name = "step_" + str(i)
        if kind == "F":
            method = module.FilterMethod("openai/cpu-fixture", "Accept {" + field + "}")
            generation = GenerationSpec(max_tokens=8)
        elif kind == "M":
            output = "derived_" + str(i)
            method = module.MapMethod("openai/cpu-fixture", "Rewrite {" + field + "}", output)
            generation = GenerationSpec(max_tokens=32, stop=())
            field = output
        else:
            output = "field_" + str(i)
            method = module.ExtractMethod("openai/cpu-fixture", (field,), {output: "A text field"})
            generation = GenerationSpec(max_tokens=48, stop=(), response_format={"type": "json_object"})
            field = output
        stages.append(module.StageSpec(name, method, generation, model_role=role))
    return module.LotusRecordChain(tuple(stages), query_id=query, relation_barriers="E" in pattern[:-1])


def source_rows(*, module=chains, column="passage", size=3):
    return [module.RecordOccurrence("occ-" + str(i), i, 7 if i < 2 else 19,
        {column: "text " * (i + 1), "row_id": "business-value", "call_id": "business-call"},
        source_refs={"source": {"position": i}}) for i in range(size)]


class ResponseFixture:
    """Declared responses only; no model, timer, dataset label or production policy."""

    def __init__(self, chain, *, all_true=False):
        self.chain, self.all_true, self.requests = chain, all_true, {}
        self.stage_by_id = {s.stage_id: s for s in chain.stages}

    def one(self, row, generation):
        stage = self.stage_by_id[row["algorithm_stage"]]
        self.requests[row["call_id"]] = (row["messages_json"], asdict(generation))
        i = int(stage.stage_id.rsplit("_", 1)[1])
        if stage.kind == "filter":
            text = str(self.all_true or (row["source_position"] + i) % 3 != 1)
        elif stage.kind == "map":
            text = "derived/" + stage.stage_id + "/" + row["row_id"]
        else:
            text = json.dumps({next(iter(stage.method.output_columns)): "value-" + str(row["source_position"])})
        return dict({k: row[k] for k in IDENTITY_FIELDS}, status="succeeded", text=text,
            prompt_tokens=11, completion_tokens=3, total_tokens=14, finish_reason="stop",
            engine_request_id="fixture/" + row["call_id"], submission_confirmed=True, logprobs_json="null")

    def __call__(self, prepared, *, generation):
        # Out-of-order completion is legal; method-owned identity restores association.
        return [self.one(row, generation) for row in reversed(prepared)]


def normalized(result):
    names = (*IDENTITY_FIELDS, "parent_call_ids", "status", "text", "decision",
             "prompt_tokens", "completion_tokens", "total_tokens")
    return dict(records=sorted([r.describe() if hasattr(r, "describe") else r for r in result["records"]],
                              key=lambda r: r["source_position"]),
        columns=result["columns"], calls=sorted([{k: c[k] for k in names if k in c} for c in result["calls"]],
                                               key=lambda c: c["call_id"]))


def make_driver(chain, *, slots=2, live_rows=2):
    core, old, backend, _clock = setup(held_tasks=slots, active_requests=slots, active_work=slots,
        input_bytes=slots * 65536, item_input_bytes=65536,
        result_bytes=slots * 65536, item_result_bytes=65536, metadata_bytes=4096)
    old.close()
    session = core.open(SessionSpec("chain", "flow", "fixture",
        task_profiles=tuple(TaskProfile(s.stage_id, "fixture") for s in chain.stages)))
    limits = MethodLimits(65536, 131072, 131072, len(chain.stages))
    pool = MethodBudgetPool(MethodCapacity(live_rows, live_rows * row_reservation(limits)))
    driver = LotusChainFIFO(session, chain, limits, pool.allocate(pool.capacity))
    return driver, core, backend, pool


def complete_one(backend, key, fixture):
    task = backend.pending[key][1]
    value = json.loads(task.task.payload)
    result = fixture.one(value["row"], GenerationSpec.from_dict(value["generation"]))
    backend.complete(key, json.dumps(result).encode())


def run_driver(chain, rows):
    driver, core, backend, pool = make_driver(chain)
    fixture = ResponseFixture(chain)
    outputs, cursor, eof, backpressure = [], 0, False, False
    try:
        for _ in range(300):
            if cursor < len(rows):
                if driver.offer_record(rows[cursor]):
                    cursor += 1
                else:
                    backpressure = True
            if cursor == len(rows) and not eof:
                driver.end_input()
                eof = True
            driver.advance(driver.session.limits.held_tasks)
            if backend.pending:
                # CPU fixture deliberately returns the latest available occurrence first.
                key = max(backend.pending, key=lambda k: json.loads(backend.pending[k][1].task.payload)["row"]["source_position"])
                complete_one(backend, key, fixture)
            for result in driver.results(4):
                outputs.append(json.loads(result.value))
                driver.release_result(result.row)
            assert core.capacity.usage().active_requests <= 2
            assert pool.used[0] <= 2
            if driver.finished:
                assert core.capacity.usage().held_tasks == 0 and pool.used == (0, 0)
                break
        else:
            raise AssertionError("finite fixture did not finish")
        calls = [c for out in outputs for c in out["calls"]]
        final = [r for out in outputs for r in out["records"]]
        columns = outputs[0]["columns"] if outputs else list(dict.fromkeys(
            [k for r in rows for k in r.values] + [s.method.output_column for s in chain.stages if s.kind == "map"]))
        audit = ChainBatchResults(rows, chain)
        for i, (row, out) in enumerate(zip(sorted(rows, key=lambda r: r.source_position),
                sorted(outputs, key=lambda x: x["calls"][0]["source_position"]), strict=True)):
            audit.accept(dict(out, batch_id=str(i), inputs=[_source_binding(row)]))
        audit.verify_success(source_exhausted=True)
        return dict(records=final, calls=calls, columns=columns), fixture.requests, backpressure
    finally:
        driver.close()


class CompositionAcceptance(unittest.TestCase):
    def setUp(self):
        import lotus
        scope = lotus.settings.context(enable_cache=False)
        scope.__enter__()
        self.addCleanup(scope.__exit__, None, None, None)

    def test_all_F_M_sequences_through_four_stages_match_fused(self):
        for length in range(1, 5):
            for sequence in product("FM", repeat=length):
                pattern = "".join(sequence)
                with self.subTest(pattern=pattern):
                    chain, rows = chain_for(pattern), source_rows()
                    reference = ResponseFixture(chain)
                    fused = chain.run_batch(rows, reference)
                    actual, requests, _ = run_driver(chain_for(pattern), rows)
                    self.assertEqual(normalized(actual), normalized(fused))
                    self.assertEqual(requests, reference.requests)
                    self.assertEqual(len({c["call_id"] for c in actual["calls"]}), len(actual["calls"]))

    def test_renaming_size_and_longer_sequence_do_not_choose_another_strategy(self):
        for column, size, pattern in (("other_field", 1, "MFMF"), ("内容", 8, "FMFMFM")):
            with self.subTest(column=column, size=size):
                chain, rows = chain_for(pattern, column=column, query="different-name"), source_rows(column=column, size=size)
                actual, _, backpressure = run_driver(chain, rows)
                expected = chain.run_batch(rows, ResponseFixture(chain))
                self.assertEqual(normalized(actual), normalized(expected))
                if size > 2:
                    self.assertTrue(backpressure)

    def test_a_legal_successor_can_submit_while_an_unrelated_filter_is_pending(self):
        chain, rows = chain_for("FM"), source_rows(size=2)
        driver, core, backend, pool = make_driver(chain)
        self.addCleanup(driver.close)
        fixture = ResponseFixture(chain, all_true=True)
        for row in rows:
            self.assertTrue(driver.offer_record(row))
        driver.end_input()
        for _ in range(8):
            driver.advance(driver.session.limits.held_tasks)
            if len(backend.pending) == 2:
                break
        first = min(backend.pending, key=lambda k: k.sequence)
        complete_one(backend, first, fixture)
        for _ in range(8):
            driver.advance(driver.session.limits.held_tasks)
            pending = [json.loads(v[1].task.payload)["row"] for v in backend.pending.values()]
            if any(p["algorithm_stage"] == "step_1" for p in pending):
                break
        self.assertEqual({(p["row_id"], p["algorithm_stage"]) for p in pending},
                         {("occ-0", "step_1"), ("occ-1", "step_0")})
        driver.close()
        self.assertEqual(pool.used, (0, 0))
        self.assertEqual(core.capacity.usage().held_tasks, 2)
        for key in list(backend.pending):
            complete_one(backend, key, fixture)
        core.reap(driver.session.limits.held_tasks)
        self.assertEqual(core.capacity.usage().held_tasks, 0)

    def test_repeated_map_can_read_and_replace_the_same_declared_column(self):
        stages = tuple(chains.StageSpec("step_" + str(i), chains.MapMethod(
            "openai/cpu-fixture", "Rewrite {passage}", "passage"),
            GenerationSpec(max_tokens=32, stop=())) for i in range(2))
        chain = chains.LotusRecordChain(stages, query_id="opaque-flow")
        rows = source_rows(size=1)
        actual, requests, _ = run_driver(chain, rows)
        self.assertEqual(actual["records"][0]["values"]["passage"], "derived/step_1/occ-0")
        second = next(v[0] for k, v in requests.items() if 'step_1' in k)
        self.assertIn("derived/step_0/occ-0", second)
        self.assertEqual(normalized(actual), normalized(chain.run_batch(rows, ResponseFixture(chain))))

    def test_map_first_and_repeated_stages_reach_actual_actor_and_CPU_IPC(self):
        # Reuse the existing prepared model owner. Its Filter-only preparation
        # configuration is never used to render these chain requests.
        from tests.execution_provider import test_lotus_chain_token_budget as fixture
        for pattern, role in (("M", "main"), ("MF", "main"), ("MM", "helper"), ("FFMF", "main")):
            with self.subTest(pattern=pattern, role=role):
                harness = fixture.ChainTokens()
                harness.setUp()
                try:
                    runtime = harness.make(W=1000)
                    runtime.driver.close()
                    chain = chain_for(pattern, role=role)
                    budget = harness.budget(1000, chain.stages)
                    runtime.backend.token_budget = budget
                    runtime.session = runtime.core.open(SessionSpec("prepared-" + pattern, "chain", "fixture",
                        work_unit="tokens", task_profiles=tuple(TaskProfile(s.stage_id, "fixture") for s in chain.stages)))
                    limits = MethodLimits(65536, 131072, 131072, len(chain.stages))
                    runtime.driver = LotusChainFIFO(runtime.session, chain, limits,
                        runtime.method_budget.allocate(runtime.method_budget.capacity), token_budget=budget)
                    try:
                        submitted, original_request = {}, runtime.backend._request
                        def capture_request(task):
                            value = original_request(task)
                            submitted[value["row"]["call_id"]] = deepcopy(value)
                            return value
                        row = source_rows(size=1)[0]
                        with patch.object(runtime.backend, "_request", side_effect=capture_request):
                            self.assertTrue(runtime.driver.offer_record(row))
                            runtime.driver.end_input()
                            outputs = harness.finish(runtime)
                        self.assertEqual(len(runtime.receipts), len(pattern))
                        self.assertEqual(len({c["engine_uid"] for _, c in runtime.receipts}), 1)
                        self.assertEqual({c["model_role"] for _, c in runtime.receipts}, {role})
                        by_id = {c["call_id"]: c for _, c in runtime.receipts}
                        def replay(prepared, *, generation):
                            for item in prepared:
                                call = by_id[item["call_id"]]
                                actual = submitted[item["call_id"]]
                                self.assertEqual(actual["row"]["messages_json"], item["messages_json"])
                                self.assertEqual(GenerationSpec.from_dict(actual["generation"]), generation)
                                # A second formatting pass has fresh preparation timestamps.
                                self.assertEqual(call["token_estimate"], budget.estimate(actual["row"], generation))
                                self.assertEqual(call["fixture_generation"], generation.kwargs())
                            return [by_id[item["call_id"]] for item in prepared]
                        expected = chain.run_batch([row], replay)
                        self.assertEqual(normalized(outputs[0]), normalized(expected))
                        self.assertEqual(runtime.core.capacity.usage().active_work, 0)
                        self.assertEqual(runtime.method_budget.used, (0, 0))
                    finally:
                        runtime.driver.close()
                finally:
                    harness.doCleanups()

    def test_extract_multiple_models_and_roles_are_not_FIFO_programs(self):
        for pattern in ("E", "FE", "EF", "MEM"):
            with self.subTest(pattern=pattern), self.assertRaisesRegex(ValueError, "Filter/Map"):
                LotusChainProgram(chain_for(pattern), max_result_bytes=65536)
        original = chain_for("FM")
        for field, value in (("model_role", "helper"), ("model", "openai/another-model")):
            changed = (replace(original.stages[1], model_role=value) if field == "model_role" else
                       replace(original.stages[1], method=replace(original.stages[1].method, model_name=value)))
            chain = chains.LotusRecordChain((original.stages[0], changed), query_id="opaque-flow")
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "one"):
                LotusChainProgram(chain, max_result_bytes=65536)

    def test_repeated_operator_occurrences_require_distinct_stage_ids_and_real_columns(self):
        first = chain_for("F").stages[0]
        with self.assertRaisesRegex(ValueError, "distinct stage_id"):
            chains.LotusRecordChain((first, first), query_id="query")
        chain = chain_for("MF", column="missing")
        with self.assertRaisesRegex(ValueError, "present text input"):
            chain.run_batch(source_rows(), ResponseFixture(chain))
        chain = chain_for("F")
        empty = chain.run_batch([], ResponseFixture(chain))
        self.assertEqual((empty["records"], empty["calls"]), ([], []))

    def test_failed_or_unknown_map_never_prepares_its_successor(self):
        chain, row = chain_for("MF"), source_rows(size=1)[0]
        for status in ("failed", "unknown"):
            program = LotusChainProgram(chain, max_result_bytes=65536)
            step = program.start(program.input(row))
            value = json.loads(step.request.payload)
            result = ResponseFixture(chain).one(value["row"], GenerationSpec.from_dict(value["generation"]))
            result.update(status=status, error_type="FixtureFailure", submission_confirmed=True)
            with self.subTest(status=status), self.assertRaises(RuntimeError) as failure:
                program.resume(step.state, json.dumps(result).encode())
            self.assertEqual([c["algorithm_stage"] for c in failure.exception.chain_calls], ["step_0"])
            self.assertEqual(failure.exception.chain_calls[0]["status"], status)

    def test_nonterminal_extract_runs_only_as_complete_relation_segments(self):
        for pattern in ("E", "FE", "EF", "EM", "FEM", "EE", "MEFE"):
            with self.subTest(pattern=pattern):
                chain, original = chain_for(pattern), source_rows()
                current, parents, columns = original, None, None
                for segment in chain.relation_segments():
                    result = segment.run_batch(list(current), ResponseFixture(segment),
                        initial_parents=parents, columns=columns, allow_nulls=parents is not None)
                    audit = ChainBatchResults(current, segment, initial_parents=parents, columns=columns)
                    audit.accept(dict(result, records=[r.describe() for r in result["records"]],
                        inputs=[_source_binding(r) for r in current], batch_id=segment.stages[0].stage_id))
                    audit.verify_success(source_exhausted=True)
                    current, columns = result["records"], result["columns"]
                    if segment.stages[-1].kind == "extract":
                        completed = complete_extract_relation(current, columns=columns,
                            extract_stage=segment.stages[-1], query_id=chain.query_id,
                            calls=result["calls"], source_exhausted=True)
                        current, parents, columns = completed.records, completed.parent_call_ids, completed.columns
                        self.assertTrue(all(not r.index_updates for r in current))
                self.assertIsNotNone(current)
                if "E" in pattern[:-1]:
                    with self.assertRaisesRegex(ValueError, "relation barrier"):
                        chain.run_batch(original, ResponseFixture(chain))

    def test_extract_duplicate_index_writers_and_dynamic_schema_prevent_early_publication(self):
        chain, rows = chain_for("E"), source_rows()
        result = chain.run_batch(rows, ResponseFixture(chain))
        audit = ChainBatchResults(rows, chain)
        partial = {k: v for k, v in result.items() if k not in ("records", "calls")}
        audit.accept(dict(partial, records=[result["records"][0].describe()],
            calls=[result["calls"][0]], inputs=[_source_binding(rows[0])], batch_id="partial"))
        with self.assertRaisesRegex(ValueError, "omitted"):
            audit.verify_success(source_exhausted=True)
        complete = complete_extract_relation(result["records"], columns=result["columns"],
            extract_stage=chain.stages[0], query_id=chain.query_id, calls=result["calls"], source_exhausted=True)
        self.assertEqual([r.values["field_0"] for r in complete.records], ["value-1", "value-1", "value-2"])
        self.assertEqual(len(complete.parent_call_ids[("occ-0", 0)]), 2)
        with self.assertRaisesRegex(ValueError, "successful source EOF"):
            complete_extract_relation(result["records"], columns=result["columns"],
                extract_stage=chain.stages[0], query_id=chain.query_id, calls=result["calls"], source_exhausted=False)


if __name__ == "__main__":
    unittest.main()
