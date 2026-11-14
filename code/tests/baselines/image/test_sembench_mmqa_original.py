"""Original visual Join/Extract with real LOTUS and fixed CPU answers only."""

import base64
from copy import deepcopy
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import patch

from src.baselines.image.sembench_mmqa import describe_inputs, evaluate_original, run_original_query


CHECKOUT = os.environ.get("SEMLOOM_NATIVE_TEST_CHECKOUT")
READY = bool(CHECKOUT and Path(CHECKOUT).is_dir() and importlib.util.find_spec("lotus"))
GOLD_IMAGE = "117d500aaa630023c4038b8268b309c0.png"


@unittest.skipUnless(READY, "requires LOTUS and the pinned SEMLOOM_NATIVE_TEST_CHECKOUT")
class OriginalMMQAImageTests(unittest.TestCase):
    def setUp(self):
        import pandas as pd
        from PIL import Image

        temporary = tempfile.TemporaryDirectory(prefix="sembench-mmqa-image-fixture-")
        self.addCleanup(temporary.cleanup)
        self.data = Path(temporary.name)
        (self.data / "images").mkdir()
        Image.new("RGB", (5, 3), (0, 0, 255)).save(self.data / "images" / GOLD_IMAGE)
        Image.new("RGB", (5, 3), (255, 0, 0)).save(self.data / "images" / "other.jpg")
        (self.data / "images" / "ignored.PNG").write_text("not an original selected extension")
        pd.DataFrame({"ID": [0, 5, 6, 9, 10], "Track": ["software racetrack"] * 5}).to_csv(
            self.data / "ap_warrior.csv", index=False)
        connect = socket.socket.connect

        def offline(sock, address):
            if sock.family in (socket.AF_INET, socket.AF_INET6):
                self.fail("visual CPU fixture attempted a network connection")
            return connect(sock, address)

        guard = patch.object(socket.socket, "connect", offline)
        guard.start()
        self.addCleanup(guard.stop)
        aliases = ("src.runner", "src.runner.generic_lotus_runner",
                   "src.runner.generic_lotus_runner.generic_lotus_runner")
        before = {name: sys.modules.get(name) for name in aliases}
        self.addCleanup(lambda: self.assertEqual({name: sys.modules.get(name) for name in aliases}, before))

    def test_reference_extract_preparation_uses_original_formatter_and_preserves_all_occurrences(self):
        from lotus.sem_ops.sem_join import SemJoinDataframe
        from .prepare_mmqa_extract_stage import prepare

        original = SemJoinDataframe.__call__
        output = self.data / 'component-preparation'
        report = prepare(CHECKOUT, self.data, output)
        self.assertIs(SemJoinDataframe.__call__, original)
        self.assertEqual(report['software_Join_responses'], 10)
        self.assertEqual(report['proposed_Extract_model_calls'], 5)
        self.assertEqual(report['reference_pairs'], [[i, GOLD_IMAGE] for i in (0, 5, 6, 9, 10)])
        captured = report['original_Extract']
        self.assertEqual(captured['generation']['max_completion_tokens'], 8192)
        self.assertEqual(captured['generation']['response_format'], {'type': 'json_object'})
        self.assertEqual(len(captured['messages']), 5)
        self.assertEqual(len({json.dumps(m, sort_keys=True) for m in captured['messages']}), 1)
        self.assertTrue(all('The color of the logo in the image' in json.dumps(m)
                            for m in captured['messages']))
        self.assertEqual(report['model_calls'], 0)
        self.assertFalse(report['completed_query'])
        self.assertFalse(report['full_query_model_qualified'])
        self.assertIsNone(report['query_quality'])
        self.assertEqual((output / 'extract-stage.json').stat().st_mode & 0o777, 0o600)

    def test_reference_extract_preparation_rejects_missing_reference_pairs_and_restores_sdk(self):
        import pandas as pd
        from lotus.sem_ops.sem_join import SemJoinDataframe
        from .prepare_mmqa_extract_stage import prepare

        original = SemJoinDataframe.__call__
        pd.DataFrame(dict(ID=[99], Track=['not a reference pair'])).to_csv(
            self.data / 'ap_warrior.csv', index=False)
        output = self.data / 'must-not-exist'
        with self.assertRaisesRegex(ValueError, 'no official positive reference pairs'):
            prepare(CHECKOUT, self.data, output)
        self.assertIs(SemJoinDataframe.__call__, original)
        self.assertFalse(output.exists())

    def _run(self, query, *, none_match=False, extract_text='{"color":"blue"}'):
        import lotus
        from lotus.models import LM
        from litellm.types.utils import ModelResponse
        from PIL import Image

        lm = LM("openai/local-image-fixture", max_tokens=8192, max_batch_size=64)
        batches = []
        self.last_batches = batches

        def respond(data, options, *_args):
            messages = [deepcopy(message) for message, _ in data]
            batches.append((messages, deepcopy(options)))
            replies = []
            for index, message in enumerate(messages):
                parts = [part for item in message if isinstance(item["content"], list)
                         for part in item["content"] if part["type"] == "image_url"]
                self.assertEqual(len(parts), 1)
                uri = parts[0]["image_url"]["url"]
                self.assertTrue(uri.startswith("data:image/png;base64,"))
                with Image.open(io.BytesIO(base64.b64decode(uri.split(",", 1)[1]))) as image:
                    self.assertEqual(image.size, (5, 3))
                    blue = image.convert("RGB").getpixel((0, 0)) == (0, 0, 255)
                if options.get("response_format") == {"type": "json_object"}:
                    text = extract_text
                else:
                    text = "Reasoning:\nsoftware answer\nAnswer: " + str(blue and not none_match)
                replies.append(ModelResponse(model=lm.model, id="software-" + str(index),
                    choices=[dict(index=0, finish_reason="stop", message=dict(role="assistant", content=text))],
                    usage=dict(prompt_tokens=0, completion_tokens=0, total_tokens=0)))
            return replies

        lm._process_uncached_messages = respond
        with lotus.settings.context(lm=lm, enable_cache=False):
            result = run_original_query(CHECKOUT, self.data, query)
        return result, batches

    def test_q2a_calls_every_pair_and_uses_original_gold(self):
        result, batches = self._run("Q2a")
        self.assertEqual([len(messages) for messages, _ in batches], [10])
        self.assertEqual(list(result.columns), ["ID", "image_id"])
        self.assertEqual(set(result["ID"]), {0, 5, 6, 9, 10})
        self.assertEqual(set(result["image_id"]), {GOLD_IMAGE})
        self.assertEqual(evaluate_original(CHECKOUT, "2a", result)["original_metric"]["f1_score"], 1)

    def prepared_image_join(self, query_id):
        import pandas as pd
        from PIL import Image
        from src.baselines.text.sembench_text import SemBenchTextQuery
        from src.baselines.text.sembench_prepared import prepare
        from src.serving.backends.vllm_protocol import GenerationSpec

        if query_id == '7':
            pd.DataFrame(dict(Airlines=['DomesticOnly', 'OtherAirline', 'OtherAirline'],
                              Destinations=['Chicago', 'London', 'Berlin'])).to_csv(
                                  self.data / 'tampa_international_airport.csv', index=False)
        expected, batches = self._run(query_id)
        query = SemBenchTextQuery(CHECKOUT, self.data, 'mmqa', query_id,
            execution_id='image-join-'+query_id, model_name='openai/local-image-fixture',
            generation=GenerationSpec(max_tokens=8192, stop=()), image_join=True)
        lm, prepared, requests = prepare(query)
        responses = []
        for row in requests:
            parts = [part for message in row['messages'] if isinstance(message['content'], list)
                     for part in message['content'] if part['type'] == 'image_url']
            self.assertEqual(len(parts), 1)
            with Image.open(io.BytesIO(base64.b64decode(parts[0]['image_url']['url'].split(',', 1)[1]))) as image:
                blue = image.convert('RGB').getpixel((0, 0)) == (0, 0, 255)
            responses.append(dict(call_id=row['call_id'], generated_text=
                'Reasoning:\nsoftware answer\nAnswer: ' + str(blue)))
        return query, lm, prepared, requests, responses, expected, batches[0][0]

    def test_prepared_image_joins_keep_original_cot_pngs_order_relations_and_gold(self):
        from src.baselines.text.sembench_prepared import finish, result_value

        for query_id in ('2a', '7'):
            with self.subTest(query=query_id):
                query, lm, prepared, requests, responses, expected, messages = self.prepared_image_join(query_id)
                self.assertEqual([row['messages'] for row in requests], messages)
                self.assertEqual(query.operation.strategy.name, 'ZS_COT')
                self.assertEqual(len(requests), 10 if query_id == '2a' else 6)
                self.assertEqual(len({row['call_id'] for row in requests}), len(requests))
                self.assertTrue(query.describe()['visual_inputs']['exact_join_candidates'])
                sinks = []
                result, semantic = finish(query, lm, prepared, responses[::-1],
                    execute_query=query.bind_execution(), on_result_ready=lambda frame:sinks.append(frame))
                self.assertEqual(result_value(result), result_value(expected))
                self.assertEqual(len(sinks), 1)
                self.assertEqual(query.evaluate(result), evaluate_original(CHECKOUT, query_id, expected))
                self.assertTrue(semantic['native_join_inputs'][0]['all_declared_columns_selected'])

    def test_native_ray_image_join_driver_retains_complete_sink_before_close_and_exact_budget(self):
        from src.baselines.text.sembench_prepared import result_value
        from src.experiments import native_ray_sembench as driver
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.cell_budget import CellBudgetLedger
        from src.serving.backends.vllm_protocol import EngineSpec
        from tests.baselines.text.ray_native_fixtures import recorded_processor

        for query_id in ('2a', '7'):
            with self.subTest(query=query_id):
                query, _, _, requests, responses, expected, messages = self.prepared_image_join(query_id)
                root = self.data / ('ray-driver-'+query_id); root.mkdir()
                budget = dict(path=str(root/'budget.sqlite'),id='image-join',limit=len(requests),unit_id='query')
                ledger = CellBudgetLedger.create(Path(budget['path']),
                    AttemptBudget(budget['id'],budget['limit']),deadline_utc=None)
                ledger.reserve_unit('query',len(requests))
                config = dict(profile={},budget=budget,
                    ray_native=dict(engine_kwargs={},processor_options=dict(prepare_image_stage=True)))
                engine = EngineSpec(model_path='CPU-no-model',model_python='unused',driver_python='unused')
                def reply(row, request):
                    return responses[int(row['call_id'].rsplit('-', 1)[1])]['generated_text']
                with recorded_processor(reply) as sdk:
                    report = driver.run_query(config,query,engine,None,root)
                self.assertEqual(report['status'],'completed')
                self.assertEqual(report['result'],result_value(expected))
                self.assertEqual([row['messages'] for row in sdk.requests],messages)
                self.assertTrue(sdk.configs[0]['prepare_image_stage'])
                self.assertEqual(report['physical_attempts_reserved'],len(requests))
                self.assertEqual(report['native_observation']['unknown_attempts'],[])
                self.assertLessEqual(report['sink_ready_ns'],report['native']['events']['close_started_ns'])
                self.assertFalse(sdk.initialized)

    def test_image_query_requires_native_image_stage_and_known_minimum_budget(self):
        from src.baselines.text.sembench_text import SemBenchTextQuery
        from src.experiments.sembench_text_query import check, query_from_config
        from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec

        generation = GenerationSpec(max_tokens=8192, stop=())
        base = dict(scenario='mmqa', query='2a', query_id='image-stage-check',
            model_name='openai/local-image-fixture', backend='ray-data', profile={})
        config = dict(base, ray_native=dict(engine_kwargs={},processor_options=dict(prepare_image_stage=True)))
        query = query_from_config(config, CHECKOUT, self.data, generation)
        engine = EngineSpec(model_path='/tmp/unused', model_python='/tmp/unused',
            driver_python='/tmp/unused', max_model_calls=10)
        self.assertEqual(check(config, query, engine)['query']['expected_model_calls'], 10)
        for backend in ('daft', 'imlane', 'lotus-native'):
            with self.subTest(backend=backend), self.assertRaises(ValueError):
                check(dict(base,backend=backend),query,engine)
        with self.assertRaisesRegex(ValueError, 'native Ray image'):
            check(dict(base,ray_native=dict(processor_options={})),query,engine)
        with self.assertRaises(ValueError):
            query_from_config(base, CHECKOUT, self.data, generation)
        mixed = query_from_config(dict(config, query='2b'), CHECKOUT, self.data, generation)
        self.assertIsNone(mixed.describe()['expected_model_calls'])
        self.assertEqual(mixed.describe()['minimum_model_calls'], 10)
        self.assertEqual(mixed.describe()['maximum_model_calls'], 20)
        self.assertEqual(check(dict(config, query='2b'), mixed, engine)['status'], 'checked_cpu_only')
        too_small = EngineSpec(model_path='/tmp/unused', model_python='/tmp/unused',
            driver_python='/tmp/unused', max_model_calls=9)
        with self.assertRaisesRegex(ValueError, 'max_model_calls'):
            check(dict(config, query='2b'), mixed, too_small)

    def test_native_ray_query_runs_extract_on_actual_join_matches_or_empty_path(self):
        from PIL import Image
        from src.baselines.text.sembench_prepared import result_value
        from src.baselines.text.sembench_text import SemBenchTextQuery
        from src.experiments.native_ray_sembench import run_query
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.cell_budget import CellBudgetLedger
        from src.serving.backends.vllm_protocol import EngineSpec, GenerationSpec
        from tests.baselines.text.ray_native_fixtures import recorded_processor

        for none_match in (False, True):
            with self.subTest(none_match=none_match):
                if none_match:
                    with self.assertRaises(KeyError):
                        self._run('2b', none_match=True)
                    reference_batches = self.last_batches
                else:
                    expected, reference_batches = self._run('2b')
                query = SemBenchTextQuery(CHECKOUT, self.data, 'mmqa', '2b',
                    execution_id='complete-image-chain', model_name='openai/local-image-fixture',
                    generation=GenerationSpec(max_tokens=8192, stop=()), image_join=True)
                root = self.data / ('full-query-empty' if none_match else 'full-query-nonempty'); root.mkdir()
                budget = dict(path=str(root/'budget.sqlite'), id='full-query', limit=10 if none_match else 15, unit_id='query')
                ledger = CellBudgetLedger.create(Path(budget['path']), AttemptBudget(budget['id'], budget['limit']), deadline_utc=None)
                ledger.reserve_unit('query', budget['limit'])
                config = dict(profile={}, budget=budget,
                    ray_native=dict(engine_kwargs={}, processor_options=dict(prepare_image_stage=True)))
                engine = EngineSpec(model_path='CPU-no-model', model_python='unused', driver_python='unused')

                def answer(row, request):
                    if request['sampling_params'].get('structured_outputs') == {'json_object': True}:
                        return '{"color":"blue"}'
                    parts = [part for message in request['messages'] if isinstance(message['content'], list)
                             for part in message['content'] if part['type'] == 'image_url']
                    with Image.open(io.BytesIO(base64.b64decode(parts[0]['image_url']['url'].split(',', 1)[1]))) as image:
                        blue = image.convert('RGB').getpixel((0, 0)) == (0, 0, 255)
                    return 'Reasoning:\nsoftware answer\nAnswer: ' + str(blue and not none_match)

                with recorded_processor(answer) as sdk:
                    if none_match:
                        with self.assertRaises(KeyError) as caught:
                            run_query(config, query, engine, None, root)
                        report = caught.exception.query_report
                    else:
                        report = run_query(config, query, engine, None, root)
                if none_match:
                    self.assertEqual(report['status'], 'failed')
                    self.assertNotIn('result', report)
                    self.assertNotIn('quality', report)
                    self.assertIsNone(report.get('sink_ready_ns'))
                else:
                    self.assertEqual(report['result'], result_value(expected))
                    self.assertEqual(report['quality'], query.evaluate(expected))
                    self.assertLess(report['sink_ready_ns'], report['native']['events']['close_started_ns'])
                self.assertEqual(report['physical_attempts_reserved'], 10 if none_match else 15)
                self.assertEqual(sdk.events.count('init'), 1)
                self.assertEqual(sdk.events.count('close'), 1)
                self.assertEqual(len({row['call_id'] for row in sdk.requests}), len(sdk.requests))
                actual_messages = [row['messages'] for row in sdk.requests]
                self.assertEqual(actual_messages, [message for messages, options in reference_batches for message in messages])
                batches = json.loads((root/'native-lm-batches.json').read_text())
                self.assertEqual([len(batch['requests']) for batch in batches if batch['requests']],
                                 [10] if none_match else [10, 5])
                if not none_match:
                    self.assertEqual(batches[1]['sampling_params']['response_format'], {'type': 'json_object'})
                    native = json.loads((root/'native-predictions.json').read_text())
                    self.assertEqual(native['batches'][1]['native_sampling_params']['structured_outputs'], {'json_object': True})
                self.assertFalse(sdk.initialized)

    def test_shared_operator_callback_consumes_real_followup_relation_and_keeps_stage_ids(self):
        import lotus
        from src.baselines.text.sembench_prepared import make_query_lm, result_value
        from src.baselines.text.sembench_text import SemBenchTextQuery
        from src.serving.backends.vllm_protocol import GenerationSpec

        expected, reference = self._run('2b')
        query = SemBenchTextQuery(CHECKOUT, self.data, 'mmqa', '2b', execution_id='operator-chain',
            model_name='openai/local-image-fixture', generation=GenerationSpec(max_tokens=8192, stop=()), image_join=True)
        stages = []

        def predict(requests, *, sampling_params):
            batch = len(stages) - 1
            self.assertEqual([row['messages'] for row in requests], reference[batch][0])
            answers = []
            for row in requests:
                if batch:
                    answers.append('{"color":"blue"}')
                    continue
                parts = [part for message in row['messages'] if isinstance(message['content'], list)
                         for part in message['content'] if part['type'] == 'image_url']
                from PIL import Image
                with Image.open(io.BytesIO(base64.b64decode(parts[0]['image_url']['url'].split(',', 1)[1]))) as image:
                    blue = image.convert('RGB').getpixel((0, 0)) == (0, 0, 255)
                answers.append('Reasoning:\nsoftware answer\nAnswer: ' + str(blue))
            return dict(predictions=[dict(call_id=row['call_id'], generated_text=text)
                for row, text in zip(requests, answers, strict=True)][::-1])

        lm, batches = make_query_lm(query, predict)

        def execute(operation, invoke):
            stages.append(operation)
            return dict(frame=invoke(), native_owner='software fixture')

        with lotus.settings.context(lm=lm, enable_cache=False):
            result, reports = query.bind_execution(multiple=True)(execute)
        self.assertEqual(result_value(result), result_value(expected))
        self.assertEqual([op.describe()['kind'] for op in stages], ['join', 'extract'])
        self.assertEqual([len(op.records) for op in stages], [10, 5])
        ids = [record.row_id for op in stages for record in op.records]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertTrue(all(record.row_id.startswith('operator-chain-operator-1-') for record in stages[1].records))
        self.assertEqual(len(reports), 2)
        self.assertEqual([len(batch['requests']) for batch in batches], [10, 5])

    def test_shared_operator_failure_preserves_completed_reports_and_current_failure(self):
        import lotus
        from src.baselines.text.sembench_prepared import make_query_lm
        from src.baselines.text.sembench_text import SemBenchTextQuery
        from src.serving.backends.vllm_protocol import GenerationSpec

        query = SemBenchTextQuery(CHECKOUT, self.data, 'mmqa', '2b', execution_id='failing-chain',
            model_name='openai/local-image-fixture', generation=GenerationSpec(), image_join=True)
        completed = []
        lm, _ = make_query_lm(query, lambda requests, **kw: dict(predictions=[dict(
            call_id=row['call_id'], generated_text='Reasoning:\nsoftware answer\nAnswer: True') for row in requests]))

        def execute(operation, invoke):
            if operation.describe()['kind'] == 'extract':
                error = RuntimeError('software second-stage failure')
                error.operator_report = dict(status='failed', kind='extract')
                raise error
            report = dict(frame=invoke(), status='completed')
            completed.append(report)
            return report

        with lotus.settings.context(lm=lm, enable_cache=False), self.assertRaises(RuntimeError) as caught:
            query.bind_execution(multiple=True)(execute)
        self.assertEqual(caught.exception.query_operator_reports, completed)
        self.assertEqual(caught.exception.operator_report, dict(status='failed', kind='extract'))

    def test_prepared_image_join_detects_changed_pixels_before_native_execution(self):
        from PIL import Image
        from src.baselines.text.sembench_text import SemBenchTextQuery
        from src.serving.backends.vllm_protocol import GenerationSpec

        query = SemBenchTextQuery(CHECKOUT, self.data, 'mmqa', '2a', execution_id='image-identity',
            model_name='openai/local-image-fixture', generation=GenerationSpec(), image_join=True)
        Image.new('RGB', (5, 3), (0, 255, 0)).save(self.data / 'images' / GOLD_IMAGE)
        with self.assertRaisesRegex(ValueError, 'visual input changed'):
            query.bind_execution()

    def test_ray_worker_rejects_images_changed_since_checked_snapshot_before_budget_claim(self):
        from PIL import Image
        from src.experiments import native_ray_sembench as driver
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.cell_budget import CellBudgetLedger
        from src.serving.backends.vllm_protocol import EngineSpec

        query, _, _, requests, _, _, _ = self.prepared_image_join('2a')
        root = self.data/'changed-image-worker';root.mkdir()
        budget = dict(path=str(root/'budget.sqlite'),id='changed-image',limit=len(requests),unit_id='query')
        ledger = CellBudgetLedger.create(Path(budget['path']),
            AttemptBudget(budget['id'],budget['limit']),deadline_utc=None)
        ledger.reserve_unit('query',len(requests))
        config = dict(profile={},budget=budget,
            ray_native=dict(engine_kwargs={},processor_options=dict(prepare_image_stage=True)))
        Image.new('RGB',(5,3),(0,255,0)).save(self.data/'images'/GOLD_IMAGE)
        with self.assertRaisesRegex(ValueError,'fresh source differs'):
            driver.run_query(config,query,EngineSpec(model_path='unused',model_python='unused',
                driver_python='unused'),None,root,
                scope=lambda *args,**kwargs:self.fail('changed image allocated native graph'))
        self.assertEqual(ledger.snapshot()['units'][0]['claimed'],0)

    def test_all_visual_joins_request_native_zero_shot_cot(self):
        import pandas as pd
        from lotus.templates.task_instructions import cot_prompt_formatter

        (self.data / "images" / "other.jpg").unlink()
        pd.DataFrame({"ID": [0], "Track": ["software racetrack"]}).to_csv(
            self.data / "ap_warrior.csv", index=False)
        pd.DataFrame({"Airlines": ["software airline"], "Destinations": ["London"]}).to_csv(
            self.data / "tampa_international_airport.csv", index=False)
        expected = cot_prompt_formatter(answer_instructions="The answer should be either True or False")
        for query in ("2a", "2b", "7"):
            with self.subTest(query=query):
                _, batches = self._run(query)
                for message in batches[0][0]:
                    self.assertIn(expected, message[0]["content"])

    def test_native_join_receives_enum_and_restores_its_accessor_on_success_and_failure(self):
        from lotus.sem_ops.sem_extract import SemExtractDataFrame
        from lotus.sem_ops.sem_join import SemJoinDataframe
        from lotus.types import ReasoningStrategy

        original, seen = SemJoinDataframe.__call__, []
        original_extract, extract_rows = SemExtractDataFrame.__call__, []

        def observe(accessor, *args, **kwargs):
            seen.append(kwargs["strategy"])
            return original(accessor, *args, **kwargs)

        def observe_extract(accessor, *args, **kwargs):
            extract_rows.append(len(accessor._obj))
            return original_extract(accessor, *args, **kwargs)

        with (patch.object(SemJoinDataframe, "__call__", observe),
              patch.object(SemExtractDataFrame, "__call__", observe_extract)):
            self._run("2a")
            self.assertIs(SemJoinDataframe.__call__, observe)
            with self.assertRaises(KeyError):
                self._run("2b", none_match=True)
            self.assertIs(SemExtractDataFrame.__call__, observe_extract)
            with self.assertRaises(KeyError):
                self._run("2b", extract_text="{}")
            self.assertIs(SemJoinDataframe.__call__, observe)
            self.assertIs(SemExtractDataFrame.__call__, observe_extract)
        self.assertIs(SemJoinDataframe.__call__, original)
        self.assertIs(SemExtractDataFrame.__call__, original_extract)
        self.assertEqual(seen, [ReasoningStrategy.ZS_COT] * 3)
        self.assertEqual(extract_rows, [0, 5])

    def test_q2b_runs_join_then_extract_for_every_actual_match(self):
        result, batches = self._run("2b")
        self.assertEqual([len(messages) for messages, _ in batches], [10, 5])
        self.assertNotIn("response_format", batches[0][1])
        self.assertEqual(batches[1][1]["response_format"], {"type": "json_object"})
        self.assertEqual(list(result.columns), ["ID", "image_id", "color"])
        self.assertEqual(result["color"].tolist(), ["blue"] * 5)
        self.assertEqual(evaluate_original(CHECKOUT, "2b", result)["original_metric"]["f1_score"], 1)
        # The repeated image remains five Extract occurrences, not one cached answer.
        self.assertEqual(len({json.dumps(message, sort_keys=True) for message in batches[1][0]}), 1)

    def test_no_q2a_matches_returns_original_empty_schema(self):
        result, batches = self._run("2a", none_match=True)
        self.assertEqual([len(messages) for messages, _ in batches], [10])
        self.assertTrue(result.empty)
        self.assertEqual(list(result.columns), ["ID", "image_id"])

    def test_q2b_missing_color_is_not_repaired_after_native_parse(self):
        with self.assertRaises(KeyError):
            self._run("2b", extract_text="{}")

    def test_q2b_no_matches_keeps_native_missing_color_error_without_extract_requests(self):
        from lotus.sem_ops.sem_extract import SemExtractDataFrame

        original_extract = SemExtractDataFrame.__call__
        with self.assertRaises(KeyError):
            self._run("2b", none_match=True)
        batches = self.last_batches
        self.assertEqual([len(messages) for messages, _ in batches], [10, 0])
        self.assertEqual(batches[1][1]["response_format"], {"type": "json_object"})
        self.assertIs(SemExtractDataFrame.__call__, original_extract)

    def test_q7_preserves_original_all_airline_join_and_separate_gold(self):
        import pandas as pd
        from PIL import Image

        (self.data / "images" / GOLD_IMAGE).unlink()
        airport_image = "cwncgabxti09zmf36t4phvaz1xzv10wu.png"
        Image.new("RGB", (5, 3), (0, 0, 255)).save(self.data / "images" / airport_image)
        pd.DataFrame({"Airlines": ["British Airways", "software domestic airline"],
                      "Destinations": ["London", "Chicago"]}).to_csv(
                          self.data / "tampa_international_airport.csv", index=False)
        result, batches = self._run("7")
        self.assertEqual([len(messages) for messages, _ in batches], [4])
        self.assertEqual(list(result.columns), ["Airlines", "image_id"])
        # The pinned query does not apply the natural question's Europe filter.
        self.assertEqual(set(result["Airlines"]), {"British Airways", "software domestic airline"})
        metric = evaluate_original(CHECKOUT, "7", result)["original_metric"]
        self.assertEqual(metric["precision"], 0.5)
        self.assertEqual(metric["recall"], 0.2)
        plan = describe_inputs(CHECKOUT, self.data, "7")
        self.assertEqual(plan["exact_join_candidates"], 4)
        self.assertEqual(plan["maximum_model_calls"], 4)
        self.assertIn("Europe", plan["original_source_difference"])

    def test_plan_uses_original_directory_selection_and_full_join_budget(self):
        names = ["other.jpg", GOLD_IMAGE, "ignored.PNG"]
        with patch("os.listdir", return_value=names):
            report = describe_inputs(CHECKOUT, self.data, "Q2b")
        self.assertEqual([Path(image["path"]).name for image in report["images"]], names[:2])
        self.assertEqual(report["exact_join_candidates"], 10)
        self.assertEqual(report["maximum_model_calls"], 20)
        self.assertIsNone(report["actual_followup_extracts"])
        self.assertEqual(report["model_calls"], 0)
        for image in report["images"]:
            self.assertEqual((image["width"], image["height"]), (5, 3))
            self.assertEqual(len(image["rgb_sha256"]), 64)

    def test_check_reports_pending_visual_providers_without_claiming_execution(self):
        from src.experiments.sembench_image_query import check

        report = check(CHECKOUT, "mmqa", "2a", data_dir=self.data)
        self.assertFalse(report["execution_enabled"])
        self.assertEqual(report["requests_claimed"], 0)
        self.assertEqual(report["inputs"]["maximum_model_calls"], 10)
        self.assertTrue(all(value is None for value in report["results"].values()))
        self.assertIn('image Join wired', report['baseline_states']['ray-data'])
        mixed = check(CHECKOUT, 'mmqa', '2b', data_dir=self.data)
        self.assertIn('Q2b follows actual Join matches', mixed['baseline_states']['ray-data'])

    def test_check_cli_writes_private_report_and_keeps_source_bytes(self):
        from src.experiments.sembench_image_query import main

        before = {str(path): path.read_bytes() for path in self.data.rglob("*") if path.is_file()}
        output = self.data / "check-artifact"
        with patch("sys.stdout", new=io.StringIO()):
            status = main(["--sembench-checkout", CHECKOUT, "--scenario", "mmqa", "--query", "2b",
                           "--data-dir", str(self.data), "--output-dir", str(output)])
        self.assertEqual(status, 0)
        report = json.loads((output / "check.json").read_text())
        self.assertEqual(report["inputs"]["maximum_model_calls"], 20)
        self.assertEqual({name: Path(name).read_bytes() for name in before}, before)

    def test_unsupported_visual_query_is_rejected_before_data_access(self):
        with self.assertRaisesRegex(ValueError, "Q2a, Q2b and Q7"):
            describe_inputs(CHECKOUT, self.data / "does-not-exist", "q8")
