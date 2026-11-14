"""Original LOTUS visual queries through native SDK HTTP, with no service/model.

Only HTTPTransport.handle_request is replaced by HTTPX MockTransport. Original
LM batching/parsing and the real POST budget/event observer remain in use.
"""

import base64
from collections import Counter
from contextlib import contextmanager, redirect_stdout
import csv
import hashlib
from io import BytesIO, StringIO
import importlib.util
import itertools
import json
import os
from pathlib import Path
import re
import socket
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from src.baselines.image import sembench_animals as animals
from src.infrastructure.private_artifacts import content_digest, write_private_json


CHECKOUT = os.environ.get("SEMLOOM_NATIVE_TEST_CHECKOUT")
HAS_SDK = all(importlib.util.find_spec(name) is not None for name in
    ("lotus", "litellm", "httpx", "pandas", "PIL", "duckdb", "cdlib", "overrides", "scipy"))
KEY_ENV = "SEMLOOM_IMAGE_FIXTURE_KEY"
KEY_VALUE = "test-key"


class NativeHTTPReplies:
    def __init__(self, case, scenario, *, mode="success", failure=None, response_model=None, query='Q12'):
        self.case, self.scenario = case, scenario
        self.mode, self.failure, self.response_model = mode, failure, response_model
        self.query = query
        self.requests, self.lock = [], threading.Lock()

    @staticmethod
    def image_uris(body):
        return [part["image_url"]["url"] for message in body["messages"]
            if isinstance(message["content"], list) for part in message["content"]
            if part["type"] == "image_url"]

    def respond(self, request):
        import httpx
        from PIL import Image

        self.case.assertEqual(request.method, "POST")
        self.case.assertEqual(str(request.url), "http://localhost:18473/v1/chat/completions")
        self.case.assertEqual(request.headers["authorization"], "Bearer " + KEY_VALUE)
        body = json.loads(request.content)
        self.case.assertEqual(body["model"], "local-image-fixture")
        uris = self.image_uris(body)
        extract = body.get("response_format") == {"type": "json_object"}
        native_join = self.scenario == 'ecomm' and 'first product' in json.dumps(body).lower()
        if self.scenario == "ecomm" and self.query == 'Q12' and not extract:
            self.case.assertFalse(uris, "the original first Filter uses text only")
        elif not native_join:
            self.case.assertTrue(uris, "original visual requests must contain their actual image")
        self.case.assertTrue(all(uri.startswith("data:image/png;base64,") for uri in uris))
        self.case.assertNotIn("REFERENCE_ONLY_", json.dumps(body))
        self.case.assertEqual(body.get("max_tokens", body.get("max_completion_tokens")), 32)
        if self.scenario == "mmqa" and body.get("response_format") != {"type": "json_object"}:
            from lotus.templates.task_instructions import cot_prompt_formatter
            expected = cot_prompt_formatter(answer_instructions="The answer should be either True or False")
            self.case.assertIn(expected, body["messages"][0]["content"])
        self.case.assertEqual(body.get("stop", []), [])
        pixels = []
        for uri in uris:
            with Image.open(BytesIO(base64.b64decode(uri.partition(",")[2]))) as image:
                self.case.assertEqual(image.format, "PNG")
                self.case.assertEqual(image.size, (3, 2))
                pixels.append(image.convert("RGB").getpixel((0, 0)))
        with self.lock:
            request_id = "chatcmpl-fixture-" + str(len(self.requests))
            self.requests.append(dict(body=body, payload=request.content))
        if self.failure == "timeout":
            raise httpx.ReadTimeout("software fixture transport timeout", request=request)
        if self.failure == "api":
            return httpx.Response(503, request=request,
                json={"error": {"message": "software fixture API failure", "type": "server_error"}})
        if self.scenario == "ecomm":
            text_parts = [message['content'] if isinstance(message['content'], str) else
                '\n'.join(part['text'] for part in message['content'] if part['type'] == 'text')
                for message in body['messages']]
            content = '\n'.join(text_parts)
            if self.query != 'Q12':
                claim = content.rsplit('Claim:', 1)[-1].lower()
                if self.query == 'Q4':
                    text = '{"category":"Black"}'
                elif self.query == 'Q6':
                    text = 'Bottomwear' if pixels[0] == (0,128,0) else 'Topwear'
                elif self.query in ('Q2','Q8','Q13'):
                    target = (0,0,255) if self.query == 'Q13' else (255,0,0)
                    text = 'True' if pixels[0] == target else 'False'
                elif self.query == 'Q9':
                    text = 'True' if pixels[0] == pixels[1] else 'False'
                elif 'Document 1' in content and 'Document 2' in content:
                    text = 'Document 1'
                elif native_join or len(uris) > 1:
                    text = 'True'
                elif self.query == 'Q14':
                    text = 'True' if 'white socks' not in claim or pixels[0] == (255,0,0) else 'False'
                else:
                    color = ((255,0,0) if 'feet' in claim or 'pair of' in claim else
                             (0,128,0) if 'lower part' in claim else
                             (0,0,255) if 'upper part' in claim else (255,255,0))
                    text = 'True' if pixels[0] == color else 'False'
            elif extract:
                product = int(re.search(r'\[Id\]:\s*«?(\d+)', content, re.IGNORECASE).group(1))
                value = dict(id=product, brand='adidas' if product == 1 else 'puma',
                             category='footwear' if product == 1 else 'apparel')
                text = '{}' if self.mode == 'empty_json' else json.dumps(
                    dict(generated_json=json.dumps(value, separators=(',', ':'))))
            else:
                text = 'True' if self.mode != 'no_matches' and 'keep-' in content else 'False'
        elif extract:
            text = "{}" if self.mode == "empty_json" else '{"color":"red"}'
        elif self.scenario == "animals":
            pixel = pixels[0]
            instruction = json.dumps(body["messages"]).lower()
            keep = pixel in ((0, 0, 255), (255, 255, 0)) if "impala" in instruction else \
                pixel in ((255, 0, 0), (0, 128, 0))
            text = "True" if keep else "False"
        elif self.scenario == 'cars':
            keep = pixels[0] == ((255,0,0) if self.query == 'Q3' else (0,128,0))
            text = 'True' if keep else 'False'
        elif self.scenario == 'medical':
            self.case.assertEqual(len(uris), 2 if self.query == 'Q9' else 1)
            text = 'False' if self.mode == 'no_matches' else 'True'
        else:
            keep = self.mode != 'no_matches' and not (
                self.mode == 'partial_matches' and 'TrackB' in json.dumps(body['messages']))
            text = ("Reasoning:\nThe word True is in this software reasoning, not the decision.\n\nAnswer: False"
                if self.mode == "distracting_reasoning" else
                "Reasoning: deterministic CPU fixture.\nAnswer: " + ("True" if keep else "False"))
        return httpx.Response(200, request=request, json=dict(
            id=request_id, object="chat.completion", created=1,
            model=self.response_model or "local-image-fixture",
            choices=[dict(index=0, finish_reason="stop", message=dict(role="assistant", content=text))],
            usage=dict(prompt_tokens=11, completion_tokens=2, total_tokens=13)))

    @contextmanager
    def install(self):
        import httpx
        transport = httpx.MockTransport(self.respond)

        def handle(_native_transport, request):
            return transport.handle_request(request)

        try:
            with patch.object(httpx.HTTPTransport, "handle_request", handle):
                yield self
        finally:
            transport.close()


@unittest.skipUnless(CHECKOUT and HAS_SDK, "requires the pinned SemBench checkout and real native SDK")
class NativeLotusImageQueryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from src.experiments import sembench_lotus_query
        cls.entry = sembench_lotus_query
        cls.checkout = Path(CHECKOUT)
        animals.verify_animals_checkout(cls.checkout)

    def setUp(self):
        from PIL import Image
        from src.serving.backends.vllm_async_engine import VllmAsyncEngine
        from src.serving.backends.vllm_engine import VllmEngine

        self.Image, self.serial = Image, itertools.count()
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        scope = patch.dict(os.environ, {KEY_ENV: KEY_VALUE})
        scope.start()
        self.addCleanup(scope.stop)
        self.guard_attempts = dict(network=0, real_engine=0)
        connect = socket.socket.connect

        def forbid_network(sock, address):
            if sock.family in (socket.AF_INET, socket.AF_INET6):
                self.guard_attempts["network"] += 1
                self.fail("visual CPU integration attempted an actual IP connection")
            return connect(sock, address)

        def forbid_engine(*args, **kwargs):
            self.guard_attempts["real_engine"] += 1
            self.fail("visual CPU integration constructed an actual model engine")

        for scope in (patch.object(socket.socket, "connect", forbid_network),
                patch.object(VllmAsyncEngine, "__init__", forbid_engine),
                patch.object(VllmEngine, "__init__", forbid_engine)):
            scope.start()
            self.addCleanup(scope.stop)
        self.addCleanup(lambda: self.assertEqual(self.guard_attempts, dict(network=0, real_engine=0)))

    def new_directory(self, label):
        path = self.root / (label + "-" + str(next(self.serial)))
        path.mkdir(mode=0o700)
        return path

    def animals_inputs(self, *, count=4):
        directory = self.new_directory("animals")
        rows = []
        for index, color in enumerate(("red", "green", "blue", "yellow")[:count]):
            path = directory / (str(index) + ".png")
            self.Image.new("RGB", (3, 2), color).save(path)
            rows.append(dict(row_id="animals-row-" + str(index), ImagePath=str(path),
                City="CityA" if index % 2 == 0 else "CityB",
                StationID="StationA" if index % 2 == 0 else "StationB",
                Species="REFERENCE_ONLY_ZEBRA" if index < 2 else "REFERENCE_ONLY_IMPALA",
                image=animals.image_identity(path)))
        value = dict(schema=animals.MANIFEST_SCHEMA, role="software_fixture",
            sembench_commit=animals.SEMBENCH_COMMIT, rows=rows)
        value["sha256"] = content_digest(value)
        path = directory / "manifest.json"
        write_private_json(path, value)
        return dict(manifest=path, manifest_digest=value["sha256"])

    def mmqa_inputs(self):
        directory = self.new_directory("mmqa")
        (directory / "images").mkdir()
        # Different image occurrences intentionally carry identical PNG pixels.
        for name in ("first.png", "duplicate.png"):
            self.Image.new("RGB", (3, 2), "red").save(directory / "images" / name)
        for filename, fields, rows in (
            ("ap_warrior.csv", ("ID", "Track"), ((1, "TrackA"), (2, "TrackB"))),
            ("tampa_international_airport.csv", ("Airlines", "Destinations"),
             (("DomesticOnly", "Chicago"), ("EuropeanLine", "Frankfurt"))),
        ):
            with (directory / filename).open("w", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(fields)
                writer.writerows(rows)
        return dict(data_dir=directory)

    def ecomm_inputs(self):
        import pandas as pd

        directory = self.new_directory('ecomm')
        (directory / 'images').mkdir()
        rows = []
        for product, category, brand, name in ((1, 'Footwear', 'ADIDAS', 'keep-shoe'),
                (2, 'Apparel', 'PUMA', 'keep-tee'), (3, 'Accessories', 'OTHER', 'drop-watch'),
                (4, 'Home', 'OTHER', 'drop-home')):
            filename = str(product) + '.png'
            self.Image.new('RGB', (3, 2), 'red').save(directory / 'images' / filename)
            rows.append(dict(id=product, masterCategory={'typeName':category}, brandName=brand,
                productDisplayName=name, productDescriptors={'description':name}, gold='REFERENCE_ONLY_'))
        pd.DataFrame(rows).to_parquet(directory / 'styles_details.parquet', index=False)
        pd.DataFrame(dict(id=['1','1','2','3','4'], filename=['1.png','1.png','2.png','3.png','4.png'])).to_parquet(
            directory / 'image_mapping.parquet', index=False)
        return dict(data_dir=directory)

    def cars_inputs(self, scale=7):
        import pandas as pd
        from src.baselines.text.sembench_cars import visual_files

        directory = self.new_directory('cars')
        cars, images, full_cars, full_images = visual_files(scale)
        for name in (cars, images, full_cars, full_images):
            (directory / name).parent.mkdir(parents=True,exist_ok=True)
        paths=[]
        for color in ('red','green','blue'):
            path=directory/(color+'.png')
            self.Image.new('RGB',(3,2),color).save(path)
            paths.append(str(path))
        frame=pd.DataFrame(dict(car_id=[1,2,3],vin=['vin-1','vin-2','vin-3'],
                                transmission=['Manual','Manual','Automatic']))
        frame.to_csv(directory/cars,index=False)
        pd.concat([frame,pd.DataFrame(dict(car_id=[4],vin=['REFERENCE_ONLY_OUTSIDE'],
                    transmission=['Manual']))]).to_csv(directory/full_cars,index=False)
        records=pd.DataFrame(dict(car_id=[1]*12+[2,2,3],image_id=list(range(15)),
                                 image_path=[paths[0]]*12+[paths[1],paths[1],paths[2]]))
        records.to_csv(directory/images,index=False)
        records['damage_status']=['no_damage']*12+['puncture;paint_scratches']*2+['dented']
        pd.concat([records,pd.DataFrame(dict(car_id=[4],image_id=[99],image_path=[paths[0]],
                    damage_status=['no_damage']))]).to_csv(directory/full_images,index=False)
        return dict(data_dir=directory)

    def medical_inputs(self, count=6, scale=29):
        import pandas as pd
        from src.baselines.text.sembench_medical import visual_files

        directory = self.new_directory('medical')
        (directory / 'data').mkdir()
        (patients, skin, xray), labels = visual_files(9, scale)
        people = pd.DataFrame(dict(patient_id=list(range(count + 1)),
            did_family_have_cancer=[0] + [1] * count))
        people.to_csv(directory / patients, index=False)
        people.assign(x_ray_diagnosis='10_ill', skin_cancer_diagnosis='malignant',
            text_diagnosis='REFERENCE_ONLY_').to_csv(directory / labels, index=False)
        for name, column, color in ((skin, 'skin_image_id', 'red'), (xray, 'xray_id', 'blue')):
            path = directory / (color + '.png')
            self.Image.new('RGB', (3, 2), color).save(path)
            ids = list(range(count + 1)) + [1]
            pd.DataFrame(dict(patient_id=ids, image_path=[str(path)] * len(ids),
                **{column: list(range(len(ids)))}, gold=['REFERENCE_ONLY_'] * len(ids))).to_csv(
                    directory / name, index=False)
        return dict(data_dir=directory)

    def config(self, scenario="animals", query="Q1", **changes):
        value = dict(schema="semloom.sembench_lotus_image_run.v1", scenario=scenario,
            query=query, query_id="cpu-image-query", model_name="openai/local-image-fixture",
            api_base="http://localhost:18473/v1", api_key_env=KEY_ENV,
            generation=dict(max_tokens=32, temperature=0, top_p=1, seed=0),
            native_lm_max_batch_size=2, query_timeout_s=60,
            service_profile=dict(role="software HTTP fixture", model_revision="fixture",
                processor_revision="fixture", dtype="fixture"))
        value.update(changes)
        path = self.root / ("config-" + str(next(self.serial)) + ".json")
        write_private_json(path, value)
        config, generation = self.entry.configuration(path)
        self.assertEqual(generation.max_tokens, value["generation"]["max_tokens"])
        return config, path

    def ecomm_outfit_inputs(self, query):
        import pandas as pd

        directory = self.new_directory('ecomm-outfit')
        (directory / 'images').mkdir()
        rows = []
        for product, master, sub, kind, color in ((1,'Footwear','Shoes','Socks','red'),
                (2,'Apparel','Bottomwear','Trousers','green'), (3,'Apparel','Topwear','Shirts','blue'),
                (4,'Accessories','Bags','Bags','yellow')):
            self.Image.new('RGB', (3,2), color).save(directory / 'images' / (str(product)+'.png'))
            rows.append(dict(id=product, masterCategory={'typeName':master}, subCategory={'typeName':sub},
                articleType={'typeName':kind}, baseColour='White' if query == 'Q14' else 'Black', price=80,
                colour1='', colour2='', gender='Men', usage='Sports', season='Summer',
                articleAttributes={'Sleeve Length':['Short Sleeves'],'Neck':['Round Neck'],'Pattern':['Striped']},
                brandName='ADIDAS', productDisplayName='adidas '+kind,
                productDescriptors={'description':{'value':'adidas '+kind}}, gold='REFERENCE_ONLY_'))
        pd.DataFrame(rows).to_parquet(directory / 'styles_details.parquet', index=False)
        pd.DataFrame(dict(id=[1,1,2,3,4],filename=['1.png','1.png','2.png','3.png','4.png'])).to_parquet(
            directory / 'image_mapping.parquet', index=False)
        return dict(data_dir=directory)

    def ledger(self, config, requests):
        from src.experiments.attempt_ledger import AttemptBudget
        from src.experiments.cell_budget import CellBudgetLedger
        path = self.root / ("budget-" + str(next(self.serial)) + ".sqlite")
        ledger = CellBudgetLedger.create(path, AttemptBudget("cpu-image-budget", requests), deadline_utc=None)
        ledger.reserve_unit(config["query_id"], requests)
        value = dict(path=str(path), id="cpu-image-budget", limit=requests, unit_id=config["query_id"])
        return ledger, value

    def invoke(self, config, inputs, replies, *, allowance=None, worker=False):
        checked = self.entry.check(config, self.checkout, **inputs)
        maximum = checked["inputs"].get("expected_model_calls", checked["inputs"].get("maximum_model_calls"))
        directory = self.new_directory("query")
        if worker:
            claim = None
        else:
            ledger, _ = self.ledger(config, maximum if allowance is None else allowance)
            claim = ledger.claim_unit(config["query_id"])
            claim.bind_execution()
        self.last = dict(directory=directory, claim=claim, replies=replies)
        with replies.install():
            report = (self.entry.run_worker(config, self.checkout, directory, **inputs) if worker else
                self.entry.run_query(config, self.checkout, directory, budget=claim, **inputs))
        return report, directory, claim

    @staticmethod
    def result_rows(report):
        value = report["result"]
        return [dict(zip(value["columns"], row, strict=True)) for row in value["data"]]

    def assert_audit(self, report, directory, replies):
        self.assertIsInstance(report["source_started_ns"], int)
        self.assertEqual(report["query_clock_version"], "lotus_service.source_to_sink.v1")
        self.assertLessEqual(report["preflight_finished_ns"], report["source_started_ns"])
        if report["sink_ready_ns"] is None:
            self.assertIsNone(report["source_to_sink_seconds"])
        else:
            self.assertEqual(report["source_to_sink_seconds"],
                (report["sink_ready_ns"] - report["source_started_ns"]) / 1e9)
            self.assertLessEqual(report["sink_ready_ns"], report["result_serialization_started_ns"])
        events = [json.loads(line) for line in (directory / "native-http.jsonl").read_text().splitlines()]
        requests = [event for event in events if event["event"] == "request"]
        self.assertEqual(Counter(event["request_values_sha256"] for event in requests),
            Counter(content_digest(request["body"]) for request in replies.requests))
        self.assertEqual(Counter(event["request_bytes_sha256"] for event in requests),
            Counter(hashlib.sha256(request["payload"]).hexdigest() for request in replies.requests))
        self.assertEqual(report["model_calls"], len(replies.requests))
        self.assertTrue(report["event_writer"]["closed"])
        if report['status']=='completed':
            self.assertTrue(report['native_sdk_response_audit']['available'])
            ordered=[event for event in events if event['event']=='native_sdk_response']
            self.assertEqual(sorted(row['http_attempt'] for row in ordered),sorted(event['attempt'] for event in requests))
        evidence = (directory / "native-http.jsonl").read_text() + json.dumps(report)
        self.assertNotIn("data:image/png;base64,", evidence)
        self.assertNotIn(KEY_VALUE, evidence)
        if any(replies.image_uris(row['body']) for row in replies.requests):
            self.assertIn("url_sha256", evidence)

    def test_check_and_run_without_execute_model_do_not_claim_or_send(self):
        config, _ = self.config()
        inputs = self.animals_inputs()
        ledger, budget = self.ledger(config, 4)
        config, path = self.config(budget=budget)
        output = self.root / "must-not-be-created"
        with patch.object(self.entry, "_make_lm", side_effect=AssertionError("check constructed an LM")):
            checked = self.entry.check(config, self.checkout, **inputs)
            self.assertTrue(checked["execution_available"])
            self.assertFalse(checked["execution_enabled"])
            self.assertFalse(checked["service_startup_checked"])
            for action in ("check", "run"):
                arguments = [action, "--config", str(path), "--sembench-checkout", str(self.checkout),
                    "--manifest", str(inputs["manifest"]), "--manifest-digest", inputs["manifest_digest"]]
                if action == "run":
                    arguments += ["--output-dir", str(output)]
                with redirect_stdout(StringIO()):
                    self.assertEqual(self.entry.main(arguments), 0)
        self.assertFalse(output.exists())
        self.assertEqual(ledger.snapshot()["units"][0]["claimed"], 0)

    def test_animals_original_queries_use_native_http_and_complete_uncached_inputs(self):
        inputs = self.animals_inputs()
        for query in ("Q1", "Q3", "Q7", "Q10"):
            with self.subTest(query=query):
                config, _ = self.config(query=query)
                replies = NativeHTTPReplies(self, "animals")
                report, directory, claim = self.invoke(config, inputs, replies)
                self.assertEqual(report["status"], "completed")
                self.assertEqual([stage["input_occurrences"] for stage in report["native_lm_calls"]],
                    [4, 4] if query == "Q7" else [4])
                self.assertEqual(report["model_calls"], 8 if query == "Q7" else 4)
                self.assertEqual(claim.attempts, report["model_calls"])
                self.assertEqual(report["quality"]["sembench_commit"], animals.SEMBENCH_COMMIT)
                self.assertEqual(report["quality"]["original_metric"][
                    "absolute_error" if query == "Q1" else "f1_score"], 0 if query == "Q1" else 1)
                self.assertFalse(report["model_process_owned"])
                self.assertIsNone(report["physical_gpu_exit_confirmed"])
                self.assertFalse(report["performance_claim"])
                self.assertEqual(report["retries"], 0)
                self.assert_audit(report, directory, replies)

    def test_source_and_pixel_audits_surround_the_complete_native_query(self):
        from src.baselines.image import sembench_mmqa as mmqa

        for scenario, query in (("animals", "Q7"), ("mmqa", "Q2b")):
            inputs = self.animals_inputs() if scenario == "animals" else self.mmqa_inputs()
            config, _ = self.config(scenario, query)
            replies = NativeHTTPReplies(self, scenario)
            source = animals if scenario == "animals" else mmqa
            checks = []
            def observe(function):
                def checked(*args, **kwargs):
                    checks.append(time.monotonic_ns())
                    return function(*args, **kwargs)
                return checked
            with patch.object(source, "load_module", observe(source.load_module)), \
                    patch.object(source, "image_identity", observe(source.image_identity)):
                report, _, _ = self.invoke(config, inputs, replies)
            self.assertTrue(checks)
            self.assertTrue(all(not report["source_started_ns"] <= event <= report["sink_ready_ns"]
                                for event in checks), scenario)
            self.assertEqual([stage["input_occurrences"] for stage in report["native_lm_calls"]], [4, 4])

    def test_mmqa_native_join_and_extract_keep_duplicate_images_and_json_format(self):
        inputs = self.mmqa_inputs()
        for query in ("Q2a", "Q2b", "Q7"):
            with self.subTest(query=query):
                config, _ = self.config("mmqa", query)
                replies = NativeHTTPReplies(self, "mmqa")
                report, directory, claim = self.invoke(config, inputs, replies)
                self.assertEqual(report["status"], "completed")
                self.assertEqual([stage["input_occurrences"] for stage in report["native_lm_calls"]],
                    [4, 4] if query == "Q2b" else [4])
                self.assertEqual(claim.attempts, 8 if query == "Q2b" else 4)
                self.assertEqual(report["quality"]["sembench_commit"], animals.SEMBENCH_COMMIT)
                self.assertEqual(report["inputs"]["join_strategy"], "zs-cot")
                self.assertEqual(report["inputs"]["native_join_strategy"], "ReasoningStrategy.ZS_COT")
                if query == "Q2b":
                    self.assertEqual([stage["kind"] for stage in report["native_lm_calls"]],
                        ["filter_or_join", "extract"])
                    extracts = [row["body"] for row in replies.requests
                        if row["body"].get("response_format") == {"type": "json_object"}]
                    self.assertEqual(len(extracts), 4)
                    self.assertEqual(len({NativeHTTPReplies.image_uris(body)[0] for body in extracts}), 1)
                    self.assertTrue(all(row["color"] == "red" for row in self.result_rows(report)))
                elif query == "Q7":
                    self.assertEqual({row["Airlines"] for row in self.result_rows(report)},
                        {"DomesticOnly", "EuropeanLine"})
                    self.assertTrue(report["inputs"]["original_source_difference"])
                self.assert_audit(report, directory, replies)

    def test_q2b_only_actual_join_survivors_reach_extract_without_deduplicating_ids_or_images(self):
        import pandas as pd

        inputs = self.mmqa_inputs()
        pd.DataFrame(dict(ID=[1, 1, 2], Track=['TrackA', 'TrackA', 'TrackB'])).to_csv(
            inputs['data_dir'] / 'ap_warrior.csv', index=False)
        config, _ = self.config('mmqa', 'Q2b')
        replies = NativeHTTPReplies(self, 'mmqa', mode='partial_matches')
        report, directory, claim = self.invoke(config, inputs, replies)
        self.assertEqual(report['status'], 'completed')
        self.assertEqual([stage['input_occurrences'] for stage in report['native_lm_calls']], [6, 4])
        self.assertEqual(claim.attempts, 10)
        self.assertEqual(claim.remaining, 2)  # The upper bound covers all six potential Extracts.
        rows = self.result_rows(report)
        self.assertEqual(len(rows), 4)
        self.assertEqual({row['ID'] for row in rows}, {1})
        self.assertEqual(Counter(row['image_id'] for row in rows), {'first.png': 2, 'duplicate.png': 2})
        extracts = [row['body'] for row in replies.requests
            if row['body'].get('response_format') == {'type': 'json_object'}]
        self.assertEqual(len(extracts), 4)
        self.assertEqual(len({NativeHTTPReplies.image_uris(body)[0] for body in extracts}), 1)
        self.assertTrue(all(row['color'] == 'red' for row in rows))
        self.assert_audit(report, directory, replies)

    def test_ecomm_full_query_passes_actual_filter_survivors_to_native_multimodal_extract(self):
        import pandas as pd
        reads = []
        original = pd.read_parquet
        def read(path, **kwargs):
            reads.append((Path(path).name, time.monotonic_ns()))
            return original(path, **kwargs)

        inputs = self.ecomm_inputs()
        config, _ = self.config('ecomm', 'Q12')
        replies = NativeHTTPReplies(self, 'ecomm')
        with patch.object(pd, 'read_parquet', read):
            report, directory, claim = self.invoke(config, inputs, replies)
        self.assertEqual(report['inputs']['maximum_model_calls'], 8)
        self.assertEqual(report['inputs']['filter_occurrences'], 4)
        self.assertEqual([s['input_occurrences'] for s in report['native_lm_calls']], [4,3])
        self.assertEqual([s['kind'] for s in report['native_lm_calls']], ['native_query','extract'])
        self.assertEqual((claim.attempts, claim.remaining), (7,1))
        self.assertEqual([json.loads(row['id'])['id'] for row in self.result_rows(report)], [1,1,2])
        self.assertEqual(report['quality']['original_metric']['accuracy'], 1)
        self.assertEqual([name for name, at in reads if report['source_started_ns'] <= at <= report['sink_ready_ns']],
                         ['styles_details.parquet','image_mapping.parquet'])
        self.assert_audit(report, directory, replies)

    def test_ecomm_keeps_original_missing_extract_field_failure_and_actual_attempts(self):
        for mode, calls in (('no_matches',4), ('empty_json',7)):
            inputs = self.ecomm_inputs()
            config, _ = self.config('ecomm', 'Q12')
            replies = NativeHTTPReplies(self, 'ecomm', mode=mode)
            with self.subTest(mode=mode), self.assertRaises(KeyError) as caught:
                self.invoke(config, inputs, replies)
            report = caught.exception.query_report
            self.assertEqual(report['model_calls'], calls)
            self.assertEqual(report['status'], 'failed')
            self.assertIsNone(report['sink_ready_ns'])
            self.assertNotIn('quality', report)
            self.assert_audit(report, self.last['directory'], replies)

    def test_ecomm_missing_assets_are_missing_files_before_any_service_or_budget(self):
        config, _ = self.config('ecomm', 'Q12')
        with patch.object(self.entry, '_make_lm', side_effect=AssertionError('missing data started a model')), \
                self.assertRaises(FileNotFoundError):
            self.entry.check(config, self.checkout, data_dir=self.new_directory('missing-ecomm'))
        with self.assertRaisesRegex(ValueError, 'supported E-Commerce bindings'):
            self.entry.check(dict(config, query='Q15'), self.checkout, data_dir='unused')

    def test_ecomm_post_sink_source_change_keeps_full_result_and_fails_the_audit(self):
        inputs = self.ecomm_inputs()
        config, _ = self.config('ecomm', 'Q12')
        replies = NativeHTTPReplies(self, 'ecomm')
        respond = replies.respond
        source = Path(inputs['data_dir']) / 'styles_details.parquet'
        original_bytes = source.read_bytes()
        changed = []
        def change_after_source_read(request):
            response = respond(request)
            if not changed:
                source.write_bytes(original_bytes + b'\n')
                changed.append(True)
            return response
        with patch.object(replies, 'respond', change_after_source_read), \
                self.assertRaisesRegex(ValueError, 'source changed') as caught:
            self.invoke(config, inputs, replies)
        report = caught.exception.query_report
        self.assertEqual(report['model_calls'], 7)
        self.assertIsNotNone(report['sink_ready_ns'])
        self.assertEqual([json.loads(row['id'])['id'] for row in self.result_rows(report)], [1,1,2])
        self.assertNotIn('quality', report)

    def test_ecomm_multibranch_joins_and_grouped_topk_use_the_unchanged_full_query(self):
        for query, expected, count in (('Q10',['1-2-3']*2,19), ('Q11',['1-2-3-4']*2,25),
                                      ('Q14',['1']*4,17)):
            inputs = self.ecomm_outfit_inputs(query)
            config, _ = self.config('ecomm',query)
            replies = NativeHTTPReplies(self,'ecomm',query=query)
            with self.subTest(query=query):
                report, directory, claim = self.invoke(config,inputs,replies,allowance=32)
                self.assertIsNone(report['inputs']['maximum_model_calls'])
                self.assertEqual(report['inputs']['minimum_model_calls'],5)
                self.assertEqual(claim.attempts,count)
                self.assertEqual([row['id'] for row in self.result_rows(report)],expected)
                self.assertEqual(report['quality']['original_metric']['accuracy'],1)
                self.assertTrue(all(not item['all_declared_columns_selected']
                                    for item in report['native_join_inputs']))
                self.assertTrue(all(len(item['selected_columns']) == 2 for item in report['native_join_inputs']))
                self.assert_audit(report,directory,replies)

    def test_ecomm_single_visual_operators_and_join_frontiers_share_the_service_entry(self):
        import pandas as pd

        for query, kind, count in (('Q2','filter',5), ('Q4','extract',4), ('Q6','map',2),
                                   ('Q8','join',5), ('Q9','join',25), ('Q13','filter',5)):
            inputs=self.ecomm_outfit_inputs(query)
            root=Path(inputs['data_dir'])
            styles=pd.read_parquet(root/'styles_details.parquet')
            if query == 'Q2':
                styles.at[0,'articleType']={'typeName':'Sports Shoes'}
                styles.at[0,'baseColour']='Yellow'
                styles.at[0,'colour1']='Silver'
            elif query == 'Q4':
                mapping=pd.read_parquet(root/'image_mapping.parquet').iloc[[0,2,3,4]]
                mapping.to_parquet(root/'image_mapping.parquet',index=False)
            elif query == 'Q8':
                styles.at[0,'productDescriptors']={'description':{'value':'long description '*200}}
            elif query == 'Q13':
                styles.at[2,'articleType']={'typeName':'Tshirts'}
            styles.to_parquet(root/'styles_details.parquet',index=False)
            config,_=self.config('ecomm',query)
            replies=NativeHTTPReplies(self,'ecomm',query=query)
            with self.subTest(query=query):
                report,directory,claim=self.invoke(config,inputs,replies,allowance=32)
                self.assertEqual(report['inputs']['first_operator']['kind'],kind)
                self.assertEqual(report['inputs']['minimum_model_calls'],count)
                self.assertEqual(claim.attempts,count)
                self.assertEqual(report['quality']['original_metric']['accuracy'],1)
                if query == 'Q2':
                    self.assertNotIn('styles_details.parquet',report['inputs']['files'])
                    self.assertTrue(report['inputs']['gold_source_identity']['sha256'])
                if query == 'Q8':self.assertEqual([row['id'] for row in self.result_rows(report)],['1-1','1-1'])
                self.assert_audit(report,directory,replies)

    def test_ecomm_empty_join_frontier_keeps_native_empty_relation_without_requests(self):
        import pandas as pd

        inputs=self.ecomm_outfit_inputs('Q9')
        path=Path(inputs['data_dir'])/'styles_details.parquet'
        frame=pd.read_parquet(path);frame['price']=900;frame.to_parquet(path,index=False)
        config,_=self.config('ecomm','Q9')
        replies=NativeHTTPReplies(self,'ecomm',query='Q9')
        report,_,claim=self.invoke(config,inputs,replies,allowance=1)
        self.assertEqual(report['inputs']['minimum_model_calls'],0)
        self.assertEqual(report['model_calls'],0)
        self.assertEqual(claim.attempts,0)
        self.assertEqual(report['result']['data'],[])
        self.assertEqual(report['quality']['original_metric']['accuracy'],1)

    def test_ecomm_duplicate_ids_keep_the_original_classification_metric_failure(self):
        config,_=self.config('ecomm','Q4')
        replies=NativeHTTPReplies(self,'ecomm',query='Q4')
        with self.assertRaises(ValueError) as caught:
            self.invoke(config,self.ecomm_outfit_inputs('Q4'),replies,allowance=5)
        report=caught.exception.query_report
        self.assertEqual(report['model_calls'],5)
        self.assertIsNotNone(report['sink_ready_ns'])
        self.assertEqual([row['id'] for row in self.result_rows(report)],[1,1,2,3,4])
        self.assertNotIn('quality',report)

    def test_ecomm_data_dependent_query_stops_at_the_actual_attempt_cap(self):
        config, _ = self.config('ecomm','Q10')
        replies = NativeHTTPReplies(self,'ecomm',query='Q10')
        inputs = self.ecomm_outfit_inputs('Q10')
        with self.assertRaises(Exception) as caught:
            self.invoke(config,inputs,replies,allowance=5)
        report = caught.exception.query_report
        self.assertEqual((self.last['claim'].attempts,self.last['claim'].remaining),(5,0))
        self.assertEqual(len(replies.requests),5)
        self.assertEqual(report['model_calls'],5)
        self.assertEqual(report['status'],'failed')
        self.assertIsNone(report['sink_ready_ns'])
        self.assertNotIn('quality',report)

    def test_cars_image_queries_keep_all_occurrences_before_original_limits_and_gold(self):
        for query, count, expected in (('Q3',14,['vin-1']*10),('Q8',15,[2,2])):
            inputs=self.cars_inputs()
            config,_=self.config('cars',query,scale_factor=7)
            replies=NativeHTTPReplies(self,'cars',query=query)
            with self.subTest(query=query):
                report,directory,claim=self.invoke(config,inputs,replies,allowance=count)
                self.assertEqual(report['inputs']['expected_model_calls'],count)
                self.assertEqual(len(report['inputs']['images']),count)
                self.assertEqual(claim.attempts,count)
                result=self.result_rows(report)
                column='vin' if query == 'Q3' else 'car_id'
                self.assertEqual([row[column] for row in result],expected)
                self.assertEqual(report['quality']['original_metric']['f1_score'],1)
                self.assertEqual(len(report['inputs']['files']),2)
                self.assertEqual(len(report['inputs']['evaluation_files']),2)
                self.assert_audit(report,directory,replies)

    def test_cars_missing_images_stop_before_service_and_scale_only_selects_paths(self):
        from src.baselines.text.sembench_cars import visual_files
        import pandas as pd

        inputs=self.cars_inputs(scale=29)
        config,_=self.config('cars','Q3',scale_factor=29)
        checked=self.entry.check(config,self.checkout,**inputs)
        self.assertEqual(checked['inputs']['expected_model_calls'],14)
        self.assertIn('data/sf_29/car_data_29.csv',checked['inputs']['files'])
        name=visual_files(29)[1]
        path=Path(inputs['data_dir'])/name
        frame=pd.read_csv(path)
        Path(frame.iloc[0]['image_path']).unlink()
        with self.assertRaises(FileNotFoundError):self.entry.check(config,self.checkout,**inputs)

    def test_medical_image_queries_reuse_original_limits_duplicates_messages_and_evaluation(self):
        import lotus
        import pandas as pd
        from lotus.models import LM
        from lotus.types import LMOutput
        from src.baselines.text import sembench_medical as medical

        for query, count, calls, rows in (('Q3', 6, 7, 5), ('Q8', 103, 104, 100), ('Q9', 7, 11, 11)):
            with self.subTest(query=query):
                inputs = self.medical_inputs(count)
                config, _ = self.config('medical', query, scale_factor=29)
                replies = NativeHTTPReplies(self, 'medical', query=query)
                report, directory, claim = self.invoke(config, inputs, replies)
                self.assertEqual(report['inputs']['expected_model_calls'], calls)
                self.assertEqual(claim.attempts, calls)
                self.assertEqual(len(report['result']['data']), rows)
                self.assertEqual(report['inputs']['images_per_occurrence'], 2 if query == 'Q9' else 1)
                self.assertEqual(len(report['inputs']['images']), calls * (2 if query == 'Q9' else 1))
                self.assertEqual(len(report['inputs']['files']), 3 if query == 'Q9' else 2)
                self.assertTrue(all('_29.csv' in name for name in report['inputs']['files']))
                messages = []
                class OriginalLM(LM):
                    def __call__(self, batch, *args, **kwargs):
                        messages.extend(batch)
                        return LMOutput(outputs=['True'] * len(batch))
                with lotus.settings.context(lm=OriginalLM('openai/local-image-fixture', cache={}), enable_cache=False):
                    reference = medical.run_original_query(self.checkout, query,
                        lambda name, **kwargs: pd.read_csv(Path(inputs['data_dir']) / name, **kwargs),
                        data_dir=inputs['data_dir'], scale_factor=29)
                self.assertEqual(report['result'], reference.to_frame().to_dict(orient='split'))
                self.assertEqual(Counter(content_digest(value) for value in messages),
                    Counter(content_digest(row['body']['messages']) for row in replies.requests))
                self.assertEqual(report['quality'], medical.evaluate_visual_original(
                    self.checkout, query, reference, report['inputs']))
                self.assert_audit(report, directory, replies)

    def test_medical_missing_image_and_unsupported_audio_stop_before_lm_or_post(self):
        inputs = self.medical_inputs()
        config, _ = self.config('medical', 'Q3', scale_factor=29)
        (Path(inputs['data_dir']) / 'blue.png').unlink()
        with patch.object(self.entry, '_make_lm', side_effect=AssertionError('preflight constructed LM')):
            with self.assertRaises(FileNotFoundError):
                self.entry.check(config, self.checkout, **inputs)
            config['query'] = 'Q2'
            with self.assertRaisesRegex(ValueError, 'unsupported Medical'):
                self.entry.check(config, self.checkout, **inputs)

    def test_medical_small_gold_keeps_original_limit_evaluator_failure_after_sink(self):
        inputs = self.medical_inputs(count=3)
        config, _ = self.config('medical', 'Q8', scale_factor=29)
        replies = NativeHTTPReplies(self, 'medical', query='Q8', mode='no_matches')
        with self.assertRaises(ValueError) as caught:
            self.invoke(config, inputs, replies)
        report = caught.exception.query_report
        self.assertEqual(report['model_calls'], 4)
        self.assertIsNotNone(report['sink_ready_ns'])
        self.assertEqual(report['result']['data'], [])
        self.assertNotIn('quality', report)
        self.assert_audit(report, self.last['directory'], replies)

    def test_medical_image_mutation_after_sink_retains_result_and_fails_audit(self):
        from src.baselines.text import sembench_medical as medical
        inputs = self.medical_inputs(count=2)
        config, _ = self.config('medical', 'Q9', scale_factor=29)
        replies = NativeHTTPReplies(self, 'medical', query='Q9')
        verify = medical.verify_visual_inputs
        def changed(value):
            self.Image.new('RGB', (3, 2), 'green').save(Path(inputs['data_dir']) / 'red.png')
            verify(value)
        with patch.object(medical, 'verify_visual_inputs', changed), self.assertRaisesRegex(
                ValueError, 'Medical image changed') as caught:
            self.invoke(config, inputs, replies)
        report = caught.exception.query_report
        self.assertEqual(report['model_calls'], 6)
        self.assertIsNotNone(report['sink_ready_ns'])
        self.assertEqual(len(report['result']['data']), 6)
        self.assertNotIn('quality', report)

    def test_cars_gold_mutation_after_sink_retains_complete_native_relation(self):
        from src.baselines.text.sembench_cars import verify_visual_inputs, visual_files

        inputs=self.cars_inputs()
        config,_=self.config('cars','Q8',scale_factor=7)
        replies=NativeHTTPReplies(self,'cars',query='Q8')
        def changed(value):
            path=Path(inputs['data_dir'])/visual_files(7)[3]
            path.write_text(path.read_text()+'\n')
            verify_visual_inputs(value)
        with patch('src.baselines.text.sembench_cars.verify_visual_inputs',changed),self.assertRaises(ValueError) as caught:
            self.invoke(config,inputs,replies,allowance=15)
        report=caught.exception.query_report
        self.assertEqual(report['model_calls'],15)
        self.assertIsNotNone(report['sink_ready_ns'])
        self.assertEqual([row['car_id'] for row in self.result_rows(report)],[2,2])
        self.assertNotIn('quality',report)

    def test_mmqa_native_parser_uses_final_answer_without_rewriting_reasoning(self):
        inputs = self.mmqa_inputs()
        for query in ("Q2a", "Q7"):
            with self.subTest(query=query):
                config, _ = self.config("mmqa", query)
                replies = NativeHTTPReplies(self, "mmqa", mode="distracting_reasoning")
                report, directory, claim = self.invoke(config, inputs, replies)
                self.assertEqual(report["status"], "completed")
                self.assertEqual(report["result"]["data"], [])
                self.assertEqual(claim.attempts, 4)
                self.assert_audit(report, directory, replies)

    def test_api_or_transport_failure_keeps_first_attempt_without_sdk_retries(self):
        inputs = self.animals_inputs(count=1)
        for failure in ("api", "timeout"):
            with self.subTest(failure=failure):
                config, _ = self.config(native_lm_max_batch_size=1)
                replies = NativeHTTPReplies(self, "animals", failure=failure)
                # Spare allowance prevents a retry from being hidden by budget exhaustion.
                with self.assertRaises(Exception) as caught:
                    self.invoke(config, inputs, replies, allowance=3)
                report = caught.exception.query_report
                self.assertEqual(report["status"], "failed")
                self.assertEqual(report["model_calls"], 1)
                self.assertEqual(self.last["claim"].attempts, 1)
                self.assertEqual(self.last["claim"].remaining, 2)
                self.assertEqual(len(replies.requests), 1)
                self.assertEqual(report["calls"]["failed"], 1)
                self.assertNotIn("quality", report)
                self.assert_audit(report, self.last["directory"], replies)

    def test_mismatched_response_model_does_not_qualify_a_query(self):
        config, _ = self.config(native_lm_max_batch_size=1)
        replies = NativeHTTPReplies(self, "animals", response_model="different-model")
        with self.assertRaisesRegex(RuntimeError, "response/model evidence") as caught:
            self.invoke(config, self.animals_inputs(count=1), replies)
        self.assertEqual(caught.exception.query_report["status"], "failed")
        self.assertEqual(caught.exception.query_report["calls"]["failed"], 1)

    def test_q2b_empty_join_keeps_native_KeyError_and_zero_extract_posts(self):
        inputs = self.mmqa_inputs()
        config, _ = self.config("mmqa", "Q2b")
        replies = NativeHTTPReplies(self, "mmqa", mode="no_matches")
        with self.assertRaises(KeyError) as caught:
            self.invoke(config, inputs, replies)
        report = caught.exception.query_report
        directory, claim = self.last["directory"], self.last["claim"]
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["error_type"], "KeyError")
        self.assertNotIn("result", report)
        self.assertNotIn("quality", report)
        self.assertIsNone(report["sink_ready_ns"])
        self.assertEqual(report["model_calls"], 4)
        self.assertEqual(claim.attempts, 4)
        self.assertEqual(report["native_lm_calls"], [
            {"input_occurrences": 4, "kind": "filter_or_join", "post_attempts": 4},
            {"input_occurrences": 0, "kind": "extract", "post_attempts": 0}])
        self.assert_audit(report, directory, replies)

    def test_q2b_nonempty_join_with_missing_json_field_remains_a_recorded_keyerror(self):
        inputs = self.mmqa_inputs()
        config, _ = self.config("mmqa", "Q2b")
        replies = NativeHTTPReplies(self, "mmqa", mode="empty_json")
        with self.assertRaises(KeyError) as caught:
            self.invoke(config, inputs, replies)
        report = caught.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["error_type"], "KeyError")
        self.assertEqual(report["model_calls"], 8)
        self.assertNotIn("result", report)
        self.assertNotIn("quality", report)
        self.assert_audit(report, self.last["directory"], replies)

    def test_worker_insufficient_full_query_allowance_stays_claimed_without_a_post(self):
        config, _ = self.config(query="Q7")
        inputs = self.animals_inputs()
        ledger, value = self.ledger(config, 7)
        config, _ = self.config(query="Q7", budget=value)
        replies = NativeHTTPReplies(self, "animals")
        with self.assertRaisesRegex(ValueError, "below this query's preflight requirement"):
            self.invoke(config, inputs, replies, worker=True)
        self.assertFalse(replies.requests)
        snapshot = ledger.snapshot()
        self.assertEqual(snapshot["allocated_requests"], 7)
        self.assertEqual(snapshot["units"][0]["claimed"], 1)
        persisted = json.loads((self.last["directory"] / "query.json").read_text())
        self.assertEqual(persisted["status"], "failed")
        self.assertEqual(persisted["model_calls"], 0)
        self.assertEqual(persisted["permanent_budget"], snapshot)

    def test_worker_uses_the_existing_reserved_unit_and_writes_private_final_evidence(self):
        config, _ = self.config()
        inputs = self.animals_inputs()
        ledger, value = self.ledger(config, 4)
        config, _ = self.config(budget=value)
        replies = NativeHTTPReplies(self, "animals")
        report, directory, _ = self.invoke(config, inputs, replies, worker=True)
        self.assertEqual(report["status"], "completed")
        self.assertEqual(ledger.snapshot()["allocated_requests"], 4)
        self.assertEqual(ledger.snapshot()["units"][0]["claimed"], 1)
        self.assertEqual(json.loads((directory / "query.json").read_text())["status"], "completed")
        self.assertEqual((directory / "query.json").stat().st_mode & 0o777, 0o600)
        self.assert_audit(report, directory, replies)

    def test_missing_api_key_does_not_claim_the_already_reserved_worker_unit(self):
        config, _ = self.config()
        inputs = self.animals_inputs()
        ledger, value = self.ledger(config, 4)
        config, _ = self.config(budget=value)
        replies = NativeHTTPReplies(self, "animals")
        with patch.dict(os.environ, {}), \
                patch.object(self.entry, "_make_lm", side_effect=AssertionError("missing key allocated LM")):
            os.environ.pop(KEY_ENV, None)
            with self.assertRaises(ValueError):
                self.invoke(config, inputs, replies, worker=True)
        self.assertFalse(replies.requests)
        self.assertEqual(ledger.snapshot()["allocated_requests"], 4)
        self.assertEqual(ledger.snapshot()["units"][0]["claimed"], 0)

    def test_global_json_generation_override_is_rejected_before_any_model_call(self):
        with self.assertRaisesRegex(ValueError, "original Extract"):
            self.config(generation=dict(max_tokens=32, response_format={"type": "json_object"}))

    def test_native_generation_defaults_and_explicit_stop_are_normalized_before_runtime(self):
        base, _ = self.config()
        for supplied, maximum, stop in (({}, 8192, []), ({"max_tokens": 19}, 19, []),
                ({"stop": ["END"]}, 8192, ["END"]), ({"max_tokens": 32, "stop": []}, 32, [])):
            with self.subTest(generation=supplied):
                path = self.root / ("generation-" + str(next(self.serial)) + ".json")
                write_private_json(path, dict(base, generation=supplied))
                config, generation = self.entry.configuration(path)
                self.assertEqual(generation.max_tokens, maximum)
                self.assertEqual(list(generation.stop), stop)
                self.assertEqual(config["generation"]["max_tokens"], maximum)
                self.assertEqual(config["generation"]["stop"], stop)
                lm = self.entry._make_lm(config)
                self.assertEqual(lm.max_tokens, maximum)
                self.assertEqual(lm.kwargs["max_completion_tokens"], maximum)
                self.assertEqual(lm.kwargs["stop"], stop)

    def test_audit_close_failure_cannot_leave_a_completed_report_or_replace_its_error_fields(self):
        from src.observability.buffered_events import BufferedEvents
        config, _ = self.config()
        inputs = self.animals_inputs()
        replies = NativeHTTPReplies(self, "animals")
        original = BufferedEvents.close

        def fail_after_drain(writer):
            original(writer)
            raise OSError("software fixture audit close failure")

        with patch.object(BufferedEvents, "close", fail_after_drain), self.assertRaises(OSError) as caught:
            self.invoke(config, inputs, replies)
        report = caught.exception.query_report
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["error_type"], "OSError")
        self.assertIn("audit close failure", report["error"])
        self.assertEqual(report["calls"]["succeeded"], 4)
        self.assertEqual(report["model_calls"], self.last["claim"].attempts)


if __name__ == "__main__":
    unittest.main()
