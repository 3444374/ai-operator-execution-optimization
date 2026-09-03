"""Independent generative Map wire vectors and strict version-five fixtures."""
import unittest
import copy
import json
import socket
import struct
import threading
from contextlib import contextmanager


@contextmanager
def _socket_pair():
    left, right = socket.socketpair()
    try:
        yield left, right
    finally:
        left.close()
        right.close()


class MapProtocolTests(unittest.TestCase):
    def test_execution_payload_and_completion_match_independent_vectors(self) -> None:
        from src.execution_provider.wire import v5
        from src.execution_provider.semantic_map import SemanticMapPlan, canonical_messages

        physical = v5.physical_algorithm_digest()
        self.assertEqual(physical, "558e50ae5e2716d2e699e09ddb8ffb953f772ba9a1be9dbb15379d9bfcf08d66")
        execution = v5.provider_execution_digest("golden-map-v1")
        self.assertEqual(execution, "8c104ecf5cbf44ca11e13f71d4ef8723a362df26a4743939a5357d788b564dd2")
        vectors = (
            ("Echo the input.", "hello", 0, 17, 1,
             "e97d97db3b315860ef5a0b39258908945f74651b94b68f4d3c319800d680266d",
             "a2b9b987591ff4579565ee15a05172eb4f6ea34cd9542e874ab4e7a186102682"),
            ("Echo the input.", "", 0, 8, 0,
             "04e2e3e0c1a42742676ad000b8078ee01818d8bf78351ebed5b570787c477df8",
             "3e6e485a1a2ac07f9f21a7d9265e471ec7c7cbbb77b785ee958e27a09321e7f2"),
            ("原样返回输入。", '甲\n"乙"\\丙\t{}', 0, 19, 9,
             "ea61042f35816954d5d477f907e3667dbfb38d0dae9ac1f589e40838aeed4b32",
             "4ddf117564c9ebffc55c579bfce150fd2c88b1df338b5db58f9f15a60db960c2"),
            ("Echo the input.", "hello", 1, 17, 1,
             "e97d97db3b315860ef5a0b39258908945f74651b94b68f4d3c319800d680266d",
             "484506d798cbf5b9c415f7d44ce2a604a4a141600ec47f0fa7cd864287008184"),
        )
        for instruction, value, sequence, prompt, output, payload_expected, evidence_expected in vectors:
            with self.subTest(instruction=instruction, value=value, sequence=sequence):
                plan = SemanticMapPlan(instruction, "golden-map-v1", 128)
                payload = v5.semantic_payload_digest(semantic_spec_sha256=plan.digest, input_value=value,
                    canonical_messages_utf8=canonical_messages(instruction, value))
                self.assertEqual(payload, payload_expected)
                evidence = v5.completion_evidence_digest(semantic_spec_sha256=plan.digest,
                    physical_algorithm_sha256=physical, provider_execution_sha256=execution,
                    semantic_payload_sha256=payload, sequence=sequence, raw_output=value,
                    finish_reason="stop", response_model_id="golden-map-v1", prompt_tokens=prompt, output_tokens=output)
                self.assertEqual(evidence, evidence_expected)
        fixed = v5.provider_execution_digest("golden-map-v1", provider_execution_id=v5.FIXED_EXECUTION_ID)
        self.assertEqual(fixed, "6782bdee3092bc43c885e0c90e57c602bd08364f20946bacc4dc9d8283feae24")
        self.assertEqual(v5.completion_evidence_digest(
            semantic_spec_sha256=SemanticMapPlan("Echo the input.", "golden-map-v1", 128).digest,
            physical_algorithm_sha256=physical, provider_execution_sha256=fixed,
            semantic_payload_sha256=vectors[0][5], sequence=0, raw_output="hello", finish_reason="stop",
            response_model_id="golden-map-v1", prompt_tokens=17, output_tokens=1),
            "1d6007a0388e09464262e7cceabd102bbd441ffdba9bf377dc0a2c944a8b0cbb")

    def test_identity_encoding_rejects_invalid_representations_and_binds_metadata(self) -> None:
        from src.execution_provider.wire import v5
        from src.execution_provider.semantic_map import SemanticMapPlan, canonical_messages

        plan = SemanticMapPlan("Echo the input.", "golden-map-v1", 128)
        payload = v5.semantic_payload_digest(semantic_spec_sha256=plan.digest, input_value="hello",
            canonical_messages_utf8=canonical_messages(plan.instruction, "hello"))
        args = dict(semantic_spec_sha256=plan.digest, physical_algorithm_sha256=v5.physical_algorithm_digest(),
            provider_execution_sha256=v5.provider_execution_digest(plan.model_id), semantic_payload_sha256=payload,
            sequence=0, raw_output="hello", finish_reason="stop", response_model_id=plan.model_id,
            prompt_tokens=17, output_tokens=1)
        original = v5.completion_evidence_digest(**args)
        for field, value in (("sequence", 1), ("raw_output", " hello"), ("finish_reason", "length"),
                             ("response_model_id", "different-model"), ("prompt_tokens", 18), ("output_tokens", 2)):
            with self.subTest(field=field):
                self.assertNotEqual(v5.completion_evidence_digest(**{**args, field: value}), original)
        for field, value in (("semantic_spec_sha256", "A" * 64), ("semantic_payload_sha256", "0" * 63),
                             ("sequence", True), ("sequence", 2**64), ("prompt_tokens", -1), ("output_tokens", None),
                             ("raw_output", "x\0y"), ("finish_reason", ""), ("response_model_id", "\ud800")):
            with self.subTest(field=field):
                with self.assertRaises((ValueError, TypeError)):
                    v5.completion_evidence_digest(**{**args, field: value})
        with self.assertRaises(ValueError):
            v5.provider_execution_digest(plan.model_id, provider_execution_id="semloom.provider.golden.uds.v4")
        with self.assertRaises(ValueError):
            v5.semantic_payload_digest(semantic_spec_sha256=plan.digest, input_value="hello",
                                       canonical_messages_utf8=b"x" * 1048577)

    def test_strict_open_and_task_bind_the_actual_instruction_and_sequence(self) -> None:
        from src.execution_provider.wire import v5
        from src.execution_provider.wire.framing import ProtocolError, encode_frame, read_frame
        from src.execution_provider.semantic_map import SemanticMapPlan

        plan = SemanticMapPlan("原样返回输入。", "golden-map-v1", 128)
        opened = v5.build_open_message(plan)
        self.assertEqual(len(opened), 22)
        self.assertEqual(opened["generation_constraints"],
            {"temperature": 0, "top_p": 1, "max_tokens": 128, "n": 1, "stream": False, "stop": None})
        self.assertEqual((opened["max_input_bytes"], opened["max_output_bytes"]), (163840, 65536))
        context = v5.validate_open(opened)
        self.assertEqual(context.semantic_spec_digest, plan.digest)
        for sequence in (0, 1):
            task = v5.build_task_message(plan, sequence=sequence, input_value="")
            self.assertEqual(len(task), 8)
            self.assertEqual(task["sequence"], str(sequence))
            with _socket_pair() as pair:
                pair[0].sendall(encode_frame(task))
                decoded = read_frame(pair[1])
            self.assertEqual(v5.validate_task(decoded, expected_sequence=sequence, open_context=context),
                             (sequence, task["semantic_payload_digest"]))
        with self.assertRaises(ProtocolError):
            v5.validate_task(v5.build_task_message(plan, sequence=0, input_value=""), expected_sequence=1, open_context=context)
        altered = SemanticMapPlan("different instruction", plan.model_id, 128)
        with self.assertRaises(ProtocolError):
            v5.validate_task(v5.build_task_message(altered, sequence=1, input_value=""), expected_sequence=1, open_context=context)
        opened["semantic_spec_digest"] = "0" * 64
        unverified_context = v5.validate_open(opened)
        task = v5.build_task_message(plan, sequence=0, input_value="")
        task["semantic_spec_digest"] = "0" * 64
        with self.assertRaises(ProtocolError):
            v5.validate_task(task, expected_sequence=0, open_context=unverified_context)

    def test_open_rejects_wrong_types_constraints_limits_and_duplicate_fields(self) -> None:
        from src.execution_provider.wire import v5
        from src.execution_provider.wire.framing import ProtocolError, read_frame
        from src.execution_provider.semantic_map import SemanticMapPlan

        opened = v5.build_open_message(SemanticMapPlan("echo", "golden-map-v1", 128))
        for field, value in (("protocol_version", 4), ("protocol_version", True), ("semantic_spec_version", True),
                             ("model_id", "x\0y"), ("semantic_spec_digest", "A" * 64),
                             ("provider_execution_id", "semloom.provider.golden.uds.v4"),
                             ("max_input_bytes", 163841), ("max_output_bytes", 65535), ("operator_kind", "SEM_FILTER")):
            with self.subTest(field=field, value=value):
                with self.assertRaises(ProtocolError):
                    v5.validate_open({**opened, field: value})
        for field, value in (("stop", []), ("stop", "\n"), ("stream", 0), ("n", True),
                             ("max_tokens", 0), ("max_tokens", 4097), ("temperature", 0.1)):
            with self.subTest(field=field, value=value):
                mutated = copy.deepcopy(opened)
                mutated["generation_constraints"][field] = value
                with self.assertRaises(ProtocolError):
                    v5.validate_open(mutated)
        for field in opened:
            mutated = dict(opened)
            del mutated[field]
            with self.subTest(missing=field), self.assertRaises(ProtocolError):
                v5.validate_open(mutated)
        with self.assertRaises(ProtocolError):
            v5.validate_open({**opened, "generation_profile": None})
        payload = json.dumps(opened).replace('"max_tokens": 128', '"max_tokens": 128, "max_tokens": 128').encode()
        with _socket_pair() as pair:
            pair[0].sendall(struct.pack("!I", len(payload)) + payload)
            decoded = read_frame(pair[1])
        with self.assertRaises(ProtocolError):
            v5.validate_open(decoded)

    def test_task_rejects_wire_counter_types_and_nested_mutations(self) -> None:
        from src.execution_provider.wire import v5
        from src.execution_provider.wire.framing import ProtocolError
        from src.execution_provider.semantic_map import SemanticMapPlan

        plan = SemanticMapPlan("echo", "golden-map-v1", 128)
        context = v5.validate_open(v5.build_open_message(plan))
        task = v5.build_task_message(plan, sequence=0, input_value="hello")
        for label, value in (("number", 0), ("boolean", True), ("null", None), ("empty", ""),
                             ("leading-zero", "00"), ("positive-sign", "+0"), ("negative", "-1"),
                             ("fraction", "0.0"), ("exponent", "0e0"), ("non-ascii", "０"),
                             ("uint64-overflow", "18446744073709551616"), ("too-long", "1" * 5000)):
            with self.subTest(invalid_sequence=label):
                with self.assertRaises(ProtocolError):
                    v5.validate_task({**task, "sequence": value}, expected_sequence=0, open_context=context)
        mutations = []
        for field in task:
            mutated = copy.deepcopy(task)
            del mutated[field]
            mutations.append(("missing-" + field, mutated))
        for label, role, content in (("wrong-role", "assistant", "hello"), ("null-content", "user", None),
                                    ("nul-content", "user", "x\0y"), ("surrogate-content", "user", "\udfff"),
                                    ("too-long-content", "user", "x" * 163841)):
            mutated = copy.deepcopy(task)
            mutated["canonical_messages"][1] = {"role": role, "content": content}
            mutations.append((label, mutated))
        mutated = copy.deepcopy(task)
        mutated["canonical_messages"][0]["extra"] = 1
        mutations.append(("extra-message-field", mutated))
        for label, mutated in mutations:
            with self.subTest(mutation=label), self.assertRaises(ProtocolError):
                v5.validate_task(mutated, expected_sequence=0, open_context=context)

    def test_error_frames_are_strict_and_only_v5_admits_output_too_large(self) -> None:
        from src.execution_provider.wire import v3, v4, v5
        from src.execution_provider.wire.framing import ProtocolError

        error = v5.build_error_message("OUTPUT_TOO_LARGE", sequence=0)
        self.assertEqual(error, {"type": "error", "protocol_version": 5, "sequence": "0", "code": "OUTPUT_TOO_LARGE"})
        self.assertEqual(v5.validate_error(error, expected_sequence=0), "OUTPUT_TOO_LARGE")
        self.assertEqual(v5.validate_error(v5.build_error_message("INVALID_OPEN", sequence=None), expected_sequence=None), "INVALID_OPEN")
        mutations = [{**error, "extra": 1}, {"type": "error"}, {**error, "sequence": None},
                     {**error, "sequence": 0}, {**error, "sequence": "01"}, {**error, "sequence": "1"},
                     {**error, "protocol_version": 4}, {**error, "code": "secret-provider-output"}]
        for mutated in mutations:
            with self.subTest(fields=sorted(mutated)), self.assertRaises(ProtocolError):
                v5.validate_error(mutated, expected_sequence=0)
        for codec in (v3, v4):
            with self.subTest(version=codec.PROTOCOL_VERSION), self.assertRaises(ValueError):
                codec.build_error_message("OUTPUT_TOO_LARGE", sequence=0)

    def test_completion_metadata_and_wire_counters_survive_without_text_changes(self) -> None:
        from src.execution_provider.completion import Completion
        from src.execution_provider.wire import v5
        from src.execution_provider.wire.framing import ProtocolError
        from src.execution_provider.semantic_map import SemanticMapPlan

        plan = SemanticMapPlan("echo", "golden-map-v1", 128)
        context = v5.validate_open(v5.build_open_message(plan))
        payload = v5.build_task_message(plan, sequence=0, input_value="hello")["semantic_payload_digest"]
        result = Completion(" \nTRUE\t", plan.model_id, 19, 3, "length")
        frame = v5.build_completion_message(context, sequence=0, payload_digest=payload, completion=result)
        self.assertEqual(len(frame), 13)
        self.assertEqual((frame["sequence"], frame["prompt_tokens"], frame["output_tokens"]), ("0", "19", "3"))
        self.assertEqual(frame["raw_output"], " \nTRUE\t")
        self.assertEqual(frame["finish_reason"], "length")
        self.assertEqual(v5.validate_completion(frame, expected_sequence=0, payload_digest=payload, open_context=context), result)
        for field, value in (("raw_output", "different"), ("response_model_id", "other-model"), ("prompt_tokens", 19),
                             ("output_tokens", "129"), ("sequence", "1"), ("finish_reason", "")):
            with self.subTest(field=field), self.assertRaises(ProtocolError):
                v5.validate_completion({**frame, field: value}, expected_sequence=0, payload_digest=payload, open_context=context)

    def test_frame_bounds_use_decimal_strings_and_output_policy_order(self) -> None:
        from src.execution_provider.completion import Completion
        from src.execution_provider.wire import v5
        from src.execution_provider.wire.framing import ProtocolError, encode_frame
        from src.execution_provider.semantic_map import SemanticMapPlan

        plan = SemanticMapPlan("\x01" * 4096, "golden-map-v1", 4096)
        task = v5.build_task_message(plan, sequence=2**64 - 1, input_value="\x01" * 163840)
        framed = encode_frame(task)
        self.assertEqual(struct.unpack("!I", framed[:4])[0], 1008142)
        self.assertEqual(len(framed) - 4, 1008142)
        model = "\x01" * 128
        maximum = str(2**64 - 1)
        completion = {
            "type": "completion", "protocol_version": 5, "sequence": maximum,
            "semantic_spec_digest": "a" * 64, "physical_algorithm_digest": "b" * 64,
            "provider_execution_digest": "c" * 64, "semantic_payload_digest": "d" * 64,
            "raw_output": "\x01" * 65536, "response_model_id": model,
            "prompt_tokens": maximum, "output_tokens": maximum, "finish_reason": "\x01" * 32,
            "completion_evidence_digest": "e" * 64,
        }
        self.assertEqual(len(encode_frame(completion)) - 4, 394857)
        completion["output_tokens"] = "4096"
        self.assertEqual(len(encode_frame(completion)) - 4, 394841)
        context = v5.validate_open(v5.build_open_message(plan))
        payload = task["semantic_payload_digest"]
        for result, code in ((Completion("x" * 65537, plan.model_id, 1, 1, "length"), "OUTPUT_TOO_LARGE"),
                             (Completion("x" * 65537, "wrong-model", 1, 1, "length"), "MODEL_RESPONSE_INVALID")):
            with self.subTest(code=code):
                with self.assertRaises(ProtocolError) as caught:
                    v5.build_completion_message(context, sequence=0, payload_digest=payload, completion=result)
                self.assertEqual(caught.exception.code, code)


class MapSessionTests(unittest.TestCase):
    def test_unrepresentable_json_integer_is_a_v5_input_error_not_an_adapter_error(self) -> None:
        import sys
        from src.execution_provider.adapters.golden import GoldenCompletionAdapter
        from src.execution_provider.adapters.semantic_session import run_v3_session, run_v5_session
        from src.execution_provider.wire import v3, v5
        from src.execution_provider.wire.framing import encode_frame, read_frame
        from src.execution_provider.semantic_map import SemanticMapPlan

        previous_limit = sys.get_int_max_str_digits()
        sys.set_int_max_str_digits(4300)
        self.addCleanup(sys.set_int_max_str_digits, previous_limit)
        cases = ((run_v3_session, v3, v3.SemanticFilterPlan("echo", "model"), "GATEWAY_INTERNAL"),
                 (run_v5_session, v5, SemanticMapPlan("echo", "model", 128), "INVALID_TASK"))
        for runner, codec, plan, code in cases:
            with self.subTest(version=codec.PROTOCOL_VERSION), _socket_pair() as pair:
                pair[0].settimeout(3)
                worker = threading.Thread(target=runner, args=(pair[1], GoldenCompletionAdapter({})), daemon=True)
                worker.start()
                pair[0].sendall(encode_frame(codec.build_open_message(plan)))
                self.assertEqual(read_frame(pair[0])["type"], "opened")
                payload = b'{"sequence":' + b"9" * 5000 + b"}"
                pair[0].sendall(struct.pack("!I", len(payload)) + payload)
                error = read_frame(pair[0])
                self.assertEqual(error["code"], code)
                if codec is v5:
                    self.assertEqual(error["sequence"], "0")
                self.assertIsNone(read_frame(pair[0]))
                worker.join(3)
                self.assertFalse(worker.is_alive())

    def test_adapter_value_error_remains_internal_and_redacted(self) -> None:
        from src.execution_provider.adapters.golden import GoldenCompletionAdapter
        from src.execution_provider.adapters.semantic_session import run_v5_session
        from src.execution_provider.wire import v5
        from src.execution_provider.wire.framing import encode_frame, read_frame
        from src.execution_provider.semantic_map import SemanticMapPlan

        class BrokenAdapter(GoldenCompletionAdapter):
            def complete(self, request):
                raise ValueError("secret-payload")

        plan = SemanticMapPlan("echo", "golden-map-v1", 128)
        with _socket_pair() as pair:
            pair[0].settimeout(3)
            worker = threading.Thread(target=run_v5_session, args=(pair[1], BrokenAdapter({})), daemon=True)
            worker.start()
            pair[0].sendall(encode_frame(v5.build_open_message(plan)))
            self.assertEqual(read_frame(pair[0])["type"], "opened")
            pair[0].sendall(encode_frame(v5.build_task_message(plan, sequence=0, input_value="hello")))
            self.assertEqual(read_frame(pair[0]), {"type": "error", "protocol_version": 5,
                                                "sequence": "0", "code": "GATEWAY_INTERNAL"})
            self.assertIsNone(read_frame(pair[0]))
            worker.join(3)
            self.assertFalse(worker.is_alive())

    def test_map_fixture_errors_are_terminal_and_never_invent_metadata(self) -> None:
        from src.execution_provider.completion import Completion
        from src.execution_provider.adapters.golden import GoldenCompletionAdapter
        from src.execution_provider.adapters.semantic_session import run_v5_session
        from src.execution_provider.wire import v5
        from src.execution_provider.wire.framing import encode_frame, read_frame
        from src.execution_provider.semantic_map import SemanticMapPlan

        plan = SemanticMapPlan("Echo the input.", "golden-map-v1", 128)
        task = v5.build_task_message(plan, sequence=0, input_value="")
        cases = (
            ("legacy raw output", "GOLDEN_FIXTURE_INVALID"),
            (None, "GOLDEN_FIXTURE_MISSING"),
            (Completion("secret-payload", "wrong-model", 1, 1, "stop"), "MODEL_RESPONSE_INVALID"),
            (Completion("secret-payload", plan.model_id, True, 1, "stop"), "MODEL_RESPONSE_INVALID"),
            (Completion("secret-payload", plan.model_id, 1, 129, "stop"), "MODEL_RESPONSE_INVALID"),
            (Completion("x" * 65537, plan.model_id, 1, 1, "length"), "OUTPUT_TOO_LARGE"),
        )
        for result, code in cases:
            with self.subTest(code=code, result_type=type(result).__name__), _socket_pair() as pair:
                pair[0].settimeout(3)
                fixtures = {} if result is None else {task["semantic_payload_digest"]: result}
                worker = threading.Thread(target=run_v5_session, args=(pair[1], GoldenCompletionAdapter(fixtures)), daemon=True)
                worker.start()
                pair[0].sendall(encode_frame(v5.build_open_message(plan)))
                self.assertEqual(read_frame(pair[0])["type"], "opened")
                pair[0].sendall(encode_frame(task))
                error = read_frame(pair[0])
                self.assertEqual(error, {"type": "error", "protocol_version": 5, "sequence": "0", "code": code})
                self.assertIsNone(read_frame(pair[0]))
                worker.join(3)
                self.assertFalse(worker.is_alive())

    def test_version_mismatch_and_legacy_adapter_never_fall_back(self) -> None:
        from src.execution_provider.adapters.golden import GoldenCompletionAdapter
        from src.execution_provider.adapters.semantic_session import run_v3_session, run_v4_session, run_v5_session
        from src.execution_provider.wire import v5
        from src.execution_provider.wire.framing import encode_frame, read_frame
        from src.execution_provider.semantic_map import SemanticMapPlan

        class LegacyAdapter:
            execution_id = "semloom.provider.golden.uds.v3"
            choice_execution_id = "semloom.provider.golden.uds.v4"
            model_id = None

            def complete(self, request):
                raise AssertionError("unsupported version reached adapter")

        opened = v5.build_open_message(SemanticMapPlan("echo", "golden-map-v1", 128))
        cases = ((run_v3_session, GoldenCompletionAdapter({}), 3, "INVALID_OPEN"),
                 (run_v4_session, GoldenCompletionAdapter({}), 4, "INVALID_OPEN"),
                 (run_v5_session, LegacyAdapter(), 5, "MODEL_REQUEST_REJECTED"))
        for runner, adapter, version, code in cases:
            with self.subTest(version=version), _socket_pair() as pair:
                pair[0].settimeout(3)
                worker = threading.Thread(target=runner, args=(pair[1], adapter), daemon=True)
                worker.start()
                pair[0].sendall(encode_frame(opened))
                self.assertEqual(read_frame(pair[0]), {"type": "error", "protocol_version": version, "sequence": None, "code": code})
                self.assertIsNone(read_frame(pair[0]))
                worker.join(3)
                self.assertFalse(worker.is_alive())

    def test_invalid_json_task_reports_current_sequence_and_closes(self) -> None:
        from src.execution_provider.adapters.golden import GoldenCompletionAdapter
        from src.execution_provider.adapters.semantic_session import run_v5_session
        from src.execution_provider.wire import v5
        from src.execution_provider.wire.framing import encode_frame, read_frame
        from src.execution_provider.semantic_map import SemanticMapPlan

        with _socket_pair() as pair:
            pair[0].settimeout(3)
            worker = threading.Thread(target=run_v5_session, args=(pair[1], GoldenCompletionAdapter({})), daemon=True)
            worker.start()
            pair[0].sendall(encode_frame(v5.build_open_message(SemanticMapPlan("echo", "golden-map-v1", 128))))
            self.assertEqual(read_frame(pair[0])["type"], "opened")
            payload = b'{"secret-payload":'
            pair[0].sendall(struct.pack("!I", len(payload)) + payload)
            self.assertEqual(read_frame(pair[0]), {"type": "error", "protocol_version": 5, "sequence": "0", "code": "INVALID_TASK"})
            self.assertIsNone(read_frame(pair[0]))
            worker.join(3)
            self.assertFalse(worker.is_alive())

    def test_shared_session_returns_explicit_fixture_metadata_and_rejects_repeated_sequence(self) -> None:
        from src.execution_provider.completion import Completion
        from src.execution_provider.adapters.golden import GoldenCompletionAdapter
        from src.execution_provider.adapters.semantic_session import run_v5_session
        from src.execution_provider.wire import v5
        from src.execution_provider.wire.framing import encode_frame, read_frame
        from src.execution_provider.semantic_map import SemanticMapPlan

        plan = SemanticMapPlan("Echo the input.", "golden-map-v1", 128)
        task = v5.build_task_message(plan, sequence=0, input_value="hello")
        raw = Completion(" hello\n\"世界\" ", plan.model_id, 17, 5, "stop")

        class CountingGolden(GoldenCompletionAdapter):
            def __init__(self):
                super().__init__({task["semantic_payload_digest"]: raw})
                self.requests = []

            def complete(self, request):
                self.requests.append(request)
                return super().complete(request)

        adapter = CountingGolden()
        with _socket_pair() as pair:
            pair[0].settimeout(3)
            worker = threading.Thread(target=run_v5_session, args=(pair[1], adapter), daemon=True)
            worker.start()
            pair[0].sendall(encode_frame(v5.build_open_message(plan)))
            opened = read_frame(pair[0])
            self.assertEqual(opened["type"], "opened")
            self.assertEqual((opened["max_input_bytes"], opened["max_output_bytes"]), (163840, 65536))
            pair[0].sendall(encode_frame(task))
            result = read_frame(pair[0])
            self.assertEqual(v5.validate_completion(result, expected_sequence=0, payload_digest=task["semantic_payload_digest"],
                open_context=v5.validate_open(v5.build_open_message(plan))), raw)
            pair[0].sendall(encode_frame(task))
            error = read_frame(pair[0])
            self.assertEqual(error["code"], "INVALID_TASK")
            self.assertIsNone(read_frame(pair[0]))
            worker.join(3)
            self.assertFalse(worker.is_alive())
        self.assertEqual(len(adapter.requests), 1)
        self.assertEqual(adapter.requests[0].protocol_version, 5)
        self.assertEqual(adapter.requests[0].generation_constraints["stop"], None)

    def test_invalid_semantic_identity_never_calls_adapter(self) -> None:
        from src.execution_provider.adapters.golden import GoldenCompletionAdapter
        from src.execution_provider.adapters.semantic_session import run_v5_session
        from src.execution_provider.wire import v5
        from src.execution_provider.wire.framing import encode_frame, read_frame
        from src.execution_provider.semantic_map import SemanticMapPlan

        class NeverCalled(GoldenCompletionAdapter):
            def __init__(self):
                super().__init__({})
                self.calls = 0

            def complete(self, request):
                self.calls += 1
                raise AssertionError("unverified task reached adapter")

        plan = SemanticMapPlan("echo", "golden-map-v1", 128)
        opened = v5.build_open_message(plan)
        opened["semantic_spec_digest"] = "0" * 64
        task = v5.build_task_message(plan, sequence=0, input_value="hello")
        task["semantic_spec_digest"] = "0" * 64
        adapter = NeverCalled()
        with _socket_pair() as pair:
            pair[0].settimeout(3)
            worker = threading.Thread(target=run_v5_session, args=(pair[1], adapter), daemon=True)
            worker.start()
            pair[0].sendall(encode_frame(opened))
            self.assertEqual(read_frame(pair[0])["type"], "opened")
            pair[0].sendall(encode_frame(task))
            self.assertEqual(read_frame(pair[0])["code"], "INVALID_TASK")
            self.assertIsNone(read_frame(pair[0]))
            worker.join(3)
            self.assertFalse(worker.is_alive())
        self.assertEqual(adapter.calls, 0)


if __name__ == "__main__":
    unittest.main()
