"""Static fail-closed checks for PostgreSQL semantic operator sources."""

from __future__ import annotations

import unittest
from pathlib import Path


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
EXTENSION_ROOT = CODE_ROOT / "postgres" / "semloom_pg"


def _c_function_body(source: str, name: str) -> str:
    definition_start = source.index(f"\n{name}(")
    body_start = source.index("{", definition_start)
    depth = 0
    for index in range(body_start, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[body_start : index + 1]
    raise AssertionError(f"unterminated C function: {name}")


class SemloomPgStaticContractTests(unittest.TestCase):
    def test_pgxs_layout_is_versioned_and_has_regression_entry(self) -> None:
        makefile = (EXTENSION_ROOT / "Makefile").read_text(encoding="utf-8")
        control = (EXTENSION_ROOT / "semloom_pg.control").read_text(encoding="utf-8")

        self.assertIn("MODULE_big = semloom_pg", makefile)
        self.assertIn("REGRESS = semloom_pg", makefile)
        self.assertIn("TAP_TESTS = 1", makefile)
        self.assertIn("SEMLOOM_PG_TARGET_VERSION ?= 18.3", makefile)
        self.assertIn("PG_CONFIG reports", makefile)
        self.assertIn("default_version = '0.1.0'", control)
        self.assertIn("module_pathname = '$libdir/semloom_pg'", control)

    def test_marker_is_never_an_implicit_remote_udf(self) -> None:
        install_sql = (EXTENSION_ROOT / "sql" / "semloom_pg--0.1.0.sql").read_text(
            encoding="utf-8"
        )
        marker_source = (EXTENSION_ROOT / "src" / "marker.c").read_text(encoding="utf-8")

        self.assertIn("VOLATILE", install_sql)
        self.assertIn("PARALLEL UNSAFE", install_sql)
        self.assertNotIn("STRICT", install_sql)
        self.assertIn("LOAD 'MODULE_PATHNAME'", install_sql)
        self.assertIn("marker was not lowered", marker_source)
        self.assertNotIn("http", marker_source.lower())

    def test_exact_semfilter_has_a_constant_plan_owned_sql_contract(self) -> None:
        install_sql = (EXTENSION_ROOT / "sql" / "semloom_pg--0.1.0.sql").read_text(
            encoding="utf-8"
        )
        filter_path = (EXTENSION_ROOT / "src" / "sem_filter_path.c").read_text(
            encoding="utf-8"
        )
        plan_header = (EXTENSION_ROOT / "src" / "sem_plan_spec.h").read_text(
            encoding="utf-8"
        )
        contract_header = (
            EXTENSION_ROOT / "src" / "semantic_filter_contract.h"
        ).read_text(encoding="utf-8")

        self.assertIn(
            "ai_semantic.filter(input text, instruction text, options jsonb)",
            install_sql,
        )
        self.assertIn("semloom_exact_filter_function_oid", filter_path)
        self.assertIn("SEMLOOM_FILTER_INSTRUCTION_MAX_BYTES 4096", contract_header)
        self.assertIn("SEMLOOM_FILTER_MODEL_MAX_BYTES 128", contract_header)
        self.assertIn("JB_ROOT_IS_OBJECT", filter_path)
        self.assertIn("ERRCODE_INVALID_PARAMETER_VALUE", filter_path)
        for field_name in (
            "instruction",
            "prompt_program_digest",
            "result_parser_digest",
            "model_id",
            "semantic_spec_digest",
            "physical_algorithm_digest",
        ):
            self.assertIn(field_name, plan_header)

    def test_exact_semfilter_cost_is_planner_visible_but_not_semantic_identity(self) -> None:
        makefile = (EXTENSION_ROOT / "Makefile").read_text(encoding="utf-8")
        cost_header = (EXTENSION_ROOT / "src" / "sem_filter_cost.h").read_text(
            encoding="utf-8"
        )
        filter_path = (EXTENSION_ROOT / "src" / "sem_filter_path.c").read_text(
            encoding="utf-8"
        )
        pump_source = (EXTENSION_ROOT / "src" / "sem_pump.c").read_text(
            encoding="utf-8"
        )
        runtime_source = (
            EXTENSION_ROOT / "src" / "pg_semantic_runtime.c"
        ).read_text(encoding="utf-8")
        plan_header = (EXTENSION_ROOT / "src" / "sem_plan_spec.h").read_text(
            encoding="utf-8"
        )

        self.assertIn("src/sem_filter_cost.o", makefile)
        self.assertIn("semloom.exact_filter.analytical.v1", cost_header)
        for field_name in (
            "semantic_input_rows",
            "output_selectivity",
            "estimated_model_calls",
            "estimated_prompt_tokens",
            "estimated_output_tokens",
            "ai_work_cost",
        ):
            self.assertIn(field_name, cost_header)
        self.assertIn("clauselist_selectivity", filter_path)
        self.assertIn("clause_selectivity", filter_path)
        self.assertIn("get_attavgwidth", filter_path)
        self.assertIn("semloom_filter_cost_explain", pump_source)
        for counter_name in ("model_calls", "prompt_tokens", "output_tokens"):
            self.assertIn(counter_name, runtime_source)
        self.assertNotIn("SEMLOOM_FILTER_COST_MODEL_ID", plan_header)
        self.assertNotIn("ai_work_cost", plan_header)

    def test_planner_wraps_an_ordinary_child_path_and_chains_hooks(self) -> None:
        extension_source = (EXTENSION_ROOT / "src" / "extension.c").read_text(encoding="utf-8")
        path_source = (EXTENSION_ROOT / "src" / "sem_path.c").read_text(encoding="utf-8")
        common_path_source = (EXTENSION_ROOT / "src" / "sem_path_common.c").read_text(
            encoding="utf-8"
        )

        self.assertIn("previous_create_upper_paths_hook", extension_source)
        self.assertIn("previous_create_upper_paths_hook(root", extension_source)
        self.assertIn("path->custom_paths = list_make1(child_path)", path_source)
        self.assertIn("output_rel->pathlist = semantic_paths", path_source)
        self.assertIn("CUSTOMPATH_SUPPORT_PROJECTION", path_source)
        self.assertIn("set_customscan_references()", path_source)
        self.assertNotIn("makeVar(INDEX_VAR", path_source)
        self.assertIn("semloom_is_insert_source", path_source)
        self.assertIn("source_entry->rtekind == RTE_SUBQUERY", common_path_source)
        self.assertNotIn("source_entry->subquery == root->parse", common_path_source)
        self.assertIn("parent_root->parse->onConflict != NULL", path_source)

    def test_executor_is_incremental_and_rejects_rescan(self) -> None:
        scan_source = (EXTENSION_ROOT / "src" / "sem_scan.c").read_text(encoding="utf-8")
        pump_source = (EXTENSION_ROOT / "src" / "sem_pump.c").read_text(encoding="utf-8")

        self.assertNotIn("ExecProcNode", scan_source)
        self.assertIn("ExecProcNode(pump->child_state)", pump_source)
        self.assertIn("return ExecScan(", scan_source)
        self.assertIn('errmsg("rescan is not supported', scan_source)
        self.assertIn("EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK | EXEC_FLAG_REWIND", pump_source)
        self.assertNotIn("to_arrow", scan_source)

    def test_scan_delegates_tuple_flow_to_the_pump_and_runtime(self) -> None:
        makefile = (EXTENSION_ROOT / "Makefile").read_text(encoding="utf-8")
        scan_source = (EXTENSION_ROOT / "src" / "sem_scan.c").read_text(encoding="utf-8")
        pump_source = (EXTENSION_ROOT / "src" / "sem_pump.c").read_text(encoding="utf-8")
        runtime_source = (EXTENSION_ROOT / "src" / "pg_semantic_runtime.c").read_text(
            encoding="utf-8"
        )
        machine_source = (EXTENSION_ROOT / "src" / "sem_operator_machine.c").read_text(
            encoding="utf-8"
        )

        self.assertIn("src/sem_pump.o", makefile)
        self.assertIn("src/pg_semantic_runtime.o", makefile)
        self.assertIn("semloom_pump_begin", scan_source)
        self.assertIn("semloom_pump_next", scan_source)
        self.assertIn("semloom_pump_stop", scan_source)
        self.assertIn("semloom_pump_explain", scan_source)
        self.assertNotIn("AiPreparedTask", scan_source)
        self.assertNotIn("provider_session", scan_source)
        self.assertNotIn("SEMLOOM_RECORDING_PREFIX", scan_source)
        self.assertNotIn("AiPreparedTask", pump_source)
        self.assertNotIn("next_sequence", pump_source)
        self.assertIn("PgSemanticRuntime", pump_source)
        self.assertIn("SemloomOperatorMachine", pump_source)
        self.assertIn("ecxt_per_tuple_memory", pump_source)
        self.assertIn("semloom_pump_bind_text", pump_source)
        self.assertIn("MemoryContextSwitchTo(task_context)", pump_source)
        self.assertNotIn("MemoryContext", machine_source)
        self.assertIn("MemoryContextRegisterResetCallback", runtime_source)
        self.assertIn("owner_context", runtime_source)

    def test_two_operators_share_one_postgres_semantic_runtime(self) -> None:
        makefile = (EXTENSION_ROOT / "Makefile").read_text(encoding="utf-8")
        pump_source = (EXTENSION_ROOT / "src" / "sem_pump.c").read_text(encoding="utf-8")
        runtime_source = (EXTENSION_ROOT / "src" / "pg_semantic_runtime.c").read_text(
            encoding="utf-8"
        )
        runtime_header = (EXTENSION_ROOT / "src" / "pg_semantic_runtime.h").read_text(
            encoding="utf-8"
        )
        map_machine = (EXTENSION_ROOT / "src" / "sem_map_machine.c").read_text(
            encoding="utf-8"
        )
        filter_machine = (EXTENSION_ROOT / "src" / "sem_filter_machine.c").read_text(
            encoding="utf-8"
        )

        for object_name in (
            "src/pg_semantic_runtime.o",
            "src/sem_map_machine.o",
            "src/sem_filter_machine.o",
        ):
            self.assertIn(object_name, makefile)
        self.assertIn("typedef struct PgSemanticRuntime", runtime_header)
        self.assertIn("PgSemanticCompletion", runtime_header)
        self.assertIn("pg_semantic_runtime_begin", runtime_source)
        self.assertIn("pg_semantic_runtime_drive", runtime_source)
        self.assertIn("pg_semantic_runtime_record_emitted", runtime_source)
        self.assertIn("MemoryContextRegisterResetCallback", runtime_source)
        self.assertIn("semloom_provider_select", runtime_source)
        self.assertIn("next_sequence", runtime_source)
        self.assertIn("AiPreparedTask", runtime_source)
        self.assertIn("MemoryContextSwitchTo(result_context)", runtime_source)
        self.assertIn("PgSemanticRuntime", pump_source)
        self.assertIn("SemloomOperatorMachine", pump_source)

        for leaked_lifecycle_detail in (
            "AiProviderSession",
            "AiPreparedTask",
            "provider_session",
            "next_sequence",
            "MemoryContextRegisterResetCallback",
            "semloom_raise_provider_error",
        ):
            self.assertNotIn(leaked_lifecycle_detail, pump_source)
        self.assertNotIn("AI_PROVIDER_OPERATOR_MAP", pump_source)
        self.assertNotIn("AI_PROVIDER_OPERATOR_FILTER", pump_source)
        for machine_source in (map_machine, filter_machine):
            self.assertNotIn("AiProviderSession", machine_source)
            self.assertNotIn("semloom_provider_select", machine_source)
            self.assertNotIn("MemoryContextRegisterResetCallback", machine_source)
        self.assertNotIn('"true"', runtime_source)
        self.assertNotIn('"false"', runtime_source)
        self.assertNotIn('"unknown"', runtime_source)
        self.assertIn("SEMLOOM_TUPLE_EMIT", map_machine)
        self.assertNotIn('"true"', map_machine)
        self.assertIn('"true"', filter_machine)
        self.assertIn('"false"', filter_machine)
        self.assertIn('"unknown"', filter_machine)
        self.assertIn('"TRUE"', filter_machine)
        self.assertIn('"FALSE"', filter_machine)
        self.assertIn('"UNKNOWN"', filter_machine)

        machine_header = (EXTENSION_ROOT / "src" / "sem_operator_machine.h").read_text(
            encoding="utf-8"
        )
        for postgres_type in (
            "TupleTableSlot",
            "Datum",
            "AttrNumber",
            "MemoryContext",
            "ExplainState",
        ):
            self.assertNotIn(postgres_type, machine_header)

    def test_planner_owns_the_versioned_semantic_plan_spec(self) -> None:
        makefile = (EXTENSION_ROOT / "Makefile").read_text(encoding="utf-8")
        plan_header = (EXTENSION_ROOT / "src" / "sem_plan_spec.h").read_text(
            encoding="utf-8"
        )
        plan_source = (EXTENSION_ROOT / "src" / "sem_plan_spec.c").read_text(
            encoding="utf-8"
        )
        map_path = (EXTENSION_ROOT / "src" / "sem_path.c").read_text(encoding="utf-8")
        filter_path = (EXTENSION_ROOT / "src" / "sem_filter_path.c").read_text(
            encoding="utf-8"
        )
        machine_source = (EXTENSION_ROOT / "src" / "sem_operator_machine.c").read_text(
            encoding="utf-8"
        )
        pump_source = (EXTENSION_ROOT / "src" / "sem_pump.c").read_text(
            encoding="utf-8"
        )
        runtime_source = (EXTENSION_ROOT / "src" / "pg_semantic_runtime.c").read_text(
            encoding="utf-8"
        )

        self.assertIn("src/sem_plan_spec.o", makefile)
        self.assertIn("SEMLOOM_PLAN_SPEC_SCHEMA_VERSION", plan_header)
        for field_name in (
            "schema_version",
            "operator_kind",
            "input_value_kind",
            "output_value_kind",
            "null_policy",
            "error_policy",
            "semantic_spec_version",
            "semantic_spec_id",
            "physical_algorithm",
            "physical_role",
        ):
            self.assertIn(f'"{field_name}"', plan_source)
        self.assertIn("unknown semantic plan specification field", plan_source)
        self.assertIn("incomplete semantic plan specification", plan_source)

        for planner_source in (map_path, filter_path):
            self.assertNotIn('#include "ai_provider_port.h"', planner_source)
            self.assertNotIn("AI_PROVIDER_", planner_source)
            self.assertIn("semloom_plan_spec", planner_source)
        self.assertNotIn("SEMLOOM_RECORDING_SPEC", machine_source)
        self.assertNotIn("SEMLOOM_RECORDING_ALGORITHM", machine_source)
        self.assertNotIn("AiOpenSpec", machine_source)
        self.assertIn("semloom_plan_spec_decode", pump_source)
        self.assertIn("runtime->plan_spec.physical_role", runtime_source)
        self.assertNotIn('ExplainPropertyText("Physical Role", "reference"', runtime_source)

    def test_neutral_provider_contract_has_no_postgres_dependencies(self) -> None:
        header = (EXTENSION_ROOT / "src" / "ai_provider_port.h").read_text(encoding="utf-8")

        self.assertIn("#include <stdbool.h>", header)
        self.assertIn("#include <stdint.h>", header)
        self.assertIn("typedef struct AiByteSlice", header)
        self.assertIn("typedef struct AiOpenSpec", header)
        self.assertIn("typedef struct AiPreparedTask", header)
        self.assertIn("typedef struct AiCompletion", header)
        self.assertIn("typedef struct AiProviderError", header)
        self.assertIn("AI_PROVIDER_ERROR_DETAIL_CAPACITY 160", header)
        self.assertIn("uint32_t code", header)
        self.assertIn("int32_t system_errno", header)
        self.assertIn("uint32_t limit_bytes", header)
        self.assertIn("uint16_t detail_length", header)
        self.assertIn("AiProviderStatus (*open)", header)
        self.assertIn("AiProviderStatus (*drive)", header)
        self.assertIn("void (*close)", header)
        self.assertIn("non-OK open or drive result is terminal", header)
        self.assertNotIn("AiProviderRawOutputKind", header)
        for forbidden in (
            "postgres.h",
            "Oid",
            "Datum",
            "MemoryContext",
            "Jsonb",
            "pgsocket",
            "AttrNumber",
        ):
            self.assertNotIn(forbidden, header)

    def test_neutral_provider_error_interface_hides_adapter_operations(self) -> None:
        header = (EXTENSION_ROOT / "src" / "ai_provider_port.h").read_text(
            encoding="utf-8"
        )
        runtime_source = (EXTENSION_ROOT / "src" / "pg_semantic_runtime.c").read_text(
            encoding="utf-8"
        )

        self.assertNotIn("AiProviderOperation", header)
        self.assertNotIn("operation;", header)
        for transport_term in ("SOCKET", "JSON", "FRAME", "RESPONSE_FIELD"):
            self.assertNotIn(transport_term, header)
        self.assertIn("int32_t system_errno", header)
        self.assertIn("uint32_t limit_bytes", header)
        self.assertIn("uint16_t detail_length", header)
        self.assertIn("char detail[AI_PROVIDER_ERROR_DETAIL_CAPACITY]", header)

        self.assertNotIn("AI_PROVIDER_OPERATION_", runtime_source)
        self.assertNotIn("error->operation", runtime_source)
        self.assertIn("error->detail", runtime_source)
        self.assertIn('errmsg("%s", message)', runtime_source)

    def test_recording_and_uds_are_separate_provider_adapters(self) -> None:
        makefile = (EXTENSION_ROOT / "Makefile").read_text(encoding="utf-8")
        factory_source = (EXTENSION_ROOT / "src" / "provider.c").read_text(encoding="utf-8")
        recording_source = (EXTENSION_ROOT / "src" / "recording_provider.c").read_text(
            encoding="utf-8"
        )
        pump_source = (EXTENSION_ROOT / "src" / "sem_pump.c").read_text(encoding="utf-8")
        runtime_source = (EXTENSION_ROOT / "src" / "pg_semantic_runtime.c").read_text(
            encoding="utf-8"
        )
        uds_source = (EXTENSION_ROOT / "src" / "uds_provider.c").read_text(encoding="utf-8")
        wire_source = (EXTENSION_ROOT / "src" / "wire_v2.c").read_text(encoding="utf-8")
        wire_common_source = (EXTENSION_ROOT / "src" / "wire_common.c").read_text(
            encoding="utf-8"
        )
        wire_header = (EXTENSION_ROOT / "src" / "wire_v2.h").read_text(encoding="utf-8")
        gateway_wire_source = (
            CODE_ROOT / "src" / "execution_provider" / "wire" / "v2.py"
        ).read_text(encoding="utf-8")
        gateway_framing_source = (
            CODE_ROOT / "src" / "execution_provider" / "wire" / "framing.py"
        ).read_text(encoding="utf-8")
        legacy_protocol_source = (EXTENSION_ROOT / "gateway" / "protocol.py").read_text(
            encoding="utf-8"
        )
        legacy_cli_source = (
            EXTENSION_ROOT / "gateway" / "recording_gateway.py"
        ).read_text(encoding="utf-8")

        self.assertIn("src/recording_provider.o", makefile)
        self.assertIn("src/uds_provider.o", makefile)
        self.assertIn("src/wire_common.o", makefile)
        self.assertIn("src/wire_v2.o", makefile)
        self.assertNotIn("src/provider_protocol.o", makefile)
        self.assertIn("semloom_gateway_socket_path", factory_source)
        self.assertIn("SEMLOOM_RECORDING_PREFIX", recording_source)
        self.assertIn("semloom_recording_fail", recording_source)
        self.assertIn("semloom_recording_close(session);", recording_source)
        self.assertNotIn("socket", recording_source.lower())
        self.assertNotIn("connect", recording_source.lower())
        self.assertIn("GetDatabaseEncoding()", uds_source)
        self.assertIn("PG_UTF8", uds_source)
        self.assertIn("O_NONBLOCK", uds_source)
        self.assertIn("semloom_uds_close(session);", uds_source)
        self.assertIn("AI_PROVIDER_ERROR_INPUT_TOO_LARGE", uds_source)
        self.assertIn("SEMLOOM_WIRE_V2_MAX_INPUT_BYTES", uds_source)
        self.assertNotIn("174080", pump_source)
        self.assertNotIn("SEMLOOM_WIRE_V2_MAX_INPUT_BYTES", pump_source)
        self.assertNotIn("174080", runtime_source)
        self.assertNotIn("SEMLOOM_WIRE_V2_MAX_INPUT_BYTES", runtime_source)
        close_functions = (
            (
                "recording close",
                _c_function_body(recording_source, "semloom_recording_close"),
            ),
            ("UDS close", _c_function_body(uds_source, "semloom_uds_close")),
            (
                "UDS local release",
                _c_function_body(uds_source, "semloom_uds_release_local"),
            ),
        )
        for close_name, close_body in close_functions:
            self.assertNotIn("MemoryContextReset", close_body, close_name)
            self.assertNotIn("MemoryContextDelete", close_body, close_name)
        error_catches = (
            (
                _c_function_body(
                    wire_common_source, "semloom_wire_common_parse_json"
                ),
                "MemoryContextSwitchTo(parse_context);",
            ),
            (
                _c_function_body(
                    wire_common_source, "semloom_wire_common_json_int32"
                ),
                "MemoryContextSwitchTo(numeric_context);",
            ),
        )
        for catch_body, context_switch in error_catches:
            self.assertLess(
                catch_body.index(context_switch), catch_body.index("CopyErrorData()")
            )
        self.assertIn("WaitLatchOrSocket", wire_common_source)
        self.assertIn("CHECK_FOR_INTERRUPTS", wire_common_source)
        self.assertIn("SEMLOOM_WIRE_V2_PROTOCOL_VERSION 2", wire_header)
        self.assertIn("PGC_SUSET", (EXTENSION_ROOT / "src" / "extension.c").read_text(encoding="utf-8"))
        self.assertIn('socket_path[0] != \'/\'', uds_source)
        self.assertIn("MAX_INFLIGHT_TASKS = 1", gateway_wire_source)
        self.assertIn("MAX_FRAME_BYTES = 1024 * 1024", gateway_framing_source)
        self.assertIn("MAX_INPUT_BYTES", gateway_wire_source)
        self.assertNotIn('"mapped_column"', gateway_wire_source)
        self.assertIn("from src.execution_provider.wire.v2 import", legacy_protocol_source)
        self.assertIn("from src.execution_provider.server import main", legacy_cli_source)
        self.assertNotIn("postgres.semloom_pg.gateway", gateway_wire_source)

        allowed_transport_sources = {
            "uds_provider.c",
            "wire_common.c",
            "wire_v2.c",
            "wire_v3.c",
        }
        transport_identifiers = (
            "pgsocket",
            "connect(",
            "send(",
            "recv(",
            "WaitLatchOrSocket",
        )
        for source_path in (EXTENSION_ROOT / "src").glob("*.c"):
            if source_path.name in allowed_transport_sources:
                continue
            source = source_path.read_text(encoding="utf-8")
            for identifier in transport_identifiers:
                self.assertNotIn(identifier, source, f"{identifier} leaked into {source_path.name}")

    def test_wire_v3_is_strict_and_does_not_mutate_recording_v2(self) -> None:
        makefile = (EXTENSION_ROOT / "Makefile").read_text(encoding="utf-8")
        wire_v2_header = (EXTENSION_ROOT / "src" / "wire_v2.h").read_text(
            encoding="utf-8"
        )
        wire_v3_header = (EXTENSION_ROOT / "src" / "wire_v3.h").read_text(
            encoding="utf-8"
        )
        wire_v3_source = (EXTENSION_ROOT / "src" / "wire_v3.c").read_text(
            encoding="utf-8"
        )
        python_v2 = (
            CODE_ROOT / "src" / "execution_provider" / "wire" / "v2.py"
        ).read_text(encoding="utf-8")
        python_v3 = (
            CODE_ROOT / "src" / "execution_provider" / "wire" / "v3.py"
        ).read_text(encoding="utf-8")
        golden_adapter = (
            CODE_ROOT / "src" / "execution_provider" / "adapters" / "golden.py"
        ).read_text(encoding="utf-8")

        self.assertIn("src/wire_v2.o", makefile)
        self.assertIn("src/wire_v3.o", makefile)
        self.assertIn("SEMLOOM_WIRE_V2_PROTOCOL_VERSION 2", wire_v2_header)
        self.assertIn("SEMLOOM_WIRE_V3_PROTOCOL_VERSION 3", wire_v3_header)
        self.assertIn("SEMLOOM_WIRE_V3_MAX_INPUT_BYTES 163840", wire_v3_header)
        self.assertIn("semloom_wire_common_send_frame", wire_v3_source)
        self.assertIn("semloom_wire_common_receive_frame", wire_v3_source)
        self.assertNotIn("send(", wire_v3_source)
        self.assertNotIn("recv(", wire_v3_source)
        self.assertIn("PROTOCOL_VERSION = 2", python_v2)
        self.assertIn("PROTOCOL_VERSION = 3", python_v3)
        self.assertIn("MAX_INPUT_BYTES = 163_840", python_v3)
        self.assertIn("set(message) != _OPEN_FIELDS", python_v3)
        self.assertIn("set(message) != _TASK_FIELDS", python_v3)
        self.assertIn("_fixtures.get(request.semantic_payload_digest)", golden_adapter)
        for forbidden in ("httpx", "requests", "openai", "vllm", "ray"):
            self.assertNotIn(forbidden, golden_adapter.lower())

    def test_wire_v3_owns_and_strictly_validates_error_frames(self) -> None:
        wire_v3_source = (EXTENSION_ROOT / "src" / "wire_v3.c").read_text(
            encoding="utf-8"
        )

        self.assertIn("SEMLOOM_V3_ERROR_FIELD_COUNT 4", wire_v3_source)
        self.assertIn("semloom_v3_validate_response", wire_v3_source)
        self.assertIn("semloom_v3_validate_error", wire_v3_source)
        self.assertNotIn("semloom_wire_common_validate_response_type", wire_v3_source)
        for allowed_code in (
            "GATEWAY_INTERNAL",
            "GOLDEN_FIXTURE_INVALID",
            "GOLDEN_FIXTURE_MISSING",
            "INVALID_OPEN",
            "INVALID_TASK",
            "MODEL_REQUEST_REJECTED",
            "MODEL_RESPONSE_INVALID",
            "MODEL_TIMEOUT",
            "MODEL_UNAVAILABLE",
        ):
            self.assertIn(f'"{allowed_code}"', wire_v3_source)

    def test_fixed_model_profile_is_query_fixed_and_transport_neutral(self) -> None:
        extension_source = (EXTENSION_ROOT / "src" / "extension.c").read_text(
            encoding="utf-8"
        )
        port_header = (EXTENSION_ROOT / "src" / "ai_provider_port.h").read_text(
            encoding="utf-8"
        )
        provider_header = (EXTENSION_ROOT / "src" / "provider_private.h").read_text(
            encoding="utf-8"
        )
        runtime_source = (EXTENSION_ROOT / "src" / "pg_semantic_runtime.c").read_text(
            encoding="utf-8"
        )
        uds_source = (EXTENSION_ROOT / "src" / "uds_provider.c").read_text(
            encoding="utf-8"
        )
        wire_v3_header = (EXTENSION_ROOT / "src" / "wire_v3.h").read_text(
            encoding="utf-8"
        )

        self.assertIn("semloom_pg.provider_execution_profile", extension_source)
        self.assertIn("DefineCustomEnumVariable", extension_source)
        self.assertIn('"golden"', extension_source)
        self.assertIn('"openai-compatible-fixed"', extension_source)
        self.assertIn('"uds-golden"', provider_header)
        self.assertIn('"uds-openai-compatible-fixed"', provider_header)
        self.assertIn("semloom_uds_golden_ops", uds_source)
        self.assertIn("semloom_uds_fixed_ops", uds_source)
        self.assertNotIn("SEMLOOM_WIRE_V3_EXECUTION_ID", wire_v3_header)
        for neutral_error in (
            "AI_PROVIDER_ERROR_REMOTE_UNAVAILABLE",
            "AI_PROVIDER_ERROR_REMOTE_TIMEOUT",
            "AI_PROVIDER_ERROR_REQUEST_REJECTED",
            "AI_PROVIDER_ERROR_INVALID_RESPONSE",
            "AI_PROVIDER_ERROR_ADAPTER_INTERNAL",
        ):
            self.assertIn(neutral_error, port_header)
            self.assertIn(neutral_error, runtime_source)

    def test_input_limit_preflight_runs_before_canonical_task_construction(self) -> None:
        port_header = (EXTENSION_ROOT / "src" / "ai_provider_port.h").read_text(
            encoding="utf-8"
        )
        runtime_header = (EXTENSION_ROOT / "src" / "pg_semantic_runtime.h").read_text(
            encoding="utf-8"
        )
        runtime_source = (EXTENSION_ROOT / "src" / "pg_semantic_runtime.c").read_text(
            encoding="utf-8"
        )
        pump_source = (EXTENSION_ROOT / "src" / "sem_pump.c").read_text(
            encoding="utf-8"
        )
        pump_next = _c_function_body(pump_source, "semloom_pump_next")

        self.assertIn("uint32_t max_input_bytes;", port_header)
        self.assertIn("pg_semantic_runtime_preflight_input", runtime_header)
        self.assertIn("AI_PROVIDER_ERROR_INPUT_TOO_LARGE", runtime_source)
        self.assertLess(
            pump_next.index("pg_semantic_runtime_preflight_input"),
            pump_next.index("semloom_operator_machine_task_size"),
        )
        self.assertNotIn("SEMLOOM_WIRE_V3_MAX_INPUT_BYTES", pump_source)
        self.assertNotIn("163840", pump_source)

    def test_wire_common_c_owns_shared_transport_and_json_primitives(self) -> None:
        makefile = (EXTENSION_ROOT / "Makefile").read_text(encoding="utf-8")
        common_source = (EXTENSION_ROOT / "src" / "wire_common.c").read_text(
            encoding="utf-8"
        )
        common_header = (EXTENSION_ROOT / "src" / "wire_common.h").read_text(
            encoding="utf-8"
        )
        wire_v2_source = (EXTENSION_ROOT / "src" / "wire_v2.c").read_text(
            encoding="utf-8"
        )
        uds_source = (EXTENSION_ROOT / "src" / "uds_provider.c").read_text(
            encoding="utf-8"
        )

        self.assertIn("src/wire_common.o", makefile)
        for shared_implementation in (
            "send(",
            "recv(",
            "WaitLatchOrSocket",
            "jsonb_in",
            "CopyErrorData()",
        ):
            self.assertIn(shared_implementation, common_source)
            self.assertNotIn(shared_implementation, wire_v2_source)
        self.assertIn("semloom_wire_common_wait_connected", common_header)
        self.assertIn("semloom_wire_common_wait_connect_retry", common_header)
        self.assertIn("semloom_wire_common_wait_connected", uds_source)
        self.assertIn("semloom_wire_common_wait_connect_retry", uds_source)
        self.assertNotIn("semloom_wire_v2_wait_connected", uds_source)
        self.assertNotIn("semloom_wire_v2_wait_connect_retry", uds_source)

    def test_regression_contract_covers_explain_filter_duplicates_and_limit(self) -> None:
        regression_sql = (EXTENSION_ROOT / "sql" / "semloom_pg.sql").read_text(encoding="utf-8")
        regression_expected = (EXTENSION_ROOT / "expected" / "semloom_pg.out").read_text(
            encoding="utf-8"
        )

        self.assertIn("EXPLAIN (COSTS OFF)", regression_sql)
        self.assertEqual(regression_sql.count("'repeat'"), 2)
        self.assertIn("WHERE doc_id >= 2", regression_sql)
        self.assertIn("LIMIT 1", regression_sql)
        self.assertIn("LIMIT 0", regression_sql)
        self.assertIn("Accepted Rows", regression_expected)
        self.assertIn("\\pset null '<NULL>'", regression_sql)
        self.assertIn("upper(ai_semantic.map(payload))", regression_sql)
        self.assertIn("INSERT INTO semloom_sink", regression_sql)
        self.assertIn("ROLLBACK", regression_sql)
        self.assertIn("RETURNING completion", regression_sql)
        self.assertIn("ON CONFLICT DO NOTHING", regression_sql)
        self.assertIn("Custom Scan (SemLoom SemMap)", regression_expected)
        self.assertIn("SemFilter provider completion must be true, false, or unknown", regression_expected)
        self.assertIn("SemMap and SemFilter cannot be combined", regression_expected)
        self.assertIn("recorded:THIRD", regression_expected)
        self.assertIn("query shape is outside", regression_expected)

    def test_tap_contract_covers_preload_prepare_snapshot_and_cancel(self) -> None:
        tap_test = (EXTENSION_ROOT / "t" / "001_semloom_pg.pl").read_text(encoding="utf-8")

        self.assertIn("marker was not lowered", tap_test)
        self.assertIn("shared_preload_libraries = 'semloom_pg'", tap_test)
        self.assertIn("PREPARE semloom_map", tap_test)
        self.assertIn("PREPARE semloom_filter", tap_test)
        self.assertIn("force_generic_plan", tap_test)
        self.assertIn("ENABLE ROW LEVEL SECURITY", tap_test)
        self.assertIn("SAVEPOINT semloom_filter_failure", tap_test)
        self.assertIn("REPEATABLE READ", tap_test)
        self.assertIn("statement_timeout", tap_test)
        self.assertIn("rollback leaves the sink empty", tap_test)
        self.assertIn("committed INSERT SELECT", tap_test)
        self.assertIn("failed INSERT variants leave the committed sink unchanged", tap_test)
        self.assertIn("normal execution succeeds after cancellation", tap_test)
        self.assertIn("plain EXPLAIN does not open", tap_test)
        self.assertIn("plain SemFilter EXPLAIN does not open", tap_test)
        self.assertIn("SemFilter executes below LIMIT", tap_test)
        self.assertIn("independent SemFilter provider session", tap_test)
        self.assertIn("PROPAGATE_NULL is owned by PostgreSQL", tap_test)
        self.assertIn("provider connect wait", tap_test)
        self.assertIn("requires UTF8 database encoding", tap_test)
        self.assertIn("exact SemFilter preserves Unicode instruction/input", tap_test)
        self.assertIn("empty text as a non-NULL provider task", tap_test)
        self.assertIn("SAVEPOINT semloom_exact_filter_failure", tap_test)
        self.assertIn("v3-error-missing-field", tap_test)
        self.assertIn("v3-error-extra-field", tap_test)
        self.assertIn("v3-open-error-sequence", tap_test)
        self.assertIn("provider_execution_profile = 'openai-compatible-fixed'", tap_test)
        self.assertIn("fixed model SemFilter preserves exact keep/drop", tap_test)
        self.assertIn("SemLoom model endpoint is unavailable", tap_test)
        self.assertIn("SemLoom model endpoint timed out", tap_test)
        self.assertIn("SAVEPOINT semloom_fixed_model_failure", tap_test)
        self.assertIn("fixed model LIMIT 0 does not open", tap_test)


if __name__ == "__main__":
    unittest.main()
