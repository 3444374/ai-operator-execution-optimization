"""Static fail-closed checks for the PostgreSQL SemMap capability source."""

from __future__ import annotations

import unittest
from pathlib import Path


CODE_ROOT = next(parent for parent in Path(__file__).resolve().parents if (parent / "src").is_dir())
EXTENSION_ROOT = CODE_ROOT / "postgres" / "semloom_pg"


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

    def test_planner_wraps_an_ordinary_child_path_and_chains_hooks(self) -> None:
        extension_source = (EXTENSION_ROOT / "src" / "extension.c").read_text(encoding="utf-8")
        path_source = (EXTENSION_ROOT / "src" / "sem_path.c").read_text(encoding="utf-8")

        self.assertIn("previous_create_upper_paths_hook", extension_source)
        self.assertIn("previous_create_upper_paths_hook(root", extension_source)
        self.assertIn("path->custom_paths = list_make1(child_path)", path_source)
        self.assertIn("output_rel->pathlist = semantic_paths", path_source)
        self.assertIn("CUSTOMPATH_SUPPORT_PROJECTION", path_source)
        self.assertIn("set_customscan_references()", path_source)
        self.assertNotIn("makeVar(INDEX_VAR", path_source)
        self.assertIn("semloom_is_insert_source", path_source)
        self.assertIn("source_entry->rtekind == RTE_SUBQUERY", path_source)
        self.assertNotIn("source_entry->subquery == root->parse", path_source)
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

    def test_scan_delegates_tuple_task_binding_to_the_pump(self) -> None:
        makefile = (EXTENSION_ROOT / "Makefile").read_text(encoding="utf-8")
        scan_source = (EXTENSION_ROOT / "src" / "sem_scan.c").read_text(encoding="utf-8")
        pump_source = (EXTENSION_ROOT / "src" / "sem_pump.c").read_text(encoding="utf-8")

        self.assertIn("src/sem_pump.o", makefile)
        self.assertIn("semloom_pump_begin", scan_source)
        self.assertIn("semloom_pump_next", scan_source)
        self.assertIn("semloom_pump_stop", scan_source)
        self.assertIn("semloom_pump_explain", scan_source)
        self.assertNotIn("AiPreparedTask", scan_source)
        self.assertNotIn("provider_session", scan_source)
        self.assertNotIn("SEMLOOM_RECORDING_PREFIX", scan_source)
        self.assertIn("AiPreparedTask", pump_source)
        self.assertIn("next_sequence", pump_source)
        self.assertIn("ecxt_per_tuple_memory", pump_source)
        self.assertIn("MemoryContextRegisterResetCallback", pump_source)
        self.assertIn("es_query_cxt", pump_source)

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
        self.assertIn("uint32_t operation", header)
        self.assertIn("int32_t system_errno", header)
        self.assertIn("uint16_t detail_length", header)
        self.assertIn("AiProviderStatus (*open)", header)
        self.assertIn("AiProviderStatus (*drive)", header)
        self.assertIn("void (*close)", header)
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

    def test_recording_and_uds_are_separate_provider_adapters(self) -> None:
        makefile = (EXTENSION_ROOT / "Makefile").read_text(encoding="utf-8")
        factory_source = (EXTENSION_ROOT / "src" / "provider.c").read_text(encoding="utf-8")
        recording_source = (EXTENSION_ROOT / "src" / "recording_provider.c").read_text(
            encoding="utf-8"
        )
        uds_source = (EXTENSION_ROOT / "src" / "uds_provider.c").read_text(encoding="utf-8")
        wire_source = (EXTENSION_ROOT / "src" / "wire_v2.c").read_text(encoding="utf-8")
        wire_header = (EXTENSION_ROOT / "src" / "wire_v2.h").read_text(encoding="utf-8")
        gateway_source = (EXTENSION_ROOT / "gateway" / "protocol.py").read_text(encoding="utf-8")

        self.assertIn("src/recording_provider.o", makefile)
        self.assertIn("src/uds_provider.o", makefile)
        self.assertIn("src/wire_v2.o", makefile)
        self.assertNotIn("src/provider_protocol.o", makefile)
        self.assertIn("semloom_gateway_socket_path", factory_source)
        self.assertIn("SEMLOOM_RECORDING_PREFIX", recording_source)
        self.assertNotIn("socket", recording_source.lower())
        self.assertNotIn("connect", recording_source.lower())
        self.assertIn("GetDatabaseEncoding()", uds_source)
        self.assertIn("PG_UTF8", uds_source)
        self.assertIn("O_NONBLOCK", uds_source)
        self.assertIn("WaitLatchOrSocket", wire_source)
        self.assertIn("CHECK_FOR_INTERRUPTS", wire_source)
        self.assertIn("SEMLOOM_WIRE_V2_PROTOCOL_VERSION 2", wire_header)
        self.assertIn("PGC_SUSET", (EXTENSION_ROOT / "src" / "extension.c").read_text(encoding="utf-8"))
        self.assertIn('socket_path[0] != \'/\'', uds_source)
        self.assertIn("MAX_INFLIGHT_TASKS = 1", gateway_source)
        self.assertIn("MAX_FRAME_BYTES = 1024 * 1024", gateway_source)
        self.assertIn("MAX_INPUT_BYTES", gateway_source)
        self.assertNotIn('"mapped_column"', gateway_source)

        allowed_transport_sources = {"uds_provider.c", "wire_v2.c"}
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
        self.assertIn("recorded:THIRD", regression_expected)
        self.assertIn("query shape is outside", regression_expected)

    def test_tap_contract_covers_preload_prepare_snapshot_and_cancel(self) -> None:
        tap_test = (EXTENSION_ROOT / "t" / "001_semloom_pg.pl").read_text(encoding="utf-8")

        self.assertIn("marker was not lowered", tap_test)
        self.assertIn("shared_preload_libraries = 'semloom_pg'", tap_test)
        self.assertIn("PREPARE semloom_map", tap_test)
        self.assertIn("REPEATABLE READ", tap_test)
        self.assertIn("statement_timeout", tap_test)
        self.assertIn("rollback leaves the sink empty", tap_test)
        self.assertIn("committed INSERT SELECT", tap_test)
        self.assertIn("failed INSERT variants leave the committed sink unchanged", tap_test)
        self.assertIn("normal execution succeeds after cancellation", tap_test)
        self.assertIn("plain EXPLAIN does not open", tap_test)
        self.assertIn("PROPAGATE_NULL is owned by PostgreSQL", tap_test)
        self.assertIn("provider connect wait", tap_test)
        self.assertIn("requires UTF8 database encoding", tap_test)


if __name__ == "__main__":
    unittest.main()
