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

    def test_executor_is_incremental_and_rejects_rescan(self) -> None:
        scan_source = (EXTENSION_ROOT / "src" / "sem_scan.c").read_text(encoding="utf-8")

        self.assertIn("ExecProcNode(state->child_state)", scan_source)
        self.assertIn("return ExecScan(", scan_source)
        self.assertIn('errmsg("rescan is not supported', scan_source)
        self.assertIn("EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK | EXEC_FLAG_REWIND", scan_source)
        self.assertNotIn("to_arrow", scan_source)

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
        self.assertIn("upper(ai_semantic.map(payload))", regression_sql)
        self.assertIn("Custom Scan (SemLoom SemMap)", regression_expected)
        self.assertIn("recorded:THIRD", regression_expected)
        self.assertIn("query shape is outside", regression_expected)


if __name__ == "__main__":
    unittest.main()
