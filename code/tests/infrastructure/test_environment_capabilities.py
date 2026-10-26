"""Capability selection controls DB requirements without weakening machine checks."""

from contextlib import ExitStack
import json
import importlib.util
import io
from pathlib import Path
import platform
import tempfile
import unittest
from unittest import mock

from src.infrastructure.environment import CheckResult, MachineObservation, check_environment, missing_install_specs


ROOT = Path(__file__).resolve().parents[3]
MANIFEST = json.loads((ROOT / "deploy/runtime/assets.json").read_text())
PROFILES = tuple(sorted((ROOT / "deploy/runtime/profiles").glob("*.json")))
PSYCOPG_AVAILABLE = importlib.util.find_spec("psycopg") is not None
CLI_SPEC = importlib.util.spec_from_file_location("capability_environment_cli", ROOT / "code/scripts/environment/manage_environment.py")
CLI = importlib.util.module_from_spec(CLI_SPEC)
CLI_SPEC.loader.exec_module(CLI)


class CapabilityRequirementsTests(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        directory = self.stack.enter_context(tempfile.TemporaryDirectory())
        self.env = {key: directory for key in
                    ("PROJECT_ROOT", "ARTIFACT_ROOT", "MODEL_ROOT", "DATA_ROOT", "VENV_ROOT")}
        self.env["GPU_IDS"] = "0"
        self.stack.enter_context(mock.patch("src.infrastructure.environment.platform.system", return_value="Linux"))
        self.stack.enter_context(mock.patch("src.infrastructure.environment.os.cpu_count", return_value=64))
        self.stack.enter_context(mock.patch("src.infrastructure.environment.os.sched_getaffinity",
                                           return_value=set(range(64)), create=True))
        self.stack.enter_context(mock.patch("src.infrastructure.environment.shutil.disk_usage",
                                           return_value=mock.Mock(free=1024**4)))
        self.which = self.stack.enter_context(mock.patch("src.infrastructure.environment.shutil.which",
                                                        side_effect=lambda name: None if name == "psql" else "/fixture/" + name))
        self.find = self.stack.enter_context(mock.patch("src.infrastructure.environment.importlib.util.find_spec",
                                                       return_value=object()))
        self.stack.enter_context(mock.patch("src.infrastructure.environment._module_version", return_value="fixture"))
        self.gpu = self.stack.enter_context(mock.patch("src.infrastructure.environment._check_gpus",
            side_effect=lambda spec, results: results.append(CheckResult("machine:gpu", "ok", "fixture"))))

    def check(self, groups, profile=None, manifest=None):
        profile = profile or json.loads(PROFILES[0].read_text())
        return {item.check: item for item in check_environment(profile, manifest or MANIFEST, self.env, groups)}

    def test_csv_groups_need_neither_database_url_psql_nor_driver_in_all_profiles(self):
        for path in PROFILES:
            with self.subTest(profile=path.name):
                self.which.reset_mock()
                self.find.reset_mock()
                checks = self.check(("core", "text", "semantic-benchmarks"), json.loads(path.read_text()))
                self.assertNotIn("environment:DATABASE_URL", checks)
                self.assertNotIn("configuration:DATABASE_URL", checks)
                self.assertNotIn("command:psql", checks)
                self.assertNotIn(mock.call("psycopg"), self.find.call_args_list)
                self.assertNotIn(mock.call("psql"), self.which.call_args_list)
                self.assertTrue(all(item.status in {"ok", "optional_missing"} for item in checks.values()))
                self.assertIn("machine:gpu", checks)
        self.assertEqual(self.gpu.call_count, len(PROFILES))

    def test_database_group_fails_missing_config_and_psql(self):
        checks = self.check(("database",))
        self.assertEqual(checks["environment:DATABASE_URL"].status, "missing")
        self.assertEqual(checks["command:psql"].status, "missing")
        self.assertIn(mock.call("psycopg"), self.find.call_args_list)

    def test_sql_reader_selection_also_requires_database(self):
        checks = self.check(("sql-readers",))
        self.assertEqual(checks["environment:DATABASE_URL"].status, "missing")
        self.assertEqual(checks["command:psql"].status, "missing")
        self.assertIn(mock.call("psycopg"), self.find.call_args_list)

    def test_database_config_requirement_also_applies_to_custom_cpu_profile(self):
        checks = self.check(("database",), {"platforms": ["Linux"], "gpu": {"required": False}})
        self.assertEqual(checks["environment:DATABASE_URL"].status, "missing")

    def test_mixed_groups_keep_union_of_database_and_machine_requirements(self):
        checks = self.check(("core", "text", "database", "sql-readers"))
        self.assertEqual(checks["environment:DATABASE_URL"].status, "missing")
        self.assertIn("python:core:ray", checks)
        self.assertIn("python:text:httpx", checks)
        self.assertIn("machine:gpu", checks)
        self.assertEqual(self.find.call_args_list.count(mock.call("psycopg")), 1)
        self.assertEqual(self.which.call_args_list.count(mock.call("psql")), 1)

    @unittest.skipUnless(PSYCOPG_AVAILABLE, "explicit database parsing requires database capability")
    def test_valid_database_uri_is_parsed_without_connecting_or_disclosing_it(self):
        self.env["DATABASE_URL"] = "postgresql://postgres:postgres@localhost:5432/ai_operator"
        self.which.side_effect = lambda name: "/fixture/" + name
        checks = self.check(("database",))
        self.assertEqual(checks["configuration:DATABASE_URL"].status, "ok")
        self.assertNotIn(self.env["DATABASE_URL"], str(checks))
        self.assertIn("connectivity not checked", checks["configuration:DATABASE_URL"].detail)

    def test_invalid_or_template_database_config_fails_without_leaking_value(self):
        for value in ("secret-invalid-value", "https://localhost/db", "postgresql://${DB_HOST}/db", "postgresql://<host>/db"):
            with self.subTest(value=value):
                self.env["DATABASE_URL"] = value
                checks = self.check(("database",))
                self.assertEqual(checks["configuration:DATABASE_URL"].status, "missing")
                self.assertNotIn(value, str(checks))

    def test_whitespace_database_config_is_missing(self):
        self.env["DATABASE_URL"] = " \t"
        self.assertEqual(self.check(("database",))["environment:DATABASE_URL"].status, "missing")

    def test_explicit_database_without_parser_fails_closed(self):
        self.env["DATABASE_URL"] = "postgresql://postgres:postgres@localhost:5432/ai_operator"
        with mock.patch.dict("sys.modules", {"psycopg.conninfo": None}):
            checks = self.check(("database",))
        self.assertEqual(checks["configuration:DATABASE_URL"].status, "missing")
        self.assertNotIn(self.env["DATABASE_URL"], str(checks))

    def test_unselected_database_value_does_not_change_csv_requirements(self):
        self.env["DATABASE_URL"] = "invalid-unused-value"
        checks = self.check(("core",))
        self.assertNotIn("environment:DATABASE_URL", checks)
        self.assertNotIn("configuration:DATABASE_URL", checks)

    def test_legacy_unconditional_requirements_remain_required(self):
        profile = {"platforms": [platform.system()], "required_environment": ["DATABASE_URL"],
                   "commands": [{"name": "psql", "required": True}]}
        checks = self.check(("core",), profile)
        self.assertEqual(checks["environment:DATABASE_URL"].status, "missing")
        self.assertEqual(checks["command:psql"].status, "missing")

    def test_csv_does_not_skip_other_required_paths_or_commands(self):
        del self.env["PROJECT_ROOT"]
        self.which.side_effect = lambda name: None
        checks = self.check(("core",))
        self.assertEqual(checks["environment:PROJECT_ROOT"].status, "missing")
        self.assertEqual(checks["path:PROJECT_ROOT"].status, "missing")
        self.assertEqual(checks["command:git"].status, "missing")
        self.assertIn("machine:gpu", checks)

    def test_install_groups_keep_database_driver_optional_for_csv(self):
        self.find.return_value = None
        self.assertNotIn("psycopg[binary]>=3.2", missing_install_specs(MANIFEST, ("core", "text")))
        self.assertIn("psycopg[binary]>=3.2", missing_install_specs(MANIFEST, ("database",)))
        self.assertIn("psycopg[binary]>=3.2", missing_install_specs(MANIFEST, ("sql-readers",)))

    def test_unknown_capability_still_fails(self):
        with self.assertRaisesRegex(ValueError, "unknown Python capability"):
            self.check(("typo",))

    def test_malformed_conditional_requirements_fail_closed(self):
        for value in ([], {"database": "DATABASE_URL"}, {"database": [""]}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.check(("core",), manifest={**MANIFEST, "required_environment_by_group": value})
        for value in ([], "database", [""]):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.check(("core",), {"commands": [{"name": "psql", "groups": value}]})

    def test_cli_csv_database_and_mixed_groups_use_same_capability_contract(self):
        observation = MachineObservation("fixture", "Linux", "3.12", 64, ("fixture",), (24000,), ("fixture",))
        for groups, expected_code in (("core,text,semantic-benchmarks", 0), ("database", 1),
                                      ("core,database", 1), ("sql-readers", 1)):
            with self.subTest(groups=groups), tempfile.TemporaryDirectory() as directory:
                env_file = Path(directory) / "runtime.env"
                env_file.write_text("".join(f"{key}={value}\n" for key, value in self.env.items()))
                report_file = Path(directory) / "preflight.json"
                with mock.patch.dict("os.environ", {}, clear=True), mock.patch.object(CLI, "observe_machine", return_value=observation), mock.patch("sys.stdout", new_callable=io.StringIO):
                    code = CLI.main(["--env-file", str(env_file), "check", "--machine-profile", str(PROFILES[0]), "--groups", groups, "--json-out", str(report_file)])
                report = json.loads(report_file.read_text())
                self.assertEqual(code, expected_code)
                self.assertEqual(report["groups"], groups.split(","))
                self.assertEqual(report["status"], "ok" if expected_code == 0 else "missing")

    def test_cli_has_no_implicit_database_or_other_capability_default(self):
        with mock.patch("sys.stderr", new_callable=io.StringIO), self.assertRaises(SystemExit) as error:
            CLI.parse_args(["check"])
        self.assertEqual(error.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
