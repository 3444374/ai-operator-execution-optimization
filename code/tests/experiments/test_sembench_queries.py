"""Data/identity/config rejection tests; no optional library or model runtime."""

from dataclasses import replace
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from src.baselines.common.private_artifacts import content_digest, file_identity
from src.baselines.text.sembench_movie import verify_checkout, SEMBENCH_COMMIT
from src.baselines.text.frameworks.prepared_map import NativeGraphOptions
from src.execution_provider.adapters.model_config import FixedModelConfig
from src.execution_provider.semantic_map import SemanticMapPlan
from src.experiments.postgresql.sembench_queries import MovieDataset, MovieQuery, LOTUS_MOVIE_ARMS
from src.experiments.postgresql.sembench_query_cli import load_config, _env_path, arm_options
from src.experiments.postgresql.query_workloads import prepare
from src.experiments.postgresql.supplier_adapter_query import run_supplier_query


def fixture_dataset(root, checkout):
    examples = [
        (
            "movie-row-" + str(i),
            "taken_3" if i < 6 else "another_movie",
            "fixture-positive" if i not in (4, 7) else "fixture-negative",
            "NEGATIVE" if i in (3, 7) else "POSITIVE",
            "same-original-id" if i < 2 else "review-" + str(i),
        )
        for i in range(8)
    ]
    manifest = prepare(
        root, "movie", iter(examples), {"source": "synthetic SemBench software fixture"}, max_rows=8
    )
    return MovieDataset(
        checkout,
        root / "manifest.json",
        expected_manifest_digest=manifest["sha256"],
        role="software_fixture",
    )


class MovieDataTests(unittest.TestCase):
    def test_duplicate_public_review_ids_are_independent_occurrences_without_labels_in_query_rows(
        self,
    ):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = fixture_dataset(root / "data", root / "source")
            rows = dataset.load_rows()
            self.assertEqual(len({r["row_id"] for r in rows}), 8)
            self.assertEqual(rows[0]["review_id"], rows[1]["review_id"])
            self.assertEqual(rows[0]["text"], rows[1]["text"])
            self.assertEqual(set(rows[0]), {"row_id", "movie_id", "review_id", "text"})
            self.assertEqual(
                [len(MovieQuery(dataset, q).model_rows(rows)) for q in (1, 2, 3)], [8, 6, 6]
            )
            self.assertEqual(len(dataset.references(rows)), 8)

    def test_missing_hash_corrupt_file_and_different_label_occurrences_fail(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = fixture_dataset(root / "data", root / "source")
            rows = dataset.load_rows()
            with self.assertRaisesRegex(ValueError, "selected identity"):
                replace(dataset, expected_manifest_digest="0" * 64).load_rows()
            raw = dataset.manifest.parent / "raw.jsonl"
            saved = raw.read_bytes()
            raw.write_bytes(saved + b"[]\n")
            with self.assertRaisesRegex(ValueError, "file identity"):
                dataset.load_rows()
            raw.write_bytes(saved)
            rows[0]["review_id"] = "another-public-id"
            with self.assertRaisesRegex(ValueError, "label occurrence"):
                dataset.references(rows)

    def test_reordered_source_positions_fail_even_with_new_valid_fixture_hashes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = fixture_dataset(root / "data", root / "source")
            raw = dataset.manifest.parent / "raw.jsonl"
            rows = raw.read_text().splitlines()
            rows[0], rows[1] = rows[1], rows[0]
            raw.write_text("\n".join(rows) + "\n")
            manifest = json.loads(dataset.manifest.read_text())
            manifest["files"]["raw.jsonl"] = file_identity(raw)
            manifest["sha256"] = content_digest(
                {k: v for k, v in manifest.items() if k != "sha256"}
            )
            dataset.manifest.write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "source position"):
                replace(dataset, expected_manifest_digest=manifest["sha256"]).load_rows()

    def test_fixture_cannot_claim_selected_original_benchmark_role(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = fixture_dataset(root / "data", root / "source")
            with self.assertRaisesRegex(ValueError, "selected CSV and manifest"):
                replace(dataset, role="original_movie_sf2000")
            for invalid in (0, 4, True, "1"):
                with self.assertRaises(ValueError):
                    MovieQuery(dataset, invalid)

    def test_git_trust_is_per_command_and_dirty_or_other_revision_is_rejected(self):
        with patch(
            "src.baselines.text.sembench_movie.subprocess.check_output",
            side_effect=[SEMBENCH_COMMIT + "\n", ""],
        ) as read:
            root = verify_checkout("/tmp/original-sembench")
            for call in read.call_args_list:
                self.assertEqual(
                    call.args[0][:5], ["git", "-c", "safe.directory=" + str(root), "-C", str(root)]
                )
        for returned in [("0" * 40, ""), (SEMBENCH_COMMIT, " M src/query.py")]:
            with patch(
                "src.baselines.text.sembench_movie.subprocess.check_output", side_effect=returned
            ):
                with self.assertRaisesRegex(ValueError, "pinned source"):
                    verify_checkout("/tmp/original-sembench")

    def test_no_generic_map_source_or_map_quality_can_enter_original_movie_query(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            query = MovieQuery(fixture_dataset(root / "data", root / "source"), 1)
            args = dict(
                arm=LOTUS_MOVIE_ARMS[0],
                plan=SemanticMapPlan("ignored by original method", "fixture", 8),
                model=FixedModelConfig("http://127.0.0.1:1/v1/chat/completions", "fixture", 1000),
                ledger=None,
                unit_id="fixture",
                root=root / "query",
                options=NativeGraphOptions(),
                sembench_query=query,
            )
            for override in (
                {"load_source": lambda: iter(())},
                {"allowed_outputs": ("POSITIVE",)},
                {"reference_outputs": {}},
            ):
                with (
                    self.subTest(override=tuple(override)),
                    self.assertRaisesRegex(ValueError, "selected relation"),
                ):
                    run_supplier_query(**args, **override)
            self.assertFalse((root / "query").exists())


class MovieConfigTests(unittest.TestCase):
    def test_readonly_checkout_can_be_git_while_private_paths_keep_rejecting_git(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            checkout = root / "sembench"
            subprocess.run(
                ["git", "init", "--quiet", str(checkout)], check=True, capture_output=True
            )
            private = root / "model.json"
            private.write_text("{}")
            inside = checkout / "model.json"
            inside.write_text("{}")
            with patch.dict(
                "os.environ",
                {
                    "MOVIE_SOURCE": str(checkout),
                    "MOVIE_MODEL": str(private),
                    "MOVIE_INSIDE_MODEL": str(inside),
                },
            ):
                self.assertEqual(_env_path("MOVIE_SOURCE", private=False), checkout.resolve())
                self.assertEqual(_env_path("MOVIE_MODEL"), private.resolve())
                with self.assertRaisesRegex(ValueError, "outside a Git checkout"):
                    _env_path("MOVIE_SOURCE")
                with self.assertRaisesRegex(ValueError, "outside a Git checkout"):
                    _env_path("MOVIE_INSIDE_MODEL")

    def test_multiple_query_progress_files_are_distinct_and_final_matrix_is_written_once(self):
        from src.experiments.postgresql.sembench_query_cli import run

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dataset = fixture_dataset(root / "data", root / "source")
            model = FixedModelConfig("http://127.0.0.1:1/v1/chat/completions", "fixture", 1000)
            config = dict(
                arms=[LOTUS_MOVIE_ARMS[0]],
                queries=[1, 2, 3],
                warmups=0,
                repeats=1,
                max_attempts=20,
                owner_timeout_s=240,
                query_timeout_s=20,
                native_options={},
            )

            class Group:
                def __init__(self, *args, **kwargs):
                    pass

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    pass

                def run(self, arm, **kwargs):
                    return dict(
                        status="passed",
                        actual_posts=8 if kwargs["sembench_query"].query_id == 1 else 6,
                        full_query_seconds=1,
                        execution={"ready_query_seconds": 0.5},
                        quality={"fixture": True},
                    )

            ready = (
                dataset,
                model,
                NativeGraphOptions(),
                None,
                None,
                root / "output",
                None,
                {"fixture": True},
            )
            with (
                patch(
                    "src.experiments.postgresql.sembench_query_cli.preflight", return_value=ready
                ),
                patch(
                    "src.experiments.postgresql.sembench_query_cli.PersistentAdapterGroup", Group
                ),
            ):
                value = run(config)
            self.assertEqual(value["status"], "passed")
            self.assertEqual(len(list((root / "output").glob("matrix-progress-*.json"))), 3)
            self.assertEqual(len(json.loads((root / "output/matrix.json").read_text())["cells"]), 3)

    def test_example_has_exact_three_queries_four_owners_and_matching_post_count(self):
        path = (
            Path(__file__).resolve().parents[3]
            / "deploy/autodl/sembench_lotus_queries.example.json"
        )
        config = load_config(path)
        self.assertEqual(tuple(config["arms"]), LOTUS_MOVIE_ARMS)
        self.assertEqual(config["queries"], [1, 2, 3])
        self.assertEqual(config["max_attempts"], (2000 + 120 + 120) * 4 * 4)
        from src.execution_provider.adapters.ray_map_transport import RayMapConfig

        RayMapConfig(**config["physical"])

    def test_missing_environment_and_unknown_config_fields_do_not_fall_back(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaisesRegex(ValueError, "required SemBench path"):
                _env_path("SEMBENCH_MANIFEST")
        path = (
            Path(__file__).resolve().parents[3]
            / "deploy/autodl/sembench_lotus_queries.example.json"
        )
        config = load_config(path)
        config["map_instruction"] = "unrequested replacement"
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "config.json"
            target.write_text(json.dumps(config))
            with self.assertRaisesRegex(ValueError, "unsupported fields"):
                load_config(target)

    def test_native_lower_concurrency_remains_per_executor_and_unknown_arm_is_rejected(self):
        config = dict(
            arms=list(LOTUS_MOVIE_ARMS),
            native_options={"concurrency": 4},
            arm_native_options={LOTUS_MOVIE_ARMS[-1]: {"concurrency": 6}},
        )
        for arm in LOTUS_MOVIE_ARMS[:-1]:
            self.assertEqual(arm_options(config, arm).concurrency, 4)
        self.assertEqual(arm_options(config, LOTUS_MOVIE_ARMS[-1]).concurrency, 6)
        config["arm_native_options"]["another-executor"] = {"concurrency": 8}
        with self.assertRaisesRegex(ValueError, "explicitly selected arms"):
            arm_options(config, LOTUS_MOVIE_ARMS[0])


if __name__ == "__main__":
    unittest.main()
