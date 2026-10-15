"""Pinned Animals Q1, its original aggregate evaluator, and frozen image inputs."""

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace

from src.baselines.common.private_artifacts import content_digest
from src.baselines.text.sembench_movie import SEMBENCH_COMMIT, _load, verify_checkout


SOURCE_FILES = (
    "src/scenario/animals/runner/lotus_runner/lotus_runner.py",
    "src/scenario/animals/evaluation/evaluate.py",
    "src/scenario/animals/preparation/generate_data.py",
    "src/scenario/animals/animals_scenario.py",
    "src/runner/generic_lotus_runner/generic_lotus_runner.py",
    "src/evaluator/generic_evaluator.py",
    "files/animals/query/gold_sql/Q1.sql",
)
MANIFEST_SCHEMA = "semloom.sembench_animals.v1"


def upstream_bytes(checkout, relative):
    root = Path(checkout).resolve()
    raw = subprocess.check_output(
        ["git", "-c", "safe.directory=" + str(root), "-C", str(root), "show", "HEAD:" + relative],
        timeout=15,
        stderr=subprocess.PIPE,
    )
    materialized = root / relative
    if materialized.exists() and materialized.read_bytes() != raw:
        raise ValueError("materialized Animals source differs from its pinned object")
    return raw


def verify_animals_checkout(checkout):
    root = verify_checkout(checkout)
    # Include the sparse SQL path: the shared Movie verifier only covers Movie SQL.
    dirty = subprocess.check_output(
        [
            "git",
            "-c",
            "safe.directory=" + str(root),
            "-C",
            str(root),
            "status",
            "--porcelain",
            "--untracked-files=no",
            "--",
            "files/animals/query",
        ],
        text=True,
        timeout=15,
    )
    if dirty:
        raise ValueError("Animals SQL checkout is modified")
    return {name: hashlib.sha256(upstream_bytes(root, name)).hexdigest() for name in SOURCE_FILES}


def image_identity(path):
    """Verify encoded content and effective RGB pixels without resizing."""
    from PIL import Image

    path = Path(path)
    raw = path.read_bytes()
    with Image.open(path) as image:
        rgb = image.convert("RGB")
        return dict(
            sha256=hashlib.sha256(raw).hexdigest(),
            bytes=len(raw),
            width=rgb.width,
            height=rgb.height,
            rgb_sha256=hashlib.sha256(rgb.tobytes()).hexdigest(),
        )


@dataclass(frozen=True)
class AnimalsDataset:
    checkout: Path
    manifest: Path
    expected_digest: str

    def read(self):
        value = json.loads(Path(self.manifest).read_text())
        digest = value.get("sha256")
        payload = {k: v for k, v in value.items() if k != "sha256"}
        if (
            value.get("schema") != MANIFEST_SCHEMA
            or digest != self.expected_digest
            or digest != content_digest(payload)
            or value.get("sembench_commit") != SEMBENCH_COMMIT
        ):
            raise ValueError("Animals frozen manifest identity differs")
        role = value.get("role")
        if role not in ("official_animals_sf64", "software_fixture"):
            raise ValueError("Animals input needs an explicit official or fixture identity")
        rows = value.get("rows")
        if not isinstance(rows, list) or not 1 <= len(rows) <= 64:
            raise ValueError("Animals qualification accepts at most 64 image occurrences")
        if role == "official_animals_sf64":
            provenance = value.get("provenance", {})
            if (
                len(rows) != 64
                or provenance.get("seed") != 42
                or provenance.get("scale_factor") != 64
                or provenance.get("selection") != "upstream_generator_over_complete_archive_catalog"
                or provenance.get("image_catalog_count") != 8718
                or provenance.get("audio_catalog_count") != 650
            ):
                raise ValueError(
                    "official Animals requires the complete original catalog and seed42 selection"
                )
        for position, row in enumerate(rows):
            if (
                row.get("row_id") != "animals-row-" + str(position)
                or any(
                    type(row.get(name)) is not str
                    for name in ("ImagePath", "Species", "City", "StationID")
                )
                or image_identity(row["ImagePath"]) != row.get("image")
            ):
                raise ValueError("Animals source occurrence, bytes or pixels differ")
        return value

    def preflight(self):
        sources = verify_animals_checkout(self.checkout)
        value = self.read()
        if value["role"] == "official_animals_sf64":
            if value["provenance"].get("generator_sha256") != sources[SOURCE_FILES[2]]:
                raise ValueError("Animals generation source is not the pinned upstream function")
        return dict(
            schema="semloom.sembench_animals_preflight.v1",
            status="passed",
            sembench_commit=SEMBENCH_COMMIT,
            role=value["role"],
            manifest_digest=value["sha256"],
            rows=len(value["rows"]),
            source_sha256=sources,
            upstream_lotus_version="1.1.3",
            selected_lotus_version="1.2.4",
            model_calls=0,
            input_scope="finite external ImageArray relation; no database claim",
        )

    def frame(self, *, references=False):
        import pandas as pd

        rows = self.read()["rows"]
        fields = ["ImagePath", "City", "StationID"]
        if references:
            fields.append("Species")
        frame = pd.DataFrame([{k: row[k] for k in fields} for row in rows])
        if not references:
            frame["_execution_row_id"] = [row["row_id"] for row in rows]
        return frame


def run_original_query(checkout, load_images):
    """Call the unchanged upstream Q1; its constructor warmup is intentionally separate."""
    verify_animals_checkout(checkout)
    module = _load(
        checkout,
        "sembench_animals_lotus",
        "src/scenario/animals/runner/lotus_runner/lotus_runner.py",
    )

    def load(name):
        if name != "image_data.csv":
            raise ValueError("unexpected Animals Q1 source")
        frame = load_images()
        if "Species" in frame.columns:
            raise ValueError("reference Species must not enter the semantic query")
        return frame

    return module.LotusRunner._execute_q1(SimpleNamespace(load_data=load))


def evaluate_original(checkout, result, labeled_images):
    """Execute original Species SQL and the original Animals aggregation evaluator."""
    import duckdb

    verify_animals_checkout(checkout)
    sql = upstream_bytes(checkout, SOURCE_FILES[-1]).decode()
    with duckdb.connect() as connection:
        connection.register("ImageData", labeled_images)
        truth = connection.execute(sql).fetchdf()
    module = _load(
        checkout, "sembench_animals_evaluator", "src/scenario/animals/evaluation/evaluate.py"
    )
    evaluator = object.__new__(module.AnimalsEvaluator)
    metric = evaluator._evaluate_q1(result, truth)
    return dict(
        original_metric=asdict(metric),
        ground_truth=int(truth.iloc[0, 0]),
        predicted=int(result.iloc[0, 0]),
        sembench_commit=SEMBENCH_COMMIT,
    )


def classification_audit(rows, decisions):
    """COUNT errors can cancel; keep every original Filter decision as a supplement."""
    expected = [row["row_id"] for row in rows]
    if [identity for identity, _ in decisions] != expected or any(
        type(kept) is not bool for _, kept in decisions
    ):
        raise ValueError("Animals Filter decisions must cover every occurrence exactly once")
    counts = dict(true_positive=0, false_positive=0, true_negative=0, false_negative=0)
    for row, (_, kept) in zip(rows, decisions):
        positive = "ZEBRA" in row["Species"]
        counts[
            ("true_" if kept == positive else "false_") + ("positive" if kept else "negative")
        ] += 1
    return dict(**counts, evaluated_rows=len(rows))
