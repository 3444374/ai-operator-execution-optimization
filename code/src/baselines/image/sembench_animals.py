"""Pinned Animals image queries, original evaluators, and frozen inputs.

Query preparation is independent of the caller's LM. A current local visual
model provider is not supplied by this adapter.
"""

from dataclasses import asdict, dataclass
from functools import partial
import hashlib
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace

from src.infrastructure.private_artifacts import content_digest
from src.baselines.text.sembench_source import SEMBENCH_COMMIT, load_module, verify_checkout


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
IMAGE_QUERY_IDS = (1, 3, 7, 10)


def filter_passes(query_id):
    """Semantic workload shape, shared by preflight and execution adapters."""
    if type(query_id) is not int or query_id not in IMAGE_QUERY_IDS:
        raise ValueError('only original Animals image queries Q1/Q3/Q7/Q10 are supported')
    return 2 if query_id == 7 else 1


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


def verify_animals_checkout(checkout, query_id=1):
    if type(query_id) is not int or query_id not in IMAGE_QUERY_IDS:
        raise ValueError("only original Animals image queries Q1/Q3/Q7/Q10 are supported")
    root = verify_checkout(checkout)
    # Include Animals SQL, which is outside the shared source verifier's roots.
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
    files = SOURCE_FILES + ((f"files/animals/query/gold_sql/Q{query_id}.sql",) if query_id != 1 else ())
    return {name: hashlib.sha256(upstream_bytes(root, name)).hexdigest() for name in files}


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
        if not isinstance(rows, list) or not rows:
            raise ValueError("Animals manifest requires a nonempty image occurrence list")
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

    def preflight(self, query_id=1):
        sources = verify_animals_checkout(self.checkout, query_id)
        value = self.read()
        passes = filter_passes(query_id)
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
            query_id=query_id,
            query="Q" + str(query_id),
            native_filter_passes=passes,
            expected_model_calls=len(value["rows"]) * passes,
            call_count_scope="uncached complete image Filter on every pass; repeated occurrences retained",
            source_sha256=sources,
            upstream_lotus_version="1.1.3",
            selected_lotus_version="1.2.4",
            model_calls=0,
            local_vision_provider_status="unwired",
            input_scope="finite external ImageArray relation; no database claim",
        )

    def frame(self, *, references=False):
        return self._frame(self.read()["rows"], references=references)

    def bind_frame(self):
        """Verify before timing, then freshly read manifest rows without pixel audit.

        Native ImageArray still reads/decodes the actual images. The caller runs
        read() again after the complete sink to audit manifest and image identity.
        """
        self.read()

        def load():
            return self._frame(json.loads(Path(self.manifest).read_text())["rows"])

        return load

    @staticmethod
    def _frame(rows, *, references=False):
        import pandas as pd

        fields = ["ImagePath", "City", "StationID"]
        if references:
            fields.append("Species")
        frame = pd.DataFrame([{k: row[k] for k in fields} for row in rows])
        if not references:
            frame["_execution_row_id"] = [row["row_id"] for row in rows]
        return frame


def run_original_query(checkout, load_images, query_id=1):
    """Call an unchanged upstream image query, without constructor warmup."""
    return bind_original_query(checkout, load_images, query_id)()


def bind_original_query(checkout, load_images, query_id=1):
    """Bind the original function before timing; its source remains freshly read."""
    verify_animals_checkout(checkout, query_id)
    module = load_module(
        checkout,
        "sembench_animals_lotus",
        "src/scenario/animals/runner/lotus_runner/lotus_runner.py",
    )

    def load(name):
        if name != "image_data.csv":
            raise ValueError("unexpected source requested by the original Animals query")
        frame = load_images()
        if "Species" in frame.columns:
            raise ValueError("reference Species must not enter the semantic query")
        return frame

    return partial(getattr(module.LotusRunner, f"_execute_q{query_id}"), SimpleNamespace(load_data=load))


def evaluate_original(checkout, result, labeled_images, query_id=1):
    """Execute original Species SQL and the original query-specific evaluator."""
    import duckdb

    verify_animals_checkout(checkout, query_id)
    sql = upstream_bytes(checkout, f"files/animals/query/gold_sql/Q{query_id}.sql").decode()
    with duckdb.connect() as connection:
        connection.register("ImageData", labeled_images)
        truth = connection.execute(sql).fetchdf()
    module = load_module(
        checkout, "sembench_animals_evaluator", "src/scenario/animals/evaluation/evaluate.py"
    )
    evaluator = object.__new__(module.AnimalsEvaluator)
    metric = getattr(evaluator, f"_evaluate_q{query_id}")(result, truth)
    if query_id == 7:
        return dict(original_metric=asdict(metric), ground_truth=sorted(truth.iloc[:, 0].tolist()),
                    predicted=result.iloc[:, 0].tolist(), sembench_commit=SEMBENCH_COMMIT)
    if query_id in (3, 10):
        return dict(original_metric=asdict(metric), ground_truth=truth.to_dict(orient="records"),
                    predicted=result.to_dict(orient="records"), sembench_commit=SEMBENCH_COMMIT)
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
