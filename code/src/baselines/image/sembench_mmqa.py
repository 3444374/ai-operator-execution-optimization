"""Original finite MMQA Q2b inputs, query and evaluator; no execution policy."""

from dataclasses import asdict, dataclass
import hashlib
import importlib
import json
from pathlib import Path
import re
import subprocess
import sys
from types import SimpleNamespace

from src.baselines.common.private_artifacts import content_digest, file_identity, new_private_directory, open_private_text, write_private_json
from src.baselines.image.sembench_animals import image_identity, upstream_bytes
from src.baselines.text.sembench_movie import SEMBENCH_COMMIT, _load, verify_checkout


SCHEMA = "semloom.sembench_mmqa_q2b.v1"
RUNNER = "src/scenario/mmqa/runner/lotus_runner/lotus_runner.py"
GENERATOR = "src/scenario/mmqa/preparation/generate_data.py"
EVALUATOR = "src/scenario/mmqa/evaluation/evaluate.py"
GOLD = "files/mmqa/query/natural_language/q2b.json"
SOURCES = (RUNNER, GENERATOR, EVALUATOR, GOLD,
           "src/runner/generic_lotus_runner/generic_lotus_runner.py",
           "src/runner/generic_runner.py", "src/evaluator/generic_evaluator.py")
SOURCE_URL = "https://drive.google.com/uc?id=1oHXq5oxIfsyoNy9aCQ0V9G3W4puqHC6i"
SOURCE_ARCHIVE_SHA256 = "c3dcb4902955e6da8a7170002f9036cc6ffb471c3ee112855425ef0789badf52"
ELIGIBLE_SOURCE_IMAGES = 925


def verify_mmqa_checkout(checkout):
    root = verify_checkout(checkout)
    dirty = subprocess.check_output(
        ["git", "-c", "safe.directory=" + str(root), "-C", str(root), "status", "--porcelain",
         "--untracked-files=no", "--", "files/mmqa/query"], text=True, timeout=15)
    if dirty:
        raise ValueError("MMQA query checkout is modified")
    return {name: hashlib.sha256(upstream_bytes(root, name)).hexdigest() for name in SOURCES}


def load_mmqa_module(checkout, name, relative):
    # MMQA alone imports src.runner; the other upstream runners import runner.
    # Expose that exact pinned package during import without editing upstream.
    root = verify_checkout(checkout)
    package = importlib.import_module("src")
    original = package.__path__
    for key, module in tuple(sys.modules.items()):
        if key.startswith("src.runner") and module is not None:
            file = getattr(module, "__file__", None)
            paths = [file] if file else list(getattr(module, "__path__", ()))
            if not paths or any(not Path(p).resolve().is_relative_to(root / "src") for p in paths):
                raise RuntimeError("another MMQA namespace is loaded; use an isolated process")
    package.__path__ = [*original, str(root / "src")]
    try:
        return _load(root, name, relative)
    finally:
        package.__path__ = original


def call_budget(images, table_rows):
    if any(type(v) is not int or v < 1 for v in (images, table_rows)):
        raise ValueError("Q2b needs nonempty finite image and table relations")
    pairs = images * table_rows
    return dict(join=pairs, extract_maximum=pairs, total_maximum=2 * pairs,
                rule="native exact cartesian Join plus at most one Extract per matched pair",
                warmup=0, retries=0)


def prepare_dataset(checkout, source_data, output, *, scale_factor, source_archive):
    """Run only the original fixed-table/image generator methods, without download."""
    sources = verify_mmqa_checkout(checkout)
    if type(scale_factor) is not int or not 25 <= scale_factor <= 64:
        raise ValueError("Q2b observation supports original scale factors 25 through 64")
    source_data, output, source_archive = map(lambda p: Path(p).resolve(), (source_data, output, source_archive))
    if file_identity(source_archive)["sha256"] != SOURCE_ARCHIVE_SHA256:
        raise ValueError("MMQA source archive differs from the verified official download")
    # These two pinned generator methods invoke shell cp. Reject paths the
    # unquoted upstream command cannot represent safely, including whitespace.
    if any(re.fullmatch(r"[A-Za-z0-9_./-]+", str(p)) is None for p in (source_data, output)):
        raise ValueError("upstream image generator requires shell-safe source/output paths")
    import zipfile
    catalog = list((source_data / "mmqa_1k_images").iterdir())
    images = [p for p in catalog if p.suffix in (".png", ".jpg")]
    if len(images) != ELIGIBLE_SOURCE_IMAGES or any(not p.is_file() or p.is_symlink()
            or re.fullmatch(r"[A-Za-z0-9_.-]+", p.name) is None for p in images):
        raise ValueError("Q2b requires all original lowercase png/jpg source candidates")
    # Check the supplied extraction against one retained archive; the retrieval
    # URL and archive digest are provenance, not an author-published checksum.
    with zipfile.ZipFile(source_archive) as archive:
        for path in [source_data / "ap_warrior.csv", *images]:
            suffix = path.relative_to(source_data).as_posix()
            members = [n for n in archive.namelist() if n == suffix or n.endswith("/" + suffix)]
            if len(members) != 1 or hashlib.sha256(archive.read(members[0])).hexdigest() != file_identity(path)["sha256"]:
                raise ValueError("MMQA extracted input does not match its retained archive")
    module = load_mmqa_module(checkout, "sembench_mmqa_generator", GENERATOR)
    new_private_directory(output)
    owner = object.__new__(module.MMQADataGenerator)
    owner.source_data_dir, owner.output_data_dir = str(source_data), str(output / "data")
    owner.scale_factor = scale_factor
    Path(owner.output_data_dir).mkdir()
    # _copy_fixed_files also copies ben_piazza.csv; it remains an unused original asset.
    owner._copy_fixed_files()
    owner._generate_images_per_scale_factor()
    data = Path(owner.output_data_dir)
    order = [p.name for p in (data / "images").iterdir() if p.name.endswith((".png", ".jpg"))]
    payload = dict(schema=SCHEMA, sembench_commit=SEMBENCH_COMMIT, role="upstream_generated_q2b_slice",
        scale_factor=scale_factor, data_path=str(data), table=file_identity(data / "ap_warrior.csv"),
        images=[dict(name=n, image=image_identity(data / "images" / n)) for n in order],
        provenance=dict(source_url=SOURCE_URL, archive=file_identity(source_archive),
            archive_checksum_scope="locally measured; no author-published digest available",
            source_catalog=[p.name for p in images], source_catalog_digest=content_digest([p.name for p in images]),
            generator_sha256=sources[GENERATOR], source_sha256=sources))
    payload["sha256"] = content_digest(payload)
    write_private_json(output / "manifest.json", payload)
    dataset = MmqaDataset(Path(checkout), output / "manifest.json", payload["sha256"])
    dataset.preflight()
    return payload


@dataclass(frozen=True)
class MmqaDataset:
    checkout: Path
    manifest: Path
    expected_digest: str

    def read(self):
        value = json.loads(Path(self.manifest).read_text())
        payload = {k: v for k, v in value.items() if k != "sha256"}
        if (value.get("schema") != SCHEMA or value.get("sembench_commit") != SEMBENCH_COMMIT
                or value.get("sha256") != self.expected_digest or content_digest(payload) != self.expected_digest):
            raise ValueError("MMQA manifest identity differs")
        role, images = value.get("role"), value.get("images")
        if role not in ("software_fixture", "upstream_generated_q2b_slice"):
            raise ValueError("MMQA source role must be explicit")
        if not isinstance(images, list) or not 1 <= len(images) <= 64:
            raise ValueError("Q2b accepts at most 64 original image occurrences")
        data = Path(value["data_path"])
        if file_identity(data / "ap_warrior.csv") != value.get("table"):
            raise ValueError("MMQA table bytes differ")
        names = [p.name for p in (data / "images").iterdir() if p.name.endswith((".png", ".jpg"))]
        if names != [r.get("name") for r in images] or len(set(names)) != len(names):
            raise ValueError("MMQA native image enumeration differs")
        for row in images:
            if Path(row["name"]).name != row["name"] or image_identity(data / "images" / row["name"]) != row.get("image"):
                raise ValueError("MMQA image bytes or pixels differ")
        if role == "upstream_generated_q2b_slice":
            provenance = value.get("provenance", {})
            catalog = provenance.get("source_catalog", [])
            if (value.get("scale_factor") != len(images) or not 25 <= len(images) <= 64
                    or len(catalog) != ELIGIBLE_SOURCE_IMAGES or len(set(catalog)) != ELIGIBLE_SOURCE_IMAGES
                    or content_digest(catalog) != provenance.get("source_catalog_digest")
                    or provenance.get("source_url") != SOURCE_URL
                    or provenance.get("archive", {}).get("sha256") != SOURCE_ARCHIVE_SHA256):
                raise ValueError("MMQA generated input provenance is incomplete")
        self.table_frame(value)
        return value

    def table_frame(self, value=None):
        import pandas as pd
        if value is None:
            value = self.read()
        frame = pd.read_csv(Path(value["data_path"]) / "ap_warrior.csv", sep=",", quotechar='"')
        if (not {"ID", "Track"} <= set(frame.columns) or not 1 <= len(frame) <= 32
                or frame["ID"].isna().any() or frame["ID"].duplicated().any()
                or any(type(v) is not str or not v for v in frame["Track"])):
            raise ValueError("Q2b needs unique non-null IDs and complete Track values, at most 32 rows")
        return frame

    def preflight(self):
        sources, value = verify_mmqa_checkout(self.checkout), self.read()
        if value["role"] == "upstream_generated_q2b_slice" and value["provenance"].get("source_sha256") != sources:
            raise ValueError("MMQA generator/query/evaluator source identity differs")
        return dict(schema="semloom.mmqa_q2b_preflight.v1", status="passed", role=value["role"],
            manifest_digest=self.expected_digest, sembench_commit=SEMBENCH_COMMIT, source_sha256=sources,
            images=len(value["images"]), table_rows=len(self.table_frame(value)),
            budget=call_budget(len(value["images"]), len(self.table_frame(value))), model_calls=0,
            scope="original Q2b on a finite external relation; no PostgreSQL integration claim")


def run_original_query(dataset):
    verify_mmqa_checkout(dataset.checkout)
    value = dataset.read()
    module = load_mmqa_module(dataset.checkout, "sembench_mmqa_lotus", RUNNER)

    def load(name, **kwargs):
        if name != "ap_warrior.csv" or kwargs != dict(sep=",", quotechar='"'):
            raise ValueError("unexpected Q2b table load")
        return dataset.table_frame(value)

    owner = SimpleNamespace(load_data=load, data_path=value["data_path"], policy="exact")
    owner._configure_lotus_for_join_type = lambda kind: module.LotusRunner._configure_lotus_for_join_type(owner, kind)
    return module.LotusRunner._execute_q2b(owner)


def evaluate_original(checkout, result, output):
    verify_mmqa_checkout(checkout)
    if list(result.columns) != ["ID", "image_id", "color"]:
        raise ValueError("original Q2b result must have exactly ID/image_id/color")
    path = Path(output) / "original-gold.json"
    with open_private_text(path) as stream:
        stream.write(upstream_bytes(checkout, GOLD).decode())
    module = load_mmqa_module(checkout, "sembench_mmqa_evaluator", EVALUATOR)
    evaluator = object.__new__(module.MMQAEvaluator)
    metric = evaluator._evaluate_single_query("2b", result.copy(), path)
    return dict(original_metric=asdict(metric), gold_sha256=file_identity(path)["sha256"],
                gold_scope="unchanged original q2b triples, including color; no subset gold rewrite",
                sembench_commit=SEMBENCH_COMMIT)
