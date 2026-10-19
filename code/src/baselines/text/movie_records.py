"""Finite raw Movie records and separate evaluation labels for plain Q3."""

from contextlib import contextmanager
import csv
import hashlib
import json
from pathlib import Path
import threading

from src.baselines.common.private_artifacts import (
    new_private_directory, open_private_text, write_private_json,
)
from src.semantic_methods.lotus.records import FilterMethod, MOVIE_Q3_INSTRUCTION, RecordScan, identity


SCHEMA = "semloom.movie_records.v1"
RECORD_COLUMNS = ("row_id", "source_position", "reviewId", "id", "reviewText")
REFERENCE_COLUMNS = ("row_id", "source_position", "reviewId", "id", "scoreSentiment")
PREDICATE = dict(column="id", operator="eq", value="taken_3")
# The fixed input profile also used by experiments.postgresql.sembench_queries.
MOVIE_CSV_DIGEST = "ad5def7df4ece425c741c82d73687b8cb4a61952e1580c0427879cbef3033aaf"
MAX_ROWS = 4096
MAX_CSV_BYTES = 16 * 1024 * 1024
MAX_REVIEW_BYTES = 1024 * 1024
MAX_MANIFEST_BYTES = 2 * 1024 * 1024
_CSV_LOCK = threading.RLock()


def validate_movie_method(method):
    """Bind the original Q3 instruction and parser policy at the task boundary."""
    if type(method) is not FilterMethod:
        raise ValueError("Movie source requires the plain FilterMethod")
    method.describe()
    if (method.instruction != MOVIE_Q3_INSTRUCTION or method.default is not True
            or method.output_tokens != ("True", "False") or method.system_prompt is not None):
        raise ValueError("original Movie Q3 requires its instruction, output tokens, default parser policy and system prompt")


def _bounded_file(path, limit=MAX_CSV_BYTES):
    if not path.is_file() or path.stat().st_size > limit:
        raise ValueError("Movie source or artifact exceeds its declared file byte limit")


def _sha256(path):
    path = Path(path)
    _bounded_file(path)
    digest = hashlib.sha256()
    total = 0
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            total += len(block)
            if total > MAX_CSV_BYTES:
                raise ValueError("Movie source or artifact exceeds its declared file byte limit")
            digest.update(block)
    return digest.hexdigest()


def _digest(value):
    if (type(value) is not str or len(value) != 64
            or any(char not in "0123456789abcdef" for char in value)):
        raise ValueError("Movie source requires a complete lowercase SHA256 digest")
    return value


def _row_limit(value):
    if type(value) is not int or not 1 <= value <= MAX_ROWS:
        raise ValueError("max_rows must be a positive integer at most " + str(MAX_ROWS))
    return value


def _role(role, source_digest):
    if role not in ("software_fixture", "sembench_movie_subset"):
        raise ValueError("Movie source requires an explicit fixture or SemBench subset role")
    if role == "sembench_movie_subset" and source_digest != MOVIE_CSV_DIGEST:
        raise ValueError("SemBench Movie subset must use the fixed original CSV SHA256")


@contextmanager
def _csv_reader(path):
    _bounded_file(path)
    # csv's limit is in characters; UTF-8 bytes are separately checked below.
    with _CSV_LOCK:
        old_limit = csv.field_size_limit()
        try:
            csv.field_size_limit(MAX_REVIEW_BYTES)
            with path.open(encoding="utf-8", newline="") as stream:
                yield csv.DictReader(stream)
        except csv.Error as error:
            raise ValueError("Movie CSV is malformed or exceeds the declared field limit") from error
        finally:
            csv.field_size_limit(old_limit)


def _read_csv(path, columns):
    with _csv_reader(path) as reader:
        if reader.fieldnames != list(columns):
            raise ValueError("Movie materialized CSV columns differ from the declared schema")
        rows = []
        for position, row in enumerate(reader):
            if position >= MAX_ROWS:
                raise ValueError("Movie materialized CSV exceeds its declared row limit")
            rows.append(row)
    if any(set(row) != set(columns) or any(value is None for value in row.values()) for row in rows):
        raise ValueError("Movie CSV contains a malformed row")
    return rows


def _original_prefix(source, max_rows, expected_digest):
    if source.is_symlink() or _sha256(source) != expected_digest:
        raise ValueError("Movie input CSV SHA256 differs from the expected source")
    selected, references, source_rows = [], [], 0
    with _csv_reader(source) as reader:
        required = ("reviewId", "id", "reviewText", "scoreSentiment")
        if (not reader.fieldnames or len(set(reader.fieldnames)) != len(reader.fieldnames)
                or not set(required) <= set(reader.fieldnames)):
            raise ValueError("Movie input CSV lacks unique original Movie columns")
        for position, row in enumerate(reader):
            source_rows += 1
            if position >= max_rows:
                continue
            if (None in row or any(type(row.get(key)) is not str for key in required)
                    or not row["reviewId"] or not row["id"]
                    or row["scoreSentiment"] not in ("POSITIVE", "NEGATIVE")):
                raise ValueError("Movie input CSV contains an invalid selected row")
            if len(row["reviewText"].encode("utf-8")) > MAX_REVIEW_BYTES:
                raise ValueError("Movie reviewText exceeds its declared UTF-8 byte limit")
            record = dict(
                row_id="movie-row-" + str(position), source_position=position,
                reviewId=row["reviewId"], id=row["id"], reviewText=row["reviewText"],
            )
            selected.append(record)
            references.append(dict(
                {key: record[key] for key in REFERENCE_COLUMNS[:-1]},
                scoreSentiment=row["scoreSentiment"],
            ))
    if _sha256(source) != expected_digest:
        raise ValueError("Movie input CSV changed during preparation or preflight")
    return selected, references, source_rows


def prepare_movie_source(input_csv, output_dir, *, expected_csv_sha256, max_rows,
                         role="software_fixture") -> Path:
    """Copy the first bounded prefix without labels in the execution records."""
    _row_limit(max_rows)
    expected_csv_sha256 = _digest(expected_csv_sha256)
    _role(role, expected_csv_sha256)
    source = Path(input_csv).resolve(strict=True)
    selected, references, source_rows = _original_prefix(source, max_rows, expected_csv_sha256)
    root = Path(output_dir)
    new_private_directory(root)
    root = root.resolve(strict=True)
    for filename, columns, rows in (
        ("records.csv", RECORD_COLUMNS, selected),
        ("references.csv", REFERENCE_COLUMNS, references),
    ):
        with open_private_text(root / filename, newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
        _bounded_file(root / filename)
    eligible = [dict(row_id=row["row_id"], source_position=row["source_position"])
                for row in selected if row["id"] == PREDICATE["value"]]
    manifest = dict(
        schema=SCHEMA, task="movie-q3", predicate=PREDICATE,
        instruction=MOVIE_Q3_INSTRUCTION, columns=list(RECORD_COLUMNS), role=role,
        original_csv=str(source), source_csv_sha256=expected_csv_sha256, max_rows=max_rows,
        source_rows=source_rows, rows=len(selected), eligible_rows=len(eligible),
        eligible_occurrences=eligible,
        records=dict(path="records.csv", sha256=_sha256(root / "records.csv")),
        references=dict(path="references.csv", sha256=_sha256(root / "references.csv")),
    )
    manifest_path = root / "manifest.json"
    write_private_json(manifest_path, manifest)
    _bounded_file(manifest_path, MAX_MANIFEST_BYTES)
    return manifest_path


class MovieRecordSource:
    def __init__(self, manifest_path, manifest_digest):
        self.manifest_path = manifest_path
        self._manifest_digest = manifest_digest

    @classmethod
    def from_manifest(cls, path):
        path = Path(path)
        if path.is_symlink():
            raise ValueError("Movie manifest must not be a symlink")
        path = path.resolve(strict=True)
        _bounded_file(path, MAX_MANIFEST_BYTES)
        source = cls(path, _sha256(path))
        source._load()
        return source

    def _artifact(self, value, filename):
        if type(value) is not dict or set(value) != {"path", "sha256"}:
            raise ValueError("Movie artifact identity must contain path and SHA256")
        if value["path"] != filename:
            raise ValueError("Movie artifact path must name its same-directory CSV")
        path = self.manifest_path.parent / filename
        if (path.is_symlink() or not path.is_file()
                or path.resolve(strict=True).parent != self.manifest_path.parent):
            raise ValueError("Movie artifact path is not a same-directory regular file")
        if _sha256(path) != _digest(value["sha256"]):
            raise ValueError("Movie artifact SHA256 differs from its manifest")
        return path

    def _load(self):
        _bounded_file(self.manifest_path, MAX_MANIFEST_BYTES)
        if (self.manifest_path.is_symlink() or not self.manifest_path.is_file()
                or _sha256(self.manifest_path) != self._manifest_digest):
            raise ValueError("Movie manifest changed after source selection")
        with self.manifest_path.open("rb") as stream:
            raw = stream.read(MAX_MANIFEST_BYTES + 1)
        if len(raw) > MAX_MANIFEST_BYTES:
            raise ValueError("Movie manifest exceeds its declared file byte limit")
        manifest = json.loads(raw)
        if (type(manifest) is not dict or manifest.get("schema") != SCHEMA
                or manifest.get("task") != "movie-q3"
                or manifest.get("predicate") != PREDICATE
                or manifest.get("instruction") != MOVIE_Q3_INSTRUCTION
                or manifest.get("columns") != list(RECORD_COLUMNS)):
            raise ValueError("Movie manifest task/predicate/schema/instruction/columns differ")
        _role(manifest.get("role"), _digest(manifest.get("source_csv_sha256")))
        max_rows = _row_limit(manifest.get("max_rows"))
        original_csv = manifest.get("original_csv")
        if type(original_csv) is not str or not Path(original_csv).is_absolute():
            raise ValueError("Movie original_csv must retain its caller-selected absolute path")
        for name in ("source_rows", "rows", "eligible_rows"):
            if type(manifest.get(name)) is not int or manifest[name] < 0:
                raise ValueError("Movie manifest row counts must be nonnegative integers")
        if manifest["rows"] != min(max_rows, manifest["source_rows"]):
            raise ValueError("Movie manifest prefix row count differs")
        records = _read_csv(self._artifact(manifest.get("records"), "records.csv"), RECORD_COLUMNS)
        references = _read_csv(
            self._artifact(manifest.get("references"), "references.csv"), REFERENCE_COLUMNS,
        )
        if len(records) != manifest["rows"] or len(references) != len(records):
            raise ValueError("Movie records and references do not cover the declared prefix")
        for position, (record, reference) in enumerate(zip(records, references)):
            if (record["row_id"] != "movie-row-" + str(position)
                    or record["source_position"] != str(position)
                    or not record["reviewId"] or not record["id"]):
                raise ValueError("Movie occurrence identity differs from the source prefix")
            if len(record["reviewText"].encode("utf-8")) > MAX_REVIEW_BYTES:
                raise ValueError("Movie reviewText exceeds its declared UTF-8 byte limit")
            if (any(reference[key] != record[key] for key in REFERENCE_COLUMNS[:-1])
                    or reference["scoreSentiment"] not in ("POSITIVE", "NEGATIVE")):
                raise ValueError("Movie label identity differs from its execution occurrence")
            record["source_position"] = position
            reference["source_position"] = position
        original_rows, original_references, source_rows = _original_prefix(
            Path(original_csv), max_rows, manifest["source_csv_sha256"],
        )
        if (records != original_rows or references != original_references
                or manifest["source_rows"] != source_rows):
            raise ValueError("Movie materialized records/references differ from the original CSV prefix")
        eligible = [dict(row_id=row["row_id"], source_position=row["source_position"])
                    for row in records if row["id"] == PREDICATE["value"]]
        occurrences = manifest.get("eligible_occurrences")
        if (type(occurrences) is not list
                or any(type(item) is not dict or set(item) != {"row_id", "source_position"}
                       or type(item["row_id"]) is not str
                       or type(item["source_position"]) is not int
                       for item in occurrences)
                or occurrences != eligible
                or manifest["eligible_rows"] != len(eligible)):
            raise ValueError("Movie eligible occurrences differ from the fixed predicate")
        return manifest, records, references

    @property
    def csv_path(self):
        manifest, _, _ = self._load()
        return self._artifact(manifest["records"], "records.csv")

    def preflight(self, method):
        validate_movie_method(method)
        manifest, records, _ = self._load()
        return dict(
            schema="semloom.movie_records_preflight.v1", status="passed",
            manifest_sha256=self._manifest_digest, role=manifest["role"],
            task=manifest["task"], predicate=dict(PREDICATE), method=method.describe(),
            source_csv_sha256=manifest["source_csv_sha256"], rows=len(records),
            eligible_rows=manifest["eligible_rows"], records=dict(manifest["records"]),
            references=dict(manifest["references"]),
            label_scope="preflight association and post-query evaluation only",
            logical_limits=dict(max_rows=MAX_ROWS, file_bytes=MAX_CSV_BYTES,
                                review_utf8_bytes=MAX_REVIEW_BYTES,
                                manifest_bytes=MAX_MANIFEST_BYTES),
            limit_scope="logical input limits; not RSS or GPU memory hard limits",
        )

    def scan_spec(self, method):
        self.preflight(method)
        return RecordScan(
            csv_schema=tuple((name, "int64" if name == "source_position" else "string")
                             for name in RECORD_COLUMNS),
            payload_columns=("reviewText",), predicate=(PREDICATE["column"], PREDICATE["value"]),
        )

    def expected(self, query_id):
        if type(query_id) is not str or not query_id.strip():
            raise ValueError("query_id must be a nonempty string")
        _, records, _ = self._load()
        return [identity(query_id, row["row_id"], row["source_position"])
                for row in records if row["id"] == PREDICATE["value"]]

    def rows(self):
        return self._load()[1]

    def labeled_frame(self):
        _, records, references = self._load()
        import pandas as pd
        return pd.DataFrame([
            dict(reviewId=row["reviewId"], id=row["id"], reviewText=row["reviewText"],
                 scoreSentiment=reference["scoreSentiment"])
            for row, reference in zip(records, references)
        ], columns=["reviewId", "id", "reviewText", "scoreSentiment"])
