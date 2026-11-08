"""Finite private CSV values for native cascade backends, without task predicates."""

import csv
import hashlib
import io
import os
from pathlib import Path
import stat

from src.baselines.common.private_artifacts import open_private_text, require_outside_git
from src.semantic_methods.lotus.record_chain import RecordOccurrence
from src.semantic_methods.lotus.records import FilterMethod, RecordScan, identity


MAX_ROWS = 4096
MAX_CSV_BYTES = 16 * 1024 * 1024
MAX_FIELD_CHARS = 128 * 1024


def _check_reader_limit():
    if csv.field_size_limit() < MAX_FIELD_CHARS:
        raise ValueError("current CSV reader field limit is below the cascade source profile")


def _write_csv_row(writer, values):
    fields = tuple(values)
    if any(len(value) > MAX_FIELD_CHARS for value in fields):
        raise ValueError("cascade source CSV field exceeds " + str(MAX_FIELD_CHARS) + " Unicode characters")
    writer.writerow(fields)


class _BoundedCsvText(io.StringIO):
    def __init__(self):
        super().__init__(newline="")
        self.utf8_bytes = 0

    def write(self, value):
        size = len(value.encode("utf-8"))
        if self.utf8_bytes + size > MAX_CSV_BYTES:
            raise ValueError("cascade source CSV exceeds its UTF-8 byte limit")
        self.utf8_bytes += size
        return super().write(value)


class MaterializedCascadeSource:
    """Project one finite relation to the existing native CSV scan contract.

    The caller supplies the ordered union of main/helper text columns. Original
    records and relation metadata remain with its controller; this source keeps
    only occurrence keys, the bound main method and immutable scan/file values.
    """

    def __init__(self, directory, records, *, method, payload_columns):
        directory = Path(directory)
        if directory.is_symlink() or not directory.is_dir():
            raise ValueError("cascade source needs an existing nonsymlink private directory")
        directory = directory.resolve(strict=True)
        require_outside_git(directory)
        _check_reader_limit()
        if type(method) is not FilterMethod or not method.resolved_input_columns:
            raise ValueError("cascade source requires a bound FilterMethod")
        self._method = FilterMethod.from_dict(method.describe())
        if type(payload_columns) not in (list, tuple):
            raise ValueError("cascade source payload columns must be a finite ordered sequence")
        self._scan = RecordScan(
            (("row_id", "string"), ("source_position", "int64"),
             *((name, "string") for name in payload_columns)),
            tuple(payload_columns), predicate=None,
        )
        if not set(self._method.resolved_input_columns).issubset(self._scan.payload_columns):
            raise ValueError("cascade source payload omits a bound method column")
        if type(records) is not tuple or len(records) > MAX_ROWS:
            raise ValueError("cascade source requires a finite tuple of at most 4096 occurrences")

        keys, row_ids, positions = [], set(), set()
        with _BoundedCsvText() as text:
            writer = csv.writer(text)
            _write_csv_row(writer, (name for name, _ in self._scan.csv_schema))
            for value in records:
                if type(value) is not RecordOccurrence:
                    raise TypeError("cascade source requires RecordOccurrence values")
                record = RecordOccurrence.from_dict(value.describe())
                if (record.row_id in row_ids or record.source_position in positions
                        or record.source_position >= 2**63):
                    raise ValueError("cascade source requires unique IDs and unique int64 positions")
                if record.index_updates:
                    raise ValueError("cascade source cannot consume pending index_updates")
                if any(name not in record.values or type(record.values[name]) is not str
                       for name in self._scan.payload_columns):
                    raise ValueError("cascade source requires every projected payload column to be text")
                row_ids.add(record.row_id)
                positions.add(record.source_position)
                keys.append((record.row_id, record.source_position))
                _write_csv_row(writer, (record.row_id, str(record.source_position),
                                       *(record.values[name] for name in self._scan.payload_columns)))
            payload = text.getvalue()
            self._csv_bytes = text.utf8_bytes

        self._directory = directory
        self._path = directory / "cascade-source.csv"
        self._occurrences = tuple(keys)
        self._csv_sha256 = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        with open_private_text(self._path, newline="") as output:
            output.write(payload)
        self._check_csv()

    @property
    def csv_path(self):
        return self._path

    def _check_csv(self):
        _check_reader_limit()
        if self._directory.is_symlink():
            raise ValueError("cascade source directory must not become a symlink")
        require_outside_git(self._directory)
        try:
            descriptor = os.open(self._path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(descriptor, "rb") as source:
                info = os.fstat(source.fileno())
                if (not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600
                        or info.st_size > MAX_CSV_BYTES):
                    raise ValueError("cascade source CSV must remain a private bounded regular file")
                digest, size = hashlib.sha256(), 0
                for block in iter(lambda: source.read(1024 * 1024), b""):
                    size += len(block)
                    if size > MAX_CSV_BYTES:
                        raise ValueError("cascade source CSV exceeds its byte limit")
                    digest.update(block)
        except OSError as error:
            raise ValueError("cascade source CSV must remain a nonsymlink regular file") from error
        if size != self._csv_bytes or digest.hexdigest() != self._csv_sha256:
            raise ValueError("cascade source CSV bytes or SHA256 changed")

    def preflight(self, method):
        if type(method) is not FilterMethod or method.describe() != self._method.describe():
            raise ValueError("cascade source bound method changed")
        self._check_csv()
        return dict(
            schema="semloom.materialized_cascade_source.preflight.v1", status="passed",
            method=self._method.describe(), scan=self._scan.describe(),
            rows=len(self._occurrences), csv_sha256=self._csv_sha256, csv_bytes=self._csv_bytes,
            logical_limits=dict(max_rows=MAX_ROWS, csv_bytes=MAX_CSV_BYTES,
                                field_unicode_chars=MAX_FIELD_CHARS),
            limit_scope="finite CSV values; not an RSS or GPU memory bound",
        )

    def scan_spec(self, method):
        self.preflight(method)
        return RecordScan.from_dict(self._scan.describe())

    def expected(self, query_id):
        if type(query_id) is not str or not query_id.strip():
            raise ValueError("query_id must be a nonempty string")
        self._check_csv()
        return [identity(query_id, row_id, position) for row_id, position in self._occurrences]
