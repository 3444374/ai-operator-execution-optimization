"""Small hash-bound CSV source and explicit CPU kernels for backend tests only."""

import csv
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import uuid

from src.baselines.text.frameworks.daft_engine_process import EngineProcess
from src.baselines.text.frameworks.ray_data_lotus_records import InferBatch
from src.execution_provider.adapters.lotus_records_actor_owner import _ModelActor, _check
from src.execution_provider.adapters.lotus_records_core import _CoreModelActor
from src.semantic_methods.lotus.records import RecordScan, identity
from src.serving.backends.vllm_engine import EngineSpec


class CsvFilterFixture:
    """A bounded test producer; it owns schema, predicate and method binding."""

    def __init__(self, directory, method, scan, rows):
        if not isinstance(scan, RecordScan) or not rows or len(rows) > 16:
            raise ValueError("CSV fixture requires a RecordScan and at most sixteen rows")
        self.csv_path = Path(directory) / "generic.csv"
        self.method = method.describe()
        self.scan = scan
        with self.csv_path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        self.sha256 = hashlib.sha256(self.csv_path.read_bytes()).hexdigest()

    def _rows(self):
        with self.csv_path.open(newline="", encoding="utf-8") as stream:
            rows = list(csv.DictReader(stream))
        for row in rows:
            for name, dtype in self.scan.csv_schema:
                if dtype == "int64":
                    row[name] = int(row[name])
                elif type(row[name]) is not str:
                    raise TypeError("fixture text changed")
        return rows

    def preflight(self, method):
        if method.describe() != self.method:
            raise ValueError("fixture method binding changed")
        if (self.csv_path.stat().st_size > 64 * 1024
                or hashlib.sha256(self.csv_path.read_bytes()).hexdigest() != self.sha256):
            raise ValueError("fixture CSV hash or byte bound changed")
        rows = self._rows()
        if not 0 < len(rows) <= 16 or len({row["row_id"] for row in rows}) != len(rows):
            raise ValueError("fixture row bound or occurrence identity changed")
        self.scan.describe()
        return dict(status="passed", csv_sha256=self.sha256, rows=len(rows), fixture=True)

    def scan_spec(self, method):
        self.preflight(method)
        return self.scan

    def selected(self):
        rows = self._rows()
        if self.scan.predicate is not None:
            column, value = self.scan.predicate
            rows = [row for row in rows if row[column] == value]
        columns = ("row_id", "source_position", *self.scan.payload_columns)
        return [{name: row[name] for name in columns} for row in rows]

    def expected(self, query_id):
        return [identity(query_id, row["row_id"], row["source_position"])
                for row in self.selected()]

    def rows(self):
        raise AssertionError("execution backends must scan CSV, not source.rows()")


class CpuEngine:
    """Fixed output and synthetic tokens; it never imports a model or LOTUS."""

    def __init__(self, engine=None, generation=None):
        self.uid, self.calls = "generic-cpu-" + uuid.uuid4().hex, 0
        self.maximum = getattr(engine, "max_model_calls", None) or 32

    def ready(self):
        assert "lotus" not in sys.modules and "vllm" not in sys.modules
        return dict(engine_uid=self.uid, pid=os.getpid(), physical_gpu_work=False,
                    inference_kind="fixed CPU output and synthetic token IDs/usage")

    def run_prepared(self, rows, *, deadline_monotonic):
        assert time.monotonic() < deadline_monotonic
        self.calls += len(rows)
        assert self.calls <= self.maximum
        results = []
        for row in rows:
            messages = json.loads(row["messages_json"])
            assert "DO_NOT_SEND_ORACLE" not in row["messages_json"]
            digest = hashlib.sha256(json.dumps(messages, ensure_ascii=False, sort_keys=True,
                                                separators=(",", ":")).encode()).hexdigest()
            results.append(dict(
                **{name: value for name, value in row.items() if name != "messages_json"},
                messages_sha256=digest, status="succeeded", error_type=None,
                text="Answer: True" if row["source_position"] == 0 else "Answer: False",
                prompt_token_ids_json="[1,2,3]", output_token_ids_json="[4]", logprobs_json="null",
                prompt_tokens=3, completion_tokens=1, total_tokens=4, finish_reason="stop",
                engine_uid=self.uid, engine_request_id=row["call_id"], submission_confirmed=True,
                usage_source="synthetic CPU fixture; no inference", model_fixture_pid=os.getpid(),
            ))
        return results

    def close(self, timeout=10.0):
        return dict(status="closed", engine_api_shutdown_confirmed=True, gpu_exit_confirmed=None,
                    reserved_model_calls=self.calls)


class CpuInferBatch(InferBatch):
    def __init__(self, engine, generation, expected_count, deadline, startup_timeout_s):
        self.engine = CpuEngine(EngineSpec.from_dict(engine))
        self.deadline, self.expected_count, self.completed = deadline, expected_count, 0


def _cpu_ready(actor, deadline):
    _check(deadline)
    if actor.engine is None:
        actor.engine = CpuEngine(actor.engine_spec, actor.generation)
    return actor.engine.ready()


class CpuStaticActor(_ModelActor):
    ready = _cpu_ready


class CpuCoreActor(_CoreModelActor):
    ready = _cpu_ready


class CpuEngineProcess(EngineProcess):
    def _command(self, engine, result_fd):
        return [sys.executable, "-m", __name__, str(result_fd)]


if __name__ == "__main__":
    from src.baselines.text.frameworks.daft_engine_process import serve
    from src.serving.backends import vllm_engine

    vllm_engine.VllmEngine = CpuEngine
    serve(int(sys.argv[1]))
