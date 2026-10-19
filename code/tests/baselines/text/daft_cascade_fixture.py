"""Bounded CSV producer and declared CPU engine for real Daft cascade tests."""

import csv
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import uuid

from src.baselines.text.frameworks.daft_engine_process import EngineProcess
from src.semantic_methods.lotus.records import RecordScan, identity


class CascadeCsvFixture:
    def __init__(self, directory, method, *, payload_columns, rows):
        self.csv_path = Path(directory) / "cascade.csv"
        self.method = method.describe()
        self.scan = RecordScan(
            (("row_id", "string"), ("source_position", "int64"), ("selected", "string"),
             *((name, "string") for name in payload_columns)),
            tuple(payload_columns), ("selected", "yes"))
        with self.csv_path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=dict(self.scan.csv_schema))
            writer.writeheader()
            writer.writerows(rows)
        self.sha256 = hashlib.sha256(self.csv_path.read_bytes()).hexdigest()

    def selected(self):
        with self.csv_path.open(newline="", encoding="utf-8") as stream:
            rows = list(csv.DictReader(stream))
        for row in rows:
            row["source_position"] = int(row["source_position"])
        return [row for row in rows if row["selected"] == "yes"]

    def preflight(self, method):
        if method.describe() != self.method:
            raise ValueError("fixture main method binding changed")
        if (self.csv_path.stat().st_size > 64 * 1024
                or hashlib.sha256(self.csv_path.read_bytes()).hexdigest() != self.sha256):
            raise ValueError("fixture CSV hash or byte bound changed")
        rows = self.selected()
        if len(rows) > 16 or len({(row["row_id"], row["source_position"]) for row in rows}) != len(rows):
            raise ValueError("fixture occurrence identity or row bound changed")
        self.scan.describe()
        return dict(status="passed", rows=len(rows), fixture=True)

    def scan_spec(self, method):
        self.preflight(method)
        return self.scan

    def expected(self, query_id):
        return [identity(query_id, row["row_id"], row["source_position"]) for row in self.selected()]

    def rows(self):
        raise AssertionError("Daft must scan its native CSV source")


def response(row, role, *, engine_uid="declared-cpu-fixture", mode="normal"):
    position = row["source_position"]
    probability = (position + 1) / 12
    token = "True" if (position % 2 if role == "helper" else position % 3 != 0) else "False"
    logprobs = [dict(token=token, logprob=math.log(probability if token == "True" else 1-probability),
        top_logprobs=[dict(token="True", logprob=math.log(probability)),
                      dict(token="False", logprob=math.log(1-probability))])]
    messages = json.loads(row.get("messages_json", "[]"))
    digest = hashlib.sha256(json.dumps(messages, ensure_ascii=False, sort_keys=True,
                                      separators=(",", ":")).encode()).hexdigest()
    failed = mode == "failed-row" and position == 4
    return dict(row, messages_sha256=digest, status="failed" if failed else "succeeded",
        error_type="DeclaredFixtureFailure" if failed else None,
        submission_confirmed=True, text=None if failed else token,
        finish_reason=None if failed else "stop", prompt_tokens=12,
        completion_tokens=0 if failed else 1, total_tokens=12 if failed else 13,
        prompt_token_ids_json=json.dumps(list(range(12))),
        output_token_ids_json=json.dumps([] if failed else [1 if token == "True" else 2]),
        logprobs_json=json.dumps(logprobs) if role == "helper" and not failed else "null",
        engine_uid=engine_uid, engine_request_id=row["call_id"], model_fixture_pid=os.getpid(),
        usage_source="declared CPU fixture; zero model inference")


class CpuCascadeEngineProcess(EngineProcess):
    mode = "normal"

    def _command(self, engine, result_fd):
        return [sys.executable, "-m", __name__, str(result_fd), self.mode]


class FailedRowProcess(CpuCascadeEngineProcess):
    mode = "failed-row"


class FailedCloseProcess(CpuCascadeEngineProcess):
    mode = "failed-close"


if __name__ == "__main__":
    from src.baselines.text.frameworks.daft_engine_process import serve
    from src.serving.backends import vllm_engine

    mode = sys.argv[2]

    class CpuEngine:
        def __init__(self, engine, generation):
            self.role = "helper" if generation.logprobs is not None else "main"
            self.maximum, self.calls = engine.max_model_calls, 0
            self.uid = "daft-cascade-cpu-" + uuid.uuid4().hex

        def ready(self):
            assert "lotus" not in sys.modules and "vllm" not in sys.modules
            return dict(engine_uid=self.uid, pid=os.getpid(), fixture=True,
                        inference_kind="declared CPU output; no model or GPU")

        def run_prepared(self, rows, **_):
            self.calls += len(rows)
            assert self.calls <= self.maximum
            return [response(row, self.role, engine_uid=self.uid, mode=mode) for row in rows]

        def close(self):
            failed = mode == "failed-close"
            return dict(status="unknown" if failed else "closed", reserved_model_calls=self.calls,
                        engine_api_shutdown_confirmed=not failed, physical_gpu_work=False)

    vllm_engine.VllmEngine = CpuEngine
    serve(int(sys.argv[1]))
