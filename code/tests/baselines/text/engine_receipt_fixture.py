"""Self-written finite JSON worker; no model, LOTUS or network access."""

import json
import os
import sys

from src.baselines.text.frameworks.daft_engine_process import EngineProcess
from src.serving.backends.vllm_engine import _prepared


def terminal(item, engine_uid="cpu-receipt-owned"):
    return dict(item, status="succeeded", submission_confirmed=True, text="True",
        prompt_tokens=3, completion_tokens=1, total_tokens=4,
        prompt_token_ids_json="[11, 12, 13]", output_token_ids_json="[14]", logprobs_json="null",
        engine_request_id="request-" + item["call_id"], engine_uid=engine_uid,
        finish_reason="stop", error_type=None, fixture_text="原始 reply")


class ReplyEngineProcess(EngineProcess):
    mode = "short"

    def _command(self, engine, result_fd):
        return [engine.model_python, "-B", "-m", __name__, str(result_fd), self.mode]


def main():
    if "lotus" in sys.modules or "vllm" in sys.modules:
        raise AssertionError("CPU reply fixture imported a model or method runtime")
    count, uid = 0, "cpu-receipt-" + str(os.getpid())
    with os.fdopen(int(sys.argv[1]), "wb", buffering=0) as output:
        for line in sys.stdin.buffer:
            value = json.loads(line)
            if value["op"] == "open":
                response = dict(kind="ready", engine_uid=uid, pid=os.getpid(), cpu_fixture=True)
            elif value["op"] == "run":
                count += len(value["rows"])
                identities, _ = _prepared(value["rows"])
                rows = [terminal(item, uid) for item in identities]
                if sys.argv[2] == "short": rows = rows[:1]
                if sys.argv[2] == "duplicate": rows.append(dict(rows[0]))
                if sys.argv[2] == "reorder": rows.reverse()
                response = dict(kind="results", rows=rows, fixture_total_attempts=count)
            elif value["op"] == "close":
                response = dict(kind="closed", report=dict(status="closed",
                    engine_api_shutdown_confirmed=True, fixture_total_attempts=count))
            else:
                raise ValueError("unexpected CPU fixture operation")
            # Deliberate legal whitespace/Unicode proves preservation of the
            # received JSON line rather than only reconstruction of its value.
            output.write((json.dumps(response, ensure_ascii=False, separators=(",  ", ":  ")) + "\n").encode())
            if value["op"] == "close":
                return


if __name__ == "__main__":
    main()
