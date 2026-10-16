"""No-model records worker and supervisor-client behavior checks."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import time
import unittest

DIRECTORY = Path(__file__).resolve().parents[2] / "integrations/imlane_records"
sys.path.insert(0, str(DIRECTORY))
from client import RecordsBridge, RecordsRejected

try:
    import pyarrow as pa
except ImportError:
    pa = None

SERVER = r'''
import json,sys,time
def frame(value): print("IMLANE_RECORDS " + json.dumps(value), flush=True)
frame(dict(event="ready", protocol="imlane.records.v1"))
slow=False
for line in sys.stdin:
    op=json.loads(line)["op"]
    if op=="slow_poll":slow=True
    if op=="poll" and slow:time.sleep(10)
    if op=="partial":
        print("IMLANE_RECORDS {",end="",flush=True);time.sleep(10)
    elif op=="invalid":frame(dict(event="error",ok=False,owner_terminal=False,error="rejected before author submission"))
    else:frame(dict(event=op,ok=True,ready=[]))
    if op=="close":break
'''


class ClientChecks(unittest.TestCase):
    def setup_client(self, callback=None):
        process = subprocess.Popen([sys.executable, "-u", "-c", SERVER], stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        def cleanup():
            if process.poll() is None:
                process.kill()
            process.wait(timeout=2)
            for stream in (process.stdin, process.stdout, process.stderr):
                stream.close()
        self.addCleanup(cleanup)
        def abort():
            if callback:
                callback()
            if process.poll() is None:
                process.kill()
            process.wait(timeout=2)
        client = RecordsBridge(process, abort_owner=abort, timeout_s=.2)
        return client

    def test_pre_submission_rejection_allows_next_command(self):
        client = self.setup_client()
        with self.assertRaises(RecordsRejected):
            client.request("invalid")
        client.begin("query")
        client.close()
        client.close()

    def test_empty_ready_polls_do_not_hide_native_task_deadline(self):
        client = self.setup_client()
        start = time.monotonic()
        with self.assertRaises(TimeoutError):
            client.wait_ready(timeout_s=.1)
        self.assertLess(time.monotonic() - start, 1)
        self.assertTrue(client._closed)
        with self.assertRaises(RuntimeError):
            client.begin("another")

    def test_partial_response_cannot_block_past_rpc_deadline(self):
        client = self.setup_client()
        with self.assertRaises(TimeoutError):
            client.request("partial")
        self.assertIsNotNone(client.process.poll())

    def test_native_task_deadline_caps_a_blocked_poll_response(self):
        client = self.setup_client()
        client.request("slow_poll")
        client.timeout_s = 2
        start = time.monotonic()
        with self.assertRaises(TimeoutError):
            client.wait_ready(timeout_s=.05)
        self.assertLess(time.monotonic() - start, .5)

    def test_failed_owner_cleanup_is_not_marked_closed_and_can_retry(self):
        calls = []
        def callback():
            calls.append(True)
            if len(calls) == 1:
                raise RuntimeError("cleanup failure")
        client = self.setup_client(callback)
        with self.assertRaises(TimeoutError) as caught:
            client.wait_ready(timeout_s=.05)
        self.assertIn("cleanup remains unconfirmed", caught.exception.__notes__[0])
        self.assertFalse(client._closed)
        client.close()
        self.assertTrue(client._closed)
        self.assertEqual(len(calls), 2)


@unittest.skipIf(pa is None, "uses an explicitly supplied existing Arrow environment")
class EchoChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("records_echo_test", DIRECTORY / "echo_udf.py")
        cls.worker = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.worker)

    def source(self):
        return pa.Table.from_pylist([dict(call_id=name, row_id=7, query_id="q", stage_id="s",
            record_json=json.dumps(dict(text="repeat\n\0中文"))) for name in ("a", "b")], schema=self.worker.INPUT)

    def test_repeat_source_has_distinct_occurrences_and_persistent_worker(self):
        source = self.source()
        first, second = self.worker.infer_records(source), self.worker.infer_records(source)
        a, b = [json.loads(table["outcome_json"][0].as_py()) for table in (first, second)]
        self.assertEqual(first["call_id"].to_pylist(), ["a", "b"])
        self.assertEqual(a["worker"]["worker_uid"], b["worker"]["worker_uid"])
        self.assertEqual(a["worker"]["completed_batches"] + 1, b["worker"]["completed_batches"])
        self.assertEqual(a["result"]["record"]["text"], "repeat\n\0中文")
        self.assertFalse(a["model_entry_attempted"])

    def test_nonnullable_arrow_fields_match_the_host_input_policy(self):
        source = self.source()
        schema = pa.schema([pa.field(field.name, field.type, nullable=False) for field in source.schema])
        table = pa.Table.from_arrays(source.columns, schema=schema)
        self.assertEqual(len(self.worker.infer_records(table)), 2)

    def test_input_types_nulls_query_and_occurrences_are_checked(self):
        source = self.source()
        invalid = [source.slice(0, 0), source.rename_columns(["different", *source.column_names[1:]])]
        for key, value in (("call_id", "a"), ("query_id", "other"), ("row_id", -1), ("record_json", None)):
            rows = source.to_pylist(); rows[1][key] = value
            invalid.append(pa.Table.from_pylist(rows, schema=self.worker.INPUT))
        for table in invalid:
            with self.subTest(table=table), self.assertRaises(ValueError):
                self.worker.infer_records(table)

    def test_error_envelope_is_explicit_and_disjoint_from_success(self):
        failed = self.worker.error_records(self.source())
        for row in failed.to_pylist():
            value = json.loads(row["outcome_json"])
            self.assertEqual(value["status"], "error")
            self.assertIsNone(value["result"])
            self.assertFalse(value["completion_known"])
            self.assertEqual(value["error_type"], "EchoFailure")


if __name__ == "__main__":
    unittest.main()
