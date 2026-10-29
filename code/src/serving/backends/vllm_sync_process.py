"""A private synchronous model process with finite pipe messages and owned cleanup.

The caller owns method preparation and generation choices. This transport owns
one child process, its response pipe and observed process-tree cleanup.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import asdict
from copy import deepcopy

from src.serving.backends.vllm_engine import EngineSpec, GenerationSpec, IDENTITY_FIELDS


MAX_MESSAGE_BYTES = 64 * 1024**2
CODE_ROOT = Path(__file__).resolve().parents[3]
# The worker's existing engine.close() API allows ten seconds. Leave one more
# second for its reply, then four seconds for bounded owned-process cleanup.
DEFAULT_CLOSE_TIMEOUT_S = 15.0
CLOSE_CLEANUP_RESERVE_S = 4.0
RESULT_PROTOCOL_ERROR = "private engine result cardinality, order or association differs"


def _encode(value):
    data = json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8") + b"\n"
    if len(data) > MAX_MESSAGE_BYTES:
        raise ValueError("engine IPC message exceeds its byte limit")
    return data


def _publish(path, value):
    from src.baselines.common.private_artifacts import write_private_json
    from src.baselines.common.redact import redact_json_values

    temporary = path.with_suffix(".tmp")
    write_private_json(temporary, redact_json_values(value))
    os.replace(temporary, path)


def _verified_terminal(row, expected, engine_uid):
    """Qualify a receipt from a damaged reply without inventing its evidence."""
    if (type(row) is not dict or any(type(row.get(key)) is not type(expected[key])
            or row[key] != expected[key] for key in IDENTITY_FIELDS)
            or row.get("messages_sha256") != expected["messages_sha256"]
            or type(engine_uid) is not str or not engine_uid or row.get("engine_uid") != engine_uid
            or row.get("status") not in ("succeeded", "failed", "unknown")
            or type(row.get("submission_confirmed")) is not bool):
        return False
    succeeded = row["status"] == "succeeded"
    if succeeded:
        if (row["submission_confirmed"] is not True or type(row.get("text")) is not str
                or row.get("finish_reason") not in ("stop", "length")
                or type(row.get("engine_request_id")) is not str or not row["engine_request_id"]):
            return False
    elif type(row.get("error_type")) is not str or not row["error_type"]:
        return False
    for count_name, ids_name in (("prompt_tokens", "prompt_token_ids_json"),
                                 ("completion_tokens", "output_token_ids_json")):
        count, encoded = row.get(count_name), row.get(ids_name)
        if count is not None and (type(count) is not int or count < 0):
            return False
        if ((succeeded and count is None) or (count is not None and type(encoded) is not str)
                or encoded is not None and type(encoded) is not str):
            return False
        if encoded is not None:
            try:
                ids = json.loads(encoded)
            except (TypeError, ValueError):
                return False
            if ids is None and count is None and not succeeded:
                continue
            if (type(ids) is not list or any(type(token) is not int or token < 0 for token in ids)
                    or count is not None and len(ids) != count):
                return False
    prompt, completion, total = (row.get(key) for key in ("prompt_tokens", "completion_tokens", "total_tokens"))
    return (total is None if prompt is None or completion is None else
            type(total) is int and total == prompt + completion)


class EngineProcess:
    """No retry, model sharing, batching policy or GPU routing lives here."""

    def __init__(self, engine, generation, control_dir, deadline, startup_timeout_s):
        import psutil

        self.control = Path(control_dir)
        self.deadline = deadline
        self.process = None
        self.reader = None
        self.pending = bytearray()
        self.owned = set()
        self.created = None
        self.ready_report = None
        self._native_mfu = None
        self.engine_close = None
        self.last_close_report = None
        self.last_result_failure = None
        self._last_raw_response_json = None
        self._result_failure_write_errors = []
        self.ipc_seconds = self.startup_seconds = 0.0
        self.ipc_request_bytes = self.ipc_response_bytes = 0
        self.ipc_encode_seconds = self.ipc_decode_seconds = 0.0
        self._closed = False
        self._broken = False
        self._lock = threading.RLock()
        self._reaper_stop = threading.Event()
        self.report_path = self.control / ("engine-" + uuid.uuid4().hex + ".json")
        self._check(deadline)
        started = time.monotonic()
        read_fd, write_fd = os.pipe()
        try:
            environment = dict(os.environ, PYTHONPATH=str(CODE_ROOT),
                               HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1")
            self.process = subprocess.Popen(
                self._command(engine, write_fd),
                stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                pass_fds=(write_fd,), start_new_session=True, env=environment, bufsize=0,
            )
            self.created = psutil.Process(self.process.pid).create_time()
            self.owned.add((self.process.pid, self.created))
            _publish(self.report_path, dict(status="starting", model_pid=self.process.pid,
                                          process_created=self.created,
                                          process_exit_confirmed=False, cleanup_errors=[]))
            os.set_blocking(read_fd, False)
            os.set_blocking(self.process.stdin.fileno(), False)
            self.reader = os.fdopen(read_fd, "rb", buffering=0)
            read_fd = None
            self.ready_report = self._exchange(
                dict(op="open", engine=asdict(engine), generation=asdict(generation)),
                min(deadline, started + startup_timeout_s),
            )
            if self.ready_report.get("kind") != "ready":
                raise RuntimeError("private engine initialization failed: " +
                                   str(self.ready_report.get("error_type")))
            self.startup_seconds = time.monotonic() - started
            self._reaper = threading.Thread(target=self._reap_idle, daemon=True,
                                            name="daft-private-engine-stop")
            self._reaper.start()
        except BaseException as exc:
            self._broken = True
            try:
                self.close()
            except BaseException as cleanup:
                exc.add_note("private engine cleanup also failed: " + type(cleanup).__name__)
            raise
        finally:
            os.close(write_fd)
            if read_fd is not None:
                os.close(read_fd)

    @staticmethod
    def _command(engine, result_fd):
        return [engine.model_python, "-m", __name__, "--serve", str(result_fd)]

    def _check(self, deadline, *, stopping=True):
        if stopping and (self.control / "stop").exists():
            raise InterruptedError("Daft records query cancelled")
        if time.monotonic() >= deadline:
            raise TimeoutError("Daft records engine deadline expired")

    def _exchange(self, value, deadline, *, stopping=True):
        from src.infrastructure.owned_processes import _observe

        started = time.monotonic()
        data = memoryview(_encode(value))
        self.ipc_request_bytes += len(data)
        self.ipc_encode_seconds += time.monotonic() - started
        try:
            while data:
                self._check(deadline, stopping=stopping)
                if select.select([], [self.process.stdin], [], 0.02)[1]:
                    data = data[os.write(self.process.stdin.fileno(), data):]
            while True:
                if b"\n" in self.pending:
                    line, _, rest = self.pending.partition(b"\n")
                    self.pending = bytearray(rest)
                    decode_started = time.monotonic()
                    result = json.loads(line)
                    self._last_raw_response_json = line.decode("utf-8", errors="replace")
                    self.ipc_decode_seconds += time.monotonic() - decode_started
                    self.ipc_response_bytes += len(line) + 1
                    if type(result) is not dict:
                        raise ValueError("engine IPC response must be an object")
                    if result.get("native_mfu") is not None:
                        self._native_mfu = result["native_mfu"]
                    elif result.get("kind") == "ready":
                        self._native_mfu = result.get("native_mfu_initial")
                    return result
                # Read an already-available result before acting on a stop so
                # authoritative usage is retained even when publication stops.
                if select.select([self.reader], [], [], 0.02)[0]:
                    block = os.read(self.reader.fileno(), 65536)
                    if not block:
                        raise EOFError("private engine response pipe closed")
                    self.pending.extend(block)
                    if len(self.pending) > MAX_MESSAGE_BYTES:
                        raise ValueError("engine IPC response exceeds its byte limit")
                else:
                    self._check(deadline, stopping=stopping)
                _observe(self.owned, self.process.pid, self.created)
                if b"\n" not in self.pending:
                    self._check(deadline, stopping=stopping)
        finally:
            self.ipc_seconds += time.monotonic() - started

    def run_prepared(self, rows):
        with self._lock:
            return self._run_prepared(rows)

    def mfu_snapshot(self):
        with self._lock:
            return self._native_mfu

    def _reap_idle(self):
        while not self._reaper_stop.wait(0.02):
            if (self.control / "stop").exists() or time.monotonic() >= self.deadline:
                with self._lock:
                    # An idle stop forbids reuse without breaking a healthy
                    # IPC channel before it can acknowledge engine shutdown.
                    try:
                        self.close()
                    finally:
                        self._broken = True
                return

    def _run_prepared(self, rows):
        from src.serving.backends.vllm_engine import _prepared

        identities, _ = _prepared(rows)
        self._last_raw_response_json = None
        try:
            if self._broken or self._closed:
                raise RuntimeError("private engine cannot be reused after stop or close")
            response = self._exchange(dict(op="run", rows=rows, deadline=self.deadline), self.deadline)
            results = response.get("rows")
            request_ids = [row["engine_request_id"] for row in results
                           if type(row) is dict and type(row.get("engine_request_id")) is str
                           and row["engine_request_id"]] if type(results) is list else []
            if (response.get("kind") != "results" or type(results) is not list or len(results) != len(identities)
                    or len(request_ids) != len(set(request_ids))
                    or any(type(row) is not dict or any(row.get(key) != item[key] for key in IDENTITY_FIELDS)
                           or "engine_protocol_error" in row for row, item in zip(results, identities))):
                return self._result_failure(response, identities)
            return results
        except Exception as exc:
            if self._last_raw_response_json is not None and self.last_result_failure is None:
                # A decoded non-object reply still has authoritative raw bytes,
                # although it cannot attest any input call's terminal.
                return self._result_failure(json.loads(self._last_raw_response_json), identities)
            self._broken = True
            try:
                self.close()
            except BaseException as cleanup:
                exc.add_note("private engine cleanup also failed: " + type(cleanup).__name__)
            return [dict(item, status="unknown", submission_confirmed=False,
                         error_type=type(exc).__name__, prompt_tokens=None,
                         completion_tokens=None, total_tokens=None,
                         engine_uid=(self.ready_report or {}).get("engine_uid"),
                         **({"engine_protocol_error": RESULT_PROTOCOL_ERROR} if self.last_result_failure else {}))
                    for item in identities]

    def _result_failure(self, response, identities):
        raw = response.get("rows") if type(response) is dict and response.get("kind") == "results" else None
        raw = raw if type(raw) is list else []
        engine_uid = (self.ready_report or {}).get("engine_uid")
        verified, indexes, normalized = [], set(), []
        for item in identities:
            matches = [(index, row) for index, row in enumerate(raw)
                       if type(row) is dict and row.get("call_id") == item["call_id"]]
            request_id = matches[0][1].get("engine_request_id") if len(matches) == 1 else None
            request_unique = (not (type(request_id) is str and request_id) or sum(type(row) is dict
                              and row.get("engine_request_id") == request_id for row in raw) == 1)
            if len(matches) == 1 and request_unique and _verified_terminal(matches[0][1], item, engine_uid):
                index, row = matches[0]
                value = dict(deepcopy(row), engine_protocol_error=RESULT_PROTOCOL_ERROR)
                verified.append(deepcopy(value))
                indexes.add(index)
            else:
                value = dict(item, status="unknown", text=None, submission_confirmed=False,
                    error_type="ValueError", engine_uid=engine_uid, prompt_tokens=None,
                    completion_tokens=None, total_tokens=None, engine_protocol_error=RESULT_PROTOCOL_ERROR)
            normalized.append(value)
        self.last_result_failure = dict(error_type="ValueError", verified_call_results=verified,
            raw_response_json=(self._last_raw_response_json if self._last_raw_response_json is not None else
                               json.dumps(response, ensure_ascii=False)),
            unassociated_result_indexes=[index for index in range(len(raw)) if index not in indexes])
        self._broken = True
        try:
            _publish(self.report_path, dict(status="unknown", result_failure=self.last_result_failure,
                engine_ready=self.ready_report, process_exit_confirmed=False, cleanup_errors=[],
                model_pid=None if self.process is None else self.process.pid, process_created=self.created))
        except Exception as error:
            self._result_failure_write_errors.append("result_failure_report_write: " + type(error).__name__)
        try:
            self.close()
        except Exception as error:
            # Neither failed audit I/O nor cleanup may replace received usage.
            previous = self.last_close_report or {}
            self.last_close_report = dict(previous, status="unknown", result_failure=self.last_result_failure,
                cleanup_errors=[*previous.get("cleanup_errors", []), *self._result_failure_write_errors,
                                "result_failure_cleanup: " + type(error).__name__])
        return normalized

    def close(self, timeout=DEFAULT_CLOSE_TIMEOUT_S):
        from src.infrastructure.owned_processes import _seconds

        _seconds(timeout, "timeout")
        with self._lock:
            try:
                return self._close(timeout)
            except Exception as exc:
                # Observation itself may fail. Keep a retryable handle and
                # a separate cleanup outcome instead of replacing query usage
                # or the original request error with that exception.
                self.last_close_report = dict(
                    status="unknown", process_exit_confirmed=False, gpu_exit_confirmed=None,
                    cleanup_scope="private_engine_and_observed_os_processes",
                    cleanup_errors=[*self._result_failure_write_errors, "cleanup_exception: " + type(exc).__name__],
                    model_pid=None if self.process is None else self.process.pid,
                    process_created=self.created,
                    owned_processes=[list(item) for item in sorted(self.owned)],
                    **({"result_failure": self.last_result_failure} if self.last_result_failure else {}),
                )
                try:
                    _publish(self.report_path, self.last_close_report)
                except Exception as recording:
                    self.last_close_report["cleanup_errors"].append(
                        "cleanup_report_write: " + type(recording).__name__)
                return self.last_close_report

    def _close(self, timeout):
        from src.infrastructure.owned_processes import _live, _observe, _signal, _wait

        if self._closed:
            return self.last_close_report
        self._reaper_stop.set()
        started = time.monotonic()
        deadline = started + timeout
        cleanup_reserved = min(CLOSE_CLEANUP_RESERVE_S, timeout / 3)
        api_deadline = deadline - cleanup_reserved
        errors = list(self._result_failure_write_errors)
        api_confirmed = (type(self.engine_close) is dict and
                         self.engine_close.get("engine_api_shutdown_confirmed") is True)
        if self.process is not None:
            if self.created is not None:
                try:
                    _observe(self.owned, self.process.pid, self.created)
                    if not self._broken and not api_confirmed and self.process.poll() is None:
                        reply = self._exchange(dict(op="close"), api_deadline, stopping=False)
                        if reply.get("kind") == "closed":
                            self.engine_close = reply["report"]
                            api_confirmed = self.engine_close.get("engine_api_shutdown_confirmed") is True
                            if not api_confirmed:
                                errors.append("engine_api_shutdown_unconfirmed")
                        else:
                            errors.append("engine_close_response_invalid")
                except Exception as exc:
                    errors.append("engine_close: " + type(exc).__name__)
                try:
                    _wait(self.owned, self.process.pid, self.created,
                          min(deadline, time.monotonic() + min(0.2, cleanup_reserved / 4)))
                    if _live(self.owned):
                        _signal(self.owned, self.process.pid, self.created, signal.SIGTERM)
                        _wait(self.owned, self.process.pid, self.created,
                              min(deadline, time.monotonic() +
                                  max(0.0, deadline - time.monotonic()) / 2))
                    if _live(self.owned):
                        _signal(self.owned, self.process.pid, self.created, signal.SIGKILL)
                        _wait(self.owned, self.process.pid, self.created, deadline)
                except Exception as exc:
                    errors.append("process_cleanup: " + type(exc).__name__)
                    # Process-table access can be denied independently of
                    # signalling our own child. Still terminate the direct
                    # Popen child; keep tree cleanup unconfirmed in the report.
                    if self.process.poll() is None:
                        self.process.kill()
            else:
                # A process that could not be observed still needs its direct
                # Popen handle terminated; never report its children as known.
                errors.append("process_identity_unavailable")
                self.process.kill()
            try:
                self.process.wait(timeout=max(0.0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                errors.append("engine_process_still_running")
            for stream in (self.process.stdin, self.reader):
                if stream is not None:
                    stream.close()
        live = _live(self.owned)
        exited = self.process is None or self.process.poll() is not None
        confirmed = exited and not live and self.created is not None
        if any(error.startswith("process_cleanup:") for error in errors):
            confirmed = False
        self._closed = confirmed
        if live:
            errors.append("observed_engine_children_still_running")
        if (self.process is not None and not api_confirmed
                and "engine_api_shutdown_unconfirmed" not in errors):
            errors.append("engine_api_shutdown_unconfirmed")
        self.last_close_report = dict(
            status="closed" if confirmed and not errors else "unknown",
            cleanup_scope="private_engine_and_observed_os_processes",
            process_exit_confirmed=confirmed, gpu_exit_confirmed=None,
            gpu_exit_reason="requires independent GPU qualification",
            engine_api=self.engine_close, engine_ready=self.ready_report,
            observed_processes=len(self.owned), live_observed_processes=len(live),
            startup_seconds=self.startup_seconds, ipc_wait_seconds=self.ipc_seconds,
            ipc_request_bytes=self.ipc_request_bytes, ipc_response_bytes=self.ipc_response_bytes,
            ipc_encode_seconds=self.ipc_encode_seconds, ipc_decode_seconds=self.ipc_decode_seconds,
            ipc_timing_scope="JSON encode, pipe transfer and engine response wait; includes model time",
            close_seconds=time.monotonic() - started, cleanup_errors=errors,
            close_budget_s=timeout, engine_api_rpc_budget_s=timeout - cleanup_reserved,
            process_cleanup_reserved_s=cleanup_reserved,
            worker_pid=os.getpid(), model_pid=None if self.process is None else self.process.pid,
            process_created=self.created,
            owned_processes=[list(item) for item in sorted(self.owned)],
            **({"result_failure": self.last_result_failure} if self.last_result_failure else {}),
        )
        try:
            _publish(self.report_path, self.last_close_report)
        except Exception as exc:
            self.last_close_report["status"] = "unknown"
            self.last_close_report["cleanup_errors"].append("cleanup_report_write: " + type(exc).__name__)
        return self.last_close_report


def serve(result_fd):
    """Run in model_python; the driver/LOTUS environment is never imported."""
    from src.serving.backends.vllm_engine import VllmEngine

    engine = None
    with os.fdopen(result_fd, "wb", buffering=0) as output:
        def send(value):
            if engine is not None and engine.spec.enable_mfu_metrics and value.get("kind") == "results":
                from src.observability.metrics.mfu import optional_packet
                output.write(optional_packet(value, lambda: engine.mfu_snapshot(), _encode))
            else:
                output.write(_encode(value))
        try:
            for raw in iter(lambda: sys.stdin.buffer.readline(MAX_MESSAGE_BYTES + 1), b""):
                if len(raw) > MAX_MESSAGE_BYTES or not raw.endswith(b"\n"):
                    raise ValueError("engine IPC request exceeds its byte limit")
                value = json.loads(raw)
                if engine is None and value["op"] == "open":
                    engine = VllmEngine(EngineSpec.from_dict(value["engine"]),
                                        GenerationSpec.from_dict(value["generation"]))
                    send(dict(kind="ready", **engine.ready()))
                elif engine is not None and value["op"] == "run":
                    send(dict(kind="results", rows=engine.run_prepared(
                        value["rows"], deadline_monotonic=value["deadline"])))
                elif engine is not None and value["op"] == "close":
                    report = engine.close()
                    send(dict(kind="closed", report=report))
                    engine = None
                    return
                else:
                    raise ValueError("unexpected engine IPC operation")
        except Exception as exc:
            send(dict(kind="error", error_type=type(exc).__name__))
        finally:
            if engine is not None:
                engine.close()


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "--serve":
        raise SystemExit("private engine worker requires an inherited result descriptor")
    serve(int(sys.argv[2]))
