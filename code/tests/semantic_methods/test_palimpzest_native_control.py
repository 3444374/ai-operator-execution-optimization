"""Native SDK response cleanup keeps actual-send accounting and the first error."""

from types import SimpleNamespace
import unittest

import httpx

from src.semantic_methods.palimpzest.backend import PalimpzestAdapterError
from src.semantic_methods.palimpzest.native_control import NativeAdaptedExecutor


class ResponseStream(httpx.SyncByteStream):
    def __init__(self, read_error=None, close_error=None):
        self.read_error, self.close_error = read_error, close_error
        self.closes = 0

    def __iter__(self):
        yield b'{"response":"complete"}'
        if self.read_error is not None:
            raise self.read_error

    def close(self):
        self.closes += 1
        if self.close_error is not None:
            raise self.close_error


class NativeControlCleanupTests(unittest.TestCase):
    def execute(self, stream, after_error=None, final_close_error=None):
        request = httpx.Request("POST", "http://localhost/v1/chat/completions")
        response = httpx.Response(200, request=request, stream=stream)
        events = []
        if final_close_error is not None:
            original_close, closes = response.close, []

            def close():
                closes.append(None)
                original_close()
                # HTTPX closes after iter_raw EOF; inject only the owner's final close.
                if len(closes) > 1:
                    raise final_close_error

            response.close = close

        def before(call, sdk_request):
            events.append(("before", call.call_id, sdk_request))
            return "reservation"

        def after(reservation, **values):
            events.append(("after", reservation, values))
            if after_error is not None:
                raise after_error

        executor = NativeAdaptedExecutor(max_workers=1, before_send=before, after_send=after)
        self.addCleanup(executor.close)
        self.events = events
        call = SimpleNamespace(call_id="call", row_id=0, max_response_bytes=4096)
        return executor._send(call, request, lambda actual, **kwargs: response)

    def test_read_error_keeps_identity_and_attempts_both_cleanup_steps(self):
        for primary in (ValueError("read"), InterruptedError("cancel"), KeyboardInterrupt("cancel")):
            with self.subTest(primary=type(primary).__name__):
                stream = ResponseStream(primary, RuntimeError("close"))
                with self.assertRaises(type(primary)) as caught:
                    self.execute(stream, LookupError("observation"))
                self.assertIs(caught.exception, primary)
                self.assertEqual(stream.closes, 1)
                self.assertEqual([event[0] for event in self.events], ["before", "after"])
                self.assertIs(self.events[-1][2]["error"], primary)
                notes = " ".join(primary.__notes__)
                self.assertIn("RuntimeError", notes)
                self.assertIn("LookupError", notes)

    def test_transport_failure_keeps_its_outcome_and_actual_send_error(self):
        primary = httpx.ReadError("read")
        stream = ResponseStream(primary, RuntimeError("close"))
        outcome = self.execute(stream, LookupError("observation"))
        self.assertIsNone(outcome.response)
        self.assertIn("ReadError", outcome.transport_error)
        self.assertIs(self.events[-1][2]["error"], primary)
        self.assertIn("RuntimeError", " ".join(primary.__notes__))
        self.assertIn("LookupError", " ".join(primary.__notes__))
        self.assertEqual(stream.closes, 1)

    def test_close_error_precedes_observation_error_after_complete_response(self):
        primary = RuntimeError("close")
        stream = ResponseStream()
        with self.assertRaises(RuntimeError) as caught:
            self.execute(stream, LookupError("observation"), final_close_error=primary)
        self.assertIs(caught.exception, primary)
        self.assertEqual(stream.closes, 1)
        self.assertIs(self.events[-1][2]["error"], primary)
        self.assertEqual(self.events[-1][2]["response"].status_code, 200)
        self.assertIn("LookupError", " ".join(primary.__notes__))

    def test_observation_error_remains_fatal_when_response_and_close_succeed(self):
        primary = LookupError("observation")
        with self.assertRaises(PalimpzestAdapterError) as caught:
            self.execute(ResponseStream(), primary)
        self.assertIs(caught.exception.__cause__, primary)
        self.assertIsNone(self.events[-1][2]["error"])

    def test_success_closes_once_and_reports_complete_response_once(self):
        stream = ResponseStream()
        outcome = self.execute(stream)
        self.assertEqual(outcome.response.status_code, 200)
        self.assertEqual(stream.closes, 1)
        self.assertEqual([event[0] for event in self.events], ["before", "after"])
        self.assertIsNone(self.events[-1][2]["error"])

    def test_transport_error_batch_keeps_unknown_and_refuses_another_request(self):
        observed = []
        primary = httpx.ReadError("unknown remote completion")
        request = httpx.Request("POST", "http://localhost/v1/chat/completions")
        response = httpx.Response(200, request=request,
            stream=ResponseStream(primary, RuntimeError("close")))
        executor = NativeAdaptedExecutor(max_workers=1,
            before_send=lambda call, sdk_request: call.call_id,
            after_send=lambda reservation, **values: observed.append(values))
        self.addCleanup(executor._pool.shutdown, wait=True, cancel_futures=True)
        self.addCleanup(executor.close)
        call = SimpleNamespace(call_id="call", row_id=0, payload=b"{}", max_response_bytes=4096)
        executor.bind_sdk_request(call.call_id, request, lambda actual, **kwargs: response)
        outcomes = list(executor.iter_batch((call,)))
        self.assertEqual(len(outcomes), 1)
        self.assertIn("ReadError", outcomes[0].transport_error)
        self.assertIs(observed[0]["error"], primary)
        self.assertFalse(executor.last_close_report["clean"])
        self.assertTrue(executor.last_close_report["unconfirmed_work"])
        with self.assertRaises(PalimpzestAdapterError):
            executor.bind_sdk_request("later", request, lambda actual, **kwargs: response)
        self.assertFalse(executor.close())


if __name__ == "__main__":
    unittest.main()
