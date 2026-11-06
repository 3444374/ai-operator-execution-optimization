"""Actual async request coroutine with an in-memory native engine, no model."""
import asyncio
import unittest
from unittest.mock import Mock, patch

from src.serving.backends import vllm_async_engine as kernel
from src.serving.backends.vllm_protocol import GenerationSpec
from tests.serving.test_async_engine_process import row
from tests.serving.test_model_context import native_stub, spec


class AsyncQueryDeadlineTests(unittest.TestCase):
    def test_query_without_timer_reaches_native_generation_after_sixty_seconds(self):
        for deadline, succeeds in ((None, True), (160., False), (400., True)):
            with self.subTest(deadline=deadline), native_stub(8192) as calls:
                async def run():
                    engine = kernel.VllmAsyncEngine(spec(), GenerationSpec(),
                        limits=kernel.AsyncEngineLimits(max_inflight=2))
                    native_render = engine._llm.renderer.render_chat_async
                    clock = [100.]
                    async def render(*args):
                        clock[0] = 161.
                        return await native_render(*args)
                    engine._llm.renderer.render_chat_async = render
                    with patch('time.monotonic', side_effect=lambda: clock[0]):
                        result = await engine.generate_prepared(row(0, 'fixture'), request_id='query-call',
                                                               deadline_monotonic=deadline)
                    self.assertEqual(result['status'], 'succeeded' if succeeds else 'failed')
                    self.assertTrue(result['engine_terminal_confirmed'])
                    self.assertEqual(len(engine._requests), 0)
                asyncio.run(run())
                self.assertEqual(len(calls['generated']), int(succeeds))


class SyncQueryDeadlineTests(unittest.TestCase):
    def test_sync_and_stage_process_checks_keep_cancel_without_query_timer(self):
        from pathlib import Path
        import tempfile
        from src.serving.backends.vllm_sync_process import EngineProcess
        from src.serving.backends.staged_generation_process import ChainEngineProcess
        with tempfile.TemporaryDirectory() as directory:
            for kind in (EngineProcess, ChainEngineProcess):
                with self.subTest(kind=kind.__name__):
                    worker = object.__new__(kind)
                    worker.control = Path(directory)
                    worker._check(None)
                    (worker.control / 'stop').touch()
                    with self.assertRaises(InterruptedError):
                        worker._check(None)
                    (worker.control / 'stop').unlink()

    def test_existing_fused_worker_keeps_no_query_timer_through_model_and_cleanup(self):
        from tests.serving.test_fused_model_context import FusedModelContextTests
        case = FusedModelContextTests()
        case.setUp()
        try:
            owner = case.owner()
            owner.deadline = None
            create = Mock(wraps=owner._engine_type)
            owner._engine_type = create
            result = case.run_rows(owner)
            self.assertIsNone(result['error_type'])
            self.assertEqual(len(owner.engine.calls), 1)
            self.assertEqual(owner.engine.closes, 1)
            self.assertIsNone(create.call_args.args[3])
        finally:
            case.doCleanups()


if __name__ == '__main__':
    unittest.main()
