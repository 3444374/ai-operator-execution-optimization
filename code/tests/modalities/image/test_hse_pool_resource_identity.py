import unittest

from src.modalities.image.execution import SemLoomRayWorkerPool
from tests.modalities.image import test_staged_image_execution as fixture
from tests.modalities.image.test_hse_static_lifecycle import run, source


class HSEPoolResourceIdentityTests(unittest.TestCase):
    def test_repeated_gpu_handle_fails_before_source_or_actor_access(self):
        class UnreadSource:
            reads = 0

            def into_batches(self, batch_size):
                self.reads += 1
                raise AssertionError("source must not be read for an invalid pool")

        class UncalledActor:
            def __init__(self):
                self.accesses = []

            def __getattr__(self, name):
                self.accesses.append(name)
                raise AssertionError("actor methods must not be accessed for an invalid pool")

        unread = UnreadSource()
        cpu, gpu = UncalledActor(), UncalledActor()
        pool = SemLoomRayWorkerPool((cpu,), (gpu, gpu))
        with fixture._fake_ray_module(fixture._FakeRay()):
            with self.assertRaisesRegex(ValueError, "GPU actor handles.*distinct Python objects"):
                run(unread, pool)
        self.assertEqual(unread.reads, 0)
        self.assertEqual(cpu.accesses, [])
        self.assertEqual(gpu.accesses, [])

    def test_distinct_gpu_handles_keep_exactly_once_dispatch(self):
        class ObservedGpu(fixture._GpuActor):
            def __init__(self):
                self.rows = []
                super().__init__()

            def _embed(self, batch_ref):
                self.rows.extend(batch_ref.value.doc_ids)
                return super()._embed(batch_ref)

        rows = ("a", "b", "c", "d")
        first, second = ObservedGpu(), ObservedGpu()
        pool = SemLoomRayWorkerPool(
            (fixture._CpuActor(), fixture._CpuActor()), (first, second)
        )
        with fixture._fake_ray_module(fixture._FakeRay()):
            result = run(source(rows), pool, rows=rows)
        self.assertTrue(result.audit["exactly_once"])
        self.assertCountEqual(first.rows + second.rows, rows)
        self.assertTrue(first.rows)
        self.assertTrue(second.rows)


if __name__ == "__main__":
    unittest.main()
