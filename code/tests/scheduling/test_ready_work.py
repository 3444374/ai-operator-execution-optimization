import unittest

from src.semantic_methods.driver import RowIdentity
from src.scheduling.organization.ready_work import ReadyCost, WorkProgressPriority
from tests.scheduling.test_method_driver import make_driver, tick


class ReadyWorkTests(unittest.TestCase):
    def test_priority_changes_real_driver_offer_order_without_changing_results_or_capacity(self):
        costs = {0: ReadyCost(10, 0, 'CPU-fixture'), 1: ReadyCost(1, 10, 'CPU-fixture')}
        selector = WorkProgressPriority(lambda item: costs[item.sequence], mode='progress_per_work', max_wait_s=10)
        events = []
        driver, engine, backend, pool = make_driver(rows=2, ready_order='fifo', ready_selector=selector,
                                                    clock=lambda: 0, on_ready_event=events.append)
        try:
            driver.offer_row(RowIdentity(0, 'a'), b'one')
            driver.offer_row(RowIdentity(1, 'b'), b'two')
            driver.end_input(); results = {}
            for _ in range(20):
                tick(driver, backend)
                for value in driver.results(2):
                    results[value.row.call_id] = value.value
                    driver.release_result(value.row)
                if driver.finished:
                    break
            accepted = [event for event in events if event.action == 'Core_offer_accepted']
            self.assertEqual([e.row.sequence for e in accepted], [1, 1, 0])
            self.assertEqual(results, {'a': b'OK', 'b': b'OK'})
            self.assertTrue(driver.finished)
            self.assertEqual(engine.capacity.usage().held_tasks, 0)
            self.assertEqual(pool.used, (0, 0))
        finally:
            driver.close()

    def test_oldest_ready_is_selected_after_its_wait_and_missing_cost_is_explicit(self):
        from src.semantic_methods.driver import ReadyRequest
        candidates = (ReadyRequest(0, 'a', 0, 1, 0, 'model', 'work_units'),
                      ReadyRequest(1, 'b', 0, 1, 1, 'model', 'work_units'))
        priority = WorkProgressPriority(lambda item: None, mode='shortest_work', max_wait_s=2)
        self.assertEqual(priority(candidates, 1), (0, 'costs_unavailable_fifo'))
        self.assertEqual(priority(candidates, 2), (0, 'aged_fifo'))


if __name__ == '__main__':
    unittest.main()
