"""Native snapshot -> cost proposal -> real controller wiring, with CPU data.

The positive layout/clock proof is an explicit fixture, not a claim that the
current backend or hardware already supplies either of those proofs.
"""
from types import SimpleNamespace
import unittest

from src.execution_provider.adapters.lotus_chain_token_batches import RequestSupplyFeedback
from src.scheduling.core.models import AdmissionObservation
from src.scheduling.core.session_contract import TaskKey
from src.scheduling.runtime.observations import ServiceMetricsSnapshot
from src.scheduling.submission_control.request_scan import AscendingRequestController
from src.scheduling.submission_control.stage_work import SupplyState, default_request_feedback_options


class ResourceWiringTests(unittest.TestCase):
    def feedback(self, *, proof=False):
        self.now = 0.
        self.records = [SimpleNamespace(key=TaskKey(1, i)) for i in range(11)]
        capacity = dict(kv_cache_blocks=1000, kv_cache_block_tokens=16)
        if proof:
            capacity.update(mapped_resource_clock_domain='driver.monotonic',
                            kv_cost_model='single_pool_token_blocks')
        self.owner = SimpleNamespace(resources=SimpleNamespace(max_active_batches=37),
            _actors=SimpleNamespace(model_ready=dict(engine_uid='fixture-engine', native_capacity=capacity)),
            _token_organizer=SimpleNamespace(request_window=1,
                _estimates={record.key: ('fixture', 11, 1) for record in self.records}))
        self.f = RequestSupplyFeedback(self.owner, default_request_feedback_options())
        self.f.session_id = 1
        self.f.controller = AscendingRequestController(37, clock=lambda: self.now)
        self.f.controller.reset(1, 10000)
        self.completed = 0

    def tick(self, *, fresh=True, pressure=False):
        self.now += 1
        self.completed += 20
        self.f.last_native = ServiceMetricsSnapshot(1, 0, .96 if pressure else .1,
            source_uid='fixture-engine', source_session_id=1, source_sequence=int(self.now),
            source_observed_at_s=self.now, received_at_s=self.now)
        o = AdmissionObservation(self.now, fresh, 1, 1, 0, .96 if pressure else .1,
            sample_age_s=0, completed_items_total=self.completed, pending_items=1000,
            preemptions_total=0, completed_input_work_total=11*self.completed,
            completed_output_work_total=self.completed, completed_computed_input_work_total=11*self.completed,
            completed_cached_input_work_total=0)
        s = SupplyState(self.now, 10, self.completed+10, self.completed, 1, True, False,
                        active_requests=1)
        decision = self.f._decide(o, s, self.records, self.records[1:], {self.records[0].key})
        self.f._set_window(decision.window)
        return decision

    def test_current_receipt_without_clock_and_layout_proof_uses_labelled_geometric_fallback(self):
        self.feedback()
        for _ in range(5):self.tick()
        self.assertEqual(self.owner._token_organizer.request_window, 2)
        proposal, = self.f.controller.report()['step_decisions']
        self.assertEqual(proposal['source'], 'geometric_fallback')
        self.assertEqual(proposal['reason'], 'native_kv_clock_unknown_or_invalid')
        self.assertIsNone(proposal['evidence']['physical_headroom'])
        self.assertEqual(proposal['evidence']['engine_uid'], 'fixture-engine')

    def test_costed_fresh_same_owner_ready_prefix_actually_changes_the_runtime_proposal(self):
        self.feedback(proof=True)
        for _ in range(5):self.tick()
        self.assertEqual(self.owner._token_organizer.request_window, 11)
        proposal, = self.f.controller.report()['step_decisions']
        self.assertEqual(proposal['source'], 'costed_resource_headroom')
        self.assertEqual(proposal['evidence']['reserved_committed_blocks'], 1)
        self.assertEqual(proposal['evidence']['fitting_ready_prefix'], 10)
        self.assertEqual(proposal['evidence']['hard_limit'], 37)

    def test_resource_estimate_cannot_bypass_missing_native_gate_or_pressure_shrink(self):
        self.feedback(proof=True)
        for _ in range(8):self.tick(fresh=False)
        self.assertEqual(self.owner._token_organizer.request_window, 1)
        self.assertEqual(self.f.controller.report()['step_decisions'], [])
        for _ in range(5):self.tick()
        self.assertEqual(self.owner._token_organizer.request_window, 11)
        self.tick(pressure=True)
        decision = self.tick(pressure=True)
        self.assertEqual(decision.reason, 'resource_pressure')
        self.assertLess(decision.window, 11)


if __name__ == '__main__':
    unittest.main()
