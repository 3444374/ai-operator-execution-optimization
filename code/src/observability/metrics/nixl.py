"""Constant-size totals from vLLM 0.25.1 NIXL scheduler-stat updates."""
import math


class NativeNixlTotals:
    def __init__(self, engine_uid):
        self.engine_uid = engine_uid
        self.updates = 0
        self.error = None
        self.totals = dict(transfers=0, bytes=0, transfer_s=0.0, post_s=0.0,
                           descriptors=0, failed_transfers=0, failed_notifications=0,
                           expired_requests=0)

    def record(self, values):
        if values is None:
            return
        names = dict(bytes_transferred='bytes', transfer_duration='transfer_s',
                     post_duration='post_s', num_descriptors='descriptors',
                     num_failed_transfers='failed_transfers',
                     num_failed_notifications='failed_notifications',
                     num_kv_expired_reqs='expired_requests')
        if (not isinstance(values, dict) or not names.keys() <= values.keys()
                or any(not isinstance(values[name], (list, tuple)) or any(
                    type(x) not in (int, float) or not math.isfinite(x) or x < 0
                    for x in values[name]) for name in names)
                or len({len(values[name]) for name in (
                    'bytes_transferred', 'transfer_duration', 'post_duration', 'num_descriptors')}) != 1):
            self.error = 'unsupported_native_nixl_stats'
            return
        self.updates += 1
        self.totals['transfers'] += len(values['bytes_transferred'])
        for name, output in names.items():
            self.totals[output] += sum(values[name])

    def snapshot(self):
        return dict(status='unavailable' if self.error else 'observed' if self.updates else 'no_transfer_update',
                    engine_uid=self.engine_uid, updates=self.updates, error=self.error,
                    source='vllm_0.25.1_scheduler_stats.kv_connector_stats', **self.totals)
