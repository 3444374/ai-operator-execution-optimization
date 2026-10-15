"""Streaming owner identities after explicitly releasing broker history."""

import asyncio
from dataclasses import replace
import unittest
from unittest.mock import patch

from src.scheduling.core.session_contract import TaskKey
from tests.execution_provider.test_map_preparation import batches, make_transport, wait_until
from tests.execution_provider.test_ray_map_transport import task


class MapPreparationBrokerHistoryTest(unittest.IsolatedAsyncioTestCase):
    async def test_settled_blocks_use_new_execution_ids_across_rows_and_sessions(self):
        async def execute(table, index, template):
            return template.key, table['payload'][index].as_py(), 1, 2

        transport, preparation, _ = make_transport(execute)
        identities = []
        tasks = (task(0), task(1), replace(task(0), key=TaskKey(9, 0)))
        try:
            with patch('src.execution_provider.adapters.map_preparation.iter_payload_batches', batches):
                for item in tasks:
                    self.assertEqual(preparation.try_prepare((item,)), 1)
                    await asyncio.to_thread(wait_until, lambda: preparation.is_ready(item.key))
                    descriptor = next(iter(preparation.blocks.values())).descriptor
                    identities.append((descriptor.block_id, descriptor.row_ids[0]))
                    self.assertEqual(await transport.execute(item, 'model'), b'payload')
                    self.assertTrue(preparation.release(item.key))
                    self.assertEqual(len(preparation.broker._states), 0)
                    self.assertEqual(preparation.broker.snapshot().completed, 0)
            self.assertEqual(len({identity[0] for identity in identities}), len(tasks))
            self.assertEqual(len({identity[1] for identity in identities}), len(tasks))
            self.assertEqual([identity[0] for identity in identities],
                             ['map-prepared-0', 'map-prepared-1', 'map-prepared-2'])
        finally:
            for key in tuple(preparation.rows):
                preparation.release(key)
            await transport.close()
