"""Fixture evidence retains identity and timing, never previous image bodies."""
import gc
import json
import unittest
import urllib.request
import weakref

from tests.modalities.image.stage_http_fixture import stage_http_fixture


class StageHttpFixtureTests(unittest.TestCase):
    def test_round_receipts_do_not_keep_previous_complete_image_body(self):
        class Body(dict):
            pass

        bodies = []

        def decode(raw):
            def track(value):
                body = Body(value)
                bodies.append(weakref.ref(body))
                return body
            return json.loads(raw, object_hook=track)

        def reply(body):
            return 200, b'{"ok":true}'

        with stage_http_fixture(reply, lambda b:b["id"], max_posts=2, decode=decode) as (base, receipts):
            for row_id in ("first", "second"):
                payload = dict(id=row_id, messages=[dict(content=[dict(type="image_url", image_url=dict(
                    url="data:image/png;base64," + "A" * 65536))])])
                request = urllib.request.Request(base + "/chat/completions", data=json.dumps(payload).encode(),
                                                  headers={"Content-Type":"application/json"})
                with urllib.request.urlopen(request, timeout=2) as response:
                    self.assertEqual(response.status, 200)
                receipts.wait_idle()
                gc.collect()
                self.assertTrue(all(ref() is None for ref in bodies))
                self.assertTrue(all("messages" not in row for row in receipts))
                self.assertTrue(all(row["completed_ns"] >= row["received_ns"] for row in receipts))
            self.assertEqual([r["identity"] for r in receipts], ["first", "second"])
            self.assertEqual(sum(r["uri_bytes"] for r in receipts), 2 * 65558)
