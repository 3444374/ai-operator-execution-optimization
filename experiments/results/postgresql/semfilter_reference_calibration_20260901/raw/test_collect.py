"""Pre-request checks of the one-run evidence collector (synthetic data only)."""
import copy
import unittest

import collect


class CollectorTest(unittest.TestCase):
    def setUp(self):
        self.raw = [dict(id=str(i), conversations=[dict(from_="human", value=f"input {i}")])
                    for i in range(1400)]
        for row in self.raw:
            row["conversations"][0]["from"] = row["conversations"][0].pop("from_")

    def test_split_sizes_and_disjointness(self):
        rows = collect.select_rows(self.raw)
        self.assertEqual(collect.Counter(r["split"] for r in rows),
                         {"warmup": 64, "training": 768, "held_out": 384})
        for split, sizes in (("training", collect.TRAIN_SIZES), ("held_out", collect.HOLDOUT_SIZES)):
            self.assertEqual(tuple(sum(r["split"] == split and r["cell"] == cell for r in rows)
                                   for cell in range(len(sizes))), sizes)
        self.assertEqual(len({r["payload_sha256"] for r in rows}), 1216)

    def test_deterministic_selection(self):
        self.assertEqual(collect.select_rows(self.raw), collect.select_rows(list(reversed(self.raw))))

    def test_no_payload_rewriting(self):
        for row in self.raw:
            row["conversations"][0]["value"] += "  中文\n"
        rows = collect.select_rows(self.raw)
        self.assertTrue(all(r["payload"].endswith("  中文\n") for r in rows))

    def test_excludes_invalid_and_duplicates(self):
        original = collect.select_rows(self.raw)
        extra = [dict(id=f"bad-{i}", conversations=[{"from": "human", "value": value}])
                 for i, value in enumerate(("  ", "x\0y", "中" * 1366))]
        self.assertEqual(collect.select_rows(self.raw + copy.deepcopy(self.raw) + extra), original)

    def test_finds_actual_custom_scan_name(self):
        node = {"Custom Plan Provider": "SemLoom SemFilter"}
        self.assertIs(collect.filter_node({"Plans": [node]}), node)
        self.assertIsNone(collect.filter_node({"Custom Plan Provider": "SemFilter"}))

    def test_identity_is_order_independent(self):
        self.assertEqual(collect.identity({"a": 1, "b": 2}), collect.identity({"b": 2, "a": 1}))


if __name__ == "__main__":
    unittest.main()
