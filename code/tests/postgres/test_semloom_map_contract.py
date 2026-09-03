"""Generative Map identities and completion policy at the public pure-value seam."""
import hashlib
import unittest


class SemanticMapContractTests(unittest.TestCase):
    def test_map_plan_identity_matches_independent_ascii_vector(self) -> None:
        from src.execution_provider.semantic_map import SemanticMapPlan

        plan = SemanticMapPlan("Echo the input.", "golden-map-v1", 128)
        self.assertEqual(hashlib.sha256(plan.canonical_bytes()).hexdigest(),
                         "b39cf274ee1a8c75a81995f0324cb3ab6cd18ce13ae68aaffc15fcba78e5f8ba")
        self.assertEqual(plan.digest, "b39cf274ee1a8c75a81995f0324cb3ab6cd18ce13ae68aaffc15fcba78e5f8ba")


if __name__ == "__main__":
    unittest.main()
