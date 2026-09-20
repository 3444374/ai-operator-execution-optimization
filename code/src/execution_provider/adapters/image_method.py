"""One semantic embedding call; physical CPU/GPU transitions remain in the backend."""

from dataclasses import dataclass

from ...semantic_methods.continuation import Continue, Final, Request
from ..semantic_image import SemanticImagePlan


@dataclass(frozen=True)
class ImageEmbeddingMethod:
    plan: SemanticImagePlan

    def start(self, value: bytes):
        self.plan.validate_input(value)
        return Continue(Request("image", value, self.plan.input_size ** 2, self.plan.result_bytes))

    def resume(self, state: bytes, result: bytes):
        if state != b"":
            raise ValueError("unexpected image method continuation state")
        return Final(self.plan.validate_result(result))
