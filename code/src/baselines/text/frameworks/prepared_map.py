"""Explicit request capacity and pinned native graph settings."""

from dataclasses import dataclass


@dataclass(frozen=True)
class NativeGraphOptions:
    """Declared allowances and vendor graph settings, not measured parallelism."""

    concurrency: int = 4
    num_threads: int = 8
    ray_actors: int = 2
    ray_async_batches_per_actor: int = 2
    ray_batch_rows: int = 1
    source_blocks: int = 4

    def __post_init__(self):
        values = (
            self.concurrency,
            self.num_threads,
            self.ray_actors,
            self.ray_async_batches_per_actor,
            self.ray_batch_rows,
            self.source_blocks,
        )
        if any(type(v) is not int or not 1 <= v <= 256 for v in values):
            raise ValueError("native graph sizes must be positive and bounded")

    def validate_backend(self, backend):
        if backend == "daft" and self.concurrency < 2:
            raise ValueError("pinned native Daft needs at least two request slots")
        if backend == "ray-data" and self.ray_active_http_upper_bound > self.concurrency:
            raise ValueError(
                "native Ray active HTTP upper bound exceeds the declared HTTP allowance"
            )
        if backend == "ray-data" and self.num_threads <= self.ray_actors:
            raise ValueError(
                "native Ray Data needs a spare CPU slot for preprocessing and postprocessing"
            )

    @property
    def ray_request_row_envelope(self):
        # HTTP-stage batch rows only; not active requests or whole-graph storage.
        return self.ray_actors * self.ray_async_batches_per_actor * self.ray_batch_rows

    @property
    def ray_active_http_upper_bound(self):
        # Pinned HTTP batches await each lazy post in turn; actor max_concurrency=1.
        return self.ray_actors * self.ray_async_batches_per_actor
