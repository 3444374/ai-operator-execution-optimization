"""Install request observation at Ray Serve's native initialization boundary."""

from ray.llm._internal.common.callbacks.base import CallbackBase

from src.baselines.text.frameworks.ray_native_observer import setup


class NativeObserverCallback(CallbackBase):
    """Keep native node setup and worker hooks; observe the model frontend only."""

    async def on_after_node_init(self):
        await super().on_after_node_init()
        setup()
