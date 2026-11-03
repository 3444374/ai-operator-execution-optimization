"""Fixed-tag codec/worker source probes, without importing the vLLM runtime.

Only verified class/function definitions are compiled. Model dependencies are
CPU stubs; MessagePack itself is the real msgspec implementation.
"""

import ast
from contextlib import nullcontext
from functools import partial
import hashlib
import importlib
from pathlib import Path
from types import FunctionType, ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

from src.infrastructure.gpu_allocation import VLLM_GPU_IDENTITY_EXTENSION


SOURCE_HASHES = {
    "vllm/v1/serial_utils.py": "d75c17a098231b2398cbdb7da6f9983e5b9229c0ba138b2874934d45fafd1f28",
    "vllm/v1/worker/worker_base.py": "7da44338c2645ebf03d23394e452b31a8e3da1011fd1b42fcfcccfe99551b3fe",
    "vllm/engine/arg_utils.py": "3b3ffa6b403d34188c6d2fe7a2dc36debcee7402a17fc6a6145e885130f3dacd",
    "vllm/v1/engine/core_client.py": "2d83952580e23abca33c3bb5f93edc349ea0e10da358bcb41b385f8d066e2a0b",
    "vllm/v1/engine/async_llm.py": "69ea05aea497134204f1c6fd5f53994f4f7bbc35d24f90a768553dd8d6ba0b1a",
    "vllm/v1/executor/uniproc_executor.py": "4625c8e59aef1482600b02204701d348d7f716427581d3cf23b06bff9b056abe",
    "vllm/config/parallel.py": "2bb0c2d54a0c3211c5ea1ddeed731d0e2a5d95fb58972f92845268959f0c818e",
    "vllm/envs.py": "15ab853b73b26da5dc2808699138dff7217b72b9f661da274cc0c9f6c262f631",
}


def verified_source(root, name):
    path = Path(root) / name
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != SOURCE_HASHES[name]:
        raise ValueError("vLLM v0.25.1 probe source differs: " + name)
    return path, ast.parse(raw, filename=str(path))


def definitions(root, name, names, namespace):
    path, tree = verified_source(root, name)
    selected = [node for node in tree.body
                if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in names]
    if {node.name for node in selected} != set(names):
        raise ValueError("required vLLM source definitions are unavailable")
    future = ast.parse("from __future__ import annotations").body
    module = ast.fix_missing_locations(ast.Module(body=future + selected, type_ignores=[]))
    exec(compile(module, str(path), "exec"), namespace)
    return namespace


def default_serialization_flag(root):
    path, tree = verified_source(root, "vllm/envs.py")
    values = next(node.value for node in tree.body if isinstance(node, ast.AnnAssign)
                  and isinstance(node.target, ast.Name) and node.target.id == "environment_variables")
    expression = next(value for key, value in zip(values.keys, values.values)
                      if isinstance(key, ast.Constant) and key.value == "VLLM_ALLOW_INSECURE_SERIALIZATION")
    function = eval(compile(ast.Expression(expression), str(path), "eval"),
                    {"os": SimpleNamespace(environ={}, getenv=lambda name, default=None: default)})
    return function()


def pinned_encoder(root):
    import msgspec
    import numpy as np

    def forbidden(*args, **kwargs):
        raise AssertionError("pickle/tensor paths are outside the CPU primitive-message probe")

    namespace = dict(msgpack=msgspec.msgpack, np=np, FunctionType=FunctionType, partial=partial,
        torch=SimpleNamespace(Tensor=type("UnusedTensor", (), {})),
        MultiModalKwargsItem=type("UnusedMultiModalItem", (), {}),
        MultiModalKwargsItems=type("UnusedMultiModalItems", (), {}),
        envs=SimpleNamespace(VLLM_ALLOW_INSECURE_SERIALIZATION=default_serialization_flag(root)),
        pickle=SimpleNamespace(dumps=forbidden, loads=forbidden),
        cloudpickle=SimpleNamespace(dumps=forbidden, loads=forbidden),
        _log_insecure_serialization_warning=forbidden)
    definitions(root, "vllm/v1/serial_utils.py", ("MsgpackEncoder", "UtilityResult", "run_method"), namespace)
    return SimpleNamespace(encoder=namespace["MsgpackEncoder"](size_threshold=256),
        utility=namespace["UtilityResult"], run_method=namespace["run_method"],
        decode=msgspec.msgpack.decode, insecure=namespace["envs"].VLLM_ALLOW_INSECURE_SERIALIZATION)


def pinned_worker(root, *, collision=False):
    class CpuWorkerBase:
        pass

    class CpuWorker(CpuWorkerBase):
        def __init__(self, **kwargs):
            self.received = kwargs

    if collision:
        CpuWorker.semloom_gpu_identity = lambda self: None

    def resolve(name):
        if name == "cpu_fixture.CpuWorker":
            return CpuWorker
        if name != VLLM_GPU_IDENTITY_EXTENSION:
            raise AssertionError("unexpected worker class")
        module, attribute = name.rsplit(".", 1)
        return getattr(importlib.import_module(module), attribute)

    namespace = dict(WorkerBase=CpuWorkerBase, resolve_obj_by_qualname=resolve,
        instrument=lambda **kwargs: lambda function: function,
        set_current_vllm_config=lambda config: nullcontext(), logger=Mock())
    definitions(root, "vllm/v1/worker/worker_base.py", ("WorkerWrapperBase",), namespace)
    worker = namespace["WorkerWrapperBase"]()
    config = SimpleNamespace(enable_trace_function_call_for_thread=lambda: None,
        parallel_config=SimpleNamespace(worker_cls="cpu_fixture.CpuWorker",
                                       worker_extension_cls=VLLM_GPU_IDENTITY_EXTENSION),
        model_config=SimpleNamespace(multimodal_config=None))
    package, plugins = ModuleType("vllm"), ModuleType("vllm.plugins")
    plugins.load_general_plugins = lambda: None
    with patch.dict("sys.modules", {"vllm": package, "vllm.plugins": plugins}):
        worker.init_worker([dict(vllm_config=config, shared_worker_lock=None)])
    return worker
