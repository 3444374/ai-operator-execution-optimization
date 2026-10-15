"""Opt-in CLIP object identities and integer call counters, without model retention.

SEMLOOM_IMAGE_REUSE_DIR must name an outside-Git directory. Each actual model
gets one synchronous construction receipt; calls only change in-memory counters.
No clocks, files, hooks or model attributes are added when the option is absent.
"""
from __future__ import annotations

import os
from pathlib import Path
import time
import uuid
import weakref

from src.baselines.common.private_artifacts import require_outside_git, write_private_json


_models = weakref.WeakValueDictionary()
_ATTRIBUTE = "_semloom_image_reuse_probe"


class Probe:
    """The owner retains this probe; it only weakly references the actual model."""

    def __init__(self, model, *, role, device, directory):
        self._model = weakref.ref(model)
        self._directory = directory
        self.model_uid = uuid.uuid4().hex
        self.pid = os.getpid()
        self.model_object_id = id(model)
        self.role, self.device = str(role), str(device)
        self.constructed_monotonic_ns = time.monotonic_ns()
        self._counts = {kind: [0, 0, 0, 0] for kind in ("image", "text")}
        self.hook_errors = 0
        self.hook_error_type = None
        self.hook_mode = "explicit_image_call"
        self._hooks = []

    def begin(self, rows, kind="image"):
        counts = self._count(rows, kind)
        counts[0] += 1
        counts[2] += rows

    def returned(self, rows, kind="image"):
        counts = self._count(rows, kind)
        counts[1] += 1
        counts[3] += rows

    def _count(self, rows, kind):
        if kind not in self._counts or type(rows) is not int or rows < 0:
            raise ValueError("model observations need image/text and nonnegative integer rows")
        return self._counts[kind]

    def _hook_failed(self, error):
        self.hook_errors += 1
        self.hook_error_type = type(error).__name__

    def snapshot(self):
        result = dict(model_uid=self.model_uid, pid=self.pid,
            model_object_id=self.model_object_id, role=self.role, device=self.device,
            constructed_monotonic_ns=self.constructed_monotonic_ns,
            model_alive=self._model() is not None, hook_mode=self.hook_mode,
            hook_errors=self.hook_errors, hook_error_type=self.hook_error_type)
        names = ("attempts", "returns", "attempted_rows", "returned_rows")
        for index, name in enumerate(names):
            result[name] = sum(counts[index] for counts in self._counts.values())
            for kind, counts in self._counts.items():
                result[kind + "_" + name] = counts[index]
        return result


def observe_model(model, *, role, device):
    """Return a single probe for this actual model in the current process."""
    directory = os.environ.get("SEMLOOM_IMAGE_REUSE_DIR")
    if not directory:
        return None
    directory = Path(directory).resolve()
    require_outside_git(directory)
    existing = getattr(model, _ATTRIBUTE, None)
    if isinstance(existing, Probe) and existing.pid == os.getpid():
        if existing._directory != directory:
            raise ValueError("an observed model cannot change its private output directory")
        return existing
    directory.mkdir(parents=True, mode=0o700, exist_ok=True)
    probe = Probe(model, role=role, device=device, directory=directory)
    write_private_json(directory / ("model-" + probe.model_uid + ".json"),
        dict(schema="semloom.image_model_construction.v1", **probe.snapshot()))
    # Model -> probe -> weak(model) adds no retention cycle. It also prevents a
    # second observe call from writing a new construction event for one model.
    setattr(model, _ATTRIBUTE, probe)
    _models[probe.model_uid] = probe
    return probe


def snapshot_models():
    """Return scalar snapshots of this process's live probes, without I/O."""
    directory = os.environ.get("SEMLOOM_IMAGE_REUSE_DIR")
    if not directory:
        return []
    directory = Path(directory).resolve()
    pid = os.getpid()
    return [probe.snapshot() for _, probe in sorted(list(_models.items()))
            if probe.pid == pid and probe._directory == directory]


def _stock_hooks(probe, kind, argument):
    reference = weakref.ref(probe)

    def count(args, kwargs, returned):
        current = reference()
        if current is None:
            return
        try:
            value = kwargs.get(argument, args[0] if args else None)
            rows = int(value.shape[0])
            (current.returned if returned else current.begin)(rows, kind)
        except Exception as error:
            # Diagnostic failures must not replace the native forward error or
            # change inputs/outputs. The caller rejects incomplete observations.
            current._hook_failed(error)

    def before(module, args, kwargs):
        count(args, kwargs, False)

    def after(module, args, kwargs, output):
        count(args, kwargs, True)

    return before, after


def observe_stock_model(model, *, device):
    """Count actual stock CLIP vision/text forwards using standard Torch hooks.

    Post hooks use Torch's default always_call=False: a native forward exception
    leaves an attempt without a return and propagates unchanged. Hook setup
    errors are raised after removing any successfully installed partial hooks.
    """
    probe = observe_model(model, role="lotus_stock_clip", device=device)
    if probe is None or probe.hook_mode == "torch_submodule_hooks":
        return probe
    handles = []
    try:
        for kind, attribute, argument in (("image", "vision_model", "pixel_values"),
                                          ("text", "text_model", "input_ids")):
            module = getattr(model, attribute)
            before, after = _stock_hooks(probe, kind, argument)
            handles.append(module.register_forward_pre_hook(before, with_kwargs=True))
            handles.append(module.register_forward_hook(after, with_kwargs=True))
    except BaseException as error:
        probe._hook_failed(error)
        for handle in handles:
            try:
                handle.remove()
            except BaseException as cleanup_error:
                error.add_note("image reuse hook removal also raised " + type(cleanup_error).__name__)
        raise
    probe._hooks = handles
    probe.hook_mode = "torch_submodule_hooks"
    return probe
