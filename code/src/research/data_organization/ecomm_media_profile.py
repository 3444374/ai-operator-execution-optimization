"""CPU Q11 replay: native LOTUS versus tile-scoped image preparation.

No engine, tokenizer, provider or GPU is constructed. All three authored joins
still run in native LOTUS. Private audits contain full image-bearing messages.
"""

import argparse
from collections import Counter, defaultdict
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
import gc
import hashlib
import importlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
from time import perf_counter
from types import ModuleType
from unittest.mock import patch
import weakref

from src.baselines.text.sembench_source import SEMBENCH_COMMIT, verify_checkout
from src.infrastructure.private_artifacts import (
    file_identity, new_private_directory, open_private_text, write_private_json,
)
from src.infrastructure.redact import redact_text
from src.research.data_organization.ecomm_media import RequestAudit, TileImages, asset_tiles
from src.semantic_methods.lotus.source import FILTER_FILES, source_report


QUERY = "files/ecomm/queries/dialects/lotus/q11.py"
CASES = ("branch_native", "tile_native", "tile_shared")


class MediaObserver:
    """Observe native cached strings; these byte counts are not process RSS."""

    def __init__(self, image_module, templates):
        self.module, self.templates = image_module, templates
        self.arrays = {}
        self.phase = "initial_filters"
        self.metrics = defaultdict(Counter)
        self.tile_arrays = None
        self.pool = None
        self.sample_s = 0.0
        self.start = perf_counter()
        self.samples = []

    def sample(self):
        start = perf_counter()
        unique, dead = {}, []
        for key, ref in self.arrays.items():
            array = ref()
            if array is None:
                dead.append(key)
            else:
                unique.update((id(v), v) for v in array._cached_images.values() if isinstance(v, str))
        for key in dead:
            del self.arrays[key]
        if self.pool is not None:
            unique.update((id(v), v) for v in self.pool.values.values())
        encoded_bytes = sum(len(v) for v in unique.values())
        python_bytes = sum(sys.getsizeof(v) for v in unique.values())
        footprint = (self.phase, encoded_bytes, python_bytes)
        if not self.samples or tuple(self.samples[-1][1:]) != footprint:
            self.samples.append([perf_counter() - self.start, *footprint])
        counts = self.metrics[self.phase]
        counts["peak_observed_cached_string_bytes"] = max(
            counts["peak_observed_cached_string_bytes"], encoded_bytes)
        counts["peak_observed_python_string_bytes"] = max(
            counts["peak_observed_python_string_bytes"], python_bytes)
        self.sample_s += perf_counter() - start

    @contextmanager
    def observe(self):
        image_type = self.module.ImageArray
        native_init, native_get = image_type.__init__, image_type.get_image
        native_fetch = self.module.fetch_image

        def initialize(array, *args, **kwargs):
            native_init(array, *args, **kwargs)
            ref = weakref.ref(array)
            self.arrays[id(array)] = ref
            if self.tile_arrays is not None:
                self.tile_arrays[id(array)] = ref

        def fetch(image, image_type="Image"):
            start = perf_counter()
            try:
                self.metrics[self.phase]["native_fetch_calls"] += 1
                return native_fetch(image, image_type)
            finally:
                self.metrics[self.phase]["native_fetch_s"] += perf_counter() - start

        def scoped_fetch(image, image_type="Image"):
            return fetch(image, image_type) if self.pool is None else self.pool(image, image_type)

        def get_image(array, index, image_type="Image"):
            counts = self.metrics[self.phase]
            counts["native_get_calls"] += 1
            counts["native_index_cache_hits"] += (index, image_type) in array._cached_images
            result = native_get(array, index, image_type)
            self.sample()
            return result

        def timed_formatter(function, name):
            def call(*args, **kwargs):
                start = perf_counter()
                try:
                    return function(*args, **kwargs)
                finally:
                    self.metrics[self.phase][name + "_s"] += perf_counter() - start
            return call

        self.fetch = fetch
        with ExitStack() as stack:
            stack.enter_context(patch.object(image_type, "__init__", initialize))
            stack.enter_context(patch.object(image_type, "get_image", get_image))
            stack.enter_context(patch.object(self.module, "fetch_image", scoped_fetch))
            for name in ("df2multimodal_info", "filter_formatter"):
                stack.enter_context(patch.object(self.templates, name,
                    timed_formatter(getattr(self.templates, name), name)))
            yield
            self.sample()

    @contextmanager
    def tile(self, shared):
        self.tile_arrays = {}
        self.pool = TileImages(self.fetch) if shared else None
        try:
            yield
        finally:
            self.sample()
            # Every tile request has returned synchronously. Only tile-owned
            # arrays lose cached representations; raw references are untouched.
            for ref in self.tile_arrays.values():
                array = ref()
                if array is not None:
                    array._cached_images.clear()
            if self.pool is not None:
                self.metrics[self.phase]["tile_shared_hits"] += self.pool.hits
                self.pool.clear()
            self.tile_arrays, self.pool = None, None
            self.sample()


@dataclass
class Branch:
    frame: object
    instruction: str


class BeforeOriginalJoin(Exception):
    pass


def capture_branches(query, data_dir, filter_type, join_type):
    branches = []

    def capture(accessor, instruction):
        branches.append(Branch(accessor._obj, instruction))
        return accessor._obj

    def stop(*args, **kwargs):
        raise BeforeOriginalJoin

    with patch.object(filter_type, "__call__", capture), patch.object(join_type, "__call__", stop):
        try:
            query.run(str(data_dir))
        except BeforeOriginalJoin:
            pass
    if len(branches) != 4 or any(b.frame is not branches[0].frame for b in branches[:3]):
        raise ValueError("the authored Q11 initial relation/Filter shape differs")
    if not branches[0].frame.index.is_unique:
        raise ValueError("the Q11 merged occurrence index must identify physical source rows")
    return branches


def cpu_allowance(frame, max_requests):
    n, accessories = len(frame), int((frame["price"] <= 500).sum())
    upper = 3 * n + accessories + n * accessories + n * n + n ** 3 * accessories
    if not n or upper > max_requests:
        raise ValueError(f"Q11 all-True replay needs {upper} logical requests; allowance is {max_requests}")
    return dict(merged_occurrences=n, accessory_occurrences=accessories,
                all_true_request_upper_bound=upper)


def run_case(query, data_dir, selected, args, output, *, case, fixed=None):
    import lotus
    import pandas as pd
    from lotus.models import LM
    from lotus.templates import task_instructions
    from lotus.types import LMOutput

    image_module = importlib.import_module("lotus.dtype_extensions.image")
    filter_type, join_type = type(pd.DataFrame().sem_filter), type(pd.DataFrame().sem_join)
    native_filter, native_join = filter_type.__call__, join_type.__call__
    observer = MediaObserver(image_module, task_instructions)
    branches, lineage, counters = [], {}, Counter()

    def source(path):
        return selected[Path(path).name].copy(deep=True)

    with open_private_text(output) as stream:
        audit = RequestAudit(stream, fixed=fixed, pattern=args.pattern, max_requests=args.max_logical_requests)

        class FixedLM(LM):
            def __call__(self, messages, show_progress_bar=True, progress_bar_desc="", **kwargs):
                return LMOutput(outputs=audit.reply(messages, model=self.model,
                    params={**self.kwargs, **kwargs}))

        lm = FixedLM(model=args.model, temperature=args.temperature, max_tokens=args.max_tokens,
                     cache={}, **json.loads(args.model_kwargs))

        def filter_rows(branch, frame):
            stage = "filter_" + str(branch)
            occurrences = [(stage + ":" + str(i), []) for i in frame.index]
            with audit.operation(stage, occurrences):
                result = native_filter(frame.sem_filter, instructions[branch].instruction)
            lineage[id(result)] = {i: [stage + ":" + str(i)] for i in result.index}
            observer.sample()
            return result

        def run_filter(accessor, instruction):
            branch = counters["filters"]
            counters["filters"] += 1
            if branch >= 4 or counters["joins"]:
                raise ValueError("unexpected dataframe Filter in the authored Q11 suffix")
            if case == "branch_native":
                instructions.append(Branch(accessor._obj, instruction))
                result = filter_rows(branch, accessor._obj)
                branches.append(result)
                return result
            return branches[branch]

        def run_join(accessor, other, instruction):
            if counters["filters"] != 4:
                raise ValueError("an authored Join ran before all initial Filters completed")
            stage = "join_" + str(counters["joins"])
            counters["joins"] += 1
            observer.phase = "original_joins"
            left, right = lineage[id(accessor._obj)], lineage[id(other)]
            occurrences = [(stage + f":{i}:{j}", left[i] + right[j])
                           for i in accessor._obj.index for j in other.index]
            with audit.operation(stage, occurrences):
                result = native_join(accessor, other, instruction)
            surviving = [[key] for key, _ in occurrences if audit.responses[key] == "Answer: True"]
            if len(result) != len(surviving):
                raise ValueError("native Join assembly differs from the fixed parsed responses")
            lineage[id(result)] = dict(zip(result.index, surviving, strict=True))
            observer.sample()
            return result

        gc.collect()
        start = perf_counter()
        with patch.object(pd, "read_parquet", source), observer.observe(), \
                lotus.settings.context(lm=lm, enable_cache=False):
            instructions = []
            capture_s, organization_s = 0.0, 0.0
            if case != "branch_native":
                capture_start = perf_counter()
                instructions = capture_branches(query, data_dir, filter_type, join_type)
                capture_s = perf_counter() - capture_start
                base = instructions[0].frame
                organize_start = perf_counter()
                tiles = asset_tiles(list(base["images"].array), args.tile_assets)
                organization_s = perf_counter() - organize_start
                kept = [[] for _ in instructions]
                for positions in tiles:
                    with observer.tile(case == "tile_shared"):
                        tile = base.iloc[list(positions)]
                        for branch, instruction in enumerate(instructions):
                            frame = tile if branch < 3 else instruction.frame.loc[
                                instruction.frame.index.intersection(tile.index, sort=False)]
                            result = filter_rows(branch, frame)
                            kept[branch].extend(result.index)
                for branch, instruction in enumerate(instructions):
                    keep = set(kept[branch])
                    result = instruction.frame.loc[[i for i in instruction.frame.index if i in keep]]
                    result.attrs["index_dirs"] = instruction.frame.attrs.get("index_dirs", None)
                    branches.append(result)
                    stage = "filter_" + str(branch)
                    lineage[id(result)] = {i: [stage + ":" + str(i)] for i in result.index}
            with patch.object(filter_type, "__call__", run_filter), \
                    patch.object(join_type, "__call__", run_join):
                result = query.run(str(data_dir))
            if counters != Counter(filters=4, joins=3):
                raise ValueError("the authored Q11 operator count changed")
    elapsed = perf_counter() - start
    report = dict(case=case, query_cpu_s=elapsed, capture_s=capture_s,
                  organization_s=organization_s, observer_sample_s=observer.sample_s,
                  media={k: dict(v) for k, v in observer.metrics.items()},
                  observed_cache_samples=observer.samples,
                  request_audit=audit.report(), result_rows=len(result),
                  result_sha256=hashlib.sha256(result.to_json().encode()).hexdigest(),
                  private_audit_sha256=file_identity(output)["sha256"])
    return report, audit, branches, result


def read_selected(args):
    import pandas as pd
    from PIL import Image

    start = perf_counter()
    paths = {"styles_details.parquet": args.styles_file, "image_mapping.parquet": args.mapping_file}
    if args.input_role == "ecomm_sample" and any(path.suffix != ".parquet" for path in paths.values()):
        raise ValueError("the original Ecomm sample requires Parquet; CSV is only a software fixture")
    loaded, hashes = {}, {}
    for name, path in paths.items():
        hashes[name] = file_identity(path)["sha256"]
        loaded[name] = pd.read_csv(path) if path.suffix == ".csv" else pd.read_parquet(path)
    mapping = loaded["image_mapping.parquet"].head(args.image_rows)
    styles = loaded["styles_details.parquet"]
    styles = styles[styles["id"].astype(int).isin(mapping["id"].astype(int))]
    selected = {"styles_details.parquet": styles, "image_mapping.parquet": mapping}
    merged = styles.assign(id=styles["id"].astype(int)).merge(
        mapping.assign(image_id=mapping["id"].astype(int)),
        left_on="id", right_on="image_id", suffixes=("", "_img"))
    workload = cpu_allowance(merged, args.max_logical_requests)
    assets = [args.data_dir / "images" / str(name) for name in merged.filename]
    media, identities = [], {}
    for path in dict.fromkeys(assets):
        identities[path] = file_identity(path)
        with Image.open(path) as image:
            media.append(dict(sha256=identities[path]["sha256"], encoded_bytes=identities[path]["bytes"],
                              width=image.width, height=image.height))
    source = dict(selection="first mapping rows and all matching style occurrences",
                  requested_mapping_rows=args.image_rows, mapping_rows=len(mapping),
                  source_sha256=hashes, input_formats={k: v.suffix for k, v in paths.items()},
                  asset_occurrences=len(assets), unique_asset_references=len(set(assets)),
                  assets=media, workload=workload)
    return selected, identities, source, perf_counter() - start


@contextmanager
def record_profile(output):
    report = dict(status="running")
    try:
        yield report
    except Exception as error:
        report.update(status="failed", error_type=type(error).__name__, message=redact_text(str(error)))
        raise
    finally:
        write_private_json(output / "profile.json", report)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sembench-checkout", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, required=True, help="existing root with images/; no downloads")
    parser.add_argument("--styles-file", type=Path, required=True)
    parser.add_argument("--mapping-file", type=Path, required=True)
    parser.add_argument("--input-role", choices=("software_fixture", "ecomm_sample"), required=True)
    parser.add_argument("--image-rows", type=int, required=True)
    parser.add_argument("--tile-assets", type=int, required=True)
    parser.add_argument("--max-logical-requests", type=int, required=True)
    parser.add_argument("--model", required=True, help="recorded identity only; no model is loaded")
    parser.add_argument("--max-tokens", type=int, required=True)
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--model-kwargs", default="{}")
    parser.add_argument("--pattern", choices=("all_true", "mixed"), default="all_true")
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output-dir", type=Path, required=True, help="new private run directory")
    args = parser.parse_args(argv)
    if min(args.image_rows, args.tile_assets, args.max_logical_requests, args.max_tokens, args.repeats) < 1 or args.warmup < 0:
        parser.error("sample, tile, request, token and repeat values must be positive; warmup may be zero")
    new_private_directory(args.output_dir)

    # The CPU probe never needs an external connection, even if an installed
    # SDK tries to auto-fetch metadata. A missing local dependency fails here.
    connect = socket.socket.connect

    def local_only(sock, address):
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            raise RuntimeError("network access is forbidden during this CPU-only probe")
        return connect(sock, address)

    with record_profile(args.output_dir) as report, patch.object(socket.socket, "connect", local_only), \
            patch.dict(os.environ, CUDA_VISIBLE_DEVICES="", HF_HUB_OFFLINE="1", TRANSFORMERS_OFFLINE="1",
                       GIT_NO_LAZY_FETCH="1"):
        from pandas.testing import assert_frame_equal, assert_series_equal

        root = verify_checkout(args.sembench_checkout)
        pinned = subprocess.check_output(["git", "-C", str(root), "show", "HEAD:" + QUERY], timeout=15)
        materialized = root / QUERY
        if materialized.exists() and materialized.read_bytes() != pinned:
            raise ValueError("the authored Q11 query is locally modified")
        query = ModuleType("ecomm_q11_cpu_probe")
        exec(compile(pinned, QUERY, "exec"), query.__dict__)
        selected, original_media, source, source_s = read_selected(args)
        report.update(schema="semloom.ecomm_media_cpu.v2", kind="native_cpu_fixed_output_replay",
                      sembench_commit=SEMBENCH_COMMIT,
                      query_sha256=hashlib.sha256(pinned).hexdigest(), source=source, source_read_s=source_s,
                      implementation_sha256={name: file_identity(Path(__file__).with_name(name))["sha256"]
                                             for name in ("ecomm_media.py", "ecomm_media_profile.py")},
                      actual_model_calls=0, gpu_started=False, token_ids="not produced; no VLM processor/model run",
                      native_sdk=source_report("ecomm-q11-cpu", (*FILTER_FILES, "utils.py", "dtype_extensions/image.py", "sem_ops/sem_join.py")),
                      input_role=args.input_role, pattern=args.pattern, tile_assets=args.tile_assets, runs=[])
        # Records of local paths and selected row positions stay private.
        private_selection = {k: dict(indices=v.index.tolist(),
                                     source=str((args.styles_file if k == "styles_details.parquet"
                                                 else args.mapping_file).resolve()))
                             for k, v in selected.items()}
        write_private_json(args.output_dir / "selection.private.json", private_selection)
        for repeat in range(-args.warmup, args.repeats):
            # Native reference supplies the exact per-occurrence fixed outputs.
            # Alternate candidate order; all arrays are new for every case.
            order = CASES if repeat % 2 == 0 else (CASES[0], CASES[2], CASES[1])
            reference = None
            for case in order:
                path = args.output_dir / f"requests_{repeat}_{case}.jsonl"
                measured, audit, branches, result = run_case(query, args.data_dir, selected, args, path,
                    case=case, fixed=None if reference is None else reference[1].responses)
                if reference is None:
                    reference = (measured, audit, branches, result)
                else:
                    reference[1].assert_same_requests(audit)
                    for expected, actual in zip(reference[2], branches, strict=True):
                        assert_frame_equal(expected, actual, check_exact=True)
                        if expected.attrs != actual.attrs:
                            raise ValueError("initial branch dataframe attributes differ")
                    assert_series_equal(reference[3], result, check_exact=True)
                measured.update(repeat=repeat, warmup=repeat < 0, requests_equal=True,
                                branches_equal=True, original_join_result_equal=True)
                measured["source_plus_query_cpu_s"] = source_s + measured["query_cpu_s"]
                report["runs"].append(measured)
        if original_media != {path: file_identity(path) for path in original_media}:
            raise ValueError("source image bytes changed during the CPU probe")
        report["status"] = "passed"
        # Keep the report compact; private JSONL files hold complete messages.
    print(json.dumps(dict(status="passed", runs=len(report["runs"]), actual_model_calls=0)))


if __name__ == "__main__":
    main()
