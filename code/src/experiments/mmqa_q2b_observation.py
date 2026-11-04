"""Passive process-local observation of the declared Q2b strategy profile."""

from collections import Counter
from contextlib import contextmanager
import importlib
import json
from pathlib import Path
import threading
import time

from src.baselines.common.private_artifacts import content_digest
from src.baselines.image.sembench_mmqa_compat import resolve_join_strategy, PROFILE_ID


class Q2bObservation:
    def __init__(self, dataset, lm, *, record, max_prepared_bytes):
        self.dataset, self.lm, self.record = dataset, lm, record
        self.max_prepared_bytes = max_prepared_bytes
        self.value = dataset.read()
        self.table = dataset.table_frame(self.value)
        self.image_paths = [str(Path(self.value["data_path"]) / "images" / r["name"]) for r in self.value["images"]]
        self.pairs = [(image, row) for image in range(len(self.image_paths)) for row in range(len(self.table))]
        self.stages, self.matched = [], None
        self.active = None
        self.expected, self.actual, self.members = Counter(), Counter(), {}
        self.lock = threading.RLock()

    def emit(self, event, **fields):
        self.record(dict(event=event, monotonic_ns=time.monotonic_ns(),
                         stage_id=self.active, **fields))

    def begin(self, stage, members):
        if self.active is not None or [s["stage_id"] for s in self.stages] != ([] if stage == "join" else ["join"]):
            raise ValueError("Q2b operator order differs")
        self.active = stage
        self.stages.append(dict(stage_id=stage, started_monotonic_ns=time.monotonic_ns(),
                                input_members=len(members), model_calls=0, prompt_batches=0))
        self.stage_members = members
        self.emit("operator_started", members=members)

    def finish(self, output_rows):
        stage = self.stages[-1]
        expected = Counter({k: v for k, v in self.expected.items() if k[0] == self.active})
        actual = Counter({k: v for k, v in self.actual.items() if k[0] == self.active})
        if expected != actual or sum(actual.values()) != len(self.stage_members) or stage["prompt_batches"] != 1:
            raise ValueError("Q2b model calls do not cover original stage members exactly once")
        stage.update(terminal_monotonic_ns=time.monotonic_ns(), output_rows=output_rows,
                     model_calls=sum(actual.values()))
        self.emit("operator_terminal", output_rows=output_rows)
        self.active = None

    def reserve_request(self, value):
        """Called before proxy I/O; identical prompts retain an explicit member set."""
        with self.lock:
            key = self.active, content_digest(value.get("messages"))
            if self.active is None or self.actual[key] >= self.expected[key]:
                raise ValueError("Q2b outgoing request is not an original prepared stage member")
            self.actual[key] += 1
            return dict(stage_id=self.active, messages_sha256=key[1],
                equivalent_members=self.members[key],
                identity_scope="all occurrences with this exact message; no inferred HTTP order")

    @contextmanager
    def observe(self):
        join_module = importlib.import_module("lotus.sem_ops.sem_join")
        extract_module = importlib.import_module("lotus.sem_ops.sem_extract")
        image_module = importlib.import_module("lotus.dtype_extensions.image")
        from lotus.models import LM
        Join, Extract = join_module.SemJoinDataframe, extract_module.SemExtractDataFrame
        original_join, original_extract, original_lm = Join.__call__, Extract.__call__, LM.__call__
        original_low_join, original_fetch = join_module.sem_join, image_module.fetch_image
        expected_strategy = resolve_join_strategy("zs-cot")
        if any(getattr(fn, "_mmqa_observed", False) for fn in (original_join, original_extract, original_lm)):
            raise RuntimeError("nested Q2b observation requires an isolated process")

        def join(accessor, other, instruction, *args, **kwargs):
            if (accessor._obj["image_filepath"].tolist() != self.image_paths or not other.equals(self.table)
                    or instruction != "{image} shows the logo of horse racetrack {Track}"
                    or args or kwargs != dict(strategy=expected_strategy)):
                raise ValueError("Q2b exact original Join inputs or policy differ")
            self.begin("join", self.pairs)
            self.stages[-1].update(strategy="ReasoningStrategy.ZS_COT", execution_profile=PROFILE_ID)
            self.emit("join_strategy_accepted", strategy="ReasoningStrategy.ZS_COT", execution_profile=PROFILE_ID)
            result = original_join(accessor, other, instruction, *args, **kwargs)
            if self.matched is None:
                raise ValueError("original exact Join result observation is missing")
            actual = list(zip(result["image_filepath"].tolist(), result["ID"].tolist()))
            expected = [(self.image_paths[i], self.table.iloc[j]["ID"]) for i, j in self.matched]
            if actual != expected:
                raise ValueError("Q2b materialized Join differs from native predicate decisions")
            self.finish(len(result))
            self.emit("join_relation_materialized", members=self.matched,
                      scope="logical native relation; deep bytes, RSS and physical release measured separately")
            return result

        def low_join(*args, **kwargs):
            output = original_low_join(*args, **kwargs)
            if (self.active != "join" or self.matched is not None or len(output.filter_outputs) != len(self.pairs)):
                raise ValueError("Q2b native predicate coverage differs")
            self.matched = [pair for pair, keep in zip(self.pairs, output.filter_outputs) if keep]
            if [(i, j) for i, j, _ in output.join_results] != self.matched:
                raise ValueError("native Join pair order differs")
            self.emit("join_predicates_complete", decisions=output.filter_outputs, matched=self.matched)
            return output

        def extract(accessor, *args, **kwargs):
            if self.matched is None:
                raise ValueError("Extract appeared before original Join completed")
            relation = accessor._obj
            actual = list(zip(relation["image_filepath"].tolist(), relation["ID"].tolist()))
            expected = [(self.image_paths[i], self.table.iloc[j]["ID"]) for i, j in self.matched]
            if (actual != expected or args != (["image"], {"color": "The color of the logo in the image"})
                    or kwargs != dict(extract_quotes=False, return_raw_outputs=False)):
                raise ValueError("Extract input is not the unchanged original Join result")
            self.begin("extract", self.matched)
            result = original_extract(accessor, *args, **kwargs)
            if len(result) != len(self.matched):
                raise ValueError("Q2b Extract changed matched occurrence count")
            self.finish(len(result))
            self.emit("extract_consumer_returned", rows=len(result),
                      scope="accessor returned; no claim that pandas or allocator released physical memory")
            return result

        def lm_call(lm, messages, *args, **kwargs):
            if lm is not self.lm or self.active is None or len(messages) != len(self.stage_members):
                raise ValueError("Q2b LM batch does not match its original stage relation")
            stage = self.stages[-1]
            if stage["prompt_batches"]:
                raise ValueError("Q2b stage unexpectedly invoked the LM twice")
            prepared_bytes = 0
            with self.lock:
                for member, prompt in zip(self.stage_members, messages):
                    prepared_bytes += len(json.dumps(prompt, ensure_ascii=False).encode())
                    if prepared_bytes > self.max_prepared_bytes:
                        raise ValueError("Q2b prepared message bytes exceed the declared maximum")
                    key = self.active, content_digest(prompt)
                    self.expected[key] += 1
                    self.members.setdefault(key, []).append(member)
                stage.update(prompt_batches=1, prepared_message_bytes=prepared_bytes,
                             prompts_ready_monotonic_ns=time.monotonic_ns())
            self.emit("stage_prompts_ready", rows=len(messages), prepared_message_bytes=prepared_bytes,
                      response_format=kwargs.get("response_format"))
            return original_lm(lm, messages, *args, **kwargs)

        def fetch(value, kind):
            started = time.monotonic_ns()
            result = original_fetch(value, kind)
            self.emit("native_media_prepared", source_name=Path(str(value)).name, kind=kind,
                started_monotonic_ns=started, representation_bytes=len(result) if isinstance(result, str) else None,
                scope="unchanged native fetch wall time; decode/encode included, CPU core and HBM unavailable")
            return result

        for fn in (join, extract, lm_call):
            fn._mmqa_observed = True
        Join.__call__, Extract.__call__, LM.__call__ = join, extract, lm_call
        join_module.sem_join, image_module.fetch_image = low_join, fetch
        try:
            yield self
        finally:
            Join.__call__, Extract.__call__, LM.__call__ = original_join, original_extract, original_lm
            join_module.sem_join, image_module.fetch_image = original_low_join, original_fetch
