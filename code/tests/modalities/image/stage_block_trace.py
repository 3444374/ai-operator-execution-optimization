"""Opt-in block accounting observations; never change admission or refund work."""
from copy import deepcopy
from dataclasses import asdict
import threading
import time
from unittest.mock import patch

from src.execution_provider.adapters.native_tasks import NativeTaskSession
from src.scheduling.core.session_contract import State, TaskKey


class StageBlockTrace:
    def __init__(self, execution, stage, *, limit=4096):
        self.execution, self.stage, self.limit = execution, stage, limit
        self.preparation = execution.engine.preparation
        self.inputs, self.blocks, self.refusals, self.checkpoints = {}, {}, [], []

    def append(self, target, value):
        if len(target) >= self.limit:
            raise ValueError("stage block trace limit reached")
        target.append(value)

    def attach(self, stack):
        preparation, broker = self.preparation, self.preparation.broker
        original_prepare, original_enqueue = preparation.try_prepare, broker.enqueue_encoded
        original_lease, original_settle = broker.lease_prepare, preparation._settle

        def prepare(tasks):
            for task in tasks:
                key = f"{task.key.session_id}:{task.key.sequence}"
                if key not in self.inputs and len(self.inputs) >= self.limit:
                    raise ValueError("stage block member trace limit reached")
                self.inputs[key] = dict(encoded_bytes=len(task.task.payload), work=task.task.estimated_work)
            return original_prepare(tasks)

        def enqueue(descriptor):
            original_enqueue(descriptor)
            self.blocks[descriptor.block_id] = dict(
                stage=self.stage(), job_id=descriptor.job_id, row_ids=list(descriptor.row_ids),
                encoded_bytes=descriptor.physical_bytes, ready_reservation=descriptor.ready_bytes_estimate,
                work=descriptor.model_work_units,work_unit=descriptor.work.primary.unit,
                calibration_signature=descriptor.work.calibration_signature,
                members={key:dict(self.inputs[key], authority=None) for key in descriptor.row_ids})

        def lease(**options):
            snapshot = broker.snapshot()
            next_id = broker._eligible_encoded_block(options.get("preferred_job_id"))
            result = original_lease(**options)
            if result is None and next_id is not None:
                descriptor = broker._descriptors[next_id]
                reasons = []
                if snapshot.prepare_inflight >= broker.limits.prepare_inflight:
                    reasons.append("prepare_inflight")
                if snapshot.ready_held_bytes + descriptor.ready_bytes_estimate > broker.limits.ready_bytes:
                    reasons.append("ready_bytes")
                if snapshot.ready_held_work + descriptor.model_work_units > broker.limits.ready_work:
                    reasons.append("ready_work")
                self.append(self.refusals, dict(observed_ns=time.monotonic_ns(), stage=self.stage(),
                    block_id=next_id, reasons=reasons, snapshot=asdict(snapshot)))
            return result

        def settle(key, *, failed):
            row = preparation.rows[key]
            block_id, prior_state = row.block_id, row.state
            result = original_settle(key, failed=failed)
            self.blocks[block_id]["members"][f"{key.session_id}:{key.sequence}"]["authority"] = dict(
                confirmed_ns=time.monotonic_ns(), prior_state=prior_state, failed=failed,
                source="MapInputPreparation._settle; unknown remote outcomes do not call this")
            return result

        stack.enter_context(patch.object(preparation, "try_prepare", prepare))
        stack.enter_context(patch.object(broker, "enqueue_encoded", enqueue))
        stack.enter_context(patch.object(broker, "lease_prepare", lease))
        stack.enter_context(patch.object(preparation, "_settle", settle))
        for name in ("advance", "release", "wait"):
            original = getattr(NativeTaskSession, name)
            def observed(flow, *args, _name=name, _original=original, **kwargs):
                if _name == "wait" and flow.execution is self.execution:
                    progress=args[0] if args else kwargs.get("progress")
                    if progress is not None:
                        self.checkpoint("before_wait", getattr(progress,"has_immediate_work",None))
                result = _original(flow, *args, **kwargs)
                if _name != "wait" and flow.execution is self.execution:
                    self.checkpoint("after_"+_name, getattr(result, "has_immediate_work", None))
                return result
            stack.enter_context(patch.object(NativeTaskSession, name, observed))

    def checkpoint(self, label, immediate):
        engine, preparation = self.execution.engine, self.preparation
        if threading.get_ident() != engine._owner or engine._busy:
            raise RuntimeError("block checkpoint requires the inactive Core owner")
        # The owner alone mutates Core records; this lock freezes block stages.
        # Worker refusal snapshots never pretend to be synchronized Core views.
        with preparation.lock:
            global_usage = asdict(engine.capacity.usage())
            scopes = {}
            for session in engine._sessions.values():
                job_id = session.spec.job_id
                scopes[str(session.session_id)] = dict(job_id=job_id,
                    session_usage=asdict(engine.capacity.usage(session.session_id)),
                    job_usage=asdict(engine.capacity.usage(job_id=job_id)),
                    session_limits=asdict(session.limits),
                    job_limits=asdict(engine.capacity.job_limits[job_id]))
            broker, snapshot = preparation.broker, preparation.broker.snapshot()
            tails, returned_work = [], 0
            for block_id, block in preparation.blocks.items():
                state = broker.state_of(block_id)
                meta = self.blocks[block_id]
                confirmed = {key:value for key,value in meta["members"].items() if value["authority"]}
                if state in ("ready", "modeling") and block.members and confirmed:
                    amount = sum(member["work"] for member in confirmed.values())
                    returned_work += amount
                    tails.append(dict(block_id=block_id, state=state, physical_bytes=block.amount,
                        original_work=meta["work"], confirmed_work=amount,
                        remaining_members=[f"{key.session_id}:{key.sequence}" for key in sorted(
                            block.members,key=lambda key:(key.session_id,key.sequence))],
                        members=deepcopy(meta["members"])))
            candidates = []
            for block_id in broker._encoded_queue:
                descriptor = broker._descriptors[block_id]
                keys = [TaskKey(*(int(value) for value in key.split(":"))) for key in descriptor.row_ids]
                records = [engine.capacity.records[key] for key in keys if key in engine.capacity.records]
                legal = []
                for record in records:
                    session = engine._sessions.get(record.key.session_id)
                    if (session is not None and session.state not in (State.FAILED,State.CANCELLED,State.FINISHED)
                            and not engine.error and session._dispatch_enabled and not session._cancel.is_set()
                            and engine.clock() >= session._retry_at and session._capacity_ready()
                            and record.phase == "QUEUED" and
                            engine.capacity.can_dispatch(record,session.limits)):
                        legal.append(asdict(record.key))
                bytes_blocked = snapshot.ready_held_bytes + descriptor.ready_bytes_estimate > broker.limits.ready_bytes
                work_blocked = snapshot.ready_held_work + descriptor.model_work_units > broker.limits.ready_work
                candidates.append(dict(block_id=block_id, job_id=descriptor.job_id,
                    eligible_fifo_head=block_id==broker._eligible_encoded_block(None),
                    row_ids=list(descriptor.row_ids), next_ready_reservation=descriptor.ready_bytes_estimate,
                    next_physical_upper_bound=descriptor.physical_bytes+8, next_work=descriptor.model_work_units,
                    reasons=[name for name,blocked in (("ready_bytes",bytes_blocked),("ready_work",work_blocked),
                        ("prepare_inflight",snapshot.prepare_inflight>=broker.limits.prepare_inflight)) if blocked],
                    legal_model_rows=legal, authority_work_refund_would_fit=(not bytes_blocked and
                        snapshot.ready_held_work-returned_work+descriptor.model_work_units<=broker.limits.ready_work),
                    bytes_remain_fully_held=True))
            self.append(self.checkpoints, dict(observed_ns=time.monotonic_ns(), stage=self.stage(), label=label,
                has_immediate_work=immediate, global_usage=global_usage, global_limits=asdict(engine.capacity.limits),
                core_records=[dict(key=asdict(record.key),job_id=record.spec.job_id,phase=record.phase,
                    compute=record.compute,estimated_work=record.task.estimated_work,
                    payload_bytes=len(record.task.payload),result_reservation=record.task.max_result_bytes,
                    preparation_held=record.preparation_held) for record in engine.capacity.records.values()],
                scopes=scopes, preparation=asdict(snapshot), preparation_worker_running=preparation.running,
                tails=tails, candidates=candidates))

    def summary(self):
        opportunities = [dict(checkpoint=index, candidate=candidate["block_id"])
            for index,event in enumerate(self.checkpoints) for candidate in event["candidates"]
            if event["tails"] and candidate["eligible_fifo_head"] and candidate["legal_model_rows"] and
               candidate["reasons"]==["ready_work"] and candidate["authority_work_refund_would_fit"]]
        idle = [item for item in opportunities if self.checkpoints[item["checkpoint"]]["label"]=="before_wait"
            and self.checkpoints[item["checkpoint"]]["has_immediate_work"] is False]
        return dict(blocks=self.blocks, refusal_events=self.refusals, owner_checkpoints=self.checkpoints,
                    work_only_opportunities=opportunities, idle_before_wait_opportunities=idle,
                    eligibility_scope="global/Job/session capacity and flow flags; not router/credit/organizer decisions",
                    waiting_scope="before_wait carries the preceding progress hint, not an observed wait duration",
                    policy="read-only counterfactual: physical bytes unchanged; no actual refund")
