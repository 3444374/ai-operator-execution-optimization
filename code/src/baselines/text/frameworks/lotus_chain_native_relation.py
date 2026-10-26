"""Explicit complete-Extract boundaries between independently owned native graphs.

Each graph still owns its batches and fused method worker. The driver completes
only the necessary whole relation; it neither schedules model calls nor keeps a
model outside its vendor worker. Native segments have independent model lifetimes.
"""

from pathlib import Path
import shutil
import time

from src.semantic_methods.lotus.record_relation import complete_extract_relation


class NativeExtractRelationRows:
    """Consume each native segment once, crossing only verified Extract EOF."""

    def __init__(self, chain, expected, open_phase, control, deadline, cancelled, *,
                 scheduler_owner, columns=None):
        self.chain, self.expected = chain, tuple(expected)
        self.segments = chain.relation_segments()
        self._open_phase, self.control = open_phase, Path(control)
        self.deadline, self.cancelled = deadline, cancelled
        self.scheduler_owner = scheduler_owner
        self._columns = [] if columns is None else list(columns)
        self._index, self._phases = 0, []
        self._records, self._parents = self.expected, None
        self._current, self._phase_started = None, None
        self._prefix_records, self.phase_history = [], []
        self._done = self.completed = self._phase_build_unconfirmed = False
        self.last_close_report = None

    def __iter__(self):
        return self

    @property
    def calls(self):
        return [call for phase in self._phases for call in phase.calls]

    @property
    def columns(self):
        return list(self._columns if self._current is None else self._current.columns)

    @property
    def batch_stage_stats(self):
        # These remain worker-lifetime snapshots, not additive token totals.
        return [stats for phase in self._phases for stats in phase.batch_stage_stats]

    @property
    def raw_result_batches(self):
        return {(index, *key): rows for index, phase in enumerate(self._phases)
                for key, rows in phase.raw_result_batches.items()}

    @property
    def unassociated_results(self):
        return [row for phase in self._phases for row in phase.unassociated_results]

    def _check(self):
        if self.cancelled is not None and self.cancelled():
            raise InterruptedError("native Extract relation query cancelled")
        if time.monotonic() >= self.deadline:
            raise TimeoutError("native Extract relation query deadline expired")

    def _start_phase(self):
        self._check()
        self._phase_started = time.monotonic()
        self._phase_build_unconfirmed = True
        phase = self._open_phase(self._index, self.segments[self._index], self._records,
            initial_parents=self._parents, columns=self._columns or None,
            allow_nulls=self._index > 0)
        self._phases.append(phase)
        self._current = phase
        self._phase_build_unconfirmed = False

    def _verified_eof(self):
        self._check()
        if not self._current.completed:
            raise RuntimeError("native Extract prefix did not finish its source and cleanup")
        self._current.verify_success(source_exhausted=True)
        self._current.finalize_columns()
        report = self._current.last_close_report
        if report is None or report.get("status") != "closed":
            raise RuntimeError("native Extract prefix cleanup remains unconfirmed")
        self._columns = list(self._current.columns)
        self.phase_history.append(dict(
            phase_stage_ids=[stage.stage_id for stage in self.segments[self._index].stages],
            source_exhausted=True, verified_success=True,
            input_occurrences=len(self._current.expected),
            received_occurrences=len(self._current.received),
            elapsed_s=time.monotonic() - self._phase_started,
            native_close_status=report["status"],
        ))

    def __next__(self):
        if self._done:
            raise StopIteration
        try:
            while True:
                self._check()
                if self._current is None:
                    self._start_phase()
                if self._index + 1 == len(self.segments):
                    try:
                        return next(self._current)
                    except StopIteration:
                        self._verified_eof()
                        self.completed = True
                        report = self.close()
                        if report["status"] != "closed":
                            self.completed = False
                            raise RuntimeError("native relation owner cleanup remains unconfirmed")
                        raise
                try:
                    self._prefix_records.append(next(self._current))
                except StopIteration:
                    self._verified_eof()
                    started = time.monotonic()
                    completed = complete_extract_relation(
                        self._prefix_records, columns=self._columns,
                        extract_stage=self.segments[self._index].stages[-1],
                        query_id=self.chain.query_id, calls=self.calls, source_exhausted=True,
                    )
                    self._check()
                    self.phase_history[-1].update(
                        relation_completion_elapsed_s=time.monotonic() - started,
                        output_occurrences=len(completed.records),
                        barrier="complete_extract_relation",
                    )
                    self._records, self._parents = completed.records, completed.parent_call_ids
                    self._columns = list(completed.columns)
                    self._prefix_records = []
                    self._index += 1
                    self._current = None
        except StopIteration:
            raise
        except BaseException as error:
            try:
                report = self.close()
                if report["status"] != "closed":
                    error.add_note("native Extract owner cleanup remains unconfirmed")
            except BaseException as cleanup:
                error.add_note("native Extract owner cleanup: " + type(cleanup).__name__)
            error.chain_calls = list(self.calls)
            error.chain_stage_stats = list(self.batch_stage_stats)
            error.chain_raw_results = [row for batch in self.raw_result_batches.values() for row in batch]
            raise

    def close(self):
        self._done = True
        if self.last_close_report is not None and self.last_close_report["status"] == "closed":
            return self.last_close_report
        errors, reports, engine_reports = [], [], []
        if self._phase_build_unconfirmed:
            errors.append("native_phase_construction_unconfirmed")
        for index, phase in enumerate(self._phases):
            report = None
            try:
                report = phase.close()
                if not isinstance(report, dict):
                    raise TypeError("native phase close report must be a dictionary")
            except BaseException as error:
                errors.append("phase_" + str(index) + "_close: " + type(error).__name__)
                report = None
                try:
                    retained = phase.last_close_report
                    if isinstance(retained, dict):
                        report = dict(retained, status="unknown", close_error_type=type(error).__name__)
                except BaseException as report_error:
                    errors.append("phase_" + str(index) + "_close_report: " + type(report_error).__name__)
            if report is not None:
                try:
                    reports.append(report)
                    engine_reports.extend(report.get("engine_reports", []))
                    if report.get("status") != "closed":
                        errors.append("phase_" + str(index) + "_cleanup_unconfirmed")
                    errors.extend("phase_" + str(index) + ": " + item
                                  for item in report.get("cleanup_errors", []))
                except BaseException as report_error:
                    errors.append("phase_" + str(index) + "_close_report: " + type(report_error).__name__)
        closed = not errors
        if closed:
            try:
                shutil.rmtree(self.control)
            except OSError as error:
                errors.append("relation_control_cleanup: " + type(error).__name__)
                closed = False
        self.last_close_report = dict(
            status="closed" if closed else "unknown", query_completed=self.completed and closed,
            cleanup_errors=errors, engine_reports=engine_reports,
            gpu_exit_confirmed=(True if all(report.get("gpu_exit_confirmed") is True
                                           for report in reports) and not errors else None),
            physical_gpu_exit_checked=False,
            scheduler_owner=self.scheduler_owner, relation_barriers=True,
            layout="materialized_extract_relation_segments", phase_reports=reports,
            relation_phase_count=len(self.segments), phase_history=list(self.phase_history),
            native_batch_owner="native graph in each segment",
            method_owner="FusedChainBatch/LotusRecordChain.run_batch",
            engine_reuse_across_segments=False,
            model_lifetime_scope="independent native worker per nonempty segment",
            registered_engine_report_count=len(engine_reports),
            model_startup_scope="included in each segment elapsed_s; no separate startup measurement",
            model_evaluations=len(self.calls),
            expected_occurrences=len(self.expected),
            received_occurrences=0 if not self._phases else len(self._phases[0].received),
            application_call_upper_bound=len(self.expected) * len(self.chain.stages),
            model_call_cap_scope="EngineSpec.max_model_calls applies separately to each engine lifetime",
            stage_stats_scope="worker cumulative snapshots; use unique call receipts for additive usage",
            driver_retained_state="finite expected source, complete Extract relation, phase calls/raw and native buffers; no RSS bound",
            cleanup_artifacts=None if closed else str(self.control),
        )
        if any("executor_shutdown_confirmed" in report for report in reports):
            self.last_close_report["executor_shutdown_confirmed"] = all(
                report.get("executor_shutdown_confirmed") is True for report in reports)
        return self.last_close_report
