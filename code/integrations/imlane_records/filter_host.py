"""Ordinary Filter values over the bounded author-owned v2 carrier."""

import json

from integrations.imlane_records.filter_workload import Occurrences, input_record, validate_terminal
from integrations.imlane_records.records_host import RecordsResults


class FilterResults(RecordsResults):
    """Return every terminal to the original Filter consumer before its parser."""

    def __init__(self, rows, client, *, query_id, max_calls, columns, **kwargs):
        super().__init__(rows, client, query_id=query_id,
                         mapping=Occurrences(query_id, max_calls=max_calls, columns=columns),
                         stage_id="filter", block_prefix="filter", **kwargs)

    def close(self):
        primary = None
        try:
            super().close()
        except BaseException as error:
            primary = error
        receipts = dict(self.mapping.receipts)
        raw, unassociated = list(self.mapping.raw_result_batches), list(self.mapping.unassociated_results)
        try:
            model = self.model_close or {}
            uids = {row.get("engine_uid") for row in receipts.values() if row.get("engine_uid")}
            ready = model.get("engine_ready") or {}
            if ready.get("engine_uid"):
                uids.add(ready["engine_uid"])
            for path in sorted(self.control.glob("filter-audit-*.json")):
                if path.is_symlink() or not path.is_file() or path.stat().st_size > 64 * 1024**2:
                    raise ValueError("Filter audit exceeds its private receipt boundary")
                value = json.loads(path.read_text())
                if value["query_id"] != self.query_id or value["engine_uid"] not in uids:
                    raise ValueError("Filter audit belongs to another query or engine")
                raw.append(value["raw_results"])
                unassociated.extend(value["unassociated_results"])
                for row in value["known_receipts"]:
                    source = self.mapping.inputs.get(row.get("call_id"))
                    if source is None:
                        raise ValueError("Filter audit names an undeclared call")
                    validate_terminal(row, input_record(source, columns=self.mapping.columns),
                                      engine_uid=value["engine_uid"])
                    if row["call_id"] in receipts:
                        known = {key: item for key, item in receipts[row["call_id"]].items() if key != "imlane_worker"}
                        if known != row:
                            raise ValueError("Filter audit conflicts with an existing terminal")
                    else:
                        receipts[row["call_id"]] = row
            if model.get("receipt_audit_errors"):
                raise RuntimeError("Filter worker receipt audit remains unconfirmed")
        except BaseException as error:
            if primary is None:
                primary = error
            else:
                primary.add_note("Filter receipt recovery: " + type(error).__name__)
        self.last_close_report = dict(self.last_close_report or {},
            unconsumed_call_results=list(receipts.values()), raw_result_batches=raw, unassociated_results=unassociated)
        if primary is not None:
            self.closed = False
            self.last_close_report.update(status="unknown", receipt_recovery_error_type=type(primary).__name__)
            raise primary
        return self.last_close_report
