"""Ordinary Filter values over the bounded author-owned v2 carrier."""

from integrations.imlane_records.filter_workload import Occurrences
from integrations.imlane_records.records_host import RecordsResults


class FilterResults(RecordsResults):
    """Return every terminal to the original Filter consumer before its parser."""

    def __init__(self, rows, client, *, query_id, max_calls, columns, **kwargs):
        super().__init__(rows, client, query_id=query_id,
                         mapping=Occurrences(query_id, max_calls=max_calls, columns=columns),
                         stage_id="filter", block_prefix="filter", **kwargs)
