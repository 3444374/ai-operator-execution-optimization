"""Original SemBench Movie task semantics, independent of Filter execution owners."""

from dataclasses import dataclass

from src.semantic_methods.lotus.records import CallLedger, FilterMethod


MOVIE_Q3_INSTRUCTION = 'Determine if the following review is clearly positive. Review: {reviewText}'
RETRIEVAL_INSTRUCTION = 'Determine if the following movie review is clearly positive. Review: "{reviewText}".'
RATIO_INSTRUCTION = 'Determine if the following review is clearly positive. Review: {reviewText}.'


@dataclass(frozen=True)
class MovieTask:
    query: int = 3

    def __post_init__(self):
        if type(self.query) is not int or self.query not in (1, 2, 3, 4):
            raise ValueError("supported ordinary Filter Movie tasks are Q1/Q2/Q3/Q4")

    @classmethod
    def from_config(cls, value="movie-q3"):
        if type(value) is not str or value not in ("movie-q1", "movie-q2", "movie-q3", "movie-q4"):
            raise ValueError("task must explicitly select movie-q1, movie-q2, movie-q3 or movie-q4")
        return cls(int(value[-1]))

    @property
    def name(self):
        return "movie-q" + str(self.query)

    @property
    def instruction(self):
        if self.query == 4:
            return RATIO_INSTRUCTION
        return MOVIE_Q3_INSTRUCTION if self.query == 3 else RETRIEVAL_INSTRUCTION

    @property
    def predicate(self):
        return None if self.query == 1 else dict(column="id", operator="eq", value="taken_3")

    def accepts(self, row):
        return self.predicate is None or row["id"] == "taken_3"

    def method(self, model_name):
        return FilterMethod(model_name, instruction=self.instruction)

    def validate_method(self, method):
        if type(method) is not FilterMethod:
            raise ValueError("Movie tasks require the ordinary FilterMethod")
        method.describe()
        if (method.instruction != self.instruction or method.default is not True
                or method.output_tokens != ("True", "False") or method.system_prompt is not None
                or method.resolved_input_columns not in (None, ("reviewText",))):
            raise ValueError("original Movie Q" + str(self.query) + " requires its instruction, columns and parser policy")

    def describe(self):
        return dict(schema="semloom.movie_task.v1", name=self.name, query=self.query,
            operator="ordinary_filter", predicate=self.predicate, instruction=self.instruction,
            output=(dict(operation="ratio", column="positivity_ratio", denominator="eligible_rows") if self.query == 4 else
                    dict(operation="count", column="positive_review_cnt") if self.query == 3 else
                    dict(operation="source_order_head", limit=5, column="reviewId")),
            evaluator="MovieEvaluator._evaluate_q" + str(self.query), scheduling_owner="selected backend")

    def result_frame(self, rows, decisions):
        """Restore occurrence order at the sink; never stop Filter after five matches."""
        import pandas as pd
        expected = [row for row in rows if self.accepts(row)]
        values = {row["row_id"]: row["decision"] for row in decisions}
        if (len(values) != len(decisions) or set(values) != {row["row_id"] for row in expected}
                or any(type(value) is not bool for value in values.values())):
            raise ValueError("task output requires every unique eligible occurrence decision")
        positive = [row for row in expected if values[row["row_id"]]]
        if self.query == 3:
            return pd.DataFrame({"positive_review_cnt": [len(positive)]})
        if self.query == 4:
            return pd.DataFrame({"positivity_ratio": [len(positive) / len(expected) if expected else 0.0]})
        # reviewId is a business column and is not unique: preserve occurrences.
        return pd.DataFrame({"reviewId": [row["reviewId"] for row in positive[:5]]})

    def encode_result(self, frame):
        if self.query == 4:
            if list(frame.columns) != ["positivity_ratio"] or len(frame) != 1:
                raise ValueError("Q4 must return one complete positivity ratio")
            return dict(positivity_ratio=float(frame.iloc[0, 0]))
        if self.query == 3:
            if list(frame.columns) != ["positive_review_cnt"] or len(frame) != 1:
                raise ValueError("Q3 must return one complete COUNT")
            return dict(positive_review_cnt=int(frame.iloc[0, 0]))
        if not len(frame):
            return dict(reviewId=[])
        if list(frame.columns) != ["reviewId"] or len(frame) > 5:
            raise ValueError("Q1/Q2 must return at most five original reviewId occurrences")
        return dict(reviewId=list(frame["reviewId"]))

    def validate_result(self, result, rows, decisions):
        if result != self.encode_result(self.result_frame(rows, decisions)):
            raise ValueError("task result differs from complete decisions and source-order projection")


def validate_query_result(report, expected, *, source=None):
    if report.get("status") != "completed":
        raise ValueError("query did not complete")
    if report.get("inference_calls") == 0 or report.get("execution_kind") == "fixed-response software qualification":
        raise ValueError("CPU/preflight evidence cannot qualify a model execution")
    ledger = CallLedger(expected, max_calls=len(expected))
    for row in report["calls"]["records"]:
        ledger.record(row)
    calls = ledger.report()
    if calls["counts"]["succeeded"] != len(expected) or calls["counts"]["missing"] or calls["usage"]["total_tokens"] is None:
        raise ValueError("query has failed/unknown/missing calls or incomplete usage")
    if any(type(row.get("text")) is not str or row.get("finish_reason") not in ("stop", "length")
            or row.get("total_tokens") != row["prompt_tokens"] + row["completion_tokens"] for row in calls["records"]):
        raise ValueError("successful calls require real terminal text/finish and consistent usage")
    if any(type(row.get("messages_sha256")) is not str or len(row["messages_sha256"]) != 64
            or any(c not in "0123456789abcdef" for c in row["messages_sha256"]) for row in calls["records"]):
        raise ValueError("actual per-call untemplated message identities are required")
    decisions = report["decisions"]
    if (len(decisions) != len(expected) or len({r["row_id"] for r in decisions}) != len(expected)
            or {r["row_id"] for r in decisions} != {r["row_id"] for r in expected}
            or any(type(r["decision"]) is not bool for r in decisions)
            or "original_metric" not in report["quality"]):
        raise ValueError("complete row decisions and original evaluation required")
    if source is None:
        if report["result"].get("positive_review_cnt") != sum(r["decision"] for r in decisions):
            raise ValueError("COUNT differs from complete decisions")
    else:
        source.task.validate_result(report["result"], source.rows(), decisions)
    return dict(decisions=sorted((r["row_id"], r["decision"]) for r in decisions),
        messages=sorted((r["row_id"], r["messages_sha256"]) for r in calls["records"]),
        result=report["result"], original_metric=report["quality"]["original_metric"])
