"""Original SemBench Movie task semantics, independent of Filter execution owners."""

from dataclasses import dataclass

from src.semantic_methods.lotus.records import FilterMethod


MOVIE_Q3_INSTRUCTION = 'Determine if the following review is clearly positive. Review: {reviewText}'
RETRIEVAL_INSTRUCTION = 'Determine if the following movie review is clearly positive. Review: "{reviewText}".'


@dataclass(frozen=True)
class MovieTask:
    query: int = 3

    def __post_init__(self):
        if type(self.query) is not int or self.query not in (1, 2, 3):
            raise ValueError("supported Movie tasks are Q1/Q2/Q3 ordinary Filter only")

    @classmethod
    def from_config(cls, value="movie-q3"):
        if type(value) is not str or value not in ("movie-q1", "movie-q2", "movie-q3"):
            raise ValueError("task must explicitly select movie-q1, movie-q2 or movie-q3")
        return cls(int(value[-1]))

    @property
    def name(self):
        return "movie-q" + str(self.query)

    @property
    def instruction(self):
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
            output=(dict(operation="count", column="positive_review_cnt") if self.query == 3 else
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
        # reviewId is a business column and is not unique: preserve occurrences.
        return pd.DataFrame({"reviewId": [row["reviewId"] for row in positive[:5]]})

    def encode_result(self, frame):
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
