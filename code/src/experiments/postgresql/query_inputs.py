"""Raw database columns and bounded message construction for query comparisons.

Labels are evaluation-only. Tables contain source order, row identity and the
declared input columns; this module never selects answers or sentiment labels.
"""
from dataclasses import dataclass
import re

from src.baselines.text.squad_map import INPUT_TEMPLATE


@dataclass(frozen=True)
class QueryInputs:
    kind: str
    table: str
    max_rows: int
    max_source_bytes: int = 65536
    max_input_bytes: int = 65536
    movie_id: str | None = None

    def __post_init__(self):
        if self.kind not in ('squad', 'movie'):
            raise ValueError('unsupported raw input task')
        if not re.fullmatch(r'[a-z_][a-z0-9_]{0,62}', self.table):
            raise ValueError('input table must be an unqualified SQL identifier')
        if any(type(v) is not int or v <= 0 for v in (
            self.max_rows, self.max_source_bytes, self.max_input_bytes
        )):
            raise ValueError('positive source limits required')
        if self.movie_id is not None and (self.kind != 'movie' or not isinstance(self.movie_id, str)):
            raise ValueError('movie selection requires a movie input')

    @property
    def columns(self):
        return ('context', 'question') if self.kind == 'squad' else ('movie_id', 'review_text','review_id')

    def normalize(self,row):
        # Synthetic callers may use the execution ID as the original review ID.
        if self.kind=='movie' and len(row)==4:
            return (*row,row[1])
        return tuple(row)

    def select_sql(self, *, ordered=True):
        """Return bound SQL; no LIMIT silently discards an unexpected extra row."""
        # Both identifier sets are validated above; predicate values remain bound.
        statement = f'SELECT source_position,row_id,{",".join(self.columns)} FROM ONLY "{self.table}"'
        parameters = ()
        if self.movie_id is not None:
            statement += ' WHERE movie_id=%s'
            parameters = (self.movie_id,)
        if ordered:
            statement += ' ORDER BY source_position'
        return statement, parameters

    def convert(self, row):
        row=self.normalize(row)
        if len(row) != (5 if self.kind=='movie' else 4) or type(row[0]) is not int or row[0] < 0 or not isinstance(row[1], str) or not row[1]:
            raise ValueError('invalid raw source row')
        values = row[1:]
        # Character checks precede UTF-8 allocation; each field and the sum are bounded.
        if any(not isinstance(v, str) or '\x00' in v or len(v) > self.max_source_bytes for v in values):
            raise ValueError('raw source value exceeds its representation contract')
        source_bytes = sum(len(v.encode('utf-8')) for v in values)
        if source_bytes > self.max_source_bytes:
            raise ValueError('raw source row exceeds its byte limit')
        text = INPUT_TEMPLATE.format(context=row[2], question=row[3]) if self.kind == 'squad' else row[3]
        if len(text) > self.max_input_bytes or len(text.encode('utf-8')) > self.max_input_bytes:
            raise ValueError('constructed Map input exceeds its byte limit')
        return dict(source_example_id=row[1], input_text=text, source_position=row[0],
                    source_bytes=source_bytes)
