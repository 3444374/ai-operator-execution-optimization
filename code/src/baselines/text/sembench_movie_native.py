"""Movie Q3 workload and quality checks for direct framework AI APIs.

The query is SemBench's original taken_3 Filter followed by COUNT. The message
and answer contract matches LOTUS 1.2.4 DEFAULT, non-CoT Filter for the selected
Qwen model, without importing its SDK or any execution owner.
"""

import csv
import hashlib
import io
from pathlib import Path


Q3_INSTRUCTION = 'Determine if the following review is clearly positive. Review: {reviewText}'
Q3_PROMPT_PROFILE = 'lotus-1.2.4-default-non-cot-filter'
_SYSTEM = (
    'The user will provide a claim and some relevant context.\n'
    '    Your job is to determine whether the claim is true for the given context.\n'
    '     Use the following format to provide your answer:\n'
    '            Answer: <Your answer here. The answer should be either True or False>\n'
    '            '
)


def q3_messages(review_text):
    return [dict(role='system', content=_SYSTEM), dict(role='user', content=(
        f'Context:\n[Reviewtext]: «{review_text}»\n\n\nClaim: '
        + Q3_INSTRUCTION.format(reviewText='Reviewtext')))]


def _answer(text):
    # Ordinary LOTUS strips the first Answer: prefix before matching tokens.
    _, marker, tail = text.partition('Answer:')
    return (tail if marker else text).strip().lower()


def q3_decision(text):
    answer = _answer(text)
    return True if 'true' in answer else False if 'false' in answer else True


def load_q3_source(path):
    """Keep every source occurrence and original value; never expose gold to AI."""
    data = Path(path).read_bytes()
    labeled = list(csv.DictReader(io.StringIO(data.decode('utf-8'), newline='')))
    required = {'id', 'reviewId', 'reviewText', 'scoreSentiment'}
    if not labeled or not required <= labeled[0].keys():
        raise ValueError('Movie Q3 requires the original labeled Reviews.csv columns')
    rows = [dict(occurrence_id=f'q3-row-{position:08d}', **{
        column: row[column] for column in ('id', 'reviewId', 'reviewText')})
        for position, row in enumerate(labeled)]
    eligible = [row for row in rows if row['id'] == 'taken_3']
    return dict(rows=rows, labeled=labeled, source=dict(
        sha256=hashlib.sha256(data).hexdigest(), bytes=len(data), rows=len(rows),
        unique_review_ids=len({row['reviewId'] for row in rows}),
        eligible_rows=len(eligible), logical_model_requests=len(eligible),
        eligible_empty_reviews=sum(row['reviewText'] == '' for row in eligible),
        prompt_profile=Q3_PROMPT_PROFILE, instruction=Q3_INSTRUCTION,
        predicate=dict(column='id', operator='eq', value='taken_3'),
        result_column='positive_review_cnt'))


def evaluate_q3(source, predictions, *, positive_review_cnt, checkout):
    """Evaluate saved native predictions once, including count-cancellation errors."""
    import pandas as pd
    from .sembench_movie import classification_audit, evaluate_original

    expected = {row['occurrence_id']: row for row in source['rows'] if row['id'] == 'taken_3'}
    actual = {row['occurrence_id']: row for row in predictions}
    if len(actual) != len(predictions) or actual.keys() != expected.keys():
        raise ValueError('native Q3 predictions must cover each eligible occurrence exactly once')
    for identity, prediction in actual.items():
        original = expected[identity]
        if (any(prediction[name] != original[name] for name in ('id', 'reviewId'))
                or type(prediction['generated_text']) is not str):
            raise ValueError('native Q3 changed source association or returned invalid text')
    decisions = {identity: q3_decision(row['generated_text']) for identity, row in actual.items()}
    if type(positive_review_cnt) is not int or positive_review_cnt != sum(decisions.values()):
        raise ValueError('native Q3 COUNT differs from the same materialized predictions')
    labels = {row['occurrence_id']: source['labeled'][position]['scoreSentiment']
              for position, row in enumerate(source['rows']) if row['id'] == 'taken_3'}
    audit = classification_audit(labels, ((identity, 'POSITIVE' if value else 'NEGATIVE')
                                         for identity, value in decisions.items()))
    audit['unparsed_default_true'] = sum(
        not any(token in _answer(row['generated_text']) for token in ('true', 'false'))
        for row in predictions)
    result = pd.DataFrame({'positive_review_cnt': [positive_review_cnt]})
    return dict(result=result.to_dict(orient='split'), classification=audit,
        official=evaluate_original(checkout, 3, result, pd.DataFrame(source['labeled'])))
