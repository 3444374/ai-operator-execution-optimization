"""Official-style SQuAD v1.1 Exact Match and token-F1 metrics.

The functions in this module are execution-engine agnostic.  DuckDB, direct
HTTP, and project runners must pass their materialized predictions through the
same evaluator so quality differences are not introduced by comparator-specific
post-processing.

Normalization follows the SQuAD v1.1 evaluation contract: lowercase, remove
ASCII punctuation, remove English articles, then collapse whitespace.  Scores
are maximized independently over all accepted reference answers.
"""

from __future__ import annotations

import re
import string
from collections import Counter
from collections.abc import Mapping, Sequence


_ARTICLES = re.compile(r"\b(a|an|the)\b", flags=re.UNICODE)


def normalize_squad_answer(text: str) -> str:
    """Return the canonical SQuAD v1.1 normalized answer string."""

    lowered = text.lower()
    without_punctuation = "".join(
        character
        for character in lowered
        if character not in string.punctuation
    )
    without_articles = _ARTICLES.sub(" ", without_punctuation)
    return " ".join(without_articles.split())


def squad_exact_match_score(prediction: str, reference: str) -> float:
    """Score one prediction/reference pair as exact match (0.0 or 1.0)."""

    return float(
        normalize_squad_answer(prediction)
        == normalize_squad_answer(reference)
    )


def squad_token_f1_score(prediction: str, reference: str) -> float:
    """Compute official-style token overlap F1 for one answer pair."""

    prediction_tokens = normalize_squad_answer(prediction).split()
    reference_tokens = normalize_squad_answer(reference).split()
    if not prediction_tokens or not reference_tokens:
        return float(prediction_tokens == reference_tokens)

    common = Counter(prediction_tokens) & Counter(reference_tokens)
    shared_tokens = sum(common.values())
    if shared_tokens == 0:
        return 0.0
    precision = shared_tokens / len(prediction_tokens)
    recall = shared_tokens / len(reference_tokens)
    return 2.0 * precision * recall / (precision + recall)


def squad_example_scores(
    prediction: str,
    reference_answers: Sequence[str],
) -> tuple[float, float]:
    """Return max Exact Match and max token-F1 over accepted references."""

    if not reference_answers:
        raise ValueError("reference_answers must contain at least one answer")
    exact_match = max(
        squad_exact_match_score(prediction, reference)
        for reference in reference_answers
    )
    token_f1 = max(
        squad_token_f1_score(prediction, reference)
        for reference in reference_answers
    )
    return exact_match, token_f1


def squad_quality_metrics(
    predictions: Mapping[str, str | None],
    references: Mapping[str, Sequence[str]],
) -> dict[str, float | int | str]:
    """Aggregate SQuAD quality over the reference manifest.

    Missing or ``None`` predictions receive zero EM/F1 and are counted.  An
    observed empty string remains a real prediction and is scored normally.
    Extra prediction IDs are rejected because they indicate a manifest join
    error rather than a quality outcome.

    Percent fields use the official 0--100 scale and include failed/missing
    rows in the denominator.  ``squad_exact_match_rows`` is the numerator for
    later ``correct rows/s`` calculation; this module intentionally does not
    mix quality with a timing boundary.
    """

    if not references:
        raise ValueError("references must contain at least one example")
    extra_prediction_ids = set(predictions) - set(references)
    if extra_prediction_ids:
        sample = sorted(extra_prediction_ids)[:3]
        raise ValueError(f"predictions contain unknown example IDs: {sample}")

    exact_match_total = 0.0
    token_f1_total = 0.0
    observed_predictions = 0
    missing_predictions = 0
    for example_id, reference_answers in references.items():
        if not reference_answers:
            raise ValueError(
                f"reference example {example_id!r} has no accepted answers"
            )
        prediction = predictions.get(example_id)
        if prediction is None:
            missing_predictions += 1
            prediction = ""
        else:
            observed_predictions += 1
        exact_match, token_f1 = squad_example_scores(
            prediction,
            reference_answers,
        )
        exact_match_total += exact_match
        token_f1_total += token_f1

    evaluated_rows = len(references)
    return {
        "squad_quality_status": (
            "ok"
            if missing_predictions == 0
            else "partial:missing_predictions"
        ),
        "squad_evaluated_rows": evaluated_rows,
        "squad_prediction_rows": observed_predictions,
        "squad_missing_prediction_rows": missing_predictions,
        "squad_exact_match_rows": int(exact_match_total),
        "squad_exact_match_percent": 100.0 * exact_match_total / evaluated_rows,
        "squad_token_f1_percent": 100.0 * token_f1_total / evaluated_rows,
    }
