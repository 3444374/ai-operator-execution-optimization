"""Build and validate matched-reference SemFilter calibration artifacts.

The builder consumes offline aggregate observations.  It never connects to
PostgreSQL or a model service.  The resulting strict artifact is small enough
for PostgreSQL to validate and copy into planner-owned path metadata.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import struct
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from fractions import Fraction
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = 1
COST_MODEL_ID = "semloom.exact_filter.reference-calibrated.v1"
PROVIDER_PROFILE = "openai-compatible-fixed"
MODEL_ROLE = "reference"
_ARTIFACT_ID_DOMAIN = b"semloom-semfilter-reference-calibration-v1\0"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_UTC_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
# Engineering ceiling for the infinity-norm condition of the exact Gram matrix
# after column scaling. Independent of held-out error and measurement units.
_MAX_SERVICE_GRAM_CONDITION = 10**16

_SOURCE_FIELDS = frozenset(
    {
        "schema_version",
        "generated_at",
        "semantic_spec_digest",
        "physical_algorithm_digest",
        "provider_execution_profile",
        "model_id",
        "model_role",
        "workload_signature",
        "service_signature",
        "accepted_max_relative_error",
        "training_observations",
        "held_out_observations",
    }
)
_OBSERVATION_FIELDS = frozenset(
    {
        "semantic_input_rows",
        "output_rows",
        "model_calls",
        "prompt_tokens",
        "output_tokens",
        "service_milliseconds",
    }
)
_IDENTITY_FIELDS = (
    "schema_version",
    "cost_model_id",
    "generated_at",
    "semantic_spec_digest",
    "physical_algorithm_digest",
    "provider_execution_profile",
    "model_id",
    "model_role",
    "workload_signature",
    "service_signature",
    "training_sample_count",
    "held_out_sample_count",
    "training_semantic_input_rows",
    "held_out_semantic_input_rows",
    "output_selectivity",
    "model_calls_per_input_row",
    "prompt_tokens_per_call",
    "output_tokens_per_call",
    "service_fixed_milliseconds",
    "service_ms_per_model_call",
    "service_ms_per_prompt_token",
    "service_ms_per_output_token",
    "held_out_mean_relative_error",
    "held_out_max_relative_error",
    "held_out_signed_error_lower",
    "held_out_signed_error_upper",
    "accepted_max_relative_error",
    "evidence_digest",
)
_ARTIFACT_FIELDS = frozenset(("artifact_id", *_IDENTITY_FIELDS))


@dataclass(frozen=True)
class ReferenceCalibrationArtifact:
    """Planner-consumed values from a validated reference artifact."""

    artifact_id: str
    semantic_spec_digest: str
    physical_algorithm_digest: str
    provider_execution_profile: str
    model_id: str
    model_role: str
    workload_signature: str
    service_signature: str
    output_selectivity: float
    model_calls_per_input_row: float
    prompt_tokens_per_call: float
    output_tokens_per_call: float
    service_fixed_milliseconds: float
    service_ms_per_model_call: float
    service_ms_per_prompt_token: float
    service_ms_per_output_token: float
    held_out_max_relative_error: float
    accepted_max_relative_error: float


@dataclass(frozen=True)
class _Observation:
    semantic_input_rows: float
    output_rows: float
    model_calls: float
    prompt_tokens: float
    output_tokens: float
    service_milliseconds: float


def load_json_document(text: str) -> Mapping[str, Any]:
    """Parse one JSON object while rejecting duplicate keys at every depth."""

    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("calibration JSON contains a duplicate field")
            result[key] = value
        return result

    try:
        value = json.loads(
            text,
            object_pairs_hook=reject_duplicates,
            parse_constant=lambda _value: (_ for _ in ()).throw(
                ValueError("calibration JSON contains a non-finite number")
            ),
        )
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise ValueError("invalid calibration JSON") from exc
    if not isinstance(value, Mapping):
        raise ValueError("calibration JSON must contain one object")
    return value


def build_reference_calibration(source: Mapping[str, Any]) -> dict[str, Any]:
    """Build a strict artifact and reject a failed held-out qualification."""

    _require_exact_fields(source, _SOURCE_FIELDS, "calibration source")
    _require_equal(source["schema_version"], SCHEMA_VERSION, "schema_version")
    generated_at = _require_utc_timestamp(source["generated_at"])
    semantic_digest = _require_sha256(
        source["semantic_spec_digest"], "semantic_spec_digest"
    )
    physical_digest = _require_sha256(
        source["physical_algorithm_digest"], "physical_algorithm_digest"
    )
    _require_equal(
        source["provider_execution_profile"],
        PROVIDER_PROFILE,
        "provider_execution_profile",
    )
    model_id = _require_text(source["model_id"], "model_id", 128)
    _require_equal(source["model_role"], MODEL_ROLE, "model_role")
    workload_signature = _require_sha256(
        source["workload_signature"], "workload_signature"
    )
    service_signature = _require_sha256(
        source["service_signature"], "service_signature"
    )
    accepted_error = _require_finite_number(
        source["accepted_max_relative_error"],
        "accepted_max_relative_error",
        minimum=0.0,
        maximum=1.0,
    )
    training = _read_observations(source["training_observations"], "training")
    held_out = _read_observations(source["held_out_observations"], "held_out")

    training_input_rows = sum(row.semantic_input_rows for row in training)
    training_output_rows = sum(row.output_rows for row in training)
    training_calls = sum(row.model_calls for row in training)
    training_prompt_tokens = sum(row.prompt_tokens for row in training)
    training_output_tokens = sum(row.output_tokens for row in training)
    if training_input_rows <= 0 or training_calls <= 0:
        raise ValueError("training observations must contain input rows and model work")

    output_selectivity = training_output_rows / training_input_rows
    calls_per_input = training_calls / training_input_rows
    prompt_per_call = training_prompt_tokens / training_calls
    output_per_call = training_output_tokens / training_calls
    (
        service_fixed_ms,
        service_ms_per_call,
        service_ms_per_prompt,
        service_ms_per_output,
    ) = _fit_service_coefficients(training)

    signed_errors: list[float] = []
    for row in held_out:
        predicted_calls = row.semantic_input_rows * calls_per_input
        predicted_prompt = predicted_calls * prompt_per_call
        predicted_output = predicted_calls * output_per_call
        predicted_service = (
            service_fixed_ms
            + predicted_calls * service_ms_per_call
            + predicted_prompt * service_ms_per_prompt
            + predicted_output * service_ms_per_output
            if predicted_calls > 0
            else 0.0
        )
        predicted = (
            row.semantic_input_rows * output_selectivity,
            predicted_calls,
            predicted_prompt,
            predicted_output,
            predicted_service,
        )
        actual = (
            row.output_rows,
            row.model_calls,
            row.prompt_tokens,
            row.output_tokens,
            row.service_milliseconds,
        )
        signed_errors.extend(
            (estimate - observed) / max(abs(observed), 1.0)
            for estimate, observed in zip(predicted, actual)
        )
    absolute_errors = [abs(value) for value in signed_errors]
    mean_error = sum(absolute_errors) / len(absolute_errors)
    max_error = max(absolute_errors)
    if max_error > accepted_error:
        raise ValueError(
            "held-out maximum relative error exceeds accepted_max_relative_error"
        )

    artifact: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_id": "",
        "cost_model_id": COST_MODEL_ID,
        "generated_at": generated_at,
        "semantic_spec_digest": semantic_digest,
        "physical_algorithm_digest": physical_digest,
        "provider_execution_profile": PROVIDER_PROFILE,
        "model_id": model_id,
        "model_role": MODEL_ROLE,
        "workload_signature": workload_signature,
        "service_signature": service_signature,
        "training_sample_count": len(training),
        "held_out_sample_count": len(held_out),
        "training_semantic_input_rows": _decimal_text(training_input_rows),
        "held_out_semantic_input_rows": _decimal_text(
            sum(row.semantic_input_rows for row in held_out)
        ),
        "output_selectivity": _decimal_text(output_selectivity),
        "model_calls_per_input_row": _decimal_text(calls_per_input),
        "prompt_tokens_per_call": _decimal_text(prompt_per_call),
        "output_tokens_per_call": _decimal_text(output_per_call),
        "service_fixed_milliseconds": _decimal_text(service_fixed_ms),
        "service_ms_per_model_call": _decimal_text(service_ms_per_call),
        "service_ms_per_prompt_token": _decimal_text(service_ms_per_prompt),
        "service_ms_per_output_token": _decimal_text(service_ms_per_output),
        "held_out_mean_relative_error": _decimal_text(mean_error),
        "held_out_max_relative_error": _decimal_text(max_error),
        "held_out_signed_error_lower": _decimal_text(min(signed_errors)),
        "held_out_signed_error_upper": _decimal_text(max(signed_errors)),
        "accepted_max_relative_error": _decimal_text(accepted_error),
        "evidence_digest": hashlib.sha256(
            json.dumps(
                source,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
    }
    artifact["artifact_id"] = _artifact_id(artifact)
    validate_reference_calibration(artifact)
    return artifact


def validate_reference_calibration(
    artifact: Mapping[str, Any],
) -> ReferenceCalibrationArtifact:
    """Validate exact fields, identity, applicability values, and held-out result."""

    _require_exact_fields(artifact, _ARTIFACT_FIELDS, "calibration artifact")
    _require_equal(artifact["schema_version"], SCHEMA_VERSION, "schema_version")
    _require_equal(artifact["cost_model_id"], COST_MODEL_ID, "cost_model_id")
    _require_utc_timestamp(artifact["generated_at"])
    semantic_digest = _require_sha256(
        artifact["semantic_spec_digest"], "semantic_spec_digest"
    )
    physical_digest = _require_sha256(
        artifact["physical_algorithm_digest"], "physical_algorithm_digest"
    )
    _require_equal(
        artifact["provider_execution_profile"],
        PROVIDER_PROFILE,
        "provider_execution_profile",
    )
    model_id = _require_text(artifact["model_id"], "model_id", 128)
    _require_equal(artifact["model_role"], MODEL_ROLE, "model_role")
    workload_signature = _require_sha256(
        artifact["workload_signature"], "workload_signature"
    )
    service_signature = _require_sha256(
        artifact["service_signature"], "service_signature"
    )
    _require_positive_integer(artifact["training_sample_count"], "training_sample_count")
    _require_positive_integer(artifact["held_out_sample_count"], "held_out_sample_count")
    for field_name in ("training_semantic_input_rows", "held_out_semantic_input_rows"):
        _require_decimal_text(artifact[field_name], field_name, minimum=0.0, positive=True)
    output_selectivity = _require_decimal_text(
        artifact["output_selectivity"],
        "output_selectivity",
        minimum=0.0,
        maximum=1.0,
    )
    calls_per_input = _require_decimal_text(
        artifact["model_calls_per_input_row"],
        "model_calls_per_input_row",
        minimum=0.0,
        maximum=1.0,
    )
    prompt_per_call = _require_decimal_text(
        artifact["prompt_tokens_per_call"], "prompt_tokens_per_call", minimum=0.0
    )
    output_per_call = _require_decimal_text(
        artifact["output_tokens_per_call"], "output_tokens_per_call", minimum=0.0
    )
    service_fixed_ms = _require_decimal_text(
        artifact["service_fixed_milliseconds"],
        "service_fixed_milliseconds",
        minimum=0.0,
    )
    service_ms_per_call = _require_decimal_text(
        artifact["service_ms_per_model_call"],
        "service_ms_per_model_call",
        minimum=0.0,
    )
    service_ms_per_prompt = _require_decimal_text(
        artifact["service_ms_per_prompt_token"],
        "service_ms_per_prompt_token",
        minimum=0.0,
    )
    service_ms_per_output = _require_decimal_text(
        artifact["service_ms_per_output_token"],
        "service_ms_per_output_token",
        minimum=0.0,
    )
    if service_ms_per_call + service_ms_per_prompt + service_ms_per_output <= 0:
        raise ValueError("calibration artifact has no variable service cost")
    mean_error = _require_decimal_text(
        artifact["held_out_mean_relative_error"],
        "held_out_mean_relative_error",
        minimum=0.0,
    )
    max_error = _require_decimal_text(
        artifact["held_out_max_relative_error"],
        "held_out_max_relative_error",
        minimum=0.0,
    )
    lower = _require_decimal_text(
        artifact["held_out_signed_error_lower"],
        "held_out_signed_error_lower",
    )
    upper = _require_decimal_text(
        artifact["held_out_signed_error_upper"],
        "held_out_signed_error_upper",
    )
    accepted_error = _require_decimal_text(
        artifact["accepted_max_relative_error"],
        "accepted_max_relative_error",
        minimum=0.0,
        maximum=1.0,
    )
    if mean_error > max_error or lower > upper or max_error > accepted_error:
        raise ValueError("calibration artifact held-out validation is inconsistent")
    evidence_digest = _require_sha256(artifact["evidence_digest"], "evidence_digest")
    artifact_id = _require_sha256(artifact["artifact_id"], "artifact_id")
    if artifact_id != _artifact_id(artifact):
        raise ValueError("calibration artifact identity mismatch")

    return ReferenceCalibrationArtifact(
        artifact_id=artifact_id,
        semantic_spec_digest=semantic_digest,
        physical_algorithm_digest=physical_digest,
        provider_execution_profile=PROVIDER_PROFILE,
        model_id=model_id,
        model_role=MODEL_ROLE,
        workload_signature=workload_signature,
        service_signature=service_signature,
        output_selectivity=output_selectivity,
        model_calls_per_input_row=calls_per_input,
        prompt_tokens_per_call=prompt_per_call,
        output_tokens_per_call=output_per_call,
        service_fixed_milliseconds=service_fixed_ms,
        service_ms_per_model_call=service_ms_per_call,
        service_ms_per_prompt_token=service_ms_per_prompt,
        service_ms_per_output_token=service_ms_per_output,
        held_out_max_relative_error=max_error,
        accepted_max_relative_error=accepted_error,
    )


def _read_observations(value: Any, label: str) -> tuple[_Observation, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{label}_observations must be a non-empty array")
    observations: list[_Observation] = []
    for index, raw in enumerate(value):
        if not isinstance(raw, Mapping):
            raise ValueError(f"{label} observation {index} must be an object")
        _require_exact_fields(raw, _OBSERVATION_FIELDS, f"{label} observation {index}")
        input_rows = _require_finite_number(
            raw["semantic_input_rows"],
            f"{label} semantic_input_rows",
            minimum=0.0,
            positive=True,
        )
        output_rows = _require_finite_number(
            raw["output_rows"], f"{label} output_rows", minimum=0.0
        )
        calls = _require_finite_number(
            raw["model_calls"], f"{label} model_calls", minimum=0.0
        )
        prompt_tokens = _require_finite_number(
            raw["prompt_tokens"], f"{label} prompt_tokens", minimum=0.0
        )
        output_tokens = _require_finite_number(
            raw["output_tokens"], f"{label} output_tokens", minimum=0.0
        )
        service_ms = _require_finite_number(
            raw["service_milliseconds"],
            f"{label} service_milliseconds",
            minimum=0.0,
            positive=True,
        )
        if output_rows > input_rows or calls > input_rows:
            raise ValueError(f"{label} observation rows violate exact SemFilter cardinality")
        if calls == 0 and (prompt_tokens != 0 or output_tokens != 0):
            raise ValueError(f"{label} observation has usage without model calls")
        observations.append(
            _Observation(
                semantic_input_rows=input_rows,
                output_rows=output_rows,
                model_calls=calls,
                prompt_tokens=prompt_tokens,
                output_tokens=output_tokens,
                service_milliseconds=service_ms,
            )
        )
    return tuple(observations)


def _fit_service_coefficients(
    observations: Sequence[_Observation],
) -> tuple[float, float, float, float]:
    """Fit fixed/call/prompt/output milliseconds with a strict 4-column OLS."""

    if len(observations) < 4:
        raise ValueError("training requires at least four service observations")
    _require_identifiable_service_design(observations)
    with localcontext() as context:
        context.prec = 50
        design = [
            (
                Decimal(1),
                Decimal(str(row.model_calls)),
                Decimal(str(row.prompt_tokens)),
                Decimal(str(row.output_tokens)),
            )
            for row in observations
        ]
        targets = [Decimal(str(row.service_milliseconds)) for row in observations]
        normal = [
            [
                sum(row[left] * row[right] for row in design)
                for right in range(4)
            ]
            + [sum(row[left] * target for row, target in zip(design, targets))]
            for left in range(4)
        ]
        for column in range(4):
            pivot = max(range(column, 4), key=lambda row: abs(normal[row][column]))
            if normal[pivot][column] == 0:
                raise ValueError("service observations do not identify all cost coefficients")
            normal[column], normal[pivot] = normal[pivot], normal[column]
            divisor = normal[column][column]
            normal[column] = [value / divisor for value in normal[column]]
            for row in range(4):
                if row == column:
                    continue
                factor = normal[row][column]
                normal[row] = [
                    value - factor * pivot_value
                    for value, pivot_value in zip(normal[row], normal[column])
                ]
        coefficients = [row[4] for row in normal]
    if any(value < 0 for value in coefficients):
        raise ValueError("service calibration produced a negative cost coefficient")
    return tuple(float(value) for value in coefficients)


def _require_identifiable_service_design(observations: Sequence[_Observation]) -> None:
    """Reject exact/near dependence before fitting the normal equations.

    Exact rational arithmetic cannot resurrect a zero pivot. Unlike individual
    pivot thresholds, the full inverse also detects chained near dependencies.
    This is an infinity-norm Gram condition policy, not an SVD rank estimate.
    """
    rows = [[Fraction(1), Fraction(str(row.model_calls)),
             Fraction(str(row.prompt_tokens)), Fraction(str(row.output_tokens))]
            for row in observations]
    scales = [max(abs(row[column]) for row in rows) for column in range(4)]
    if any(scale == 0 for scale in scales):
        raise ValueError("service observations do not identify all cost coefficients")
    rows = [[value / scale for value, scale in zip(row, scales)] for row in rows]
    gram = [[sum(row[left] * row[right] for row in rows) for right in range(4)]
            for left in range(4)]
    gram_norm = max(sum(abs(value) for value in row) for row in gram)
    augmented = [row + [Fraction(int(left == right)) for right in range(4)]
                 for left, row in enumerate(gram)]
    for column in range(4):
        pivot = max(range(column, 4), key=lambda index: abs(augmented[index][column]))
        if augmented[pivot][column] == 0:
            raise ValueError("service observations do not identify all cost coefficients")
        augmented[column], augmented[pivot] = augmented[pivot], augmented[column]
        divisor = augmented[column][column]
        augmented[column] = [value / divisor for value in augmented[column]]
        for index in range(4):
            if index != column:
                factor = augmented[index][column]
                augmented[index] = [value - factor * pivot_value
                                   for value, pivot_value in zip(augmented[index], augmented[column])]
    inverse_norm = max(sum(abs(value) for value in row[4:]) for row in augmented)
    if gram_norm * inverse_norm >= _MAX_SERVICE_GRAM_CONDITION:
        raise ValueError("service observations are nearly collinear")


def _artifact_id(artifact: Mapping[str, Any]) -> str:
    digest = hashlib.sha256()
    digest.update(_ARTIFACT_ID_DOMAIN)
    for field_name in _IDENTITY_FIELDS:
        value = artifact[field_name]
        text = str(value)
        encoded = text.encode("utf-8")
        digest.update(struct.pack("!I", len(encoded)))
        digest.update(encoded)
    return digest.hexdigest()


def _decimal_text(value: float) -> str:
    if not math.isfinite(value):
        raise ValueError("calibration calculation produced a non-finite value")
    if value == 0:
        return "0"
    if value.is_integer():
        return str(int(value))
    return repr(value)


def _require_exact_fields(
    value: Mapping[str, Any], expected: frozenset[str], label: str
) -> None:
    if not isinstance(value, Mapping) or frozenset(value) != expected:
        raise ValueError(f"{label} has missing or unknown fields")


def _require_equal(value: Any, expected: Any, field_name: str) -> None:
    if type(value) is not type(expected) or value != expected:
        raise ValueError(f"invalid {field_name}")


def _require_text(value: Any, field_name: str, max_bytes: int) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError(f"invalid {field_name}")
    if len(value.encode("utf-8")) > max_bytes:
        raise ValueError(f"invalid {field_name}")
    return value


def _require_sha256(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"invalid {field_name}")
    return value


def _require_utc_timestamp(value: Any) -> str:
    if not isinstance(value, str) or _UTC_TIMESTAMP_RE.fullmatch(value) is None:
        raise ValueError("generated_at must be an RFC3339 UTC timestamp")
    parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    if parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ValueError("generated_at must use UTC")
    return value


def _require_positive_integer(value: Any, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"invalid {field_name}")
    return value


def _require_finite_number(
    value: Any,
    field_name: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
    positive: bool = False,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"invalid {field_name}")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValueError(f"invalid {field_name}")
    if minimum is not None and parsed < minimum:
        raise ValueError(f"invalid {field_name}")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"invalid {field_name}")
    if positive and parsed <= 0:
        raise ValueError(f"invalid {field_name}")
    return parsed


def _require_decimal_text(
    value: Any,
    field_name: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
    positive: bool = False,
) -> float:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ValueError(f"invalid {field_name}")
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"invalid {field_name}") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"invalid {field_name}")
    if _decimal_text(parsed) != value:
        raise ValueError(f"{field_name} is not canonical decimal text")
    if minimum is not None and parsed < minimum:
        raise ValueError(f"invalid {field_name}")
    if maximum is not None and parsed > maximum:
        raise ValueError(f"invalid {field_name}")
    if positive and parsed <= 0:
        raise ValueError(f"invalid {field_name}")
    return parsed
