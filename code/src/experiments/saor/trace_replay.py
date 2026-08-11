"""Replay paired capacity-arm observations as non-causal SAOR evidence."""

from __future__ import annotations

import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping, Sequence

from src.scheduling.core.control import CapacityArm
from src.scheduling.submission_control.saor import (
    SaorAction,
    SaorControlState,
    SaorPolicy,
)


@dataclass(frozen=True)
class PairedCapacityReplayConfig:
    arms: tuple[tuple[str, CapacityArm], ...]
    initial_arm: str
    service_field: str
    risk_proxy_weights: tuple[tuple[str, float], ...]
    v: float
    tail_weight: float
    switch_weight: float
    prediction_lag_samples: int
    calibration_signature: str

    def __post_init__(self) -> None:
        arm_names = tuple(name for name, _ in self.arms)
        if not arm_names or len(arm_names) != len(set(arm_names)):
            raise ValueError("replay arm names must be unique and non-empty")
        if self.initial_arm not in arm_names:
            raise ValueError("initial_arm must name a replay arm")
        if not self.service_field or not self.risk_proxy_weights:
            raise ValueError("service and risk proxy fields are required")
        risk_fields = tuple(name for name, _ in self.risk_proxy_weights)
        if len(risk_fields) != len(set(risk_fields)):
            raise ValueError("risk proxy fields must be unique")
        values = (
            self.v,
            self.tail_weight,
            self.switch_weight,
            *(weight for _, weight in self.risk_proxy_weights),
        )
        if any(not math.isfinite(value) or value < 0 for value in values):
            raise ValueError("replay weights must be finite and non-negative")
        if self.prediction_lag_samples <= 0:
            raise ValueError("prediction_lag_samples must be positive")
        if not self.calibration_signature:
            raise ValueError("calibration_signature must be non-empty")

    @property
    def arms_by_name(self) -> dict[str, CapacityArm]:
        return dict(self.arms)


@dataclass(frozen=True)
class PairedCapacityReplayRow:
    endpoint: str
    phase: str
    previous_arm: str
    selected_arm: str
    oracle_arm: str
    selected_reward: float
    oracle_reward: float
    regret: float
    switched: bool
    prediction_phase: str
    regret_eligible: bool
    action_scores_json: str
    reward_by_arm_json: str
    claim_scope: str = "paired_aggregate_trace_noncausal"

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def load_paired_capacity_replay_config(
    path: Path,
) -> PairedCapacityReplayConfig:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if raw.get("schema_version") != 1:
        raise ValueError("SAOR replay config schema_version must be 1")
    raw_arms = raw.get("arms")
    if not isinstance(raw_arms, dict):
        raise ValueError("replay arms must be an object")
    arms = tuple(
        (
            str(name),
            CapacityArm(
                request_limit=int(value["request_limit"]),
                work_limit=int(value["work_limit"]),
            ),
        )
        for name, value in raw_arms.items()
    )
    risk = raw.get("risk_proxy_weights")
    if not isinstance(risk, dict):
        raise ValueError("risk_proxy_weights must be an object")
    return PairedCapacityReplayConfig(
        arms=arms,
        initial_arm=str(raw["initial_arm"]),
        service_field=str(raw["service_field"]),
        risk_proxy_weights=tuple(
            (str(field), float(weight)) for field, weight in risk.items()
        ),
        v=float(raw["v"]),
        tail_weight=float(raw["tail_weight"]),
        switch_weight=float(raw["switch_weight"]),
        prediction_lag_samples=int(raw["prediction_lag_samples"]),
        calibration_signature=str(raw["calibration_signature"]),
    )


def replay_paired_capacity_trace(
    rows: Sequence[Mapping[str, object]],
    config: PairedCapacityReplayConfig,
) -> tuple[PairedCapacityReplayRow, ...]:
    """Choose one measured arm per endpoint/phase and report oracle regret."""

    grouped: dict[tuple[str, str], dict[str, Mapping[str, object]]] = {}
    for row in rows:
        endpoint = str(row.get("endpoint", ""))
        phase = str(row.get("phase", ""))
        arm_name = str(row.get("arm", ""))
        if not endpoint or not phase or arm_name not in config.arms_by_name:
            raise ValueError("trace row has an invalid endpoint, phase, or arm")
        sample = grouped.setdefault((endpoint, phase), {})
        if arm_name in sample:
            raise ValueError("trace contains a duplicate endpoint/phase/arm row")
        sample[arm_name] = row
    if not grouped:
        raise ValueError("trace must not be empty")

    policy = SaorPolicy(
        v=config.v,
        eta_f=0.0,
        tail_weight=config.tail_weight,
        energy_weight=0.0,
        switch_weight=config.switch_weight,
    )
    current_by_endpoint: dict[str, str] = {}
    history_by_endpoint: dict[
        str,
        list[tuple[str, dict[str, float], dict[str, float]]],
    ] = {}
    results = []
    for endpoint, phase in sorted(grouped, key=_sample_sort_key):
        sample = grouped[(endpoint, phase)]
        missing = set(config.arms_by_name) - set(sample)
        if missing:
            raise ValueError(
                f"paired sample {endpoint}/{phase} is missing arms: {sorted(missing)}"
            )
        current_name = current_by_endpoint.get(endpoint, config.initial_arm)
        service = {
            name: _finite_nonnegative(sample[name], config.service_field)
            for name in config.arms_by_name
        }
        risk = {
            name: sum(
                weight * _finite_nonnegative(sample[name], field)
                for field, weight in config.risk_proxy_weights
            )
            for name in config.arms_by_name
        }
        actual_service_scale = max(1e-12, max(service.values()))
        actual_risk_scale = max(1e-12, max(risk.values()))
        normalized_actual_service = {
            name: value / actual_service_scale for name, value in service.items()
        }
        normalized_actual_risk = {
            name: value / actual_risk_scale for name, value in risk.items()
        }
        history = history_by_endpoint.setdefault(endpoint, [])
        regret_eligible = len(history) >= config.prediction_lag_samples
        prediction_phase = ""
        action_scores: tuple[tuple[str, float], ...] = ()
        if regret_eligible:
            prediction_phase, predicted_service, predicted_risk = history[
                -config.prediction_lag_samples
            ]
            predicted_service_scale = max(
                1e-12,
                max(predicted_service.values()),
            )
            predicted_risk_scale = max(1e-12, max(predicted_risk.values()))
            normalized_predicted_service = {
                name: value / predicted_service_scale
                for name, value in predicted_service.items()
            }
            normalized_predicted_risk = {
                name: value / predicted_risk_scale
                for name, value in predicted_risk.items()
            }
            actions = tuple(
                SaorAction(
                    action_id=name,
                    endpoint_id=endpoint,
                    arm=arm,
                    predicted_goodput_delta=(
                        normalized_predicted_service[name]
                        - normalized_predicted_service[current_name]
                    ),
                    tail_risk_delta=(
                        normalized_predicted_risk[name]
                        - normalized_predicted_risk[current_name]
                    ),
                    switch_cost=float(name != current_name),
                )
                for name, arm in config.arms
            )
            state = SaorControlState(
                jobs=(),
                actions=actions,
                fallback_action=SaorAction(
                    "fallback",
                    endpoint,
                    config.arms_by_name[config.initial_arm],
                ),
                current_arm=config.arms_by_name[current_name],
                observed_at_s=float(len(results)),
                calibration_signature=config.calibration_signature,
            )
            decision = policy.select(
                state,
                now_s=float(len(results)),
                max_age_s=0.0,
                calibration_signature=config.calibration_signature,
            )
            selected_name = decision.action.action_id
            action_scores = decision.action_scores
        else:
            selected_name = current_name
        reward = {
            name: (
                normalized_actual_service[name]
                - config.tail_weight * normalized_actual_risk[name]
                - config.switch_weight * float(name != current_name)
            )
            for name in config.arms_by_name
        }
        oracle_name = min(
            reward,
            key=lambda name: (-reward[name], name),
        )
        regret = max(0.0, reward[oracle_name] - reward[selected_name])
        results.append(
            PairedCapacityReplayRow(
                endpoint=endpoint,
                phase=phase,
                previous_arm=current_name,
                selected_arm=selected_name,
                oracle_arm=oracle_name,
                selected_reward=reward[selected_name],
                oracle_reward=reward[oracle_name],
                regret=regret,
                switched=selected_name != current_name,
                prediction_phase=prediction_phase,
                regret_eligible=regret_eligible,
                action_scores_json=json.dumps(dict(action_scores), sort_keys=True),
                reward_by_arm_json=json.dumps(reward, sort_keys=True),
            )
        )
        current_by_endpoint[endpoint] = selected_name
        history.append((phase, service, risk))
    return tuple(results)


def _finite_nonnegative(row: Mapping[str, object], field: str) -> float:
    try:
        value = float(row[field])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"trace field {field} is missing or invalid") from exc
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"trace field {field} must be finite and non-negative")
    return value


def _sample_sort_key(key: tuple[str, str]) -> tuple[str, int, str]:
    endpoint, phase = key
    try:
        return endpoint, int(phase), ""
    except ValueError:
        return endpoint, 0, phase
