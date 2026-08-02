"""Fail closed when source modules cross the documented architecture boundaries."""

from __future__ import annotations

import ast
from pathlib import Path


CODE_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = CODE_ROOT / "src"

REMOVED_COMPATIBILITY_MODULES = {
    "profile_cli.py",
    "profile_config.py",
    "profile_ray.py",
    "profile_replay.py",
    "profile_schema.py",
    "profile_traces.py",
}

MOVED_TOP_LEVEL_MODULES = {
    "calibration.py",
    "cost_estimation.py",
    "experiment_scenarios.py",
    "metrics.py",
    "model_backends.py",
    "organizers.py",
    "packing.py",
    "request_costs.py",
    "runner_lease.py",
    "runtime_env.py",
    "shared_vllm_experiment.py",
    "sinks.py",
    "sources.py",
    "vllm_probe.py",
    "workloads.py",
}

REMOVED_SCHEDULING_SHIMS = {
    "adaptive_admission.py",
    "admission.py",
    "observations.py",
    "pid_admission.py",
    "ray_adapter.py",
    "ray_runtime.py",
    "routing.py",
    "shared_credit.py",
    "shared_credit_ray.py",
    "token_budget.py",
    "ucb_admission.py",
}

SCHEDULING_FORBIDDEN_IMPORT_PREFIXES = (
    "daft",
    "psycopg",
    "pyarrow",
    "src.baselines",
    "src.image",
    "src.modalities",
    "src.data",
)

PLANNING_FORBIDDEN_IMPORT_PREFIXES = (
    "daft",
    "psycopg",
    "ray",
    "src.baselines",
    "src.data",
    "src.modalities",
)

MODALITY_FORBIDDEN_IMPORT_PREFIXES = ("src.scheduling",)

BASELINE_FORBIDDEN_IMPORT_PREFIXES = ("src.scheduling",)


def _python_files(root: Path) -> list[Path]:
    return sorted(path for path in root.rglob("*.py") if "__pycache__" not in path.parts)


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


def _assert_no_forbidden_imports(
    root: Path,
    forbidden_prefixes: tuple[str, ...],
) -> None:
    if not root.exists():
        return
    violations: list[str] = []
    for path in _python_files(root):
        for imported in sorted(_imports(path)):
            if any(
                imported == prefix or imported.startswith(f"{prefix}.")
                for prefix in forbidden_prefixes
            ):
                violations.append(
                    f"{path.relative_to(CODE_ROOT)} imports forbidden dependency {imported}"
                )
    assert not violations, "\n".join(violations)


def test_removed_compatibility_modules_do_not_return() -> None:
    assert not (
        {path.name for path in SRC_ROOT.glob("profile_*.py")}
        & REMOVED_COMPATIBILITY_MODULES
    )
    assert not ({path.name for path in SRC_ROOT.glob("*.py")} & MOVED_TOP_LEVEL_MODULES)
    scheduling_names = {
        path.name for path in (SRC_ROOT / "scheduling").glob("*.py")
    }
    assert not (scheduling_names & REMOVED_SCHEDULING_SHIMS)


def test_scheduling_core_is_engine_and_modality_independent() -> None:
    _assert_no_forbidden_imports(
        SRC_ROOT / "scheduling",
        SCHEDULING_FORBIDDEN_IMPORT_PREFIXES,
    )


def test_planning_layer_does_not_own_execution_engines() -> None:
    _assert_no_forbidden_imports(
        SRC_ROOT / "planning",
        PLANNING_FORBIDDEN_IMPORT_PREFIXES,
    )


def test_modality_adapters_do_not_implement_scheduling() -> None:
    _assert_no_forbidden_imports(
        SRC_ROOT / "modalities",
        MODALITY_FORBIDDEN_IMPORT_PREFIXES,
    )


def test_native_baselines_do_not_import_project_scheduling() -> None:
    _assert_no_forbidden_imports(
        SRC_ROOT / "baselines",
        BASELINE_FORBIDDEN_IMPORT_PREFIXES,
    )
