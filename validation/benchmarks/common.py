import csv
import importlib.util
import statistics
import time
from pathlib import Path


def require_module(module_name: str) -> None:
    if importlib.util.find_spec(module_name) is None:
        raise SystemExit(
            f"Missing dependency: {module_name}. "
            "Install benchmark dependencies with: "
            "python -m pip install -r validation/benchmarks/requirements.txt"
        )


def now_seconds() -> float:
    return time.perf_counter()


def summarize(values):
    values = list(values)
    if not values:
        return {
            "min": 0.0,
            "max": 0.0,
            "mean": 0.0,
            "median": 0.0,
            "stdev": 0.0,
        }
    return {
        "min": min(values),
        "max": max(values),
        "mean": statistics.mean(values),
        "median": statistics.median(values),
        "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
    }


def write_csv(path: str | None, rows: list[dict]) -> None:
    if not path or not rows:
        return
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with output.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def print_table(rows: list[dict]) -> None:
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    widths = {
        name: max(len(name), *(len(format_value(row.get(name))) for row in rows))
        for name in fieldnames
    }
    print(" | ".join(name.ljust(widths[name]) for name in fieldnames))
    print("-+-".join("-" * widths[name] for name in fieldnames))
    for row in rows:
        print(" | ".join(format_value(row.get(name)).ljust(widths[name]) for name in fieldnames))


def format_value(value) -> str:
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)
