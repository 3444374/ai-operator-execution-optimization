#!/usr/bin/env python3
"""Generate simple SVG learning figures from the GPU chain breakdown CSV."""

from __future__ import annotations

import argparse
import csv
import html
import statistics
from pathlib import Path


def read_formal_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as f:
        return [row for row in csv.DictReader(f) if row["phase"] == "formal"]


def group(rows: list[dict[str, str]], experiment_id: str) -> list[dict[str, str]]:
    return [row for row in rows if row["experiment_id"] == experiment_id]


def mean(rows: list[dict[str, str]], key: str) -> float:
    return statistics.mean(float(row[key]) for row in rows)


def bar_svg(path: Path, title: str, labels: list[str], series: list[tuple[str, list[float]]], ylabel: str) -> None:
    width, height = 760, 430
    margin_left, margin_top, margin_right, margin_bottom = 86, 56, 24, 70
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    colors = ["#2563eb", "#16a34a", "#dc2626", "#7c3aed"]
    ymax = max(value for _, values in series for value in values) * 1.18
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img">',
        f"<title>{html.escape(title)}</title>",
        '<rect width="100%" height="100%" fill="white"/>',
        f'<text x="{width / 2}" y="28" text-anchor="middle" font-family="Arial" font-size="18" font-weight="700" fill="#111827">{html.escape(title)}</text>',
    ]
    for i in range(5):
        y = margin_top + plot_height - i * plot_height / 4
        value = ymax * i / 4
        parts.append(
            f'<line x1="{margin_left}" y1="{y:.1f}" x2="{margin_left + plot_width}" y2="{y:.1f}" stroke="#e5e7eb"/>'
        )
        parts.append(
            f'<text x="{margin_left - 10}" y="{y + 4:.1f}" text-anchor="end" font-family="Arial" font-size="12" fill="#374151">{value:.1f}</text>'
        )
    parts.append(
        f'<line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{margin_top + plot_height}" stroke="#374151"/>'
    )
    parts.append(
        f'<line x1="{margin_left}" y1="{margin_top + plot_height}" x2="{margin_left + plot_width}" y2="{margin_top + plot_height}" stroke="#374151"/>'
    )
    parts.append(
        f'<text transform="translate(22 {margin_top + plot_height / 2}) rotate(-90)" text-anchor="middle" font-family="Arial" font-size="13" fill="#111827">{html.escape(ylabel)}</text>'
    )

    group_width = plot_width / len(labels)
    bar_width = min(64, group_width / (len(series) + 1.2))
    for label_index, label in enumerate(labels):
        center_x = margin_left + group_width * (label_index + 0.5)
        parts.append(
            f'<text x="{center_x:.1f}" y="{margin_top + plot_height + 34}" text-anchor="middle" font-family="Arial" font-size="13" fill="#111827">{html.escape(label)}</text>'
        )
        for series_index, (_, values) in enumerate(series):
            value = values[label_index]
            bar_height = value / ymax * plot_height
            x = center_x - (len(series) * bar_width) / 2 + series_index * bar_width
            y = margin_top + plot_height - bar_height
            parts.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width * 0.78:.1f}" height="{bar_height:.1f}" fill="{colors[series_index]}"/>'
            )
            parts.append(
                f'<text x="{x + bar_width * 0.39:.1f}" y="{y - 5:.1f}" text-anchor="middle" font-family="Arial" font-size="11" fill="#111827">{value:.2f}</text>'
            )

    legend_x = margin_left
    for series_index, (name, _) in enumerate(series):
        parts.append(f'<rect x="{legend_x}" y="42" width="12" height="12" fill="{colors[series_index]}"/>')
        parts.append(
            f'<text x="{legend_x + 18}" y="53" font-family="Arial" font-size="12" fill="#111827">{html.escape(name)}</text>'
        )
        legend_x += 130
    parts.append("</svg>")
    path.write_text("\n".join(parts), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="motivation/results/gpu/ai_embed_chain_breakdown_20260712.csv")
    parser.add_argument("--output-dir", default="figures/learning")
    args = parser.parse_args()

    rows = read_formal_rows(Path(args.input))
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    coalesced = group(rows, "gpu_chain_clean_1024_actor_coalesced")
    fine = group(rows, "gpu_chain_clean_1024_actor_fine")
    bar_svg(
        output_dir / "gpu_embed_1024_granularity_e2e_20260712.svg",
        "1024 rows: e2e time by invocation granularity",
        ["coalesced", "fine"],
        [("e2e_s", [mean(coalesced, "e2e_s"), mean(fine, "e2e_s")])],
        "seconds",
    )

    python_rows = group(rows, "gpu_chain_4096_python_coalesced")
    ray_task = group(rows, "gpu_chain_4096_ray_task_coalesced")
    ray_actor = group(rows, "gpu_chain_4096_ray_actor_coalesced")
    bar_svg(
        output_dir / "gpu_embed_4096_executor_e2e_20260712.svg",
        "4096 rows coalesced: e2e time by executor",
        ["python", "ray_task", "ray_actor"],
        [("e2e_s", [mean(python_rows, "e2e_s"), mean(ray_task, "e2e_s"), mean(ray_actor, "e2e_s")])],
        "seconds",
    )

    large = group(rows, "gpu_chain_16384_ray_actor_coalesced")
    bar_svg(
        output_dir / "gpu_embed_16384_stage_breakdown_20260712.svg",
        "16384 rows ray_actor coalesced: stage time",
        ["AI operator", "writeback", "db_fetch", "arrow"],
        [
            (
                "seconds",
                [
                    mean(large, "operator_wall_s"),
                    mean(large, "writeback_s"),
                    mean(large, "db_fetch_s"),
                    mean(large, "arrow_build_s"),
                ],
            )
        ],
        "seconds",
    )


if __name__ == "__main__":
    main()
