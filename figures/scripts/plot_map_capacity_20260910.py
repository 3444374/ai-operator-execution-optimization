"""Plot audited query records; keep short diagnostics separate from long repeats."""
import argparse
import csv
from pathlib import Path
import statistics

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output-stem', type=Path, required=True)
    args = parser.parse_args()
    with args.input.open() as stream:
        rows = [r for r in csv.DictReader(stream)
                if r['status'] == 'completed' and r['evaluation_status'] == 'completed']
    colors = {'direct': '#7659a6', 'pg': '#276fa8'}
    labels = {'direct': 'Direct HTTP', 'pg': 'PostgreSQL Map'}
    plt.rcParams.update({'font.size': 10, 'axes.spines.top': False, 'axes.spines.right': False,
                         'svg.fonttype': 'none', 'font.family': 'DejaVu Sans'})
    fig, axes = plt.subplots(2, 2, figsize=(11.5, 7.6))
    for arm in ('direct', 'pg'):
        selected = sorted([r for r in rows if r['role'] == 'coarse' and r['arm'] == arm],
                          key=lambda r: int(r['concurrency']))
        axes[0, 0].plot([int(r['concurrency']) for r in selected],
                        [float(r['valid_rows_per_second']) for r in selected],
                        marker='o', color=colors[arm], label=labels[arm])
        selected = sorted([r for r in rows if r['role'] == 'scale' and r['arm'] == arm],
                          key=lambda r: int(r['rows']))
        axes[0, 1].plot([int(r['rows']) for r in selected],
                        [float(r['query_jct_seconds']) for r in selected],
                        marker='o', color=colors[arm], label=labels[arm])
    axes[0, 0].set(xscale='log', xlabel='C: maximum active HTTP requests', ylabel='Completed rows / s',
                    title='(a) Concurrency screening — 256 rows, one run')
    axes[0, 0].set_xticks([1, 2, 4, 8, 16, 32, 64, 128], labels=['1','2','4','8','16','32','64','128'])
    axes[0, 0].legend(frameon=False)
    axes[0, 1].axhline(60, ls='--', color='#737373', lw=1, label='60 s requirement')
    axes[0, 1].set(xlabel='Distinct rows in one query', ylabel='Query completion time (s)',
                    title='(b) Scale check — C=32, L=128, one run')
    axes[0, 1].legend(frameon=False)
    selected = sorted([r for r in rows if r['role'] == 'window' or
                       (r['unit_id'] == 'scale-pg-n1024')], key=lambda r: int(r['window']))
    axes[1, 0].plot([int(r['window']) for r in selected],
                    [float(r['valid_rows_per_second']) for r in selected], marker='o', color=colors['pg'])
    axes[1, 0].set(xlabel='L: retained-row window', ylabel='Completed rows / s',
                    title='(c) PG window screening — C=32, 1,024 rows')
    axes[1, 0].set_xticks([32, 64, 96, 128])
    groups = [('direct', 64, 'Direct\nC=64'), ('pg', 16, 'PG Map\nC=16'), ('pg', 32, 'PG Map\nC=32')]
    for x, (arm, concurrency, label) in enumerate(groups):
        selected = [r for r in rows if r['role'] == 'repeat' and r['arm'] == arm
                    and int(r['concurrency']) == concurrency]
        if len(selected) != 5 or any(int(r['rows']) != 4000 for r in selected):
            raise ValueError('repeat figure requires all five 4,000-row observations per arm')
        values = [float(r['valid_rows_per_second']) for r in selected]
        offsets = [-.12, -.06, 0, .06, .12]
        axes[1, 1].scatter([x+o for o in offsets], values, s=35, color=colors[arm], alpha=.8)
        axes[1, 1].plot([x-.2, x+.2], [statistics.median(values)]*2, color=colors[arm], lw=2)
    axes[1, 1].set(xticks=range(3), xticklabels=[g[2] for g in groups], ylabel='Completed rows / s',
                    title='(d) 4,000-row repeats — five values and median')
    for ax in axes.flat:
        ax.set_ylim(bottom=0)
        ax.grid(axis='y', alpha=.18)
    fig.suptitle('One RTX 4090: PostgreSQL Map and a direct HTTP reference', fontsize=14, y=.98)
    fig.text(.06, .025,
             'Qwen2.5-7B, BF16, TP1; prefix cache reset before each cell. Warm-up excluded.\n'
             'Repeat PG settings: L=64; input/result budgets 1/64 MiB; PG local window 64 MiB.\n'
             'The first direct control is retained from an interrupted block. Rejected PG runs are excluded from rates.\n'
             'Completed rows may contain wrong answers; quality and all failures are reported separately.',
             fontsize=8.5, color='#444444', va='bottom')
    fig.tight_layout(rect=(0, .14, 1, .95), h_pad=2)
    args.output_stem.parent.mkdir(parents=True, exist_ok=True)
    svg_path = args.output_stem.with_suffix('.svg')
    fig.savefig(svg_path)
    svg_path.write_text('\n'.join(line.rstrip() for line in svg_path.read_text().splitlines()) + '\n')
    fig.savefig(args.output_stem.with_suffix('.png'), dpi=300)
    plt.close(fig)


if __name__ == '__main__':
    main()
