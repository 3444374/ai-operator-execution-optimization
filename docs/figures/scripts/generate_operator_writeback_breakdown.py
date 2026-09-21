"""图 4-3：端到端耗时分解——operator 与 writeback 堆叠对比。

从 2026-07-14 GPU-backed AI_EMBED 复测 CSV 中读取原始数据，使用对数横轴处理
fine (20.614s) 与 coalesced (0.550–7.100s) 的量级差异。
每根横条 = operator_wall_s + writeback_s，直观展示 writeback 占比变化。
"""

from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd

OUT_DIR = Path(__file__).resolve().parents[1] / "data" / "report_main"
PNG_PATH = OUT_DIR / "10_e2e_operator_writeback_breakdown.png"
SVG_PATH = OUT_DIR / "10_e2e_operator_writeback_breakdown.svg"
CSV_PATH = (
    Path(__file__).resolve().parents[2]
    / "motivation" / "results" / "gpu" / "ai_embed_pgai_integrated_key_20260714.csv"
)

# ── Font setup ──
ZH_FONT = None
ZH_BOLD = None
ZH_FONT_PATH = None
for fp in [
    r"C:\Windows\Fonts\msyh.ttc",
    r"C:\Windows\Fonts\simhei.ttf",
]:
    if Path(fp).exists():
        ZH_FONT_PATH = fp
        from matplotlib.font_manager import FontProperties, fontManager
        fontManager.addfont(fp)
        ZH_FONT = FontProperties(fname=fp)
        bold_fp = fp.replace(".ttc", "bd.ttc").replace(".ttf", "bd.ttf")
        ZH_BOLD = FontProperties(fname=bold_fp) if Path(bold_fp).exists() else ZH_FONT
        break

if ZH_FONT_PATH:
    fp_props = FontProperties(fname=ZH_FONT_PATH)
    family = fp_props.get_name()
    plt.rcParams["font.family"] = family
else:
    family = "sans-serif"

plt.rcParams.update({
    "font.size": 9,
    "axes.titlesize": 10,
    "axes.labelsize": 9,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    "legend.fontsize": 8,
    "figure.dpi": 150,
    "savefig.dpi": 300,
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.1,
    "axes.unicode_minus": False,
})

# ── Load data from CSV ──
df = pd.read_csv(CSV_PATH)
assert not df.empty, f"CSV empty: {CSV_PATH}"

# Select the 6 configurations from 表 4-2 (in display order)
row_ids = [
    "gpu_key_batch_coalesced_nowrite_1024_20260714",       # 1024  Python coalesced  no wb
    "gpu_key_batch_fine_nowrite_1024_20260714",            # 1024  Python fine        no wb
    "gpu_key_chain_nowrite_4096_20260714",                 # 4096  Python coalesced  no wb
    "gpu_key_chain_json_writeback_4096_20260714",          # 4096  Python coalesced  JSON wb
    "gpu_key_ray_actor_single_endpoint_4096_20260714",     # 4096  Ray actor         JSON wb
    "gpu_key_scale_json_writeback_8192_20260714",          # 8192  Python coalesced  JSON wb
]

rows = []
for rid in row_ids:
    match = df[df["experiment_id"] == rid]
    if match.empty:
        raise SystemExit(f"experiment_id not found in CSV: {rid}")
    rows.append(match.iloc[0])

labels = [
    "1024 行\nPython coalesced",
    "1024 行\nPython fine",
    "4096 行\nPython coalesced",
    "4096 行\nPython + JSON 写回",
    "4096 行\nRay actor + JSON 写回",
    "8192 行\nPython + JSON 写回",
]

operator_vals = np.array([float(r["operator_wall_s"]) for r in rows], dtype=float)
writeback_vals = np.array([float(r["writeback_s"]) for r in rows], dtype=float)
e2e_vals = np.array([float(r["e2e_s"]) for r in rows], dtype=float)
residual_vals = e2e_vals - operator_vals - writeback_vals  # DB fetch + Arrow + fan-in

# ── Verify data integrity ──
for i in range(len(rows)):
    expected = operator_vals[i] + writeback_vals[i] + residual_vals[i]
    if abs(expected - e2e_vals[i]) > 0.001:
        print(f"  WARN: row {i} operator+writeback+residual={expected:.3f} != e2e={e2e_vals[i]:.3f}")
print(f"  Loaded {len(rows)} rows from {CSV_PATH.name}")
print(f"  e2e range: {e2e_vals.min():.3f} – {e2e_vals.max():.3f}s")
print(f"  residual range: {residual_vals.min():.3f} – {residual_vals.max():.3f}s")

# ── Colors ──
OP_COLOR = "#2F6FEB"
WB_COLOR = "#F97316"
RS_COLOR = "#94A3B8"  # residual

# ── Draw ──
fig, ax = plt.subplots(figsize=(7.2, 4.6))
fig.patch.set_facecolor("#F8FAFC")
ax.set_facecolor("#F8FAFC")
fig.subplots_adjust(bottom=0.22)

y_pos = range(len(rows))
bar_height = 0.55

# Stacked: operator → writeback → residual = e2e
left_wb = operator_vals
left_rs = operator_vals + writeback_vals

ax.barh(y_pos, operator_vals, bar_height,
        label="模型推理执行耗时（模型请求 + Ray 调度）", color=OP_COLOR, zorder=3)
ax.barh(y_pos, writeback_vals, bar_height,
        left=left_wb,
        label="writeback 写回耗时", color=WB_COLOR, zorder=3)
ax.barh(y_pos, residual_vals, bar_height,
        left=left_rs,
        label="其余（DB读取 + Arrow + 汇聚）", color=RS_COLOR, zorder=3)

# Value labels inside bars
for i in range(len(rows)):
    op = operator_vals[i]
    wb = writeback_vals[i]
    if op > 0.1:
        ax.text(op / 2, i, f"{op:.1f}s" if op >= 1 else f"{op:.3f}s",
                ha="center", va="center", fontsize=6.5, color="white", fontweight="bold")
    if wb > 0.05:
        ax.text(op + wb / 2, i, f"{wb:.2f}s",
                ha="center", va="center", fontsize=6.5, color="white", fontweight="bold")

# Total on the right
for i in range(len(rows)):
    tot = e2e_vals[i]
    ax.text(tot * 1.08, i, f"{tot:.1f}s" if tot >= 1 else f"{tot:.3f}s",
            ha="left", va="center", fontsize=7.5, color="#1F2937", fontweight="bold")

# ── Axes ──
ax.set_xscale("log")
ax.set_xlim(0.3, 45)
ax.xaxis.set_major_formatter(ticker.ScalarFormatter())
ax.xaxis.set_minor_formatter(ticker.NullFormatter())
ax.set_xticks([0.5, 1, 2, 5, 10, 20, 40])
ax.set_xticklabels(["0.5", "1", "2", "5", "10", "20", "40"])
ax.set_xlabel("端到端耗时 (s, log scale)", fontsize=9, color="#475467")

ax.set_yticks(y_pos)
ax.set_yticklabels(labels, fontsize=7.5)

ax.grid(axis="x", alpha=0.3, color="#94A3B8", linewidth=0.5, zorder=0)
ax.set_axisbelow(True)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.spines["left"].set_visible(False)
ax.tick_params(left=False)

legend = ax.legend(loc="lower right", frameon=True, facecolor="white",
                    edgecolor="#E2E8F0", fontsize=8)
legend.set_zorder(10)

title_text = "端到端耗时分解：operator 阶段 vs writeback 写回"
ax.set_title(title_text, fontsize=11, fontweight="bold", color="#1F2937", pad=12)

# ── Caption (below figure) ──
caption = (
    "图 4-3  端到端耗时按 operator 阶段与 writeback 阶段堆叠分解。"
    "fine 策略下 operator 阶段为 20.6s，约为 coalesced 的 37.5×；"
    "4096 行及以上的 coalesced 配置中 writeback 成为可见成本（1.6–3.2s）。"
    "单 endpoint 下 Ray actor（3.62s）与 Python（3.42s）端到端接近。"
    "数据来源：2026-07-14 GPU-backed AI_EMBED 复测 CSV。对数横轴。"
)
fig.text(0.5, 0.01, caption, ha="center", va="bottom", fontsize=7,
         color="#667085", transform=fig.transFigure, wrap=True)

OUT_DIR.mkdir(parents=True, exist_ok=True)
fig.savefig(PNG_PATH, facecolor=fig.get_facecolor(), edgecolor="none")
fig.savefig(SVG_PATH, facecolor=fig.get_facecolor(), edgecolor="none")
plt.close(fig)

print(f"PNG: {PNG_PATH}")
print(f"SVG: {SVG_PATH}")
