"""scipilot-figure-skill 共享 helper(VLDB/SIGMOD/ICDE 规范)。

按 scipilot-figure-skill 的五条硬性原则实现:
1. 按最终尺寸出图(figsize 直接设 inch)
2. 矢量优先(PDF + SVG + PNG)
3. 配色对色盲友好(Okabe-Ito + 冗余编码)
4. 字号在最终尺寸下可读(7-9pt)
5. 误差必有交代(图注写 SD/SEM + n)

供 5 个 v2 绘图脚本共用。无 SciencePlots 硬依赖。
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Patch


# ---------------------------------------------------------------------------
# Okabe-Ito 色盲安全配色(项目语义对齐)
# ---------------------------------------------------------------------------
COLOR_SEQ = "#0072B2"        # sequential baseline  蓝
COLOR_BFD = "#E69F00"        # classic BFD          橙
COLOR_ROWCAP = "#CC79A7"     # row-cap-first        紫
COLOR_PREFIX = "#009E73"     # prefix-aware         绿
COLOR_K8 = "#009E73"         # static K=8           绿(前台保护)
# flush 策略三档
COLOR_FIXED_25 = "#999999"     # 短窗口 中性灰
COLOR_K16 = "#999999"          # static K=16 中性灰(== COLOR_FIXED_25 别名)
COLOR_FIXED_50 = "#0072B2"     # 长窗口 蓝
COLOR_ADAPTIVE = "#E69F00"     # adaptive 橙

# 联合消融四候选
COLOR_BASELINE = "#999999"     # baseline 灰
COLOR_INDEPENDENT = "#0072B2"  # independent 蓝
COLOR_JOINT = "#E69F00"        # joint 橙
COLOR_MECHANISM = "#009E73"    # mechanism 绿

# 代价模型系数方向
COLOR_POS = "#009E73"          # 正系数 绿
COLOR_NEG = "#DC2626"          # 负系数 红
COLOR_COUNTER = "#E69F00"      # 反直觉 橙

# v1 兼容别名(供 v1 脚本继续工作)
COLOR_SEQ_V1 = "#2F6FEB"
COLOR_BFD_V1 = "#F97316"
COLOR_ROWCAP_V1 = "#7C3AED"
COLOR_PREFIX_V1 = "#16A34A"
COLOR_AIMD = "#E69F00"       # AIMD                 橙
COLOR_EWMA = "#CC79A7"       # EWMA-AIMD            紫
COLOR_PID = "#D55E00"        # PID                  红橙
COLOR_BASELINE = "#999999"   # mean baseline        灰
COLOR_RIDGE = "#0072B2"      # ridge                蓝
COLOR_RUNNING = "#0072B2"    # vLLM running         蓝
COLOR_WAITING = "#D55E00"    # vLLM waiting         红橙
COLOR_KMAX = "#000000"       # AIMD K_max           黑
COLOR_INFLIGHT = "#F0E442"   # inflight             黄

# 冗余编码:不同 scenario 不同 marker(色盲友好 + 灰度可分)
MARKERS = ["o", "s", "^", "D", "v", "P", "X", "*"]


def setup_style():
    """配置 VLDB/SIGMOD/ICDE 期刊规范 + 中文字体回退。

    VLDB/SIGMOD 双栏:每栏 3.5 inch,跨栏 7.16 inch。字号 7-9pt。
    """
    # 中文字体回退链
    plt.rcParams["font.sans-serif"] = [
        "Noto Sans CJK SC", "Source Han Sans SC",
        "Microsoft YaHei", "SimHei", "Arial", "DejaVu Sans",
    ]
    plt.rcParams["axes.unicode_minus"] = False  # 修负号方框

    # 字号(VLDB/SIGMOD 标准)
    plt.rcParams["font.size"] = 8.0                # 默认
    plt.rcParams["axes.titlesize"] = 9             # 子图标题
    plt.rcParams["axes.labelsize"] = 8             # 轴标签
    plt.rcParams["xtick.labelsize"] = 7            # 刻度
    plt.rcParams["ytick.labelsize"] = 7
    plt.rcParams["legend.fontsize"] = 7.5          # 图例
    plt.rcParams["figure.titlesize"] = 10          # 总标题

    # 网格 / 轴
    plt.rcParams["axes.grid"] = True
    plt.rcParams["grid.color"] = "#E5E7EB"
    plt.rcParams["grid.linewidth"] = 0.5
    plt.rcParams["grid.alpha"] = 0.7
    plt.rcParams["axes.axisbelow"] = True
    plt.rcParams["axes.edgecolor"] = "#374151"
    plt.rcParams["axes.labelcolor"] = "#111827"
    plt.rcParams["xtick.color"] = "#374151"
    plt.rcParams["ytick.color"] = "#374151"
    plt.rcParams["axes.linewidth"] = 0.6

    # 输出
    plt.rcParams["savefig.dpi"] = 300
    plt.rcParams["savefig.bbox"] = "tight"


# ---------------------------------------------------------------------------
# stripplot 叠加(在柱图上显示每个 formal repeat 的点)
# ---------------------------------------------------------------------------


def overlay_stripplot(ax, x_positions, values_per_x, color="#000000",
                       marker="o", jitter=0.06, size=14, alpha=0.85,
                       edgecolor="white", linewidth=0.5):
    """在已有柱图上叠加 stripplot 显示每个 repeat 的实际值。

    参数:
        x_positions: 柱图的 x 坐标(list)
        values_per_x: 每个柱对应的 repeat 值列表(list of list,内层每个元素是一个 repeat)
        color/marker: 散点颜色与形状
        jitter: 横向抖动幅度(避免点完全重叠)
        size: 散点大小
    """
    rng = np.random.default_rng(20260727)
    for xi, vals in zip(x_positions, values_per_x):
        if vals is None or len(vals) == 0:
            continue
        # 把 None / NaN 过滤
        clean = [v for v in vals if v is not None and not (isinstance(v, float) and np.isnan(v))]
        if not clean:
            continue
        offsets = rng.uniform(-jitter, jitter, size=len(clean))
        ax.scatter([xi + o for o in offsets], clean,
                   color=color, marker=marker, s=size, alpha=alpha,
                   edgecolor=edgecolor, linewidth=linewidth, zorder=5)


# ---------------------------------------------------------------------------
# 面板标签(a) (b) (c) — 自动对齐
# ---------------------------------------------------------------------------


def add_panel_labels(fig, style="parens", x_offset=-0.08, y_offset=1.02,
                      fontsize=9, fontweight="bold"):
    """给 figure 的每个 axes 加 (a) (b) (c) 标签。

    style:
        'parens'  → (a) (b) (c)(IEEE/ACM/VLDB 默认)
        'plain'   → a b c(Nature)
    """
    labels = "abcdefghijklmnopqrstuvwxyz"
    for i, ax in enumerate(fig.axes):
        label = labels[i]
        if style == "parens":
            text = f"({label})"
        else:
            text = label
        ax.text(x_offset, y_offset, text,
                transform=ax.transAxes, fontsize=fontsize,
                fontweight=fontweight, va="bottom", ha="left")


# ---------------------------------------------------------------------------
# 图注脚(统一格式)
# ---------------------------------------------------------------------------


def notes_caption(fig, text, fontsize=6.5, y=0.02):
    """在 figure 底部加统一格式的图注脚。"""
    fig.text(0.5, y, text, ha="center",
             fontsize=fontsize, color="#6B7280", style="italic", wrap=True)


# ---------------------------------------------------------------------------
# 导出(矢量优先)
# ---------------------------------------------------------------------------


def export_figure(fig, basename, output_dir, formats=None,
                   grayscale_preview=True):
    """导出多格式:默认 PDF + SVG + PNG 300 DPI。

    grayscale_preview=True 时额外生成 _gray.png 供色盲检查。
    """
    if formats is None:
        formats = ["pdf", "svg", "png"]
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    paths = []
    for fmt in formats:
        p = out / f"{basename}.{fmt}"
        if fmt == "png":
            fig.savefig(p, dpi=300, bbox_inches="tight", facecolor="white")
        else:
            fig.savefig(p, bbox_inches="tight", facecolor="white")
        paths.append(p)

    if grayscale_preview:
        gray_path = out / f"{basename}_gray.png"
        # 灰度版:复制 figure,把所有元素转灰度(简化实现 — 依赖 PIL 后处理)
        try:
            from PIL import Image
            png_path = out / f"{basename}.png"
            img = Image.open(png_path).convert("L")
            img.save(gray_path, dpi=(300, 300))
            paths.append(gray_path)
        except ImportError:
            pass  # Pillow 不可用就跳过灰度预览

    return paths


# ---------------------------------------------------------------------------
# 数据加载 helpers(summary_long / runs.csv)
# ---------------------------------------------------------------------------


def load_summary_long(csv_path):
    """读 summary_long.csv → (mean_wide, std_wide),index=scenario_id。"""
    import pandas as pd
    df = pd.read_csv(csv_path)
    mean = df.pivot_table(index="scenario_id", columns="metric",
                          values="mean", aggfunc="first")
    std = df.pivot_table(index="scenario_id", columns="metric",
                         values="sample_std", aggfunc="first")
    return mean, std


def load_runs_formal_with_values(csv_path):
    """读 runs.csv → 返回 dict {scenario_id: {metric: [repeat_values]}}。

    保留每个 formal repeat 的原始值(供 stripplot 用),不只返回 mean/std。
    """
    import pandas as pd
    df = pd.read_csv(csv_path)
    if "phase" in df.columns:
        df = df[df.phase == "formal"].copy()
    result = {}
    for scenario, sub in df.groupby("scenario_id"):
        result[scenario] = {}
        for col in sub.select_dtypes(include="number").columns:
            result[scenario][col] = sub[col].tolist()
    return result


# ---------------------------------------------------------------------------
# 项目图例元素 helper
# ---------------------------------------------------------------------------


def make_patch_legend(colors, labels, hatch=None):
    """构造 Patch 图例元素列表。"""
    return [Patch(facecolor=c, edgecolor="#111827",
                  hatch=(hatch[i] if hatch else None),
                  label=l)
            for i, (c, l) in enumerate(zip(colors, labels))]
