from pathlib import Path
from xml.sax.saxutils import escape

from PIL import Image, ImageDraw, ImageFont


OUT_DIR = Path(__file__).resolve().parents[1] / "architecture"
SVG_PATH = OUT_DIR / "cross_layer_method_framework.svg"
PNG_PATH = OUT_DIR / "cross_layer_method_framework.png"

W, H = 1600, 1000

PAL = {
    "bg":        "#F8FAFC",
    "ink":       "#1F2937",
    "muted":     "#475467",
    "d1_fill":   "#E7F0FF",
    "d1_edge":   "#2F6FEB",
    "d2_fill":   "#FFF1E6",
    "d2_edge":   "#F97316",
    "d3_fill":   "#F2ECFF",
    "d3_edge":   "#7C3AED",
    "box_fill":  "#F0FDF4",
    "box_edge":  "#16A34A",
    "pill_bg":   "#FFFFFF",
    "pill_edge": "#D8DEE8",
    "note":      "#B91C1C",
    "card_bg":   "#FFFFFF",
    "panel_bg":  "#F1F5F9",
    "warn":      "#D6A000",
    "neutral":   "#64748B",
}


def _font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    for p in [
        r"C:\Windows\Fonts\msyhbd.ttc" if bold else r"C:\Windows\Fonts\msyh.ttc",
        r"C:\Windows\Fonts\simhei.ttf",
        r"C:\Windows\Fonts\arial.ttf",
    ]:
        if Path(p).exists():
            return ImageFont.truetype(p, size=size)
    return ImageFont.load_default()


F = {
    "title":   _font(28, True),
    "sub":     _font(16),
    "sec_hd":  _font(20, True),
    "card_hd": _font(18, True),
    "pill":    _font(14),
    "tag":     _font(12),
    "note":    _font(13),
    "cap":     _font(13),
    "badge":   _font(14, True),
    "big":     _font(16, True),
}


def txtsz(draw, text, fnt):
    b = draw.textbbox((0, 0), text, font=fnt)
    return b[2] - b[0], b[3] - b[1]


def ctext(draw, box, text, fnt, fill=PAL["ink"]):
    x1, y1, x2, y2 = box
    tw, th = txtsz(draw, text, fnt)
    draw.text((x1 + (x2 - x1 - tw) / 2, y1 + (y2 - y1 - th) / 2 - 1), text, font=fnt, fill=fill)


def rrect(draw, box, fill, edge, r=14, w=2):
    draw.rounded_rectangle(box, radius=r, fill=fill, outline=edge, width=w)


def pill(draw, x, y, w, h, text, fnt=None):
    fnt = fnt or F["pill"]
    rrect(draw, (x, y, x + w, y + h), PAL["pill_bg"], PAL["pill_edge"], r=6, w=1)
    ctext(draw, (x, y, x + w, y + h), text, fnt, PAL["muted"])


def numbered_badge(draw, cx, cy, r, num, color):
    draw.ellipse((cx - r, cy - r, cx + r, cy + r), fill=color)
    tw, th = txtsz(draw, str(num), F["badge"])
    draw.text((cx - tw / 2, cy - th / 2 - 1), str(num), font=F["badge"], fill="#FFFFFF")


def draw_rc_card(draw, x, y, w, h, num, title, items, edge, tag=None):
    """Standard research content card."""
    rrect(draw, (x, y, x + w, y + h), PAL["card_bg"], edge, r=12)

    # Number badge
    numbered_badge(draw, x + 30, y + 28, 16, num, edge)

    # Title
    draw.text((x + 56, y + 18), title, font=F["card_hd"], fill=edge)

    # Optional tag top-right
    if tag:
        tw, th = txtsz(draw, tag, F["tag"])
        draw.text((x + w - tw - 16, y + 22), tag, font=F["tag"], fill=PAL["muted"])

    # Items as pills
    py = y + 56
    pw, ph, gap = w - 36, 24, 6
    for item in items:
        pill(draw, x + 18, py, pw, ph, item, F["pill"])
        py += ph + gap


# ═══════════════════════════════════════════════════════════
#  PNG
# ═══════════════════════════════════════════════════════════

def draw_png():
    img = Image.new("RGB", (W, H), PAL["bg"])
    d = ImageDraw.Draw(img)

    # ── Title ──
    d.text((64, 32), "研究方案：上游调度策略设计与端到端验证", font=F["title"], fill=PAL["ink"])
    d.text((66, 70),
           "数据组织策略与调度提交控制策略构成上游优化核心，端到端验证包含耦合实验与写回瓶颈判定。",
           font=F["sub"], fill=PAL["muted"])

    # ═══ TOP: Workload Entry ═══
    wl_y = 108
    wl_w, wl_h = 1220, 126
    wl_x = (W - wl_w) / 2
    rrect(d, (wl_x, wl_y, wl_x + wl_w, wl_y + wl_h), "#F8FAFC", PAL["pill_edge"], r=12, w=1)

    # Section label
    d.text((wl_x + 24, wl_y + 12), "三类数据库 AI workload", font=_font(13, True), fill=PAL["muted"])

    # Workload characteristic row
    wl_items = [
        ("离线文本生成", "AI_COMPLETE（主场景）", "token 长度不均、prefix 共享", PAL["d3_edge"]),
        ("批量向量生成", "AI_EMBED（预研验证）", "向量维度、batch 均衡、写回压力", PAL["d1_edge"]),
        ("AI 谓词过滤 / 分类", "AI_FILTER / AI_CLASSIFY（扩展）", "selectivity 未知、调用次数多", PAL["d2_edge"]),
    ]
    n_wl = len(wl_items)
    wl_item_w = 354
    wl_item_h = 68
    wl_gap = 36
    wl_total = n_wl * wl_item_w + (n_wl - 1) * wl_gap
    wl_start = wl_x + (wl_w - wl_total) / 2

    workload_boxes = []
    for i, (title, op, desc, color) in enumerate(wl_items):
        ix = wl_start + i * (wl_item_w + wl_gap)
        iy = wl_y + 38
        workload_boxes.append((int(ix), int(iy), wl_item_w, wl_item_h, color))
        rrect(d, (ix, iy, ix + wl_item_w, iy + wl_item_h), PAL["card_bg"], color, r=10, w=2)
        d.text((ix + 18, iy + 12), title, font=_font(15, True), fill=color)
        op_w, op_h = txtsz(d, op, F["tag"])
        d.text((ix + wl_item_w - op_w - 18, iy + 14), op, font=F["tag"], fill=PAL["muted"])
        d.text((ix + 18, iy + 42), desc, font=F["tag"], fill=PAL["ink"])

    hint = "共同进入同一执行链路，上游数据组织与调度提交控制策略决定端到端性能。"
    hint_w, hint_h = txtsz(d, hint, F["tag"])
    d.text((W / 2 - hint_w / 2, wl_y + wl_h - 18), hint, font=F["tag"], fill=PAL["muted"])

    # Arrow down
    arrow_y = wl_y + wl_h
    d.polygon([(W/2, arrow_y + 12), (W/2 - 8, arrow_y), (W/2 + 8, arrow_y)], fill=PAL["pill_edge"])

    # ═══ MIDDLE: Cross-Layer Coordination ═══
    mid_y = arrow_y + 28
    mid_w, mid_h = 1280, 340
    mid_x = (W - mid_w) / 2
    rrect(d, (mid_x, mid_y, mid_x + mid_w, mid_y + mid_h), PAL["panel_bg"], PAL["pill_edge"], r=14, w=1)

    # Section header
    header_text = "研究方案主线：分阶段剖析 -> 上游策略调优 -> 端到端验证"
    htw, hth = txtsz(d, header_text, _font(14, True))
    header_x = mid_x + 24
    header_y = mid_y + 8
    rrect(d, (header_x, header_y, header_x + htw + 20, header_y + hth + 10),
          PAL["card_bg"], PAL["pill_edge"], r=6, w=1)
    d.text((header_x + 10, header_y + 5), header_text, font=_font(14, True), fill=PAL["ink"])

    # Three upstream strategy layers. Use neutral borders so colors do not imply
    # a one-to-one mapping to the workload cards above.
    rc_w, rc_h = 350, 216
    rc1_x = mid_x + 48
    rc1_y = mid_y + 56
    draw_rc_card(d, rc1_x, rc1_y, rc_w, rc_h, 1,
                 "数据组织策略",
                 ["token-budget 动态批量",
                  "length-aligned 分组",
                  "prefix-aware 分组",
                  "异构 actor pool 按行特征路由"],
                 PAL["neutral"])

    rc2_x = mid_x + (mid_w - rc_w) / 2
    rc2_y = rc1_y
    draw_rc_card(d, rc2_x, rc2_y, rc_w, rc_h, 2,
                 "调度与提交控制策略",
                 ["queue-adaptive flush",
                  "K_max 动态控制",
                  "actor pool 分池路由",
                  "去中心化自适应提交"],
                 PAL["neutral"])

    rc3_x = mid_x + mid_w - rc_w - 48
    rc3_y = rc1_y
    draw_rc_card(d, rc3_x, rc3_y, rc_w, rc_h, 3,
                 "端到端验证：耦合与写回",
                 ["独立最优拼接 vs 联合 grid search",
                  "写回瓶颈判定与 sink 对比",
                  "防止写回吞噬上游收益",
                  "端到端效果评估"],
                 PAL["neutral"])

    # Neutral connectors between the three strategy layers.
    connector_y = rc1_y + rc_h / 2
    for left_x, right_x in [(rc1_x + rc_w, rc2_x), (rc2_x + rc_w, rc3_x)]:
        d.line((left_x + 12, connector_y, right_x - 12, connector_y), fill="#94A3B8", width=2)
        d.polygon([(right_x - 12, connector_y), (right_x - 22, connector_y - 6), (right_x - 22, connector_y + 6)], fill="#94A3B8")

    # ── Whole-chain validation bar below the cards ──
    ke_y = rc1_y + rc_h + 22
    ke_w, ke_h = mid_w - 96, 32
    ke_x = mid_x + 48
    rrect(d, (ke_x, ke_y, ke_x + ke_w, ke_y + ke_h), PAL["card_bg"], PAL["pill_edge"], r=8, w=1)
    ke_text = "端到端验证：耦合实验（独立拼接 vs 联合 grid search）+ 写回瓶颈判定"
    ktw, kth = txtsz(d, ke_text, F["tag"])
    d.text((ke_x + ke_w / 2 - ktw / 2, ke_y + ke_h / 2 - kth / 2 - 1),
              ke_text, font=F["tag"], fill=PAL["ink"])

    # ═══ BOTTOM: writeback bottleneck test + Evaluation ═══
    bot_y = mid_y + mid_h + 20

    # Writeback bottleneck card
    wb_w, wb_h = 580, 156
    wb_x = (W - wb_w) / 2
    draw_rc_card(d, wb_x, bot_y, wb_w, wb_h, 4,
                 "写回约束：瓶颈判定",
                 ["sink 对比（JSON text / pgvector / Lance）",
                  "工程最优写回方案 baseline",
                  "防止写回吞噬上游调度收益"],
                 PAL["neutral"], tag="端到端 guardrail")

    # Evaluation metrics bar
    eval_y = bot_y + wb_h + 24
    eval_w, eval_h = 1100, 44
    eval_x = (W - eval_w) / 2
    rrect(d, (eval_x, eval_y, eval_x + eval_w, eval_y + eval_h), PAL["box_fill"], PAL["box_edge"], r=10, w=1)
    eval_text = "端到端效果指标：耗时  ·  rows/s  ·  tokens/s  ·  queue wait  ·  coupling gap  ·  writeback ratio  ·  GPU utilization"
    etw, eth = txtsz(d, eval_text, F["tag"])
    d.text((eval_x + eval_w / 2 - etw / 2, eval_y + eval_h / 2 - eth / 2 - 1),
              eval_text, font=F["tag"], fill=PAL["box_edge"])

    # ── Caption ──
    cap_y = eval_y + eval_h + 20
    cap_text = (
        "图注：研究方案图。先对三类数据库 AI 算子做分阶段性能剖析，再调优计划层数据组织、运行层提交路由和服务端动态批处理，"
        "结果写回纳入端到端评价，用于判断上游调优收益是否被持久化阶段吞噬。"
    )
    ctw, cth = txtsz(d, cap_text, F["cap"])
    # Wrap to fit
    max_cap_w = W - 144
    if ctw > max_cap_w:
        # Simple manual wrap
        line1 = (
            "图注：研究方案图。以 AI_COMPLETE 为主场景，先做分阶段性能剖析，再通过数据组织策略与调度提交控制策略优化上游执行链路，"
        )
        line2 = (
            "端到端验证包含耦合实验（独立拼接 vs 联合 grid search）与写回瓶颈判定，用于确认上游优化收益是否成立。"
        )
        l1w, l1h = txtsz(d, line1, F["cap"])
        l2w, l2h = txtsz(d, line2, F["cap"])
        d.text(((W - l1w) / 2, cap_y), line1, font=F["cap"], fill=PAL["muted"])
        d.text(((W - l2w) / 2, cap_y + l1h + 4), line2, font=F["cap"], fill=PAL["muted"])
    else:
        d.text(((W - ctw) / 2, cap_y), cap_text, font=F["cap"], fill=PAL["muted"])

    img.save(PNG_PATH)

    return {
        "mid_x": mid_x, "mid_w": mid_w, "mid_y": mid_y, "mid_h": mid_h,
        "rc1_x": rc1_x, "rc1_y": rc1_y, "rc_w": rc_w, "rc_h": rc_h,
        "rc2_x": rc2_x, "rc2_y": rc2_y,
        "rc3_x": rc3_x, "rc3_y": rc3_y,
        "wb_x": wb_x, "wb_y": bot_y, "wb_w": wb_w, "wb_h": wb_h,
        "workload_boxes": workload_boxes,
        "bot_y": bot_y,
    }


# ═══════════════════════════════════════════════════════════
#  SVG
# ═══════════════════════════════════════════════════════════

def svg_text(x, y, text, size=16, weight="400", fill=PAL["ink"], anchor="start"):
    return f'<text x="{x}" y="{y}" font-family="Microsoft YaHei,Arial,sans-serif" font-size="{size}" font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{escape(text)}</text>'


def svg_rect(x, y, w, h, fill, stroke, r=14, sw=2):
    return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill}" stroke="{stroke}" stroke-width="{sw}"/>'


def svg_center(x, y, w, h, text, size=16, weight="400", fill=PAL["ink"]):
    return svg_text(x + w / 2, y + h / 2 + size * 0.35, text, size, weight, fill, "middle")


def svg_pill(x, y, w, h, text, size=14):
    return (svg_rect(x, y, w, h, PAL["pill_bg"], PAL["pill_edge"], r=6, sw=1) + "\n" +
            svg_center(x, y, w, h, text, size, fill=PAL["muted"]))


def draw_svg():
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
        f'<rect width="{W}" height="{H}" fill="{PAL["bg"]}"/>',
        svg_text(64, 58, "研究方案：上游调度策略设计与端到端验证", 28, "700"),
        svg_text(66, 92, "数据组织策略与调度提交控制策略构成上游优化核心，端到端验证包含耦合实验与写回瓶颈判定。", 16, fill=PAL["muted"]),
    ]

    # Workload entry
    wl_y = 108
    wl_w, wl_h = 1220, 126
    wl_x = (W - wl_w) / 2
    parts.append(svg_rect(wl_x, wl_y, wl_w, wl_h, "#F8FAFC", PAL["pill_edge"], r=12, sw=1))
    parts.append(svg_text(wl_x + 24, wl_y + 22, "三类数据库 AI workload", 13, "700", PAL["muted"]))

    wl_items = [
        ("离线文本生成", "AI_COMPLETE（主场景）", "token 长度不均、prefix 共享", PAL["d3_edge"]),
        ("批量向量生成", "AI_EMBED（预研验证）", "向量维度、batch 均衡、写回压力", PAL["d1_edge"]),
        ("AI 谓词过滤 / 分类", "AI_FILTER / AI_CLASSIFY（扩展）", "selectivity 未知、调用次数多", PAL["d2_edge"]),
    ]
    wl_item_w = 354
    wl_item_h = 68
    wl_gap = 36
    wl_total = 3 * wl_item_w + 2 * wl_gap
    wl_start = wl_x + (wl_w - wl_total) / 2
    for i, (title, op, desc, color) in enumerate(wl_items):
        ix = wl_start + i * (wl_item_w + wl_gap)
        iy = wl_y + 38
        parts.append(svg_rect(ix, iy, wl_item_w, wl_item_h, PAL["card_bg"], color, r=10, sw=2))
        parts.append(svg_text(ix + 18, iy + 28, title, 15, "700", color))
        parts.append(svg_text(ix + wl_item_w - 18, iy + 29, op, 12, fill=PAL["muted"], anchor="end"))
        parts.append(svg_text(ix + 18, iy + 52, desc, 12, fill=PAL["ink"]))
    parts.append(svg_text(W / 2, wl_y + wl_h - 10,
                          "共同进入同一执行链路，上游数据组织与调度提交控制策略决定端到端性能。",
                          12, fill=PAL["muted"], anchor="middle"))

    # Arrow
    arrow_y = wl_y + wl_h
    parts.append(f'<polygon points="{W/2},{arrow_y + 12} {W/2 - 8},{arrow_y} {W/2 + 8},{arrow_y}" fill="{PAL["pill_edge"]}"/>')

    # Middle cross-layer
    mid_y = arrow_y + 28
    mid_w, mid_h = 1280, 340
    mid_x = (W - mid_w) / 2
    parts.append(svg_rect(mid_x, mid_y, mid_w, mid_h, PAL["panel_bg"], PAL["pill_edge"], r=14, sw=1))

    # Section header
    parts.append(svg_rect(mid_x + 24, mid_y + 8, 392, 26, PAL["card_bg"], PAL["pill_edge"], r=6, sw=1))
    parts.append(svg_text(mid_x + 34, mid_y + 27, "研究方案主线：分阶段剖析 -> 上游策略调优 -> 端到端验证", 14, "700", PAL["ink"]))

    # Strategy layer cards use the same neutral style. Colors above represent
    # workload types, not direct bindings to these layers.
    rc_w, rc_h = 350, 216
    rc1_x = mid_x + 48
    rc1_y = mid_y + 56
    rc2_x = mid_x + (mid_w - rc_w) / 2
    rc3_x = mid_x + mid_w - rc_w - 48

    for rx, ry, num, title, items, edge in [
        (rc1_x, rc1_y, 1, "数据组织策略",
         ["token-budget 动态批量", "length-aligned 分组",
          "prefix-aware 分组", "异构 actor pool 按行特征路由"], PAL["neutral"]),
        (rc2_x, rc1_y, 2, "调度与提交控制策略",
         ["queue-adaptive flush", "K_max 动态控制",
          "actor pool 分池路由", "去中心化自适应提交"], PAL["neutral"]),
        (rc3_x, rc1_y, 3, "端到端验证：耦合与写回",
         ["独立最优拼接 vs 联合 grid search", "写回瓶颈判定与 sink 对比",
          "防止写回吞噬上游收益", "端到端效果评估"], PAL["neutral"]),
    ]:
        parts.append(svg_rect(rx, ry, rc_w, rc_h, PAL["card_bg"], edge, r=12))
        parts.append(f'<circle cx="{rx + 30}" cy="{ry + 28}" r="16" fill="{edge}"/>')
        parts.append(svg_text(rx + 30, ry + 33, str(num), 14, "700", "#FFFFFF", "middle"))
        parts.append(svg_text(rx + 56, ry + 30, title, 18, "700", edge))
        py = ry + 56
        for item in items:
            parts.append(svg_pill(rx + 18, py, rc_w - 36, 24, item, 12))
            py += 30

    # Neutral connectors between the three strategy layers.
    connector_y = rc1_y + rc_h / 2
    for left_x, right_x in [(rc1_x + rc_w, rc2_x), (rc2_x + rc_w, rc3_x)]:
        parts.append(f'<line x1="{left_x + 12}" y1="{connector_y}" x2="{right_x - 12}" y2="{connector_y}" stroke="#94A3B8" stroke-width="2"/>')
        parts.append(f'<polygon points="{right_x - 12},{connector_y} {right_x - 22},{connector_y - 6} {right_x - 22},{connector_y + 6}" fill="#94A3B8"/>')

    # Whole-chain validation bar
    ke_y = rc1_y + rc_h + 22
    ke_w, ke_h = mid_w - 96, 32
    ke_x = mid_x + 48
    parts.append(svg_rect(ke_x, ke_y, ke_w, ke_h, PAL["card_bg"], PAL["pill_edge"], r=8, sw=1))
    parts.append(svg_center(ke_x, ke_y, ke_w, ke_h,
                            "端到端验证：耦合实验（独立拼接 vs 联合 grid search）+ 写回瓶颈判定",
                            12, fill=PAL["ink"]))

    # Bottom writeback bottleneck card + Eval
    bot_y = mid_y + mid_h + 20
    wb_w, wb_h = 580, 156
    wb_x = (W - wb_w) / 2

    parts.append(svg_rect(wb_x, bot_y, wb_w, wb_h, PAL["card_bg"], PAL["neutral"], r=12))
    parts.append(f'<circle cx="{wb_x + 30}" cy="{bot_y + 28}" r="16" fill="{PAL["neutral"]}"/>')
    parts.append(svg_text(wb_x + 30, bot_y + 33, "4", 14, "700", "#FFFFFF", "middle"))
    parts.append(svg_text(wb_x + 56, bot_y + 30, "写回约束：瓶颈判定", 18, "700", PAL["neutral"]))
    # Tag
    parts.append(svg_text(wb_x + wb_w - 114, bot_y + 28, "端到端 guardrail", 12, fill=PAL["muted"]))
    py = bot_y + 56
    for item in ["sink 对比（JSON text / pgvector / Lance）", "工程最优写回方案 baseline", "防止写回吞噬上游调度收益"]:
        parts.append(svg_pill(wb_x + 18, py, wb_w - 36, 24, item, 12))
        py += 30

    # Evaluation
    eval_y = bot_y + wb_h + 24
    eval_w, eval_h = 1100, 44
    eval_x = (W - eval_w) / 2
    parts.append(svg_rect(eval_x, eval_y, eval_w, eval_h, PAL["box_fill"], PAL["box_edge"], r=10, sw=1))
    parts.append(svg_center(eval_x, eval_y, eval_w, eval_h,
                            "端到端效果指标：耗时 · rows/s · tokens/s · queue wait · coupling gap · writeback ratio · GPU utilization",
                            12, fill=PAL["box_edge"]))

    # Caption (2 lines)
    cap_y = eval_y + eval_h + 20
    l1 = "图注：研究方案图。以 AI_COMPLETE 为主场景，先做分阶段性能剖析，再通过数据组织策略与调度提交控制策略优化上游执行链路。"
    l2 = "端到端验证包含耦合实验（独立拼接 vs 联合 grid search）与写回瓶颈判定，用于确认上游优化收益是否成立。"
    parts.append(svg_text(W / 2, cap_y + 14, l1, 13, fill=PAL["muted"], anchor="middle"))
    parts.append(svg_text(W / 2, cap_y + 34, l2, 13, fill=PAL["muted"], anchor="middle"))

    parts.append("</svg>")
    SVG_PATH.write_text("\n".join(parts), encoding="utf-8")


if __name__ == "__main__":
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    params = draw_png()
    draw_svg()
    print(f"SVG: {SVG_PATH}")
    print(f"PNG: {PNG_PATH}")

    # ── Self-check ──
    img = Image.open(PNG_PATH)
    px = img.load()

    mid_x = int(params["mid_x"])
    mid_w = params["mid_w"]
    mid_y = int(params["mid_y"])
    mid_h = params["mid_h"]
    rc1_x = int(params["rc1_x"])
    rc1_y = int(params["rc1_y"])
    rc_w = params["rc_w"]
    rc_h = params["rc_h"]
    rc2_x = int(params["rc2_x"])
    rc3_x = int(params["rc3_x"])
    rc3_y = int(params["rc3_y"])
    wb_x = int(params["wb_x"])
    wb_y = int(params["wb_y"])
    wb_w = params["wb_w"]
    wb_h = params["wb_h"]
    workload_boxes = params["workload_boxes"]

    print(f"  Layout: mid=({mid_x},{mid_y},{mid_w},{mid_h}) plan=({rc1_x},{rc1_y}) runtime=({rc2_x},{rc1_y}) service=({rc3_x},{rc3_y})")

    # 1. Three strategy cards and writeback guardrail should use neutral borders.
    for name, rx, ry, rw, rh in [
        ("Plan strategy", rc1_x, rc1_y, rc_w, rc_h),
        ("Runtime strategy", rc2_x, rc1_y, rc_w, rc_h),
        ("Service strategy", rc3_x, rc3_y, rc_w, rc_h),
        ("Writeback guardrail", wb_x, wb_y, wb_w, wb_h),
    ]:
        neutral_count = 0
        for y in range(ry, ry + 3):
            for x in range(rx, rx + rw):
                r, g, b = px[x, y]
                if 70 < r < 130 and 90 < g < 150 and 120 < b < 180:
                    neutral_count += 1
        print(f"  {name} neutral edge px: {neutral_count} {'PASS' if neutral_count > 10 else 'FAIL'}")

    # 5. Green in evaluation bar
    eval_y_area = params["bot_y"] + wb_h + 16
    green_count = 0
    for y in range(eval_y_area, eval_y_area + 44):
        for x in range(250, W - 250):
            r, g, b = px[x, y]
            if g > 100 and r < 150 and b < 150:
                green_count += 1
    print(f"  Eval green px: {green_count} {'PASS' if green_count > 10 else 'FAIL'}")

    # 6. Workload cards: colored borders exist and stay inside the workload band
    for idx, (wx, wy, ww, wh, color) in enumerate(workload_boxes, start=1):
        border_count = 0
        for y in range(wy, wy + wh):
            for x in range(wx, wx + ww):
                r, g, b = px[x, y]
                if color == PAL["d1_edge"] and b > 180 and r < 120:
                    border_count += 1
                elif color == PAL["d2_edge"] and r > 180 and 70 < g < 180 and b < 120:
                    border_count += 1
                elif color == PAL["d3_edge"] and r > 90 and b > 180 and g < 130:
                    border_count += 1
        inside = wx > 0 and wy > 0 and wx + ww < W and wy + wh < H
        print(f"  Workload card {idx}: border_px={border_count} bounds={'OK' if inside else 'OUT'} {'PASS' if border_count > 100 and inside else 'FAIL'}")

    # 7. Bounds check: main method cards within middle panel
    for name, rx, rw in [("Plan strategy", rc1_x, rc_w), ("Runtime strategy", rc2_x, rc_w), ("Service strategy", rc3_x, rc_w)]:
        left_ok = rx > mid_x
        right_ok = rx + rw < mid_x + mid_w
        print(f"  {name} bounds: left={'OK' if left_ok else 'OUT'}, right={'OK' if right_ok else 'OUT'} {'PASS' if left_ok and right_ok else 'FAIL'}")

    print("Self-check complete.")
