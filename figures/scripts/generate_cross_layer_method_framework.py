from pathlib import Path
from xml.sax.saxutils import escape

from PIL import Image, ImageDraw, ImageFont


OUT_DIR = Path(__file__).resolve().parents[1] / "architecture"
SVG_PATH = OUT_DIR / "cross_layer_method_framework.svg"
PNG_PATH = OUT_DIR / "cross_layer_method_framework.png"

W, H = 1600, 920

PAL = {
    "bg":        "#F8FAFC",
    "ink":       "#1F2937",
    "muted":     "#667085",
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
    d.text((64, 32), "跨层协同优化方法框架", font=F["title"], fill=PAL["ink"])
    d.text((66, 70),
           "本课题的核心方法不是 RC1 或 RC2 孤立优化，而是二者之间的跨层协同决策——联合最优 vs 独立最优组合。",
           font=F["sub"], fill=PAL["muted"])

    # ═══ TOP: Workload Entry ═══
    wl_y = 108
    wl_w, wl_h = 1200, 100
    wl_x = (W - wl_w) / 2
    rrect(d, (wl_x, wl_y, wl_x + wl_w, wl_y + wl_h), "#F8FAFC", PAL["pill_edge"], r=12, w=1)

    # Section label
    d.text((wl_x + 24, wl_y + 12), "Workload 入口", font=_font(13, True), fill=PAL["muted"])

    # Workload characteristic row
    wl_items = [
        ("AI_EMBED", "向量维度、batch 均衡、写回压力", PAL["d1_edge"]),
        ("AI_FILTER / AI_CLASSIFY", "selectivity 未知、调用次数多", PAL["d2_edge"]),
        ("AI_COMPLETE", "token 长度不均、prefix 共享", PAL["d3_edge"]),
    ]
    n_wl = len(wl_items)
    wl_item_w = 340
    wl_gap = 36
    wl_total = n_wl * wl_item_w + (n_wl - 1) * wl_gap
    wl_start = wl_x + (wl_w - wl_total) / 2

    for i, (label, desc, color) in enumerate(wl_items):
        ix = wl_start + i * (wl_item_w + wl_gap)
        iy = wl_y + 34
        # Small colored label pill
        ltw, lth = txtsz(d, label, _font(13, True))
        rrect(d, (ix + 8, iy, ix + 8 + ltw + 20, iy + lth + 10), PAL["card_bg"], color, r=6, w=2)
        d.text((ix + 18, iy + 5), label, font=_font(13, True), fill=color)
        # Description
        d.text((ix + 8, iy + 36), desc, font=F["tag"], fill=PAL["muted"])

    # Arrow down
    arrow_y = wl_y + wl_h
    d.polygon([(W/2, arrow_y + 12), (W/2 - 8, arrow_y), (W/2 + 8, arrow_y)], fill=PAL["pill_edge"])

    # ═══ MIDDLE: Cross-Layer Coordination ═══
    mid_y = arrow_y + 28
    mid_w, mid_h = 1280, 340
    mid_x = (W - mid_w) / 2
    rrect(d, (mid_x, mid_y, mid_x + mid_w, mid_y + mid_h), PAL["panel_bg"], PAL["pill_edge"], r=14, w=1)

    # Section header
    header_text = "跨层协同优化（核心方法贡献）"
    htw, hth = txtsz(d, header_text, _font(14, True))
    header_x = mid_x + 24
    header_y = mid_y + 8
    rrect(d, (header_x, header_y, header_x + htw + 20, header_y + hth + 10),
          PAL["card_bg"], PAL["pill_edge"], r=6, w=1)
    d.text((header_x + 10, header_y + 5), header_text, font=_font(14, True), fill=PAL["ink"])

    # RC1 card (left)
    rc_w, rc_h = 440, 216
    rc1_x = mid_x + 48
    rc1_y = mid_y + 50
    draw_rc_card(d, rc1_x, rc1_y, rc_w, rc_h, 1,
                 "数据组织与批处理构造",
                 ["batch size / partition 数联合决策",
                  "operator invocation 粒度控制",
                  "object 合并与 fan-in 形态选择",
                  "输出特征向量维度感知"],
                 PAL["d1_edge"])

    # RC2 card (right)
    rc2_x = mid_x + mid_w - rc_w - 48
    rc2_y = mid_y + 50
    draw_rc_card(d, rc2_x, rc2_y, rc_w, rc_h, 2,
                 "GPU 服务感知调度与反压",
                 ["endpoint 数量与 replica 状态路由",
                  "actor pool 大小与 bounded in-flight",
                  "queue wait 监控与 backlog 控制",
                  "token 长度 / prefix 感知分发"],
                 PAL["d2_edge"])

    # ── Joint optimization indicator between RC1 and RC2 ──
    jo_center_x = mid_x + mid_w / 2
    jo_center_y = rc1_y + rc_h / 2

    # Horizontal double-headed connection line between cards
    lx1 = rc1_x + rc_w
    lx2 = rc2_x
    ly = jo_center_y
    d.line((lx1, ly, lx2, ly), fill=PAL["pill_edge"], width=2)

    # Small diamond at center
    ds = 8
    d.polygon([
        (jo_center_x, jo_center_y - ds),
        (jo_center_x + ds, jo_center_y),
        (jo_center_x, jo_center_y + ds),
        (jo_center_x - ds, jo_center_y),
    ], fill=PAL["warn"], outline=PAL["warn"])

    # Labels above and below the connection line
    jo_label1 = "batch / partition"
    jo_label2 = "in-flight / routing"
    jo_label3 = "联合搜索 ≠ 独立最优组合"
    l1w, l1h = txtsz(d, jo_label1, F["tag"])
    l2w, l2h = txtsz(d, jo_label2, F["tag"])
    l3w, l3h = txtsz(d, jo_label3, _font(13, True))
    d.text((jo_center_x - l1w / 2, ly - 30), jo_label1, font=F["tag"], fill=PAL["d1_edge"])
    d.text((jo_center_x - l2w / 2, ly - 14), jo_label2, font=F["tag"], fill=PAL["d2_edge"])
    d.text((jo_center_x - l3w / 2, ly + 14), jo_label3, font=_font(13, True), fill=PAL["ink"])

    # ── Killer experiment bar below the cards ──
    ke_y = rc1_y + rc_h + 22
    ke_w, ke_h = mid_w - 96, 32
    ke_x = mid_x + 48
    rrect(d, (ke_x, ke_y, ke_x + ke_w, ke_y + ke_h), PAL["card_bg"], PAL["pill_edge"], r=8, w=1)
    ke_text = "验证：BL3（独立最优组合）vs BL6（跨层联合最优）——若 BL6 < BL3，则跨层协同效应成立"
    ktw, kth = txtsz(d, ke_text, F["tag"])
    d.text((ke_x + ke_w / 2 - ktw / 2, ke_y + ke_h / 2 - kth / 2 - 1),
              ke_text, font=F["tag"], fill=PAL["ink"])

    # ═══ BOTTOM: RC3 + Evaluation ═══
    bot_y = mid_y + mid_h + 20

    # RC3 card
    rc3_w, rc3_h = 580, 110
    rc3_x = (W - rc3_w) / 2
    draw_rc_card(d, rc3_x, bot_y, rc3_w, rc3_h, 3,
                 "结果写回：边界确认",
                 ["sink 对比（JSON text / pgvector / Lance）",
                  "COPY + deferred index 工程最优 baseline",
                  "确认写回不成为端到端瓶颈 → 课题聚焦 RC1↔RC2"],
                 PAL["d3_edge"], tag="非独立方法创新")

    # Evaluation metrics bar
    eval_y = bot_y + rc3_h + 16
    eval_w, eval_h = 1100, 44
    eval_x = (W - eval_w) / 2
    rrect(d, (eval_x, eval_y, eval_x + eval_w, eval_y + eval_h), PAL["box_fill"], PAL["box_edge"], r=10, w=1)
    eval_text = "评价指标：端到端耗时  ·  rows/s  ·  tokens/s  ·  queue wait  ·  model request wall  ·  writeback ratio  ·  GPU utilization"
    etw, eth = txtsz(d, eval_text, F["tag"])
    d.text((eval_x + eval_w / 2 - etw / 2, eval_y + eval_h / 2 - eth / 2 - 1),
              eval_text, font=F["tag"], fill=PAL["box_edge"])

    # ── Caption ──
    cap_y = eval_y + eval_h + 20
    cap_text = (
        "图注：跨层协同优化方法框架。RC1（数据组织与批处理构造）和 RC2（GPU 服务感知调度与反压）"
        "通过 batch/partition ↔ in-flight/routing 的联合决策形成跨层协同，核心验证为联合最优 vs 独立最优组合的对照实验；"
        "RC3（结果写回）作为边界确认，确保课题聚焦于推理基础设施侧的优化。"
    )
    ctw, cth = txtsz(d, cap_text, F["cap"])
    # Wrap to fit
    max_cap_w = W - 144
    if ctw > max_cap_w:
        # Simple manual wrap
        line1 = (
            "图注：跨层协同优化方法框架。RC1（数据组织与批处理构造）和 RC2（GPU 服务感知调度与反压）"
        )
        line2 = (
            "通过 batch/partition ↔ in-flight/routing 的联合决策形成跨层协同，核心验证为联合最优 vs 独立最优组合的对照实验；"
            "RC3（结果写回）作为边界确认，确保课题聚焦于推理基础设施侧的优化。"
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
        "rc3_x": rc3_x, "rc3_y": bot_y, "rc3_w": rc3_w, "rc3_h": rc3_h,
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
        svg_text(64, 58, "跨层协同优化方法框架", 28, "700"),
        svg_text(66, 92, "本课题的核心方法不是 RC1 或 RC2 孤立优化，而是二者之间的跨层协同决策——联合最优 vs 独立最优组合。", 16, fill=PAL["muted"]),
    ]

    # Workload entry
    wl_y = 108
    wl_w, wl_h = 1200, 100
    wl_x = (W - wl_w) / 2
    parts.append(svg_rect(wl_x, wl_y, wl_w, wl_h, "#F8FAFC", PAL["pill_edge"], r=12, sw=1))
    parts.append(svg_text(wl_x + 24, wl_y + 22, "Workload 入口", 13, "700", PAL["muted"]))

    wl_items = [
        ("AI_EMBED", "向量维度、batch 均衡、写回压力", PAL["d1_edge"]),
        ("AI_FILTER / AI_CLASSIFY", "selectivity 未知、调用次数多", PAL["d2_edge"]),
        ("AI_COMPLETE", "token 长度不均、prefix 共享", PAL["d3_edge"]),
    ]
    wl_item_w = 340
    wl_gap = 36
    wl_total = 3 * wl_item_w + 2 * wl_gap
    wl_start = wl_x + (wl_w - wl_total) / 2
    for i, (label, desc, color) in enumerate(wl_items):
        ix = wl_start + i * (wl_item_w + wl_gap)
        iy = wl_y + 36
        parts.append(svg_rect(ix + 8, iy, 120, 24, PAL["card_bg"], color, r=6, sw=2))
        parts.append(svg_text(ix + 18, iy + 17, label, 13, "700", color))
        parts.append(svg_text(ix + 8, iy + 52, desc, 12, fill=PAL["muted"]))

    # Arrow
    arrow_y = wl_y + wl_h
    parts.append(f'<polygon points="{W/2},{arrow_y + 12} {W/2 - 8},{arrow_y} {W/2 + 8},{arrow_y}" fill="{PAL["pill_edge"]}"/>')

    # Middle cross-layer
    mid_y = arrow_y + 28
    mid_w, mid_h = 1280, 340
    mid_x = (W - mid_w) / 2
    parts.append(svg_rect(mid_x, mid_y, mid_w, mid_h, PAL["panel_bg"], PAL["pill_edge"], r=14, sw=1))

    # Section header
    parts.append(svg_rect(mid_x + 24, mid_y + 8, 220, 26, PAL["card_bg"], PAL["pill_edge"], r=6, sw=1))
    parts.append(svg_text(mid_x + 34, mid_y + 27, "跨层协同优化（核心方法贡献）", 14, "700", PAL["ink"]))

    # RC cards
    rc_w, rc_h = 440, 216
    rc1_x = mid_x + 48
    rc1_y = mid_y + 50
    rc2_x = mid_x + mid_w - rc_w - 48

    for rx, ry, num, title, items, edge in [
        (rc1_x, rc1_y, 1, "数据组织与批处理构造",
         ["batch size / partition 数联合决策", "operator invocation 粒度控制",
          "object 合并与 fan-in 形态选择", "输出特征向量维度感知"], PAL["d1_edge"]),
        (rc2_x, rc1_y, 2, "GPU 服务感知调度与反压",
         ["endpoint 数量与 replica 状态路由", "actor pool 大小与 bounded in-flight",
          "queue wait 监控与 backlog 控制", "token 长度 / prefix 感知分发"], PAL["d2_edge"]),
    ]:
        parts.append(svg_rect(rx, ry, rc_w, rc_h, PAL["card_bg"], edge, r=12))
        parts.append(f'<circle cx="{rx + 30}" cy="{ry + 28}" r="16" fill="{edge}"/>')
        parts.append(svg_text(rx + 30, ry + 33, str(num), 14, "700", "#FFFFFF", "middle"))
        parts.append(svg_text(rx + 56, ry + 30, title, 18, "700", edge))
        py = ry + 56
        for item in items:
            parts.append(svg_pill(rx + 18, py, rc_w - 36, 24, item, 12))
            py += 30

    # Joint optimization connector
    jo_center_x = mid_x + mid_w / 2
    jo_center_y = rc1_y + rc_h / 2
    parts.append(f'<line x1="{rc1_x + rc_w}" y1="{jo_center_y}" x2="{rc2_x}" y2="{jo_center_y}" stroke="{PAL["pill_edge"]}" stroke-width="2"/>')
    parts.append(f'<polygon points="{jo_center_x},{jo_center_y - 8} {jo_center_x + 8},{jo_center_y} {jo_center_x},{jo_center_y + 8} {jo_center_x - 8},{jo_center_y}" fill="{PAL["warn"]}" stroke="{PAL["warn"]}"/>')
    parts.append(svg_text(jo_center_x, jo_center_y - 28, "batch / partition", 12, fill=PAL["d1_edge"], anchor="middle"))
    parts.append(svg_text(jo_center_x, jo_center_y - 12, "in-flight / routing", 12, fill=PAL["d2_edge"], anchor="middle"))
    parts.append(svg_text(jo_center_x, jo_center_y + 30, "联合搜索 ≠ 独立最优组合", 13, "700", PAL["ink"], "middle"))

    # Killer experiment bar
    ke_y = rc1_y + rc_h + 22
    ke_w, ke_h = mid_w - 96, 32
    ke_x = mid_x + 48
    parts.append(svg_rect(ke_x, ke_y, ke_w, ke_h, PAL["card_bg"], PAL["pill_edge"], r=8, sw=1))
    parts.append(svg_center(ke_x + ke_w / 2, ke_y + ke_h / 2 - 8, ke_w, ke_h,
                            "验证：BL3（独立最优组合）vs BL6（跨层联合最优）— 若 BL6 &lt; BL3，则跨层协同效应成立",
                            12, fill=PAL["ink"]))

    # Bottom RC3 + Eval
    bot_y = mid_y + mid_h + 20
    rc3_w, rc3_h = 580, 110
    rc3_x = (W - rc3_w) / 2

    parts.append(svg_rect(rc3_x, bot_y, rc3_w, rc3_h, PAL["card_bg"], PAL["d3_edge"], r=12))
    parts.append(f'<circle cx="{rc3_x + 30}" cy="{bot_y + 28}" r="16" fill="{PAL["d3_edge"]}"/>')
    parts.append(svg_text(rc3_x + 30, bot_y + 33, "3", 14, "700", "#FFFFFF", "middle"))
    parts.append(svg_text(rc3_x + 56, bot_y + 30, "结果写回：边界确认", 18, "700", PAL["d3_edge"]))
    # Tag
    parts.append(svg_text(rc3_x + rc3_w - 120, bot_y + 28, "非独立方法创新", 12, fill=PAL["muted"]))
    py = bot_y + 56
    for item in ["sink 对比（JSON text / pgvector / Lance）", "COPY + deferred index 工程最优 baseline", "确认写回不成为端到端瓶颈 → 课题聚焦 RC1↔RC2"]:
        parts.append(svg_pill(rc3_x + 18, py, rc3_w - 36, 24, item, 12))
        py += 30

    # Evaluation
    eval_y = bot_y + rc3_h + 16
    eval_w, eval_h = 1100, 44
    eval_x = (W - eval_w) / 2
    parts.append(svg_rect(eval_x, eval_y, eval_w, eval_h, PAL["box_fill"], PAL["box_edge"], r=10, sw=1))
    parts.append(svg_center(eval_x + eval_w / 2, eval_y + eval_h / 2 - 6, eval_w, eval_h,
                            "评价指标：端到端耗时 · rows/s · tokens/s · queue wait · model request wall · writeback ratio · GPU utilization",
                            12, fill=PAL["box_edge"]))

    # Caption (2 lines)
    cap_y = eval_y + eval_h + 20
    l1 = "图注：跨层协同优化方法框架。RC1（数据组织与批处理构造）和 RC2（GPU 服务感知调度与反压）"
    l2 = "通过 batch/partition ↔ in-flight/routing 的联合决策形成跨层协同，核心验证为联合最优 vs 独立最优组合的对照实验；RC3（结果写回）作为边界确认，确保课题聚焦于推理基础设施侧的优化。"
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
    rc3_w = params["rc3_w"]
    rc3_h = params["rc3_h"]

    print(f"  Layout: mid=({mid_x},{mid_y},{mid_w},{mid_h}) rc1=({rc1_x},{rc1_y}) rc2=({rc2_x},{rc1_y})")

    # 1. RC1 card top-left: should have blue edge pixels
    blue_count = 0
    for y in range(rc1_y, rc1_y + 3):
        for x in range(rc1_x, rc1_x + rc_w):
            r, g, b = px[x, y]
            if b > 200 and r < 100 and g < 180:
                blue_count += 1
    print(f"  RC1 blue edge px: {blue_count} {'PASS' if blue_count > 10 else 'FAIL'}")

    # 2. RC2 card: should have orange edge pixels
    orange_count = 0
    for y in range(rc1_y, rc1_y + 3):
        for x in range(rc2_x, rc2_x + rc_w):
            r, g, b = px[x, y]
            if r > 200 and g > 100 and g < 180 and b < 100:
                orange_count += 1
    print(f"  RC2 orange edge px: {orange_count} {'PASS' if orange_count > 10 else 'FAIL'}")

    # 3. RC3 card: should have purple edge
    purple_count = 0
    for y in range(rc3_y, rc3_y + 3):
        for x in range(rc3_x, rc3_x + rc3_w):
            r, g, b = px[x, y]
            if r > 100 and b > 200 and g < 120:
                purple_count += 1
    print(f"  RC3 purple edge px: {purple_count} {'PASS' if purple_count > 10 else 'FAIL'}")

    # 4. Joint optimization diamond exists (yellow/warn pixels in center area)
    jo_center_y = rc1_y + rc_h // 2
    jo_center_x = mid_x + mid_w // 2
    warn_count = 0
    for y in range(jo_center_y - 15, jo_center_y + 15):
        for x in range(jo_center_x - 15, jo_center_x + 15):
            r, g, b = px[x, y]
            if r > 200 and g > 150 and b < 50:
                warn_count += 1
    print(f"  Joint-opt diamond px: {warn_count} {'PASS' if warn_count > 5 else 'FAIL'}")

    # 5. Green in evaluation bar
    eval_y_area = params["bot_y"] + rc3_h + 16
    green_count = 0
    for y in range(eval_y_area, eval_y_area + 44):
        for x in range(250, W - 250):
            r, g, b = px[x, y]
            if g > 100 and r < 150 and b < 150:
                green_count += 1
    print(f"  Eval green px: {green_count} {'PASS' if green_count > 10 else 'FAIL'}")

    # 6. Bounds check: RC cards within mid panel
    for name, rx, rw in [("RC1", rc1_x, rc_w), ("RC2", rc2_x, rc_w)]:
        left_ok = rx > mid_x
        right_ok = rx + rw < mid_x + mid_w
        print(f"  {name} bounds: left={'OK' if left_ok else 'OUT'}, right={'OK' if right_ok else 'OUT'} {'PASS' if left_ok and right_ok else 'FAIL'}")

    print("Self-check complete.")
