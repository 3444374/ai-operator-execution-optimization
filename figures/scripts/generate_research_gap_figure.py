from pathlib import Path
from xml.sax.saxutils import escape

from PIL import Image, ImageDraw, ImageFont


OUT_DIR = Path(__file__).resolve().parents[1] / "architecture"
SVG_PATH = OUT_DIR / "research_gap_three_islands.svg"
PNG_PATH = OUT_DIR / "research_gap_three_islands.png"

W, H = 1600, 800

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
}


def font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    for p in [
        r"C:\Windows\Fonts\msyhbd.ttc" if bold else r"C:\Windows\Fonts\msyh.ttc",
        r"C:\Windows\Fonts\simhei.ttf",
        r"C:\Windows\Fonts\arial.ttf",
    ]:
        if Path(p).exists():
            return ImageFont.truetype(p, size=size)
    return ImageFont.load_default()


F = {
    "title":   font(28, True),
    "sub":     font(16),
    "d_title": font(18, True),
    "pill":    font(14),
    "note":    font(13),
    "cap":     font(13),
    "card_hd": font(16, True),
    "badge":   font(14, True),
    "tag":     font(12),
}


def txtsz(draw, text, fnt):
    b = draw.textbbox((0, 0), text, font=fnt)
    return b[2] - b[0], b[3] - b[1]


def ctext(draw, box, text, fnt, fill=PAL["ink"]):
    x1, y1, x2, y2 = box
    tw, th = txtsz(draw, text, fnt)
    draw.text((x1 + (x2 - x1 - tw) / 2, y1 + (y2 - y1 - th) / 2 - 1), text, font=fnt, fill=fill)


def rrect(draw, box, fill, edge, r=14):
    draw.rounded_rectangle(box, radius=r, fill=fill, outline=edge, width=2)


def pill(draw, x, y, w, h, text):
    rrect(draw, (x, y, x + w, y + h), PAL["pill_bg"], PAL["pill_edge"], r=8)
    ctext(draw, (x, y, x + w, y + h), text, F["pill"], PAL["muted"])


def numbered_circle(draw, cx, cy, radius, number, color):
    """Draw a circle with centered number. Circle diameter = 2*radius."""
    d = 2 * radius
    draw.ellipse((cx - radius, cy - radius, cx + radius, cy + radius), fill=color)
    # Center the number precisely in the circle
    tw, th = txtsz(draw, str(number), F["badge"])
    draw.text((cx - tw / 2, cy - th / 2 - 1), str(number), font=F["badge"], fill="#FFFFFF")


def draw_dir_box(draw, x, y, w, h, title, items, fill, edge):
    rrect(draw, (x, y, x + w, y + h), fill, edge)
    ctext(draw, (x, y + 14, x + w, y + 44), title, F["d_title"], edge)
    py = y + 56
    pw, ph, gap = w - 44, 26, 6
    for item in items:
        pill(draw, x + 22, py, pw, ph, item)
        py += ph + gap


def draw_research_card(draw, x, y, w, h, num, title, items, edge, tag, tag_color):
    rrect(draw, (x, y, x + w, y + h), PAL["card_bg"], edge, r=10)

    # Numbered circle badge
    badge_r = 14
    badge_cx = x + 26
    badge_cy = y + 22
    draw.ellipse((badge_cx - badge_r, badge_cy - badge_r,
                  badge_cx + badge_r, badge_cy + badge_r), fill=edge)
    ctext(draw, (badge_cx - badge_r, badge_cy - badge_r,
                 badge_cx + badge_r, badge_cy + badge_r), str(num), font(13, True), "#FFFFFF")

    # Title right of badge
    draw.text((x + 48, y + 15), title, font=font(15, True), fill=edge)

    # Tag top-right
    if tag:
        tw, th = txtsz(draw, tag, F["tag"])
        draw.text((x + w - tw - 12, y + 16), tag, font=F["tag"], fill=tag_color)

    # Items
    py = y + 46
    pw, ph, gap = w - 28, 20, 4
    for item in items:
        pill(draw, x + 14, py, pw, ph, item)
        py += ph + gap


# ═══════════════════════════════════════════════════════════════════════
#  PNG
# ═══════════════════════════════════════════════════════════════════════

def draw_png():
    img = Image.new("RGB", (W, H), PAL["bg"])
    d = ImageDraw.Draw(img)

    # ── Title ──
    d.text((64, 34), "已有研究的三个方向与本课题定位", font=F["title"], fill=PAL["ink"])
    d.text(
        (66, 74),
        "数据库内置 AI 算子、分布式 AI 推理服务、AI 数据存储三个方向各自有大量 CCF-A 工作，但缺少跨方向的端到端协同优化。",
        font=F["sub"], fill=PAL["muted"],
    )

    # ── Top row: three direction boxes ──
    dy, dw, dh, dgap = 116, 374, 218, 50
    total_w = 3 * dw + 2 * dgap
    start_x = (W - total_w) / 2
    xs = [start_x + i * (dw + dgap) for i in range(3)]
    cx = [x + dw / 2 for x in xs]

    dirs = [
        ("方向一：数据库内置 AI 算子（DB4AI）",
         ["GaussML (ICDE 2024)", "Smart (VLDB J. 2025)", "NeurDB (CIDR 2025)", "LEADS (VLDB 2024)"],
         PAL["d1_fill"], PAL["d1_edge"]),
        ("方向二：分布式 AI 推理服务系统",
         ["vLLM (SOSP 2023)", "Orca (OSDI 2022)", "Sarathi-Serve (OSDI 2024)", "Ray / Daft / Ray Data"],
         PAL["d2_fill"], PAL["d2_edge"]),
        ("方向三：AI 数据存储与写回优化",
         ["Lance (LanceDB 2025)", "TurboVecDB (PVLDB 2025)", "Delta Lake (PVLDB 2020)", "pgvector / Arrow Flight"],
         PAL["d3_fill"], PAL["d3_edge"]),
    ]
    for x, (t, items, fill, edge) in zip(xs, dirs):
        draw_dir_box(d, x, dy, dw, dh, t, items, fill, edge)

    # ── Boundary notes under each box ──
    note_y = dy + dh + 16
    notes = [
        "优化范围止于数据库进程边界",
        "数据来源与去向被抽象为输入/输出",
        "优化范围止于存储层",
    ]
    for c, note in zip(cx, notes):
        tw, _ = txtsz(d, note, F["note"])
        d.text((c - tw / 2, note_y), note, font=F["note"], fill=PAL["note"])

    # ── Divider line ──
    div_y = note_y + 36
    d.line((start_x + 40, div_y, start_x + total_w - 40, div_y), fill=PAL["pill_edge"], width=1)
    gap_label = "三个方向各自深入，缺少跨方向协同 —— 本课题填补此空白"
    lw, _ = txtsz(d, gap_label, F["note"])
    d.text(((W - lw) / 2, div_y + 8), gap_label, font=F["note"], fill=PAL["note"])

    # ── Bottom: thesis box ──
    thesis_y = div_y + 40
    tw, th = 1280, 260
    tx = (W - tw) / 2
    rrect(d, (tx, thesis_y, tx + tw, thesis_y + th), PAL["box_fill"], PAL["box_edge"])

    ctext(d, (tx, thesis_y + 12, tx + tw, thesis_y + 44),
          "本课题定位：方向一与方向二协同优化，方向三为结果写回",
          F["d_title"], PAL["box_edge"])

    # ── Layout: shared panel (cards 1+2) + standalone card 3 ──
    card_y = thesis_y + 74       # pushed down so panel doesn't cover thesis title
    card_h = 138
    panel_pad = 24              # reduced since card_y already moved down
    card_gap = 22
    card_w = 320
    panel_w = 2 * card_w + card_gap + 2 * panel_pad
    card3_w = 320
    panel_card3_gap = 22

    total_inner = panel_w + panel_card3_gap + card3_w
    inner_start = tx + (tw - total_inner) / 2

    panel_x = inner_start
    card3_x = inner_start + panel_w + panel_card3_gap

    # Shared panel background
    rrect(d, (panel_x, card_y - panel_pad, panel_x + panel_w, card_y + card_h + panel_pad // 2),
          "#F1F5F9", PAL["pill_edge"], r=12)

    # Card 1
    c1x = panel_x + panel_pad
    draw_research_card(d, c1x, card_y, card_w, card_h, 1,
                       "数据组织与批处理构造",
                       ["AI workload 特征感知", "batch / partition / operator 粒度", "object 合并与 fan-in 形态"],
                       PAL["d1_edge"], "", "")

    # Card 2
    c2x = c1x + card_w + card_gap
    draw_research_card(d, c2x, card_y, card_w, card_h, 2,
                       "GPU 服务感知调度与反压",
                       ["endpoint / replica 状态路由", "actor pool 与 bounded in-flight", "queue wait 与 backlog 控制"],
                       PAL["d2_edge"], "", "")

    # Card 3 standalone
    draw_research_card(d, card3_x, card_y, card3_w, card_h, 3,
                       "结果写回优化",
                       ["fan-in 汇聚与批量写入", "driver / worker / queue 写回路径", "Lance / pgvector / PostgreSQL"],
                       PAL["d3_edge"], "", "")

    # Panel header — subtle label, no green border
    header_text = "跨层协同优化（核心方法贡献）"
    htw, hth = txtsz(d, header_text, F["tag"])
    header_pill_x = panel_x + panel_pad
    header_pill_y = card_y - panel_pad + 2
    rrect(d, (header_pill_x, header_pill_y, header_pill_x + htw + 16, header_pill_y + hth + 6),
          PAL["pill_bg"], PAL["pill_edge"], r=5)
    d.text((header_pill_x + 8, header_pill_y + 3), header_text, font=F["tag"], fill=PAL["ink"])

    # ── Caption ──
    d.text(
        (72, H - 34),
        "图注：已有研究在 DB4AI、AI 推理服务和 AI 数据存储三个方向各自深入，但缺少将它们视为端到端可协同优化的完整链路的系统研究。"
        "本课题聚焦方向一（数据组织）与方向二（GPU 调度）的跨层协同优化。",
        font=F["cap"], fill=PAL["muted"],
    )

    img.save(PNG_PATH)
    return {
        "thesis_y": thesis_y, "card_y": card_y, "tx": tx, "tw": tw,
        "panel_x": panel_x, "panel_w": panel_w, "panel_pad": panel_pad,
        "card3_x": card3_x, "card3_w": card3_w,
        "thesis_x": tx, "thesis_w": tw,
    }


# ═══════════════════════════════════════════════════════════════════════
#  SVG
# ═══════════════════════════════════════════════════════════════════════

def svg_text(x, y, text, size=16, weight="400", fill=PAL["ink"], anchor="start"):
    return f'<text x="{x}" y="{y}" font-family="Microsoft YaHei,Arial,sans-serif" font-size="{size}" font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{escape(text)}</text>'


def svg_rect(x, y, w, h, fill, stroke, r=14):
    return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill}" stroke="{stroke}" stroke-width="2"/>'


def svg_center(x, y, w, h, text, size=16, weight="400", fill=PAL["ink"]):
    return svg_text(x + w / 2, y + h / 2 + size * 0.35, text, size, weight, fill, "middle")


def svg_pill(x, y, w, h, text, size=14):
    return svg_rect(x, y, w, h, PAL["pill_bg"], PAL["pill_edge"], r=8) + "\n" + svg_center(x, y, w, h, text, size, fill=PAL["muted"])


def draw_svg():
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
        f'<rect width="{W}" height="{H}" fill="{PAL["bg"]}"/>',
        svg_text(64, 60, "已有研究的三个方向与本课题定位", 28, "700"),
        svg_text(66, 96, "数据库内置 AI 算子、分布式 AI 推理服务、AI 数据存储三个方向各自有大量 CCF-A 工作，但缺少跨方向的端到端协同优化。", 16, fill=PAL["muted"]),
    ]

    # Direction boxes
    dy, dw, dh, dgap = 116, 374, 218, 50
    total_w = 3 * dw + 2 * dgap
    start_x = (W - total_w) / 2
    xs = [start_x + i * (dw + dgap) for i in range(3)]
    cx = [x + dw / 2 for x in xs]

    dirs = [
        ("方向一：数据库内置 AI 算子（DB4AI）",
         ["GaussML (ICDE 2024)", "Smart (VLDB J. 2025)", "NeurDB (CIDR 2025)", "LEADS (VLDB 2024)"],
         PAL["d1_fill"], PAL["d1_edge"]),
        ("方向二：分布式 AI 推理服务系统",
         ["vLLM (SOSP 2023)", "Orca (OSDI 2022)", "Sarathi-Serve (OSDI 2024)", "Ray / Daft / Ray Data"],
         PAL["d2_fill"], PAL["d2_edge"]),
        ("方向三：AI 数据存储与写回优化",
         ["Lance (LanceDB 2025)", "TurboVecDB (PVLDB 2025)", "Delta Lake (PVLDB 2020)", "pgvector / Arrow Flight"],
         PAL["d3_fill"], PAL["d3_edge"]),
    ]
    for x, (t, items, fill, edge) in zip(xs, dirs):
        parts.append(svg_rect(x, dy, dw, dh, fill, edge))
        parts.append(svg_center(x, dy + 14, dw, 30, t, 18, "700", edge))
        py = dy + 56
        for item in items:
            parts.append(svg_pill(x + 22, py, dw - 44, 26, item))
            py += 32

    # Boundary notes
    note_y = dy + dh + 20
    notes = [
        "优化范围止于数据库进程边界",
        "数据来源与去向被抽象为输入/输出",
        "优化范围止于存储层",
    ]
    for c, note in zip(cx, notes):
        parts.append(svg_text(c, note_y, note, 13, fill=PAL["note"], anchor="middle"))

    # Divider
    div_y = note_y + 30
    parts.append(f'<line x1="{start_x + 40}" y1="{div_y}" x2="{start_x + total_w - 40}" y2="{div_y}" stroke="{PAL["pill_edge"]}" stroke-width="1"/>')
    parts.append(svg_center(W/2, div_y + 4, 460, 20, "三个方向各自深入，缺少跨方向协同 —— 本课题填补此空白", 13, fill=PAL["note"]))

    # Thesis box
    thesis_y = div_y + 40
    tw, th = 1280, 260
    tx = (W - tw) / 2
    parts.append(svg_rect(tx, thesis_y, tw, th, PAL["box_fill"], PAL["box_edge"]))
    parts.append(svg_center(tx, thesis_y + 12, tw, 32,
                            "本课题定位：方向一与方向二协同优化，方向三为结果写回",
                            17, "700", PAL["box_edge"]))

    # Layout: shared panel + standalone card
    card_y = thesis_y + 62
    card_h = 138
    panel_pad = 28
    card_gap = 22
    card_w = 320
    panel_w = 2 * card_w + card_gap + 2 * panel_pad
    card3_w = 320
    panel_card3_gap = 22

    total_inner = panel_w + panel_card3_gap + card3_w
    inner_start = tx + (tw - total_inner) / 2

    panel_x = inner_start
    card3_x = inner_start + panel_w + panel_card3_gap

    # Shared panel background
    parts.append(svg_rect(panel_x, card_y - panel_pad, panel_w, card_h + panel_pad + panel_pad // 2,
                          "#F1F5F9", PAL["pill_edge"], r=12))

    # Card 1
    c1x = panel_x + panel_pad
    parts.append(svg_rect(c1x, card_y, card_w, card_h, PAL["card_bg"], PAL["d1_edge"], r=10))
    parts.append(f'<circle cx="{c1x + 26}" cy="{card_y + 22}" r="14" fill="{PAL["d1_edge"]}"/>')
    parts.append(svg_text(c1x + 26, card_y + 27, "1", 13, "700", "#FFFFFF", "middle"))
    parts.append(svg_text(c1x + 48, card_y + 28, "数据组织与批处理构造", 15, "700", PAL["d1_edge"]))
    py = card_y + 48
    for item in ["AI workload 特征感知", "batch / partition / operator 粒度", "object 合并与 fan-in 形态"]:
        parts.append(svg_pill(c1x + 14, py, card_w - 28, 20, item, size=11))
        py += 25

    # Card 2
    c2x = c1x + card_w + card_gap
    parts.append(svg_rect(c2x, card_y, card_w, card_h, PAL["card_bg"], PAL["d2_edge"], r=10))
    parts.append(f'<circle cx="{c2x + 26}" cy="{card_y + 22}" r="14" fill="{PAL["d2_edge"]}"/>')
    parts.append(svg_text(c2x + 26, card_y + 27, "2", 13, "700", "#FFFFFF", "middle"))
    parts.append(svg_text(c2x + 48, card_y + 28, "GPU 服务感知调度与反压", 15, "700", PAL["d2_edge"]))
    py = card_y + 48
    for item in ["endpoint / replica 状态路由", "actor pool 与 bounded in-flight", "queue wait 与 backlog 控制"]:
        parts.append(svg_pill(c2x + 14, py, card_w - 28, 20, item, size=11))
        py += 25

    # Card 3 standalone
    parts.append(svg_rect(card3_x, card_y, card3_w, card_h, PAL["card_bg"], PAL["d3_edge"], r=10))
    parts.append(f'<circle cx="{card3_x + 26}" cy="{card_y + 22}" r="14" fill="{PAL["d3_edge"]}"/>')
    parts.append(svg_text(card3_x + 26, card_y + 27, "3", 13, "700", "#FFFFFF", "middle"))
    parts.append(svg_text(card3_x + 48, card_y + 28, "结果写回优化", 15, "700", PAL["d3_edge"]))
    py = card_y + 48
    for item in ["fan-in 汇聚与批量写入", "driver / worker / queue 写回路径", "Lance / pgvector / PostgreSQL"]:
        parts.append(svg_pill(card3_x + 14, py, card3_w - 28, 20, item, size=11))
        py += 25

    # Panel header — subtle label, no green
    parts.append(svg_text(panel_x + panel_pad, card_y - panel_pad + 16,
                          "跨层协同优化（核心方法贡献）", 12, fill=PAL["ink"]))

    # Caption
    parts.append(svg_text(
        72, H - 22,
        "图注：已有研究在 DB4AI、AI 推理服务和 AI 数据存储三个方向各自深入，但缺少将它们视为端到端可协同优化的完整链路的系统研究。"
        "本课题聚焦方向一（数据组织）与方向二（GPU 调度）的跨层协同优化。",
        13, fill=PAL["muted"],
    ))

    parts.append("</svg>")
    SVG_PATH.write_text("\n".join(parts), encoding="utf-8")


if __name__ == "__main__":
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    params = draw_png()
    draw_svg()
    print(SVG_PATH)
    print(PNG_PATH)

    # ── Self-check: verify no element occlusion ──
    from PIL import Image
    img = Image.open(PNG_PATH)
    px = img.load()

    thesis_y = params["thesis_y"]
    card_y = params["card_y"]
    tx = int(params["tx"])
    tw = params["tw"]
    panel_x = int(params["panel_x"])
    panel_w = int(params["panel_w"])
    panel_pad = params["panel_pad"]

    print(f"  Layout: thesis_y={thesis_y} card_y={card_y} panel_x={panel_x} panel_w={panel_w}")
    header_y = card_y - panel_pad + 6  # approximate text top

    # 1. Panel header area: should have non-bg content (gray pill or text above cards)
    header_y = card_y - panel_pad
    non_bg = 0
    for y in range(max(0, header_y), card_y):
        for x in range(panel_x + 4, panel_x + panel_w - 4):
            rgb = px[x, y]
            if rgb != (248, 250, 252) and rgb != (241, 245, 249):
                non_bg += 1
    print(f"  Panel header: {non_bg} non-bg px {'PASS' if non_bg > 30 else 'FAIL'}")

    # 2. Thesis title: should have green pixels (thesis box title area)
    title_green = 0
    for y in range(thesis_y + 4, thesis_y + 50):
        for x in range(tx + 20, tx + tw - 20):
            r, g, b = px[x, y]
            if g > 120 and r < 100 and b < 100:
                title_green += 1
    print(f"  Thesis title green px: {title_green} {'PASS' if title_green > 20 else 'FAIL — title occluded by panel?'}")

    # 2. Card top: should be white bg or expected card border colors (not header bleed)
    card_border_colors = {(47,111,235),(249,115,22),(124,58,237),(241,245,249)}
    unexpected = set()
    for y in range(card_y, card_y + 4):
        for x in range(panel_x + 10, panel_x + panel_w - 10):
            rgb = px[x, y]
            if rgb[0] > 245 and rgb[1] > 245 and rgb[2] > 245:
                continue  # white or near-white
            if rgb in card_border_colors:
                continue  # exact border match
            # Anti-aliased border pixels: accept any where one channel dominates
            r, g, b = rgb
            if max(r, g, b) - min(r, g, b) > 30:  # likely anti-aliased edge
                continue
            unexpected.add(rgb)
    print(f"  Card top: {'PASS' if not unexpected else f'WARN — {len(unexpected)} unexpected colors'}")

    # 3. Key colors present
    import collections
    cc = collections.Counter()
    for y in range(0, img.height, 8):
        for x in range(0, img.width, 8):
            cc[px[x, y]] += 1
    for color, name in [((47,111,235),"blue"),((249,115,22),"orange"),((124,58,237),"purple"),((22,163,74),"green")]:
        count = cc.get(color, 0)
        print(f"  {name}: {count} px {'PASS' if count > 10 else 'FAIL'}")

    # 4. Bounds check
    card3_x = int(params["card3_x"])
    card3_w = params["card3_w"]
    thesis_x = int(params["thesis_x"])
    thesis_w = params["thesis_w"]
    card_right = card3_x + card3_w
    thesis_right = thesis_x + thesis_w
    print(f"  Card3 edge {card_right} vs thesis {thesis_right}: {'PASS' if card_right <= thesis_right else 'FAIL'}")

    print("Self-check complete.")
