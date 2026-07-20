"""Generate data-organization strategy mechanism figures.

These are schematic mechanism figures, not experimental-result charts.
They explain candidate upstream batching/grouping policies for AI_COMPLETE.
"""

from pathlib import Path
from xml.sax.saxutils import escape as xe

from PIL import Image, ImageDraw, ImageFont


OUT_DIR = Path(__file__).resolve().parents[1] / "architecture"

PAL = {
    "bg": "#F8FAFC",
    "ink": "#172033",
    "muted": "#475569",
    "line": "#334155",
    "blue": "#2F6FEB",
    "blue_fill": "#E7F0FF",
    "orange": "#F97316",
    "orange_fill": "#FFF1E6",
    "green": "#16A34A",
    "green_fill": "#F0FDF4",
    "red": "#DC2626",
    "red_fill": "#FEF2F2",
    "gray": "#94A3B8",
    "gray_fill": "#F1F5F9",
    "white": "#FFFFFF",
}


def rgb(hex_color: str) -> tuple[int, int, int]:
    return tuple(int(hex_color[i : i + 2], 16) for i in (1, 3, 5))


def font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    candidates = [
        r"C:\Windows\Fonts\msyhbd.ttc" if bold else r"C:\Windows\Fonts\msyh.ttc",
        r"C:\Windows\Fonts\simhei.ttf",
        r"C:\Windows\Fonts\arial.ttf",
    ]
    for path in candidates:
        if Path(path).exists():
            return ImageFont.truetype(path, size=size)
    return ImageFont.load_default()


F = {
    "title": font(26, True),
    "sub": font(14),
    "section": font(16, True),
    "label": font(12),
    "small": font(10),
    "note": font(11),
}


def text_size(draw: ImageDraw.ImageDraw, text: str, fnt: ImageFont.FreeTypeFont) -> tuple[int, int]:
    box = draw.textbbox((0, 0), text, font=fnt)
    return box[2] - box[0], box[3] - box[1]


def centered_text(draw: ImageDraw.ImageDraw, x: int, y: int, text: str, fnt, fill) -> None:
    tw, _ = text_size(draw, text, fnt)
    draw.text((x - tw // 2, y), text, font=fnt, fill=fill)


def rr(draw: ImageDraw.ImageDraw, box, fill=None, outline=None, width=1, radius=8) -> None:
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=width)


def arrow(draw: ImageDraw.ImageDraw, x1: int, y1: int, x2: int, y2: int, color=None, width: int = 3) -> None:
    color = color or rgb(PAL["line"])
    draw.line((x1, y1, x2, y2), fill=color, width=width)
    direction = 1 if x2 >= x1 else -1
    draw.polygon([(x2, y2), (x2 - direction * 10, y2 - 6), (x2 - direction * 10, y2 + 6)], fill=color)


def svg_text(x, y, text, size=12, color="#172033", bold=False, anchor="start"):
    weight = "bold" if bold else "normal"
    return (
        f'<text x="{x}" y="{y}" font-family="Microsoft YaHei,SimHei,Arial,sans-serif" '
        f'font-size="{size}" font-weight="{weight}" fill="{color}" text-anchor="{anchor}">{xe(text)}</text>'
    )


def svg_rect(x, y, w, h, fill, stroke=None, sw=1, rx=8):
    stroke_part = f' stroke="{stroke}" stroke-width="{sw}"' if stroke else ""
    return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" ry="{rx}" fill="{fill}"{stroke_part}/>'


def svg_arrow(x1, y1, x2, y2, color="#334155", sw=3):
    direction = 1 if x2 >= x1 else -1
    return (
        f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{sw}"/>'
        f'<polygon points="{x2},{y2} {x2-direction*10},{y2-6} {x2-direction*10},{y2+6}" fill="{color}"/>'
    )


def write_svg(path: Path, width: int, height: int, body: list[str]) -> None:
    content = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<rect width="{width}" height="{height}" fill="{PAL["bg"]}"/>',
        *body,
        "</svg>",
    ]
    path.write_text("\n".join(content), encoding="utf-8")


def add_title(draw, width: int, title: str, subtitle: str) -> None:
    centered_text(draw, width // 2, 12, title, F["title"], rgb(PAL["ink"]))
    centered_text(draw, width // 2, 45, subtitle, F["sub"], rgb(PAL["muted"]))


def validate(svg_path: Path) -> None:
    text = svg_path.read_text(encoding="utf-8")
    forbidden = ["RC1", "RC2", "RC3", "BL1", "BL2", "边界确认", "Workload 入口", "Ours-v0"]
    found = [token for token in forbidden if token in text]
    if found:
        raise RuntimeError(f"{svg_path.name} contains forbidden visible tokens: {found}")


def token_budget() -> None:
    width, height = 1100, 430
    img = Image.new("RGB", (width, height), rgb(PAL["bg"]))
    draw = ImageDraw.Draw(img)
    title = "数据组织策略一：按 token 预算构造请求"
    subtitle = "候选机制：用计算量预算替代固定行数，控制单次提交的 token 尾部"
    add_title(draw, width, title, subtitle)

    left = (55, 82, 500, 298)
    right = (600, 82, 1045, 298)
    rr(draw, left, fill=rgb(PAL["red_fill"]), radius=10)
    rr(draw, right, fill=rgb(PAL["green_fill"]), radius=10)
    draw.text((75, 98), "固定行数 batch_size = 4", font=F["section"], fill=rgb(PAL["ink"]))
    draw.text((620, 98), "token-budget: max_tokens = 600", font=F["section"], fill=rgb(PAL["ink"]))

    rows = [("row1", 50), ("row2", 200), ("row3", 800), ("row4", 150)]
    bar_x, bar_y, bar_w, bar_h, gap = 85, 135, 320, 24, 10
    for idx, (name, tokens) in enumerate(rows):
        y = bar_y + idx * (bar_h + gap)
        bw = max(18, int(bar_w * tokens / 800))
        rr(draw, (bar_x, y, bar_x + bw, y + bar_h), fill=rgb(PAL["blue"]), radius=5)
        draw.text((bar_x + bw + 10, y + 4), f"{name}: {tokens} tokens", font=F["small"], fill=rgb(PAL["ink"]))
    rr(draw, (bar_x - 7, bar_y - 8, bar_x + bar_w + 8, bar_y + 4 * (bar_h + gap) - gap + 8), outline=rgb(PAL["red"]), width=3, radius=8)
    draw.text((96, 276), "风险：请求成本由最长/最大 token 行主导", font=F["note"], fill=rgb(PAL["red"]))

    arrow(draw, 525, 190, 575, 190)

    batches = [
        (630, 136, [("row1", 50), ("row2", 200), ("row4", 150)], "合计 400，合并提交", rgb(PAL["green"])),
        (630, 230, [("row3", 800)], "超过预算，单独提交", rgb(PAL["orange"])),
    ]
    note_positions = []
    for x, y, items, note, note_color in batches:
        box_h = 66 if len(items) == 1 else 78
        rr(draw, (x, y, x + 350, y + box_h), fill=rgb(PAL["blue_fill"]), outline=rgb(PAL["blue"]), width=2, radius=7)
        inner_y = y + 12
        for name, tokens in items:
            bw = max(22, int(300 * tokens / 800))
            rr(draw, (x + 16, inner_y, x + 16 + bw, inner_y + 18), fill=rgb(PAL["blue"]), radius=4)
            draw.text((x + 22, inner_y + 2), f"{name}({tokens})", font=F["small"], fill=rgb(PAL["white"]))
            inner_y += 22
        if len(items) > 1:
            note_positions.append((x + 188, y + box_h - 18, note, note_color))
        else:
            note_positions.append((x + 188, y + box_h + 14, note, note_color))
    for x, y, note, note_color in note_positions:
        draw.text((x, y), note, font=F["note"], fill=note_color)

    caption = "图注建议：该图说明 token-budget batching 如何把上游请求形状从固定行数改为计算量预算；是否提升吞吐或尾延迟需由消融实验验证。"
    draw.text((55, height - 34), caption, font=F["label"], fill=rgb(PAL["muted"]))
    img.save(OUT_DIR / "data_organization_token_budget_mechanism.png")

    body = [
        svg_text(width // 2, 34, title, 26, PAL["ink"], True, "middle"),
        svg_text(width // 2, 60, subtitle, 14, PAL["muted"], False, "middle"),
        svg_rect(*left[:2], left[2] - left[0], left[3] - left[1], PAL["red_fill"], rx=10),
        svg_rect(*right[:2], right[2] - right[0], right[3] - right[1], PAL["green_fill"], rx=10),
        svg_text(75, 116, "固定行数 batch_size = 4", 16, PAL["ink"], True),
        svg_text(620, 116, "token-budget: max_tokens = 600", 16, PAL["ink"], True),
    ]
    for idx, (name, tokens) in enumerate(rows):
        y = bar_y + idx * (bar_h + gap)
        bw = max(18, int(bar_w * tokens / 800))
        body.append(svg_rect(bar_x, y, bw, bar_h, PAL["blue"], rx=5))
        body.append(svg_text(bar_x + bw + 10, y + 17, f"{name}: {tokens} tokens", 10, PAL["ink"]))
    body.append(svg_rect(bar_x - 7, bar_y - 8, bar_w + 15, 4 * (bar_h + gap) - gap + 16, "none", PAL["red"], 3, 8))
    body.append(svg_text(96, 291, "风险：请求成本由最长/最大 token 行主导", 11, PAL["red"]))
    body.append(svg_arrow(525, 190, 575, 190))
    svg_note_positions = []
    for x, y, items, note, note_color in [
        (630, 136, [("row1", 50), ("row2", 200), ("row4", 150)], "合计 400，合并提交", PAL["green"]),
        (630, 230, [("row3", 800)], "超过预算，单独提交", PAL["orange"]),
    ]:
        box_h = 66 if len(items) == 1 else 78
        body.append(svg_rect(x, y, 350, box_h, PAL["blue_fill"], PAL["blue"], 2, 7))
        inner_y = y + 12
        for name, tokens in items:
            bw = max(22, int(300 * tokens / 800))
            body.append(svg_rect(x + 16, inner_y, bw, 18, PAL["blue"], rx=4))
            body.append(svg_text(x + 22, inner_y + 13, f"{name}({tokens})", 10, PAL["white"]))
            inner_y += 22
        if len(items) > 1:
            svg_note_positions.append((x + 188, y + box_h - 5, note, note_color))
        else:
            svg_note_positions.append((x + 188, y + box_h + 27, note, note_color))
    for x, y, note, note_color in svg_note_positions:
        body.append(svg_text(x, y, note, 11, note_color))
    body.append(svg_text(55, height - 14, caption, 12, PAL["muted"]))
    svg_path = OUT_DIR / "data_organization_token_budget_mechanism.svg"
    write_svg(svg_path, width, height, body)
    validate(svg_path)


def length_align() -> None:
    width, height = 980, 380
    img = Image.new("RGB", (width, height), rgb(PAL["bg"]))
    draw = ImageDraw.Draw(img)
    title = "数据组织策略二：按 token 长度分组"
    subtitle = "候选机制：先按 token 长度排序，再把相近计算量的行放入同一提交"
    add_title(draw, width, title, subtitle)

    values = [120, 950, 350, 720, 180, 880, 420, 620, 280, 780]
    sorted_values = sorted(values)
    x1, x2, base_y, max_h = 85, 575, 280, 130
    bar_w, gap = 22, 13
    draw.text((x1, 92), "原始顺序：长短请求混杂", font=F["section"], fill=rgb(PAL["ink"]))
    draw.text((x2, 92), "排序分组：短中长请求分区", font=F["section"], fill=rgb(PAL["ink"]))

    for idx, value in enumerate(values):
        h = int(max_h * value / 950)
        x = x1 + idx * (bar_w + gap)
        rr(draw, (x, base_y - h, x + bar_w, base_y), fill=rgb(PAL["gray"]), radius=4)
        draw.text((x - 2, base_y + 8), str(value), font=F["small"], fill=rgb(PAL["muted"]))
    draw.text((120, 322), "风险：短请求等待长请求完成", font=F["note"], fill=rgb(PAL["red"]))

    arrow(draw, 455, 202, 510, 202)

    groups = [
        ("短 token", 0, 3, PAL["green"]),
        ("中 token", 3, 6, PAL["blue"]),
        ("长 token", 6, 10, PAL["orange"]),
    ]
    for label, start, end, color in groups:
        gx = x2 + start * (bar_w + gap)
        gw = (end - start) * (bar_w + gap) - gap + bar_w
        draw.line((gx, 130, gx + gw, 130), fill=rgb(color), width=4)
        centered_text(draw, gx + gw // 2, 108, label, F["note"], rgb(color))
    for idx, value in enumerate(sorted_values):
        h = int(max_h * value / 950)
        x = x2 + idx * (bar_w + gap)
        rr(draw, (x, base_y - h, x + bar_w, base_y), fill=rgb(PAL["blue"]), radius=4)
        draw.text((x - 2, base_y + 8), str(value), font=F["small"], fill=rgb(PAL["blue"]))
    draw.text((600, 322), "目标：降低组内计算量方差", font=F["note"], fill=rgb(PAL["green"]))

    caption = "图注建议：该图说明 length-aligned grouping 的候选机制；图中柱高代表 prompt token 数，分组收益需用服务尾延迟与 tokens/s 消融验证。"
    draw.text((55, height - 32), caption, font=F["label"], fill=rgb(PAL["muted"]))
    img.save(OUT_DIR / "data_organization_length_align_mechanism.png")

    body = [
        svg_text(width // 2, 34, title, 26, PAL["ink"], True, "middle"),
        svg_text(width // 2, 60, subtitle, 14, PAL["muted"], False, "middle"),
        svg_text(x1, 111, "原始顺序：长短请求混杂", 16, PAL["ink"], True),
        svg_text(x2, 111, "排序分组：短中长请求分区", 16, PAL["ink"], True),
    ]
    for idx, value in enumerate(values):
        h = int(max_h * value / 950)
        x = x1 + idx * (bar_w + gap)
        body.append(svg_rect(x, base_y - h, bar_w, h, PAL["gray"], rx=4))
        body.append(svg_text(x - 2, base_y + 21, str(value), 10, PAL["muted"]))
    body.append(svg_text(120, 337, "风险：短请求等待长请求完成", 11, PAL["red"]))
    body.append(svg_arrow(455, 202, 510, 202))
    for label, start, end, color in groups:
        gx = x2 + start * (bar_w + gap)
        gw = (end - start) * (bar_w + gap) - gap + bar_w
        body.append(f'<line x1="{gx}" y1="130" x2="{gx+gw}" y2="130" stroke="{color}" stroke-width="4"/>')
        body.append(svg_text(gx + gw // 2, 123, label, 11, color, False, "middle"))
    for idx, value in enumerate(sorted_values):
        h = int(max_h * value / 950)
        x = x2 + idx * (bar_w + gap)
        body.append(svg_rect(x, base_y - h, bar_w, h, PAL["blue"], rx=4))
        body.append(svg_text(x - 2, base_y + 21, str(value), 10, PAL["blue"]))
    body.append(svg_text(600, 337, "目标：降低组内计算量方差", 11, PAL["green"]))
    body.append(svg_text(55, height - 13, caption, 12, PAL["muted"]))
    svg_path = OUT_DIR / "data_organization_length_align_mechanism.svg"
    write_svg(svg_path, width, height, body)
    validate(svg_path)


def prefix_aware() -> None:
    width, height = 1060, 430
    img = Image.new("RGB", (width, height), rgb(PAL["bg"]))
    draw = ImageDraw.Draw(img)
    title = "数据组织策略三：按共享 prefix 分组"
    subtitle = "候选机制：把相同 system prompt 的行聚合提交，为 prefix caching 创造命中条件"
    add_title(draw, width, title, subtitle)

    left_x, right_x, top = 60, 575, 94
    draw.text((left_x, top), "随机提交：共享 prefix 被打散", font=F["section"], fill=rgb(PAL["ink"]))
    draw.text((right_x, top), "prefix 分组：相同前缀集中提交", font=F["section"], fill=rgb(PAL["ink"]))

    rows = [
        ("sys A", "Q1", PAL["blue"]),
        ("sys B", "Q2", PAL["orange"]),
        ("sys A", "Q3", PAL["blue"]),
        ("sys C", "Q4", PAL["green"]),
        ("sys B", "Q5", PAL["orange"]),
        ("sys A", "Q6", PAL["blue"]),
        ("sys A", "Q7", PAL["blue"]),
        ("sys B", "Q8", PAL["orange"]),
    ]
    row_h, row_gap, prefix_w, suffix_w = 24, 6, 160, 190
    y0 = 132
    for idx, (prefix, query, color) in enumerate(rows):
        y = y0 + idx * (row_h + row_gap)
        rr(draw, (left_x, y, left_x + prefix_w, y + row_h), fill=rgb(color), radius=5)
        rr(draw, (left_x + prefix_w, y, left_x + prefix_w + suffix_w, y + row_h), fill=rgb(PAL["gray"]), radius=5)
        centered_text(draw, left_x + prefix_w // 2, y + 4, prefix, F["small"], rgb(PAL["white"]))
        centered_text(draw, left_x + prefix_w + suffix_w // 2, y + 4, query, F["small"], rgb(PAL["white"]))
    draw.text((105, 382), "风险：prefix 相同但缓存命中机会分散", font=F["note"], fill=rgb(PAL["red"]))

    arrow(draw, 450, 242, 525, 242)

    groups = [
        ("sys A", "4 行：Q1 / Q3 / Q6 / Q7", PAL["blue"], PAL["blue_fill"]),
        ("sys B", "3 行：Q2 / Q5 / Q8", PAL["orange"], PAL["orange_fill"]),
        ("sys C", "1 行：Q4", PAL["green"], PAL["green_fill"]),
    ]
    box_w, box_h, box_gap = 405, 76, 16
    for idx, (prefix, queries, stroke, fill) in enumerate(groups):
        y = y0 + idx * (box_h + box_gap)
        rr(draw, (right_x, y, right_x + box_w, y + box_h), fill=rgb(fill), outline=rgb(stroke), width=2, radius=8)
        rr(draw, (right_x + 3, y + 3, right_x + 9, y + box_h - 3), fill=rgb(stroke), radius=3)
        draw.text((right_x + 22, y + 12), prefix, font=F["section"], fill=rgb(stroke))
        draw.text((right_x + 22, y + 39), queries, font=F["label"], fill=rgb(PAL["ink"]))
        rr(draw, (right_x + 265, y + 13, right_x + 382, y + 37), fill=rgb(PAL["green"]), radius=5)
        centered_text(draw, right_x + 323, y + 17, "缓存命中条件", F["small"], rgb(PAL["white"]))

    caption = "图注建议：该图说明 prefix-aware grouping 只是上游创造 prefix locality；最终是否提高 APC 命中率和端到端性能必须由 vLLM 指标验证。"
    draw.text((55, height - 34), caption, font=F["label"], fill=rgb(PAL["muted"]))
    img.save(OUT_DIR / "data_organization_prefix_aware_mechanism.png")

    body = [
        svg_text(width // 2, 34, title, 26, PAL["ink"], True, "middle"),
        svg_text(width // 2, 60, subtitle, 14, PAL["muted"], False, "middle"),
        svg_text(left_x, top + 19, "随机提交：共享 prefix 被打散", 16, PAL["ink"], True),
        svg_text(right_x, top + 19, "prefix 分组：相同前缀集中提交", 16, PAL["ink"], True),
    ]
    for idx, (prefix, query, color) in enumerate(rows):
        y = y0 + idx * (row_h + row_gap)
        body.append(svg_rect(left_x, y, prefix_w, row_h, color, rx=5))
        body.append(svg_rect(left_x + prefix_w, y, suffix_w, row_h, PAL["gray"], rx=5))
        body.append(svg_text(left_x + prefix_w // 2, y + 17, prefix, 10, PAL["white"], False, "middle"))
        body.append(svg_text(left_x + prefix_w + suffix_w // 2, y + 17, query, 10, PAL["white"], False, "middle"))
    body.append(svg_text(105, 397, "风险：prefix 相同但缓存命中机会分散", 11, PAL["red"]))
    body.append(svg_arrow(450, 242, 525, 242))
    for idx, (prefix, queries, stroke, fill) in enumerate(groups):
        y = y0 + idx * (box_h + box_gap)
        body.append(svg_rect(right_x, y, box_w, box_h, fill, stroke, 2, 8))
        body.append(svg_rect(right_x + 3, y + 3, 6, box_h - 6, stroke, rx=3))
        body.append(svg_text(right_x + 22, y + 31, prefix, 16, stroke, True))
        body.append(svg_text(right_x + 22, y + 56, queries, 12, PAL["ink"]))
        body.append(svg_rect(right_x + 265, y + 13, 117, 24, PAL["green"], rx=5))
        body.append(svg_text(right_x + 323, y + 29, "缓存命中条件", 10, PAL["white"], False, "middle"))
    body.append(svg_text(55, height - 14, caption, 12, PAL["muted"]))
    svg_path = OUT_DIR / "data_organization_prefix_aware_mechanism.svg"
    write_svg(svg_path, width, height, body)
    validate(svg_path)


def token_budget_v2() -> None:
    width, height = 1120, 460
    img = Image.new("RGB", (width, height), rgb(PAL["bg"]))
    draw = ImageDraw.Draw(img)
    title = "数据组织策略一：按 token 预算构造请求"
    subtitle = "候选机制：先估计每行 token 成本，再按预算形成上游提交"
    add_title(draw, width, title, subtitle)

    left = (55, 86, 510, 350)
    right = (610, 86, 1065, 350)
    rr(draw, left, fill=rgb(PAL["red_fill"]), radius=10)
    rr(draw, right, fill=rgb(PAL["green_fill"]), radius=10)
    draw.text((78, 106), "固定行数：batch_size = 4", font=F["section"], fill=rgb(PAL["ink"]))
    draw.text((633, 106), "token-budget：max_tokens = 600", font=F["section"], fill=rgb(PAL["ink"]))

    rows = [("row1", 50), ("row2", 200), ("row3", 800), ("row4", 150)]
    card = (82, 142, 480, 312)
    rr(draw, card, fill=rgb(PAL["white"]), outline=rgb(PAL["red"]), width=2, radius=8)
    rr(draw, (98, 155, 232, 180), fill=rgb(PAL["red"]), radius=5)
    centered_text(draw, 165, 160, "总量 1200 tokens", F["small"], rgb(PAL["white"]))
    draw.text((250, 160), "行数固定，但计算量不可控", font=F["note"], fill=rgb(PAL["red"]))

    label_x, bar_x, bar_y = 103, 205, 196
    bar_w, bar_h, row_gap = 220, 16, 10
    for idx, (name, tokens) in enumerate(rows):
        y = bar_y + idx * (bar_h + row_gap)
        draw.text((label_x, y + 1), f"{name}: {tokens}", font=F["small"], fill=rgb(PAL["ink"]))
        bw = max(14, int(bar_w * tokens / 800))
        rr(draw, (bar_x, y, bar_x + bw, y + bar_h), fill=rgb(PAL["blue"]), radius=4)
    draw.text((98, 325), "问题：短行和长行被一次性合并，尾部成本由长行主导", font=F["note"], fill=rgb(PAL["red"]))

    arrow(draw, 536, 208, 584, 208)

    def submission_card(x, y, h, title_text, rows_text, outline, fill, note=None):
        rr(draw, (x, y, x + 380, y + h), fill=rgb(fill), outline=rgb(outline), width=2, radius=8)
        draw.text((x + 18, y + 12), title_text, font=F["label"], fill=rgb(outline))
        px = x + 18
        for label, tokens in rows_text:
            pill_w = max(54, int(150 * tokens / 800))
            rr(draw, (px, y + 40, px + pill_w, y + 62), fill=rgb(PAL["blue"]), radius=5)
            centered_text(draw, px + pill_w // 2, y + 44, f"{label}({tokens})", F["small"], rgb(PAL["white"]))
            px += pill_w + 10
        if note:
            draw.text((x + 18, y + h - 22), note, font=F["note"], fill=rgb(outline))

    submission_card(
        640,
        145,
        88,
        "提交 A：400 tokens <= 600",
        [("row1", 50), ("row2", 200), ("row4", 150)],
        PAL["green"],
        PAL["blue_fill"],
        "短行合并，提高单次提交装载率",
    )
    submission_card(
        640,
        252,
        66,
        "提交 B：长行单独处理",
        [("row3", 800)],
        PAL["orange"],
        PAL["orange_fill"],
    )
    draw.text((825, 297), "超过预算时不再拖住短行", font=F["note"], fill=rgb(PAL["orange"]))

    caption = "图注建议：该图说明 token-budget batching 如何把固定行数提交改为计算量预算提交；收益仍需用 token tail、queue wait、tokens/s 和端到端延迟验证。"
    draw.text((55, height - 34), caption, font=F["label"], fill=rgb(PAL["muted"]))
    img.save(OUT_DIR / "data_organization_token_budget_mechanism.png")

    body = [
        svg_text(width // 2, 34, title, 26, PAL["ink"], True, "middle"),
        svg_text(width // 2, 60, subtitle, 14, PAL["muted"], False, "middle"),
        svg_rect(*left[:2], left[2] - left[0], left[3] - left[1], PAL["red_fill"], rx=10),
        svg_rect(*right[:2], right[2] - right[0], right[3] - right[1], PAL["green_fill"], rx=10),
        svg_text(78, 124, "固定行数：batch_size = 4", 16, PAL["ink"], True),
        svg_text(633, 124, "token-budget：max_tokens = 600", 16, PAL["ink"], True),
        svg_rect(*card[:2], card[2] - card[0], card[3] - card[1], PAL["white"], PAL["red"], 2, 8),
        svg_rect(98, 155, 134, 25, PAL["red"], rx=5),
        svg_text(165, 172, "总量 1200 tokens", 10, PAL["white"], False, "middle"),
        svg_text(250, 174, "行数固定，但计算量不可控", 11, PAL["red"]),
    ]
    for idx, (name, tokens) in enumerate(rows):
        y = bar_y + idx * (bar_h + row_gap)
        bw = max(14, int(bar_w * tokens / 800))
        body.append(svg_text(label_x, y + 14, f"{name}: {tokens}", 10, PAL["ink"]))
        body.append(svg_rect(bar_x, y, bw, bar_h, PAL["blue"], rx=4))
    body.append(svg_text(98, 339, "问题：短行和长行被一次性合并，尾部成本由长行主导", 11, PAL["red"]))
    body.append(svg_arrow(536, 208, 584, 208))

    def svg_submission(x, y, h, title_text, rows_text, outline, fill, note=None):
        parts = [svg_rect(x, y, 380, h, fill, outline, 2, 8), svg_text(x + 18, y + 29, title_text, 12, outline)]
        px = x + 18
        for label, tokens in rows_text:
            pill_w = max(54, int(150 * tokens / 800))
            parts.append(svg_rect(px, y + 40, pill_w, 22, PAL["blue"], rx=5))
            parts.append(svg_text(px + pill_w // 2, y + 55, f"{label}({tokens})", 10, PAL["white"], False, "middle"))
            px += pill_w + 10
        if note:
            parts.append(svg_text(x + 18, y + h - 8, note, 11, outline))
        return parts

    body.extend(
        svg_submission(
            640,
            145,
            88,
            "提交 A：400 tokens <= 600",
            [("row1", 50), ("row2", 200), ("row4", 150)],
            PAL["green"],
            PAL["blue_fill"],
            "短行合并，提高单次提交装载率",
        )
    )
    body.extend(
        svg_submission(
            640,
            252,
            66,
            "提交 B：长行单独处理",
            [("row3", 800)],
            PAL["orange"],
            PAL["orange_fill"],
        )
    )
    body.append(svg_text(825, 311, "超过预算时不再拖住短行", 11, PAL["orange"]))
    body.append(svg_text(55, height - 14, caption, 12, PAL["muted"]))
    svg_path = OUT_DIR / "data_organization_token_budget_mechanism.svg"
    write_svg(svg_path, width, height, body)
    validate(svg_path)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    token_budget_v2()
    length_align()
    prefix_aware()
    print("Generated data organization mechanism figures:")
    for name in [
        "data_organization_token_budget_mechanism",
        "data_organization_length_align_mechanism",
        "data_organization_prefix_aware_mechanism",
    ]:
        print(f"- figures/architecture/{name}.png")
        print(f"- figures/architecture/{name}.svg")


if __name__ == "__main__":
    main()
