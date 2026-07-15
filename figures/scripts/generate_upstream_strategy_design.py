from pathlib import Path
from xml.sax.saxutils import escape

from PIL import Image, ImageDraw, ImageFont


OUT_DIR = Path(__file__).resolve().parents[1] / "architecture"
PNG_PATH = OUT_DIR / "upstream_strategy_design.png"
SVG_PATH = OUT_DIR / "upstream_strategy_design.svg"

W, H = 1700, 1040

PAL = {
    "bg": "#F8FAFC",
    "ink": "#1F2937",
    "muted": "#334155",
    "line": "#64748B",
    "panel": "#F1F5F9",
    "card": "#FFFFFF",
    "pill": "#FFFFFF",
    "pill_edge": "#CBD5E1",
    "blue_fill": "#E7F0FF",
    "blue": "#2F6FEB",
    "orange_fill": "#FFF1E6",
    "orange": "#F97316",
    "purple_fill": "#F2ECFF",
    "purple": "#7C3AED",
    "green_fill": "#F0FDF4",
    "green": "#16A34A",
    "warn_fill": "#FFF7D6",
    "warn": "#D6A000",
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
    "title": _font(30, True),
    "sub": _font(16),
    "panel": _font(19, True),
    "card": _font(17, True),
    "pill": _font(14),
    "small": _font(12),
    "cap": _font(13),
}


def text_size(draw, text, font):
    box = draw.textbbox((0, 0), text, font=font)
    return box[2] - box[0], box[3] - box[1]


def rrect(draw, box, fill, edge, radius=14, width=1):
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=edge, width=width)


def center_text(draw, box, text, font, fill=PAL["ink"]):
    x1, y1, x2, y2 = box
    tw, th = text_size(draw, text, font)
    draw.text((x1 + (x2 - x1 - tw) / 2, y1 + (y2 - y1 - th) / 2 - 1), text, font=font, fill=fill)


def pill(draw, x, y, w, h, text, fill=PAL["pill"], edge=PAL["pill_edge"], color=PAL["muted"]):
    rrect(draw, (x, y, x + w, y + h), fill, edge, radius=6, width=1)
    center_text(draw, (x, y, x + w, y + h), text, F["pill"], color)


def draw_card(draw, x, y, w, h, title, items, edge, fill="#FFFFFF", tag=None):
    rrect(draw, (x, y, x + w, y + h), fill, edge, radius=12, width=2)
    draw.text((x + 22, y + 18), title, font=F["card"], fill=edge)
    if tag:
        tw, _ = text_size(draw, tag, F["small"])
        draw.text((x + w - tw - 18, y + 22), tag, font=F["small"], fill=PAL["muted"])
    py = y + 56
    for item in items:
        pill(draw, x + 18, py, w - 36, 26, item)
        py += 32


def arrow(draw, x1, y1, x2, y2, color=PAL["line"]):
    draw.line((x1, y1, x2, y2), fill=color, width=3)
    if x2 >= x1:
        head = [(x2, y2), (x2 - 12, y2 - 7), (x2 - 12, y2 + 7)]
    else:
        head = [(x2, y2), (x2 + 12, y2 - 7), (x2 + 12, y2 + 7)]
    draw.polygon(head, fill=color)


def draw_png():
    img = Image.new("RGB", (W, H), PAL["bg"])
    d = ImageDraw.Draw(img)

    d.text((64, 34), "策略设计：面向已定位瓶颈的上游优化", font=F["title"], fill=PAL["ink"])
    d.text(
        (66, 74),
        "阶段画像用于前置诊断；策略设计从已确定瓶颈出发，选择数据组织与模型服务调度动作，并用端到端指标验证收益。",
        font=F["sub"],
        fill=PAL["muted"],
    )

    panels = [
        (80, 126, 420, 704, "前置诊断结论"),
        (560, 126, 680, 704, "针对瓶颈的优化设计"),
        (1300, 126, 320, 704, "配置与验证"),
    ]
    for x, y, w, h, title in panels:
        rrect(d, (x, y, x + w, y + h), PAL["panel"], PAL["pill_edge"], radius=16, width=1)
        d.text((x + 24, y + 18), title, font=F["panel"], fill=PAL["ink"])

    diag_cards = [
        (110, 184, 360, 160, "数据组织瓶颈", [
            "batch / partition 不匹配",
            "operator 调用粒度过细",
            "object / fan-in 过多",
        ], PAL["blue"]),
        (110, 390, 360, 160, "模型服务瓶颈", [
            "queue wait / backlog 过高",
            "GPU 利用率不足",
            "endpoint routing 不均衡",
        ], PAL["orange"]),
        (110, 596, 360, 160, "workload 特异压力", [
            "FILTER：selectivity 未知",
            "COMPLETE：token 长尾",
            "EMBED：输出写回敏感",
        ], PAL["purple"]),
    ]
    for card in diag_cards:
        draw_card(d, *card)

    method_cards = [
        (590, 174, 610, 190, "数据组织优化动作", [
            "调 batch size / partition count",
            "合并 operator invocation，减少小请求",
            "控制 object 数与 fan-in 形态",
            "按 selectivity / prefix / 输出大小重排",
        ], PAL["blue"], PAL["blue_fill"]),
        (590, 382, 610, 190, "模型服务调度动作", [
            "调 actor pool 与 bounded in-flight",
            "按 queue wait / backlog 做路由",
            "token-aware / prefix-aware dispatch",
            "避免把等待堆到模型服务队列",
        ], PAL["orange"], PAL["orange_fill"], "重点"),
        (590, 590, 610, 190, "写回约束处理", [
            "COPY + deferred index 作为工程 baseline",
            "sink mode / write batch rows 对比",
            "writeback ratio 高时切换 worker-direct",
            "判断上游收益是否被持久化吞噬",
        ], PAL["purple"], PAL["purple_fill"], "支撑"),
    ]
    for card in method_cards:
        draw_card(d, *card)

    out_cards = [
        (1330, 206, 260, 190, "优化执行配置", [
            "batch / partition",
            "actor pool / in-flight",
            "routing / dispatch",
            "fan-in / sink mode",
        ], PAL["blue"]),
        (1330, 470, 260, 190, "端到端验证", [
            "latency / rows per second",
            "tokens per second / queue wait",
            "model wall / writeback ratio",
            "GPU utilization",
        ], PAL["green"], PAL["green_fill"]),
    ]
    for card in out_cards:
        draw_card(d, *card)

    arrows = [
        ("data bottleneck", 470, 264, 590, 264),
        ("service bottleneck", 470, 470, 590, 470),
        ("workload pressure", 470, 676, 590, 676),
        ("method to config", 1200, 348, 1330, 301),
        ("method to validation", 1200, 558, 1330, 565),
    ]
    for _, x1, y1, x2, y2 in arrows:
        arrow(d, x1, y1, x2, y2)

    # Bottom note: feedback is drawn as a label rather than a long crossing arrow.
    rrect(d, (590, 782, 1000, 816), PAL["green_fill"], PAL["green"], radius=8, width=1)
    center_text(d, (590, 782, 1000, 816), "验证结果回填规则表，进入下一轮参数 sweep", F["small"], PAL["green"])

    d.text(
        (118, 882),
        "图注：策略设计图。阶段画像只产生左侧的已定位瓶颈；真正的策略设计是中间针对瓶颈选择优化动作，",
        font=F["cap"],
        fill=PAL["muted"],
    )
    d.text(
        (118, 906),
        "再输出执行配置并用包含写回的端到端指标验证收益是否成立。",
        font=F["cap"],
        fill=PAL["muted"],
    )

    img.save(PNG_PATH)
    boxes = {
        "diag_data": diag_cards[0][:4],
        "diag_service": diag_cards[1][:4],
        "diag_workload": diag_cards[2][:4],
        "method_data": method_cards[0][:4],
        "method_service": method_cards[1][:4],
        "method_writeback": method_cards[2][:4],
        "out_config": out_cards[0][:4],
        "out_verify": out_cards[1][:4],
        "feedback": (590, 782, 410, 34),
    }
    return {"boxes": boxes, "arrows": arrows}


def svg_text(x, y, text, size=14, weight="400", fill=PAL["ink"], anchor="start"):
    return f'<text x="{x}" y="{y}" font-family="Microsoft YaHei,Arial,sans-serif" font-size="{size}" font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{escape(text)}</text>'


def svg_rect(x, y, w, h, fill, stroke, r=12, sw=1):
    return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill}" stroke="{stroke}" stroke-width="{sw}"/>'


def svg_center(x, y, w, h, text, size=14, weight="400", fill=PAL["ink"]):
    return svg_text(x + w / 2, y + h / 2 + size * 0.35, text, size, weight, fill, "middle")


def svg_pill(x, y, w, h, text):
    return "\n".join([
        svg_rect(x, y, w, h, PAL["pill"], PAL["pill_edge"], r=6, sw=1),
        svg_center(x, y, w, h, text, 14, fill=PAL["muted"]),
    ])


def svg_card(x, y, w, h, title, items, edge, fill="#FFFFFF", tag=None):
    parts = [svg_rect(x, y, w, h, fill, edge, r=12, sw=2), svg_text(x + 22, y + 31, title, 17, "700", edge)]
    if tag:
        parts.append(svg_text(x + w - 18, y + 31, tag, 12, fill=PAL["muted"], anchor="end"))
    py = y + 56
    for item in items:
        parts.append(svg_pill(x + 18, py, w - 36, 26, item))
        py += 32
    return "\n".join(parts)


def svg_arrow(x1, y1, x2, y2, color=PAL["line"]):
    if x2 >= x1:
        head = f"{x2},{y2} {x2 - 12},{y2 - 7} {x2 - 12},{y2 + 7}"
    else:
        head = f"{x2},{y2} {x2 + 12},{y2 - 7} {x2 + 12},{y2 + 7}"
    return f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="3"/>\n<polygon points="{head}" fill="{color}"/>'


def draw_svg():
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
        f'<rect width="{W}" height="{H}" fill="{PAL["bg"]}"/>',
        svg_text(64, 64, "策略设计：面向已定位瓶颈的上游优化", 30, "700"),
        svg_text(66, 96, "阶段画像用于前置诊断；策略设计从已确定瓶颈出发，选择数据组织与模型服务调度动作，并用端到端指标验证收益。", 16, fill=PAL["muted"]),
    ]

    for x, y, w, h, title in [
        (80, 126, 420, 704, "前置诊断结论"),
        (560, 126, 680, 704, "针对瓶颈的优化设计"),
        (1300, 126, 320, 704, "配置与验证"),
    ]:
        parts.append(svg_rect(x, y, w, h, PAL["panel"], PAL["pill_edge"], r=16, sw=1))
        parts.append(svg_text(x + 24, y + 42, title, 19, "700"))

    for x, y, w, h, title, items, edge, *rest in [
        (110, 184, 360, 160, "数据组织瓶颈", ["batch / partition 不匹配", "operator 调用粒度过细", "object / fan-in 过多"], PAL["blue"]),
        (110, 390, 360, 160, "模型服务瓶颈", ["queue wait / backlog 过高", "GPU 利用率不足", "endpoint routing 不均衡"], PAL["orange"]),
        (110, 596, 360, 160, "workload 特异压力", ["FILTER：selectivity 未知", "COMPLETE：token 长尾", "EMBED：输出写回敏感"], PAL["purple"]),
        (590, 174, 610, 190, "数据组织优化动作", ["调 batch size / partition count", "合并 operator invocation，减少小请求", "控制 object 数与 fan-in 形态", "按 selectivity / prefix / 输出大小重排"], PAL["blue"], PAL["blue_fill"]),
        (590, 382, 610, 190, "模型服务调度动作", ["调 actor pool 与 bounded in-flight", "按 queue wait / backlog 做路由", "token-aware / prefix-aware dispatch", "避免把等待堆到模型服务队列"], PAL["orange"], PAL["orange_fill"], "重点"),
        (590, 590, 610, 190, "写回约束处理", ["COPY + deferred index 作为工程 baseline", "sink mode / write batch rows 对比", "writeback ratio 高时切换 worker-direct", "判断上游收益是否被持久化吞噬"], PAL["purple"], PAL["purple_fill"], "支撑"),
        (1330, 206, 260, 190, "优化执行配置", ["batch / partition", "actor pool / in-flight", "routing / dispatch", "fan-in / sink mode"], PAL["blue"]),
        (1330, 470, 260, 190, "端到端验证", ["latency / rows per second", "tokens per second / queue wait", "model wall / writeback ratio", "GPU utilization"], PAL["green"], PAL["green_fill"]),
    ]:
        fill = rest[0] if len(rest) >= 1 else PAL["card"]
        tag = rest[1] if len(rest) >= 2 else None
        parts.append(svg_card(x, y, w, h, title, items, edge, fill, tag))

    for _, x1, y1, x2, y2 in [
        ("data bottleneck", 470, 264, 590, 264),
        ("service bottleneck", 470, 470, 590, 470),
        ("workload pressure", 470, 676, 590, 676),
        ("method to config", 1200, 348, 1330, 301),
        ("method to validation", 1200, 558, 1330, 565),
    ]:
        parts.append(svg_arrow(x1, y1, x2, y2))

    parts.append(svg_rect(590, 782, 410, 34, PAL["green_fill"], PAL["green"], r=8, sw=1))
    parts.append(svg_center(590, 782, 410, 34, "验证结果回填规则表，进入下一轮参数 sweep", 12, fill=PAL["green"]))
    parts.append(svg_text(118, 896, "图注：策略设计图。阶段画像只产生左侧的已定位瓶颈；真正的策略设计是中间针对瓶颈选择优化动作，", 13, fill=PAL["muted"]))
    parts.append(svg_text(118, 920, "再输出执行配置并用包含写回的端到端指标验证收益是否成立。", 13, fill=PAL["muted"]))
    parts.append("</svg>")
    SVG_PATH.write_text("\n".join(parts), encoding="utf-8")


def _rgb(hex_color):
    return tuple(int(hex_color[i:i + 2], 16) for i in (1, 3, 5))


def _line_intersects_rect(x1, y1, x2, y2, rect, pad=6):
    rx, ry, rw, rh = rect
    rx1, ry1 = rx + pad, ry + pad
    rx2, ry2 = rx + rw - pad, ry + rh - pad
    if x1 == x2:
        return rx1 <= x1 <= rx2 and not (max(y1, y2) < ry1 or min(y1, y2) > ry2)
    if y1 == y2:
        return ry1 <= y1 <= ry2 and not (max(x1, x2) < rx1 or min(x1, x2) > rx2)
    # Conservative sampled check for rare diagonal arrows.
    for i in range(1, 40):
        t = i / 40
        x = x1 + (x2 - x1) * t
        y = y1 + (y2 - y1) * t
        if rx1 <= x <= rx2 and ry1 <= y <= ry2:
            return True
    return False


def self_check(params):
    img = Image.open(PNG_PATH).convert("RGB")
    px = img.load()
    checks = []
    for name, box in params["boxes"].items():
        x, y, w, h = map(int, box)
        inside = x >= 0 and y >= 0 and x + w <= W and y + h <= H
        # Count non-background edge pixels around the box as a cheap border visibility check.
        count = 0
        for xx in range(max(0, x - 2), min(W, x + w + 3)):
            for yy in [max(0, y - 2), min(H - 1, y + h + 2)]:
                if px[xx, yy] != _rgb(PAL["bg"]):
                    count += 1
        for yy in range(max(0, y - 2), min(H, y + h + 3)):
            for xx in [max(0, x - 2), min(W - 1, x + w + 2)]:
                if px[xx, yy] != _rgb(PAL["bg"]):
                    count += 1
        ok = inside and count > 100
        checks.append(ok)
        print(f"  {name}: border_px={count} bounds={'OK' if inside else 'OUT'} {'PASS' if ok else 'FAIL'}")

    # Arrows may touch their source/target rectangles at the edge, but must not run through unrelated cards.
    for arrow_name, x1, y1, x2, y2 in params["arrows"]:
        in_canvas = 0 <= x1 <= W and 0 <= x2 <= W and 0 <= y1 <= H and 0 <= y2 <= H
        hits = []
        for box_name, rect in params["boxes"].items():
            if _line_intersects_rect(x1, y1, x2, y2, rect):
                hits.append(box_name)
        ok = in_canvas and not hits
        checks.append(ok)
        print(f"  arrow {arrow_name}: canvas={'OK' if in_canvas else 'OUT'} overlaps={hits if hits else 'none'} {'PASS' if ok else 'FAIL'}")

    if not all(checks):
        raise SystemExit("Self-check failed.")
    print("Self-check complete.")


if __name__ == "__main__":
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    params = draw_png()
    draw_svg()
    print(f"PNG: {PNG_PATH}")
    print(f"SVG: {SVG_PATH}")
    self_check(params)
