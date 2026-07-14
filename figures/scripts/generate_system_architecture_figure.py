from pathlib import Path
from xml.sax.saxutils import escape

from PIL import Image, ImageDraw, ImageFont


OUT_DIR = Path(__file__).resolve().parents[1] / "architecture"
SVG_PATH = OUT_DIR / "system_architecture_ai_data_execution.svg"
PNG_PATH = OUT_DIR / "system_architecture_ai_data_execution.png"

W, H = 1600, 1000

COLORS = {
    "bg": "#F8FAFC",
    "ink": "#1F2937",
    "muted": "#667085",
    "line": "#8A95A6",
    "db": "#EEF2F6",
    "db_edge": "#64748B",
    "data": "#E7F0FF",
    "data_edge": "#2F6FEB",
    "exec": "#EAF7EF",
    "exec_edge": "#16A34A",
    "model": "#FFF1E6",
    "model_edge": "#F97316",
    "sink": "#F2ECFF",
    "sink_edge": "#7C3AED",
    "policy": "#FFF8D9",
    "policy_edge": "#D6A000",
    "research": "#FFFFFF",
    "research_edge": "#CBD5E1",
    "pill": "#FFFFFF",
}


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
    "title": font(32, True),
    "subtitle": font(17),
    "stage": font(21, True),
    "stage_compact": font(19, True),
    "body": font(16),
    "small": font(13),
    "badge": font(17, True),
}


def text_size(draw: ImageDraw.ImageDraw, text: str, fnt: ImageFont.FreeTypeFont) -> tuple[int, int]:
    x1, y1, x2, y2 = draw.textbbox((0, 0), text, font=fnt)
    return x2 - x1, y2 - y1


def center_text(draw: ImageDraw.ImageDraw, box, text: str, fnt, fill=COLORS["ink"]):
    x1, y1, x2, y2 = box
    tw, th = text_size(draw, text, fnt)
    draw.text((x1 + (x2 - x1 - tw) / 2, y1 + (y2 - y1 - th) / 2 - 2), text, font=fnt, fill=fill)


def rounded(draw: ImageDraw.ImageDraw, box, fill, outline, width=2, radius=18):
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=width)


def arrow(draw: ImageDraw.ImageDraw, start, end, color=COLORS["line"], width=3):
    x1, y1 = start
    x2, y2 = end
    draw.line((x1, y1, x2, y2), fill=color, width=width)
    if abs(x2 - x1) >= abs(y2 - y1):
        sgn = 1 if x2 >= x1 else -1
        head = [(x2, y2), (x2 - sgn * 14, y2 - 7), (x2 - sgn * 14, y2 + 7)]
    else:
        sgn = 1 if y2 >= y1 else -1
        head = [(x2, y2), (x2 - 7, y2 - sgn * 14), (x2 + 7, y2 - sgn * 14)]
    draw.polygon(head, fill=color)


def draw_pill(draw: ImageDraw.ImageDraw, x, y, w, h, label, edge="#D0D7E2", fill=COLORS["pill"]):
    rounded(draw, (x, y, x + w, y + h), fill, edge, 1, 10)
    center_text(draw, (x, y, x + w, y + h), label, F["body"], COLORS["muted"])


def draw_policy_row(draw: ImageDraw.ImageDraw, labels, x, y, w, h):
    gap = 12
    pill_w = (w - gap * (len(labels) - 1)) / len(labels)
    for i, label in enumerate(labels):
        draw_pill(draw, x + i * (pill_w + gap), y, pill_w, h, label, edge="#E2BE45", fill="#FFFDF0")


def draw_badge(draw: ImageDraw.ImageDraw, cx, cy, label, color):
    draw.ellipse((cx - 17, cy - 17, cx + 17, cy + 17), fill=color)
    center_text(draw, (cx - 17, cy - 17, cx + 17, cy + 17), label, F["badge"], "#FFFFFF")


def draw_stage(draw, x, y, w, h, title, items, fill, edge, badges=()):
    rounded(draw, (x, y, x + w, y + h), fill, edge, 2, 18)
    title_box = (x + 20, y + 20, x + w - 20, y + 54)
    if badges:
        draw_badge(draw, x + 31, y + 34, badges[0], edge)
        title_box = (x + 58, y + 20, x + w - 18, y + 54)
    center_text(draw, title_box, title, F["stage_compact"] if badges else F["stage"])
    px, py = x + 26, y + 76
    pill_w, pill_h, gap_y = w - 52, 30, 8
    for item in items:
        draw_pill(draw, px, py, pill_w, pill_h, item, edge="#D8DEE8")
        py += pill_h + gap_y


def draw_research(draw, x, y, w, h, badge, title, items, color):
    rounded(draw, (x, y, x + w, y + h), COLORS["research"], COLORS["research_edge"], 2, 18)
    draw_badge(draw, x + 38, y + 34, badge, color)
    draw.text((x + 78, y + 22), title, font=F["stage"], fill=COLORS["ink"])
    py = y + 72
    for item in items:
        draw_pill(draw, x + 28, py, w - 56, 28, item, edge="#E1E7EF", fill="#F8FAFC")
        py += 38


def draw_png():
    img = Image.new("RGB", (W, H), COLORS["bg"])
    draw = ImageDraw.Draw(img)

    draw.text((64, 42), "数据库驱动 AI workload 的分布式数据执行与存储协同架构", font=F["title"], fill=COLORS["ink"])
    draw.text(
        (66, 88),
        "数据库提供 workload 入口和结果落点，数据组织、Ray 执行、GPU 服务和 AI 数据 sink 共同决定端到端性能。",
        font=F["subtitle"],
        fill=COLORS["muted"],
    )

    # Observation and policy layer.
    policy = (354, 132, 1470, 250)
    rounded(draw, policy, COLORS["policy"], COLORS["policy_edge"], 2, 18)
    center_text(draw, (policy[0], policy[1] + 14, policy[2], policy[1] + 48), "观测与策略层", F["stage"])
    draw_policy_row(
        draw,
        ["stage time", "queue wait", "GPU utilization", "writeback time", "batch", "routing", "in-flight"],
        414,
        192,
        996,
        32,
    )

    # Main pipeline.
    y, w, h = 318, 246, 226
    xs = [64, 354, 644, 934, 1224]
    stages = [
        ("Database source", ["PostgreSQL", "SQL workflow", "tables + rows", "triggers"], COLORS["db"], COLORS["db_edge"], ()),
        ("Daft + Arrow", ["RecordBatch", "batch", "partition", "shuffle"], COLORS["data"], COLORS["data_edge"], ("1",)),
        ("Ray execution", ["task", "actor pool", "object refs", "fan-in"], COLORS["exec"], COLORS["exec_edge"], ("2",)),
        ("GPU model service", ["embedding", "classify", "LLM", "queue"], COLORS["model"], COLORS["model_edge"], ("2",)),
        ("AI data sink", ["Lance", "pgvector", "PostgreSQL", "worker writeback"], COLORS["sink"], COLORS["sink_edge"], ("3",)),
    ]
    boxes = []
    for x, (title, items, fill, edge, badges) in zip(xs, stages):
        draw_stage(draw, x, y, w, h, title, items, fill, edge, badges)
        boxes.append((x, y, x + w, y + h))

    # Data-flow arrows: only in the horizontal channel between boxes.
    for left, right in zip(boxes, boxes[1:]):
        arrow(draw, (left[2] + 10, y + h // 2), (right[0] - 10, y + h // 2), COLORS["line"], 4)

    # Policy arrows: stop above stages and do not enter text.
    for x in [477, 767, 1057, 1347]:
        arrow(draw, (x, policy[3] + 8), (x, y - 16), COLORS["policy_edge"], 3)

    # Metric strip.
    metric = (160, 592, 1440, 652)
    rounded(draw, metric, "#FFFFFF", "#D0D7E2", 2, 15)
    center_text(
        draw,
        metric,
        "统一观测指标：e2e_s   rows/s   tokens/s   operator_wall_s   queue_wait   fan-in   writeback_s",
        F["body"],
    )

    # Research cards, no crossing arrows; badges link cards to stage boxes.
    ry, rw, rh = 714, 410, 208
    research = [
        (96, "1", "数据组织与批处理调度", ["workload 特征", "batch 与 partition", "RecordBatch 粒度"], COLORS["data_edge"]),
        (595, "2", "GPU 服务感知 Ray 调度", ["endpoint 状态", "actor pool 与 routing", "bounded in-flight"], COLORS["exec_edge"]),
        (1094, "3", "结果汇聚与持久化协同", ["fan-in 位置", "worker writeback", "Lance 与 pgvector"], COLORS["sink_edge"]),
    ]
    for x, badge, title, items, color in research:
        draw_research(draw, x, ry, rw, rh, badge, title, items, color)

    draw.text(
        (72, 952),
        "图注：数据库驱动 AI workload 进入 Daft/Arrow、Ray、GPU 服务和 AI 数据 sink 后，三个研究内容分别对应编号 1、2、3。",
        font=F["small"],
        fill=COLORS["muted"],
    )

    img.save(PNG_PATH)


def svg_text(x, y, text, size=16, weight="400", fill=COLORS["ink"], anchor="start"):
    return f'<text x="{x}" y="{y}" font-family="Microsoft YaHei, Arial, sans-serif" font-size="{size}" font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{escape(text)}</text>'


def svg_rect(x, y, w, h, fill, stroke, r=18, sw=2):
    return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill}" stroke="{stroke}" stroke-width="{sw}"/>'


def svg_center(x, y, w, h, text, size=16, weight="400", fill=COLORS["ink"]):
    return svg_text(x + w / 2, y + h / 2 + size * 0.35, text, size, weight, fill, "middle")


def svg_arrow(x1, y1, x2, y2, color=COLORS["line"], sw=3):
    if abs(x2 - x1) >= abs(y2 - y1):
        sgn = 1 if x2 >= x1 else -1
        pts = f"{x2},{y2} {x2 - sgn * 14},{y2 - 7} {x2 - sgn * 14},{y2 + 7}"
    else:
        sgn = 1 if y2 >= y1 else -1
        pts = f"{x2},{y2} {x2 - 7},{y2 - sgn * 14} {x2 + 7},{y2 - sgn * 14}"
    return f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{sw}"/><polygon points="{pts}" fill="{color}"/>'


def svg_pill(x, y, w, h, label, edge="#D0D7E2", fill="#FFFFFF"):
    return "\n".join([svg_rect(x, y, w, h, fill, edge, 10, 1), svg_center(x, y, w, h, label, 16, fill=COLORS["muted"])])


def svg_policy_row(labels, x, y, w, h):
    gap = 12
    pill_w = (w - gap * (len(labels) - 1)) / len(labels)
    return "\n".join(svg_pill(x + i * (pill_w + gap), y, pill_w, h, label, "#E2BE45", "#FFFDF0") for i, label in enumerate(labels))


def svg_badge(cx, cy, label, color):
    return f'<circle cx="{cx}" cy="{cy}" r="17" fill="{color}"/>' + svg_text(cx, cy + 6, label, 17, "700", "#FFFFFF", "middle")


def svg_stage(x, y, w, h, title, items, fill, edge, badges=()):
    parts = [svg_rect(x, y, w, h, fill, edge, 18, 2)]
    if badges:
        parts.append(svg_badge(x + 31, y + 34, badges[0], edge))
        parts.append(svg_center(x + 58, y + 20, w - 76, 34, title, 19, "700"))
    else:
        parts.append(svg_center(x + 20, y + 20, w - 40, 34, title, 21, "700"))
    for i, item in enumerate(items):
        parts.append(svg_pill(x + 26, y + 76 + i * 38, w - 52, 30, item, "#D8DEE8"))
    return "\n".join(parts)


def svg_research(x, y, w, h, badge, title, items, color):
    parts = [svg_rect(x, y, w, h, COLORS["research"], COLORS["research_edge"], 18, 2), svg_badge(x + 38, y + 34, badge, color)]
    parts.append(svg_text(x + 78, y + 45, title, 21, "700"))
    for i, item in enumerate(items):
        parts.append(svg_pill(x + 28, y + 72 + i * 38, w - 56, 28, item, "#E1E7EF", "#F8FAFC"))
    return "\n".join(parts)


def draw_svg():
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
        f'<rect width="{W}" height="{H}" fill="{COLORS["bg"]}"/>',
        svg_text(64, 74, "数据库驱动 AI workload 的分布式数据执行与存储协同架构", 32, "700"),
        svg_text(66, 112, "数据库提供 workload 入口和结果落点，数据组织、Ray 执行、GPU 服务和 AI 数据 sink 共同决定端到端性能。", 17, fill=COLORS["muted"]),
        svg_rect(354, 132, 1116, 118, COLORS["policy"], COLORS["policy_edge"], 18, 2),
        svg_center(354, 146, 1116, 34, "观测与策略层", 21, "700"),
    ]
    parts.append(svg_policy_row(["stage time", "queue wait", "GPU utilization", "writeback time", "batch", "routing", "in-flight"], 414, 192, 996, 32))

    y, w, h = 318, 246, 226
    xs = [64, 354, 644, 934, 1224]
    stages = [
        ("Database source", ["PostgreSQL", "SQL workflow", "tables + rows", "triggers"], COLORS["db"], COLORS["db_edge"], ()),
        ("Daft + Arrow", ["RecordBatch", "batch", "partition", "shuffle"], COLORS["data"], COLORS["data_edge"], ("1",)),
        ("Ray execution", ["task", "actor pool", "object refs", "fan-in"], COLORS["exec"], COLORS["exec_edge"], ("2",)),
        ("GPU model service", ["embedding", "classify", "LLM", "queue"], COLORS["model"], COLORS["model_edge"], ("2",)),
        ("AI data sink", ["Lance", "pgvector", "PostgreSQL", "worker writeback"], COLORS["sink"], COLORS["sink_edge"], ("3",)),
    ]
    boxes = []
    for x, (title, items, fill, edge, badges) in zip(xs, stages):
        parts.append(svg_stage(x, y, w, h, title, items, fill, edge, badges))
        boxes.append((x, y, x + w, y + h))
    for left, right in zip(boxes, boxes[1:]):
        parts.append(svg_arrow(left[2] + 10, y + h // 2, right[0] - 10, y + h // 2, COLORS["line"], 4))
    for x in [477, 767, 1057, 1347]:
        parts.append(svg_arrow(x, 258, x, y - 16, COLORS["policy_edge"], 3))

    parts.append(svg_rect(160, 592, 1280, 60, "#FFFFFF", "#D0D7E2", 15, 2))
    parts.append(svg_center(160, 592, 1280, 60, "统一观测指标：e2e_s   rows/s   tokens/s   operator_wall_s   queue_wait   fan-in   writeback_s", 16))

    for x, badge, title, items, color in [
        (96, "1", "数据组织与批处理调度", ["workload 特征", "batch 与 partition", "RecordBatch 粒度"], COLORS["data_edge"]),
        (595, "2", "GPU 服务感知 Ray 调度", ["endpoint 状态", "actor pool 与 routing", "bounded in-flight"], COLORS["exec_edge"]),
        (1094, "3", "结果汇聚与持久化协同", ["fan-in 位置", "worker writeback", "Lance 与 pgvector"], COLORS["sink_edge"]),
    ]:
        parts.append(svg_research(x, 714, 410, 208, badge, title, items, color))

    parts.append(svg_text(72, 952, "图注：数据库驱动 AI workload 进入 Daft/Arrow、Ray、GPU 服务和 AI 数据 sink 后，三个研究内容分别对应编号 1、2、3。", 13, fill=COLORS["muted"]))
    parts.append("</svg>")
    SVG_PATH.write_text("\n".join(parts), encoding="utf-8")


if __name__ == "__main__":
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    draw_png()
    draw_svg()
    print(SVG_PATH)
    print(PNG_PATH)
