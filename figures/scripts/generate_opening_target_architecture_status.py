from pathlib import Path
from xml.sax.saxutils import escape

from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[2]
ARCH_DIR = ROOT / "figures" / "architecture"
REPORT_DIR = ROOT / "opening" / "report" / "figures"
SVG_PATH = ARCH_DIR / "opening_target_architecture_status.svg"
PNG_PATH = ARCH_DIR / "opening_target_architecture_status.png"
REPORT_PATH = REPORT_DIR / "target_architecture_status.png"

W, H = 1600, 900

C = {
    "bg": "#F8FAFC",
    "ink": "#172033",
    "muted": "#5F6B7A",
    "line": "#7B8797",
    "blue": "#2563EB",
    "blue_fill": "#EAF2FF",
    "green": "#16845B",
    "green_fill": "#E9F7F0",
    "orange": "#C46A10",
    "orange_fill": "#FFF3E5",
    "purple": "#7656B3",
    "purple_fill": "#F2EDFF",
    "gray_fill": "#EEF2F6",
    "white": "#FFFFFF",
}


def font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    candidates = [
        "/System/Library/Fonts/STHeiti Medium.ttc" if bold else "/System/Library/Fonts/STHeiti Light.ttc",
        "/System/Library/Fonts/PingFang.ttc",
        "/Library/Fonts/Arial Unicode.ttf",
        r"C:\Windows\Fonts\msyhbd.ttc" if bold else r"C:\Windows\Fonts\msyh.ttc",
    ]
    for candidate in candidates:
        if Path(candidate).exists():
            return ImageFont.truetype(candidate, size=size)
    return ImageFont.load_default()


F = {
    "title": font(34, True),
    "subtitle": font(19),
    "lane": font(22, True),
    "box": font(19, True),
    "body": font(16),
    "small": font(14),
    "pill": font(14, True),
}


def center_text(draw, box, text, fnt, fill=C["ink"], spacing=5):
    x1, y1, x2, y2 = box
    bb = draw.multiline_textbbox((0, 0), text, font=fnt, spacing=spacing, align="center")
    tw, th = bb[2] - bb[0], bb[3] - bb[1]
    draw.multiline_text(
        (x1 + (x2 - x1 - tw) / 2, y1 + (y2 - y1 - th) / 2 - 2),
        text,
        font=fnt,
        fill=fill,
        spacing=spacing,
        align="center",
    )


def dashed_line(draw, xy, fill, width=3, dash=10, gap=7):
    x1, y1, x2, y2 = xy
    if y1 == y2:
        x = x1
        while x < x2:
            draw.line((x, y1, min(x + dash, x2), y2), fill=fill, width=width)
            x += dash + gap
    else:
        y = y1
        while y < y2:
            draw.line((x1, y, x2, min(y + dash, y2)), fill=fill, width=width)
            y += dash + gap


def dashed_rect(draw, box, fill, outline, width=3, dash=10, gap=7):
    x1, y1, x2, y2 = box
    draw.rounded_rectangle(box, radius=16, fill=fill)
    dashed_line(draw, (x1 + 16, y1, x2 - 16, y1), outline, width, dash, gap)
    dashed_line(draw, (x1 + 16, y2, x2 - 16, y2), outline, width, dash, gap)
    dashed_line(draw, (x1, y1 + 16, x1, y2 - 16), outline, width, dash, gap)
    dashed_line(draw, (x2, y1 + 16, x2, y2 - 16), outline, width, dash, gap)


def arrow(draw, x1, y, x2, color=C["line"]):
    draw.line((x1, y, x2, y), fill=color, width=4)
    draw.polygon([(x2, y), (x2 - 13, y - 7), (x2 - 13, y + 7)], fill=color)


def pill(draw, x, y, label, fill, edge, text_fill):
    bb = draw.textbbox((0, 0), label, font=F["pill"])
    w = bb[2] - bb[0] + 24
    h = 28
    draw.rounded_rectangle((x, y, x + w, y + h), radius=14, fill=fill, outline=edge, width=1)
    center_text(draw, (x, y, x + w, y + h), label, F["pill"], text_fill)


def stage(draw, box, title, detail, fill, edge, status, planned=False):
    if planned:
        dashed_rect(draw, box, fill, edge)
    else:
        draw.rounded_rectangle(box, radius=16, fill=fill, outline=edge, width=3)
    x1, y1, x2, y2 = box
    center_text(draw, (x1 + 10, y1 + 24, x2 - 10, y1 + 74), title, F["box"])
    center_text(draw, (x1 + 12, y1 + 82, x2 - 12, y2 - 18), detail, F["body"], C["muted"])
    if planned:
        pill(draw, x1 + 12, y1 + 10, status, C["orange_fill"], edge, edge)
    else:
        pill(draw, x1 + 12, y1 + 10, status, C["green_fill"], edge, edge)


def svg_text(x, y, value, size=16, weight="400", fill=C["ink"], anchor="start"):
    return (
        f'<text x="{x}" y="{y}" font-family="PingFang SC, Microsoft YaHei, Arial, sans-serif" '
        f'font-size="{size}" font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{escape(value)}</text>'
    )


def svg_multiline(cx, cy, lines, size, weight="400", fill=C["ink"], line_h=25):
    first_y = cy - (len(lines) - 1) * line_h / 2
    return "\n".join(svg_text(cx, first_y + i * line_h, line, size, weight, fill, "middle") for i, line in enumerate(lines))


def svg_rect(box, fill, edge, dashed=False, radius=16, width=3):
    x1, y1, x2, y2 = box
    dash_attr = ' stroke-dasharray="10 7"' if dashed else ""
    return f'<rect x="{x1}" y="{y1}" width="{x2-x1}" height="{y2-y1}" rx="{radius}" fill="{fill}" stroke="{edge}" stroke-width="{width}"{dash_attr}/>'


def svg_pill(x, y, label, fill, edge, text_fill):
    approx_w = len(label) * 15 + 24
    return "\n".join([
        f'<rect x="{x}" y="{y}" width="{approx_w}" height="28" rx="14" fill="{fill}" stroke="{edge}" stroke-width="1"/>',
        svg_text(x + approx_w / 2, y + 20, label, 14, "700", text_fill, "middle"),
    ])


def svg_stage(box, title, detail, fill, edge, status, planned=False):
    x1, y1, x2, y2 = box
    parts = [svg_rect(box, fill, edge, planned)]
    parts.append(svg_pill(x1 + 12, y1 + 10, status, C["orange_fill"] if planned else C["green_fill"], edge, edge))
    parts.append(svg_multiline((x1 + x2) / 2, y1 + 64, title.split("\n"), 19, "700", C["ink"], 23))
    parts.append(svg_multiline((x1 + x2) / 2, y1 + 124, detail.split("\n"), 16, "400", C["muted"], 23))
    return "\n".join(parts)


def svg_arrow(x1, y, x2, color=C["line"]):
    return (
        f'<line x1="{x1}" y1="{y}" x2="{x2}" y2="{y}" stroke="{color}" stroke-width="4"/>'
        f'<polygon points="{x2},{y} {x2-13},{y-7} {x2-13},{y+7}" fill="{color}"/>'
    )


TOP = [
    ((50, 152, 240, 326), "SQL AI 算子", "PostgreSQL 18.3\nplanner 可见", C["blue_fill"], C["orange"], "待实现"),
    ((270, 152, 460, 326), "关系 child plan", "snapshot、权限\n过滤与投影", C["blue_fill"], C["orange"], "待实现"),
    ((490, 152, 680, 326), "有界行流", "RowEnvelope\n取消与错误传播", C["blue_fill"], C["orange"], "待实现"),
    ((710, 152, 915, 326), "LOTUS sem_map", "v1.2.4 语义\nprompt 与输出", C["purple_fill"], C["orange"], "迁移中"),
    ((945, 152, 1240, 326), "可替换外部物理后端", "Daft / Ray / static / SAOR\n组织、提交与路由", C["orange_fill"], C["orange"], "候选组合"),
    ((1270, 152, 1550, 326), "模型执行与结果返回", "文本 vLLM\n图像 typed GPU actor", C["green_fill"], C["orange"], "待接入"),
]

BOTTOM = [
    ((50, 472, 240, 646), "PostgreSQL 外部读取", "当前由 runner\n读取固定输入", C["gray_fill"], C["green"], "已运行"),
    ((270, 472, 460, 646), "Daft / Arrow", "分区、批量\n列式交接", C["blue_fill"], C["green"], "已运行"),
    ((490, 472, 680, 646), "WorkDescriptor", "阶段工作量\n兼容性与局部性", C["blue_fill"], C["green"], "已运行"),
    ((710, 472, 915, 646), "提交与多 Job 控制", "static / shared credit\n状态仅部分驱动", C["orange_fill"], C["green"], "有证据"),
    ((945, 472, 1240, 646), "模型服务", "vLLM 文本服务\n图像 Ray GPU actor", C["green_fill"], C["green"], "已运行"),
    ((1270, 472, 1550, 646), "结果收集与写回", "结果归并\nPostgreSQL + pgvector", C["purple_fill"], C["green"], "已运行"),
]


def draw_png():
    image = Image.new("RGB", (W, H), C["bg"])
    draw = ImageDraw.Draw(image)
    draw.text((52, 35), "目标数据库内算子路径与当前证据链", font=F["title"], fill=C["ink"])
    draw.text((54, 83), "上层表示计划实现， 下层表示已运行路径。两者共享外部物理执行思想，但不能据此声称数据库内算子已经完成。", font=F["subtitle"], fill=C["muted"])

    draw.text((52, 119), "目标路径", font=F["lane"], fill=C["orange"])
    for box, title, detail, fill, edge, status in TOP:
        stage(draw, box, title, detail, fill, edge, status, planned=True)
    for left, right in zip(TOP, TOP[1:]):
        arrow(draw, left[0][2] + 5, 239, right[0][0] - 5, C["orange"])

    draw.rounded_rectangle((330, 365, 1270, 431), radius=18, fill=C["white"], outline=C["orange"], width=2)
    center_text(draw, (350, 372, 1250, 424), "进入正式 SQL 执行路径前需完成：child plan、snapshot、查询取消、错误传播与结果生命周期", F["body"], C["orange"])

    draw.text((52, 439), "当前可运行证据链", font=F["lane"], fill=C["green"])
    for box, title, detail, fill, edge, status in BOTTOM:
        stage(draw, box, title, detail, fill, edge, status, planned=False)
    for left, right in zip(BOTTOM, BOTTOM[1:]):
        arrow(draw, left[0][2] + 5, 559, right[0][0] - 5, C["green"])

    cards = [
        ((80, 712, 505, 810), "研究内容一：数据组织", "按工作量组织、局部性与兼容性"),
        ((585, 712, 1010, 810), "研究内容二：提交与多 Job 调度", "请求释放、路由、额度与 Job 保护"),
        ((1090, 712, 1520, 810), "共同使能：算子代价估计", "预测工作量、服务时间、剩余工作与余量"),
    ]
    for box, title, detail in cards:
        draw.rounded_rectangle(box, radius=16, fill=C["white"], outline=C["blue"], width=2)
        center_text(draw, (box[0] + 12, box[1] + 12, box[2] - 12, box[1] + 49), title, F["box"], C["blue"])
        center_text(draw, (box[0] + 12, box[1] + 48, box[2] - 12, box[3] - 8), detail, F["body"], C["muted"])

    draw.text((54, 851), "图例：绿色实线为已有运行证据；橙色虚线为迁移中或待实现；浅色模块表示候选物理机制。", font=F["small"], fill=C["muted"])
    image.save(PNG_PATH, dpi=(300, 300))
    image.save(REPORT_PATH, dpi=(300, 300))


def draw_svg():
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
        f'<rect width="{W}" height="{H}" fill="{C["bg"]}"/>',
        svg_text(52, 68, "目标数据库内算子路径与当前证据链", 34, "700"),
        svg_text(54, 104, "上层表示计划实现，下层表示已运行路径。两者共享外部物理执行思想，但不能据此声称数据库内算子已经完成。", 19, "400", C["muted"]),
        svg_text(52, 141, "目标路径", 22, "700", C["orange"]),
    ]
    for box, title, detail, fill, edge, status in TOP:
        parts.append(svg_stage(box, title, detail, fill, edge, status, True))
    for left, right in zip(TOP, TOP[1:]):
        parts.append(svg_arrow(left[0][2] + 5, 239, right[0][0] - 5, C["orange"]))
    parts.extend([
        svg_rect((330, 365, 1270, 431), C["white"], C["orange"], False, 18, 2),
        svg_text(800, 404, "进入正式 SQL 执行路径前需完成：child plan、snapshot、查询取消、错误传播与结果生命周期", 16, "400", C["orange"], "middle"),
        svg_text(52, 461, "当前可运行证据链", 22, "700", C["green"]),
    ])
    for box, title, detail, fill, edge, status in BOTTOM:
        parts.append(svg_stage(box, title, detail, fill, edge, status, False))
    for left, right in zip(BOTTOM, BOTTOM[1:]):
        parts.append(svg_arrow(left[0][2] + 5, 559, right[0][0] - 5, C["green"]))
    cards = [
        ((80, 712, 505, 810), "研究内容一：数据组织", "按工作量组织、局部性与兼容性"),
        ((585, 712, 1010, 810), "研究内容二：提交与多 Job 调度", "请求释放、路由、额度与 Job 保护"),
        ((1090, 712, 1520, 810), "共同使能：算子代价估计", "预测工作量、服务时间、剩余工作与余量"),
    ]
    for box, title, detail in cards:
        parts.append(svg_rect(box, C["white"], C["blue"], False, 16, 2))
        parts.append(svg_text((box[0] + box[2]) / 2, box[1] + 37, title, 19, "700", C["blue"], "middle"))
        parts.append(svg_text((box[0] + box[2]) / 2, box[1] + 72, detail, 16, "400", C["muted"], "middle"))
    parts.extend([
        svg_text(54, 868, "图例：绿色实线为已有运行证据；橙色虚线为迁移中或待实现；浅色模块表示候选物理机制。", 14, "400", C["muted"]),
        "</svg>",
    ])
    SVG_PATH.write_text("\n".join(parts), encoding="utf-8")


if __name__ == "__main__":
    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    draw_png()
    draw_svg()
    print(PNG_PATH)
    print(SVG_PATH)
    print(REPORT_PATH)
