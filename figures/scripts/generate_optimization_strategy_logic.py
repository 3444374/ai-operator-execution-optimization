from pathlib import Path
from xml.sax.saxutils import escape

from PIL import Image, ImageDraw, ImageFont


OUT_DIR = Path(__file__).resolve().parents[1] / "architecture"
PNG_PATH = OUT_DIR / "optimization_strategy_logic.png"
SVG_PATH = OUT_DIR / "optimization_strategy_logic.svg"

W, H = 1800, 1120

PAL = {
    "bg": "#F8FAFC",
    "ink": "#172033",
    "muted": "#334155",
    "line": "#475569",
    "panel": "#F1F5F9",
    "card": "#FFFFFF",
    "pill": "#FFFFFF",
    "pill_edge": "#CBD5E1",
    "blue": "#2F6FEB",
    "blue_fill": "#E7F0FF",
    "orange": "#F97316",
    "orange_fill": "#FFF1E6",
    "purple": "#7C3AED",
    "purple_fill": "#F2ECFF",
    "green": "#16A34A",
    "green_fill": "#F0FDF4",
    "yellow": "#B7791F",
    "yellow_fill": "#FFF7D6",
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


def pill(draw, x, y, w, h, text, color=PAL["muted"]):
    rrect(draw, (x, y, x + w, y + h), PAL["pill"], PAL["pill_edge"], radius=6, width=1)
    center_text(draw, (x, y, x + w, y + h), text, F["pill"], color)


def card(draw, x, y, w, h, title, items, edge, fill=PAL["card"], tag=None):
    rrect(draw, (x, y, x + w, y + h), fill, edge, radius=12, width=2)
    draw.text((x + 20, y + 16), title, font=F["card"], fill=edge)
    if tag:
        tw, _ = text_size(draw, tag, F["small"])
        draw.text((x + w - tw - 18, y + 20), tag, font=F["small"], fill=PAL["muted"])
    py = y + 52
    for item in items:
        pill(draw, x + 18, py, w - 36, 27, item)
        py += 34


def arrow(draw, x1, y1, x2, y2):
    draw.line((x1, y1, x2, y2), fill=PAL["line"], width=3)
    head = [(x2, y2), (x2 - 13, y2 - 8), (x2 - 13, y2 + 8)]
    draw.polygon(head, fill=PAL["line"])


def draw_png():
    img = Image.new("RGB", (W, H), PAL["bg"])
    d = ImageDraw.Draw(img)

    d.text((64, 36), "优化策略设计：Ours-v0 规则表驱动上游执行策略", font=F["title"], fill=PAL["ink"])
    d.text(
        (66, 78),
        "策略不是重新实现数据库优化器或推理引擎；它在已定位瓶颈之后，用 workload 特征和模型服务状态选择上游执行配置，并用端到端指标回填规则表。",
        font=F["sub"],
        fill=PAL["muted"],
    )

    panels = [
        (70, 128, 400, 760, "输入信号"),
        (520, 128, 620, 760, "规则表选择器"),
        (1190, 128, 540, 760, "策略动作与配置"),
    ]
    for x, y, w, h, title in panels:
        rrect(d, (x, y, x + w, y + h), PAL["panel"], PAL["pill_edge"], radius=16, width=1)
        d.text((x + 24, y + 20), title, font=F["panel"], fill=PAL["ink"])

    input_cards = [
        (100, 188, 340, 142, "Workload 特征", ["算子类型 / 行数", "token length / prefix", "selectivity / 输出大小"], PAL["purple"], PAL["purple_fill"]),
        (100, 360, 340, 142, "数据组织信号", ["batch / partition", "operator invocation", "object 数 / fan-in"], PAL["blue"], PAL["blue_fill"]),
        (100, 532, 340, 142, "模型服务状态", ["queue wait / backlog", "endpoint load", "GPU utilization / P99"], PAL["orange"], PAL["orange_fill"]),
        (100, 704, 340, 120, "写回约束信号", ["writeback ratio", "sink mode / write batch"], PAL["green"], PAL["green_fill"]),
    ]
    for c in input_cards:
        card(d, *c)

    selector_cards = [
        (555, 180, 550, 138, "规则来源", ["文献机制：AI-aware / batching / backpressure", "本地画像：阶段耗时 / queue / writeback", "参数 sweep：静态最优与边界点"], PAL["yellow"], PAL["yellow_fill"]),
        (555, 350, 550, 150, "Workload 规则", ["FILTER：优先看 selectivity 与 cascade", "COMPLETE：优先看 token / prefix 分组", "EMBED：优先看 batch 与输出写回量"], PAL["purple"], PAL["purple_fill"]),
        (555, 532, 550, 150, "服务状态规则", ["queue wait 高：降低或自适应 in-flight", "GPU 利用率低：增大 batch 或并发", "endpoint 不均衡：改 routing / dispatch"], PAL["orange"], PAL["orange_fill"]),
        (555, 714, 550, 120, "写回保护规则", ["writeback ratio 高：切换写回路径", "端到端无收益：回退上游配置"], PAL["green"], PAL["green_fill"]),
    ]
    for c in selector_cards:
        card(d, *c)

    action_cards = [
        (1225, 184, 470, 158, "数据组织配置", ["batch_size / partition_count", "object_merge / fan-in shape", "按 selectivity / prefix / output 重排"], PAL["blue"], PAL["blue_fill"]),
        (1225, 384, 470, 178, "模型服务调度配置", ["actor_pool_size / K_max", "routing_policy / dispatch_policy", "bounded in-flight / adaptive backpressure", "token-aware / prefix-aware dispatch"], PAL["orange"], PAL["orange_fill"], "核心"),
        (1225, 604, 470, 158, "写回约束配置", ["COPY + deferred index baseline", "driver / worker-direct / queue-worker", "write_batch_rows / sink mode"], PAL["green"], PAL["green_fill"]),
    ]
    for c in action_cards:
        card(d, *c)

    arrows = [
        ("workload to rules", 440, 259, 555, 419),
        ("data to rules", 440, 431, 555, 431),
        ("service to rules", 440, 603, 555, 607),
        ("writeback to rules", 440, 764, 555, 774),
        ("rules to data action", 1105, 424, 1225, 263),
        ("rules to service action", 1105, 607, 1225, 473),
        ("rules to writeback action", 1105, 774, 1225, 683),
    ]
    for _, x1, y1, x2, y2 in arrows:
        arrow(d, x1, y1, x2, y2)

    rrect(d, (520, 922, 1730, 1010), PAL["card"], PAL["green"], radius=14, width=2)
    d.text((548, 944), "端到端验证与回填", font=F["card"], fill=PAL["green"])
    for i, item in enumerate(["latency / rows/s", "queue wait / P99", "model wall", "writeback ratio", "GPU utilization"]):
        pill(d, 730 + i * 158, 946, 142, 28, item)
    d.text((548, 980), "验证通过：保留规则；验证失败：回退配置并进入下一轮参数 sweep。", font=F["small"], fill=PAL["muted"])

    d.text(
        (92, 1050),
        "图注：Ours-v0 策略逻辑图。左侧输入来自前置阶段画像；中间规则表结合文献机制、本地画像和参数 sweep；右侧输出可执行配置，底部用包含写回的端到端指标回填规则。",
        font=F["cap"],
        fill=PAL["muted"],
    )

    img.save(PNG_PATH)
    boxes = {
        "input_workload": input_cards[0][:4],
        "input_data": input_cards[1][:4],
        "input_service": input_cards[2][:4],
        "input_writeback": input_cards[3][:4],
        "selector_source": selector_cards[0][:4],
        "selector_workload": selector_cards[1][:4],
        "selector_service": selector_cards[2][:4],
        "selector_writeback": selector_cards[3][:4],
        "action_data": action_cards[0][:4],
        "action_service": action_cards[1][:4],
        "action_writeback": action_cards[2][:4],
        "validation": (520, 922, 1210, 88),
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


def svg_card(x, y, w, h, title, items, edge, fill=PAL["card"], tag=None):
    parts = [svg_rect(x, y, w, h, fill, edge, r=12, sw=2), svg_text(x + 20, y + 30, title, 17, "700", edge)]
    if tag:
        parts.append(svg_text(x + w - 18, y + 30, tag, 12, fill=PAL["muted"], anchor="end"))
    py = y + 52
    for item in items:
        parts.append(svg_pill(x + 18, py, w - 36, 27, item))
        py += 34
    return "\n".join(parts)


def svg_arrow(x1, y1, x2, y2):
    head = f"{x2},{y2} {x2 - 13},{y2 - 8} {x2 - 13},{y2 + 8}"
    return f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{PAL["line"]}" stroke-width="3"/>\n<polygon points="{head}" fill="{PAL["line"]}"/>'


def draw_svg():
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
        f'<rect width="{W}" height="{H}" fill="{PAL["bg"]}"/>',
        svg_text(64, 66, "优化策略设计：Ours-v0 规则表驱动上游执行策略", 30, "700"),
        svg_text(66, 100, "策略不是重新实现数据库优化器或推理引擎；它在已定位瓶颈之后，用 workload 特征和模型服务状态选择上游执行配置，并用端到端指标回填规则表。", 16, fill=PAL["muted"]),
    ]

    for x, y, w, h, title in [
        (70, 128, 400, 760, "输入信号"),
        (520, 128, 620, 760, "规则表选择器"),
        (1190, 128, 540, 760, "策略动作与配置"),
    ]:
        parts.append(svg_rect(x, y, w, h, PAL["panel"], PAL["pill_edge"], r=16, sw=1))
        parts.append(svg_text(x + 24, y + 44, title, 19, "700"))

    all_cards = [
        (100, 188, 340, 142, "Workload 特征", ["算子类型 / 行数", "token length / prefix", "selectivity / 输出大小"], PAL["purple"], PAL["purple_fill"]),
        (100, 360, 340, 142, "数据组织信号", ["batch / partition", "operator invocation", "object 数 / fan-in"], PAL["blue"], PAL["blue_fill"]),
        (100, 532, 340, 142, "模型服务状态", ["queue wait / backlog", "endpoint load", "GPU utilization / P99"], PAL["orange"], PAL["orange_fill"]),
        (100, 704, 340, 120, "写回约束信号", ["writeback ratio", "sink mode / write batch"], PAL["green"], PAL["green_fill"]),
        (555, 180, 550, 138, "规则来源", ["文献机制：AI-aware / batching / backpressure", "本地画像：阶段耗时 / queue / writeback", "参数 sweep：静态最优与边界点"], PAL["yellow"], PAL["yellow_fill"]),
        (555, 350, 550, 150, "Workload 规则", ["FILTER：优先看 selectivity 与 cascade", "COMPLETE：优先看 token / prefix 分组", "EMBED：优先看 batch 与输出写回量"], PAL["purple"], PAL["purple_fill"]),
        (555, 532, 550, 150, "服务状态规则", ["queue wait 高：降低或自适应 in-flight", "GPU 利用率低：增大 batch 或并发", "endpoint 不均衡：改 routing / dispatch"], PAL["orange"], PAL["orange_fill"]),
        (555, 714, 550, 120, "写回保护规则", ["writeback ratio 高：切换写回路径", "端到端无收益：回退上游配置"], PAL["green"], PAL["green_fill"]),
        (1225, 184, 470, 158, "数据组织配置", ["batch_size / partition_count", "object_merge / fan-in shape", "按 selectivity / prefix / output 重排"], PAL["blue"], PAL["blue_fill"]),
        (1225, 384, 470, 178, "模型服务调度配置", ["actor_pool_size / K_max", "routing_policy / dispatch_policy", "bounded in-flight / adaptive backpressure", "token-aware / prefix-aware dispatch"], PAL["orange"], PAL["orange_fill"], "核心"),
        (1225, 604, 470, 158, "写回约束配置", ["COPY + deferred index baseline", "driver / worker-direct / queue-worker", "write_batch_rows / sink mode"], PAL["green"], PAL["green_fill"]),
    ]
    for c in all_cards:
        tag = c[8] if len(c) > 8 else None
        parts.append(svg_card(*c[:8], tag=tag))

    for _, x1, y1, x2, y2 in [
        ("workload to rules", 440, 259, 555, 419),
        ("data to rules", 440, 431, 555, 431),
        ("service to rules", 440, 603, 555, 607),
        ("writeback to rules", 440, 764, 555, 774),
        ("rules to data action", 1105, 424, 1225, 263),
        ("rules to service action", 1105, 607, 1225, 473),
        ("rules to writeback action", 1105, 774, 1225, 683),
    ]:
        parts.append(svg_arrow(x1, y1, x2, y2))

    parts.append(svg_rect(520, 922, 1210, 88, PAL["card"], PAL["green"], r=14, sw=2))
    parts.append(svg_text(548, 960, "端到端验证与回填", 17, "700", PAL["green"]))
    for i, item in enumerate(["latency / rows/s", "queue wait / P99", "model wall", "writeback ratio", "GPU utilization"]):
        parts.append(svg_pill(730 + i * 158, 946, 142, 28, item))
    parts.append(svg_text(548, 992, "验证通过：保留规则；验证失败：回退配置并进入下一轮参数 sweep。", 12, fill=PAL["muted"]))
    parts.append(svg_text(92, 1064, "图注：Ours-v0 策略逻辑图。左侧输入来自前置阶段画像；中间规则表结合文献机制、本地画像和参数 sweep；右侧输出可执行配置，底部用包含写回的端到端指标回填规则。", 13, fill=PAL["muted"]))
    parts.append("</svg>")
    SVG_PATH.write_text("\n".join(parts), encoding="utf-8")


def _rgb(hex_color):
    return tuple(int(hex_color[i:i + 2], 16) for i in (1, 3, 5))


def _line_intersects_rect(x1, y1, x2, y2, rect, pad=8):
    rx, ry, rw, rh = rect
    rx1, ry1 = rx + pad, ry + pad
    rx2, ry2 = rx + rw - pad, ry + rh - pad
    for i in range(1, 80):
        t = i / 80
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
        count = 0
        for xx in range(max(0, x - 2), min(W, x + w + 3)):
            for yy in range(max(0, y - 2), min(H, y + 3)):
                if px[xx, yy] != _rgb(PAL["bg"]):
                    count += 1
            for yy in range(max(0, y + h - 2), min(H, y + h + 3)):
                if px[xx, yy] != _rgb(PAL["bg"]):
                    count += 1
        for yy in range(max(0, y - 2), min(H, y + h + 3)):
            for xx in range(max(0, x - 2), min(W, x + 3)):
                if px[xx, yy] != _rgb(PAL["bg"]):
                    count += 1
            for xx in range(max(0, x + w - 2), min(W, x + w + 3)):
                if px[xx, yy] != _rgb(PAL["bg"]):
                    count += 1
        ok = inside and count > 100
        checks.append(ok)
        print(f"  {name}: border_px={count} bounds={'OK' if inside else 'OUT'} {'PASS' if ok else 'FAIL'}")

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
