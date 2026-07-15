from pathlib import Path
from xml.sax.saxutils import escape
from PIL import Image, ImageDraw, ImageFont

OUT_DIR = Path(__file__).resolve().parents[1] / "architecture"
FLOW_PNG = OUT_DIR / "runtime_strategy_control_loop.png"
FLOW_SVG = OUT_DIR / "runtime_strategy_control_loop.svg"
TABLE_PNG = OUT_DIR / "runtime_strategy_rule_table.png"
TABLE_SVG = OUT_DIR / "runtime_strategy_rule_table.svg"

PAL = {
    "bg": "#F8FAFC",
    "ink": "#172033",
    "muted": "#334155",
    "line": "#334155",
    "dash": "#475569",
    "panel": "#EEF2F7",
    "card": "#FFFFFF",
    "edge": "#CBD5E1",
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
    "title": _font(28, True),
    "sub": _font(15),
    "panel": _font(18, True),
    "card": _font(15, True),
    "body": _font(13),
    "small": _font(11),
    "cap": _font(12),
}


def _rgb(hex_color):
    return tuple(int(hex_color[i:i + 2], 16) for i in (1, 3, 5))


def text_size(draw, text, font):
    box = draw.textbbox((0, 0), text, font=font)
    return box[2] - box[0], box[3] - box[1]


def rrect(draw, box, fill, edge, radius=9, width=1):
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=edge, width=width)


def center_text(draw, box, text, font, fill=PAL["ink"]):
    x1, y1, x2, y2 = box
    tw, th = text_size(draw, text, font)
    draw.text((x1 + (x2 - x1 - tw) / 2, y1 + (y2 - y1 - th) / 2 - 1), text, font=font, fill=fill)


def arrow(draw, x1, y1, x2, y2, color=PAL["line"], width=3):
    if width > 0:
        draw.line((x1, y1, x2, y2), fill=color, width=width)
    if abs(x2 - x1) >= abs(y2 - y1):
        if x2 >= x1:
            head = [(x2, y2), (x2 - 8, y2 - 5), (x2 - 8, y2 + 5)]
        else:
            head = [(x2, y2), (x2 + 8, y2 - 5), (x2 + 8, y2 + 5)]
    else:
        if y2 >= y1:
            head = [(x2, y2), (x2 - 5, y2 - 8), (x2 + 5, y2 - 8)]
        else:
            head = [(x2, y2), (x2 - 5, y2 + 8), (x2 + 5, y2 + 8)]
    draw.polygon(head, fill=color)


def dashed_line(draw, points, color=PAL["dash"], width=2, dash=9, gap=6):
    for (x1, y1), (x2, y2) in zip(points, points[1:]):
        dx, dy = x2 - x1, y2 - y1
        length = (dx * dx + dy * dy) ** 0.5
        if not length:
            continue
        ux, uy = dx / length, dy / length
        pos = 0
        while pos < length:
            end = min(length, pos + dash)
            draw.line((x1 + ux * pos, y1 + uy * pos, x1 + ux * end, y1 + uy * end), fill=color, width=width)
            pos += dash + gap


def dashed_arrow(draw, points, color=PAL["dash"]):
    dashed_line(draw, points, color=color, width=2)
    x1, y1 = points[-2]
    x2, y2 = points[-1]
    arrow(draw, x1, y1, x2, y2, color=color, width=0)


def card(draw, box, title, lines, edge, fill=PAL["card"]):
    x, y, w, h = box
    rrect(draw, (x, y, x + w, y + h), fill, edge, radius=9, width=2)
    draw.text((x + 14, y + 12), title, font=F["card"], fill=edge)
    yy = y + 40
    for line in lines:
        draw.text((x + 14, yy), line, font=F["body"], fill=PAL["muted"])
        yy += 20


def decision(draw, box, title, value, edge, fill):
    x, y, w, h = box
    rrect(draw, (x, y, x + w, y + h), fill, edge, radius=7, width=1)
    draw.text((x + 10, y + 8), title, font=F["small"], fill=edge)
    draw.text((x + 10, y + 29), value, font=F["body"], fill=PAL["ink"])


def svg_text(x, y, text, size=13, weight="400", fill=PAL["ink"], anchor="start"):
    return f'<text x="{x}" y="{y}" font-family="Microsoft YaHei,Arial,sans-serif" font-size="{size}" font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{escape(text)}</text>'


def svg_rect(x, y, w, h, fill, stroke, r=9, sw=1):
    return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill}" stroke="{stroke}" stroke-width="{sw}"/>'


def svg_center(x, y, w, h, text, size=13, weight="400", fill=PAL["ink"]):
    return svg_text(x + w / 2, y + h / 2 + size * 0.35, text, size, weight, fill, "middle")


def svg_arrow(x1, y1, x2, y2, color=PAL["line"], sw=3):
    line = "" if sw == 0 else f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{sw}"/>'
    if abs(x2 - x1) >= abs(y2 - y1):
        if x2 >= x1:
            head = f"{x2},{y2} {x2 - 8},{y2 - 5} {x2 - 8},{y2 + 5}"
        else:
            head = f"{x2},{y2} {x2 + 8},{y2 - 5} {x2 + 8},{y2 + 5}"
    else:
        if y2 >= y1:
            head = f"{x2},{y2} {x2 - 5},{y2 - 8} {x2 + 5},{y2 - 8}"
        else:
            head = f"{x2},{y2} {x2 - 5},{y2 + 8} {x2 + 5},{y2 + 8}"
    return f'{line}\n<polygon points="{head}" fill="{color}"/>'


def svg_polyline(points, color=PAL["dash"]):
    pts = " ".join(f"{x},{y}" for x, y in points)
    x1, y1 = points[-2]
    x2, y2 = points[-1]
    return f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="2" stroke-dasharray="9 6"/>\n' + svg_arrow(x1, y1, x2, y2, color, sw=0)


def svg_card(box, title, lines, edge, fill=PAL["card"]):
    x, y, w, h = box
    parts = [svg_rect(x, y, w, h, fill, edge, r=9, sw=2), svg_text(x + 14, y + 30, title, 15, "700", edge)]
    yy = y + 61
    for line in lines:
        parts.append(svg_text(x + 14, yy, line, 13, fill=PAL["muted"]))
        yy += 22
    return "\n".join(parts)


def svg_decision(box, title, value, edge, fill):
    x, y, w, h = box
    return "\n".join([
        svg_rect(x, y, w, h, fill, edge, r=7, sw=1),
        svg_text(x + 10, y + 22, title, 11, fill=edge),
        svg_text(x + 10, y + 47, value, 13),
    ])


def border_check(img, boxes, w, h):
    px = img.convert("RGB").load()
    checks = []
    for name, box in boxes.items():
        x, y, bw, bh = map(int, box)
        inside = x >= 0 and y >= 0 and x + bw <= w and y + bh <= h
        count = 0
        for xx in range(max(0, x - 2), min(w, x + bw + 3)):
            for yy in range(max(0, y - 2), min(h, y + 4)):
                if px[xx, yy] != _rgb(PAL["bg"]):
                    count += 1
            for yy in range(max(0, y + bh - 4), min(h, y + bh + 3)):
                if px[xx, yy] != _rgb(PAL["bg"]):
                    count += 1
        ok = inside and count > 80
        checks.append(ok)
        print(f"  {name}: border_px={count} bounds={'OK' if inside else 'OUT'} {'PASS' if ok else 'FAIL'}")
    return all(checks)


def draw_flow_png():
    w, h = 1600, 620
    img = Image.new("RGB", (w, h), PAL["bg"])
    d = ImageDraw.Draw(img)
    d.text((48, 28), "运行时信号驱动的上游执行闭环", font=F["title"], fill=PAL["ink"])
    d.text((50, 66), "一个 AI_EMBED 查询贯穿数据批次、提交门控、模型服务队列、写回约束和端到端反馈。", font=F["sub"], fill=PAL["muted"])

    boxes = {
        "sql": (58, 142, 165, 116),
        "batch": (278, 142, 165, 116),
        "gate": (498, 142, 165, 116),
        "endpoint": (718, 142, 165, 116),
        "gpu": (938, 142, 165, 116),
        "sink": (1158, 142, 165, 116),
        "metrics": (1378, 142, 178, 116),
    }
    specs = {
        "sql": ("SQL 入口", ["SELECT AI_EMBED(text)", "FROM documents"], PAL["purple"], PAL["purple_fill"]),
        "batch": ("数据批次队列", ["观测量：行数/长度", "调节项：批量/分区"], PAL["blue"], PAL["blue_fill"]),
        "gate": ("提交门控", ["观测量：在途任务", "调节项：并发上限"], PAL["orange"], PAL["orange_fill"]),
        "endpoint": ("模型服务队列", ["观测量：积压/等待", "调节项：routing policy"], PAL["orange"], PAL["orange_fill"]),
        "gpu": ("GPU 模型服务", ["观测量：模型耗时", "判定项：是否饱和"], PAL["orange"], PAL["card"]),
        "sink": ("结果与写回", ["观测量：写回占比", "约束项：写回批量"], PAL["green"], PAL["green_fill"]),
        "metrics": ("端到端指标", ["评价项：耗时/P99", "判定项：整体收益"], PAL["green"], PAL["card"]),
    }
    for name, box in boxes.items():
        title, lines, edge, fill = specs[name]
        card(d, box, title, lines, edge, fill)

    names = list(boxes)
    main_arrows = []
    for left, right in zip(names, names[1:]):
        lx, ly, lw, lh = boxes[left]
        rx, ry, rw, rh = boxes[right]
        ymid = ly + lh // 2
        arrow(d, lx + lw + 4, ymid, rx - 8, ymid)
        main_arrows.append((lx + lw + 4, ymid, rx - 8, ymid))

    decisions = {
        "decision_batch": (278, 292, 165, 58),
        "decision_gate": (498, 292, 165, 58),
        "decision_routing": (718, 292, 165, 58),
        "decision_microbatch": (938, 292, 165, 58),
        "guard_sink": (1158, 292, 165, 58),
    }
    decision(d, decisions["decision_batch"], "计划层决策", "批量 / 分区", PAL["blue"], PAL["blue_fill"])
    decision(d, decisions["decision_gate"], "运行层决策", "K_max", PAL["orange"], PAL["orange_fill"])
    decision(d, decisions["decision_routing"], "运行层决策", "routing policy", PAL["orange"], PAL["orange_fill"])
    decision(d, decisions["decision_microbatch"], "服务端决策", "micro-batch", PAL["orange"], PAL["orange_fill"])
    decision(d, decisions["guard_sink"], "保护约束", "写回占比", PAL["green"], PAL["green_fill"])

    selector = (278, 420, 1052, 72)
    rrect(d, (selector[0], selector[1], selector[0] + selector[2], selector[1] + selector[3]), PAL["yellow_fill"], PAL["yellow"], radius=10, width=2)
    selector_title = "策略控制器：计划层 + 运行层 + 服务端批处理"
    selector_sub = "执行前定数据批次；运行中调门控/路由；服务端形成 micro-batch"
    tw, _ = text_size(d, selector_title, F["card"])
    sw, _ = text_size(d, selector_sub, F["body"])
    d.text((selector[0] + (selector[2] - tw) / 2, selector[1] + 14), selector_title, font=F["card"], fill=PAL["yellow"])
    d.text((selector[0] + (selector[2] - sw) / 2, selector[1] + 42), selector_sub, font=F["body"], fill=PAL["muted"])

    # Short, internal feedback/control arrows. No line runs along a panel boundary.
    dashed_arrow(d, [(1467, 258), (1467, 456), (1330, 456)])
    d.text((1364, 442), "反馈", font=F["small"], fill=PAL["dash"])
    for x in [361, 581, 801, 1021, 1241]:
        dashed_arrow(d, [(x, 420), (x, 362)])
    d.text((50, 562), "图注：不重切数据库侧已物化批次；动态 batch 位于模型服务侧，请求队列按等待时间、token/shape 预算形成推理 micro-batch。", font=F["cap"], fill=PAL["muted"])

    img.save(FLOW_PNG)
    return img, {**boxes, **decisions, "selector": selector}, main_arrows


def draw_flow_svg():
    w, h = 1600, 620
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}">',
        f'<rect width="{w}" height="{h}" fill="{PAL["bg"]}"/>',
        svg_text(48, 60, "运行时信号驱动的上游执行闭环", 28, "700"),
        svg_text(50, 88, "一个 AI_EMBED 查询贯穿数据批次、提交门控、模型服务队列、写回约束和端到端反馈。", 15, fill=PAL["muted"]),
    ]
    boxes = {
        "sql": (58, 142, 165, 116, "SQL 入口", ["SELECT AI_EMBED(text)", "FROM documents"], PAL["purple"], PAL["purple_fill"]),
        "batch": (278, 142, 165, 116, "数据批次队列", ["观测量：行数/长度", "调节项：批量/分区"], PAL["blue"], PAL["blue_fill"]),
        "gate": (498, 142, 165, 116, "提交门控", ["观测量：在途任务", "调节项：并发上限"], PAL["orange"], PAL["orange_fill"]),
        "endpoint": (718, 142, 165, 116, "模型服务队列", ["观测量：积压/等待", "调节项：routing policy"], PAL["orange"], PAL["orange_fill"]),
        "gpu": (938, 142, 165, 116, "GPU 模型服务", ["观测量：模型耗时", "判定项：是否饱和"], PAL["orange"], PAL["card"]),
        "sink": (1158, 142, 165, 116, "结果与写回", ["观测量：写回占比", "约束项：写回批量"], PAL["green"], PAL["green_fill"]),
        "metrics": (1378, 142, 178, 116, "端到端指标", ["评价项：耗时/P99", "判定项：整体收益"], PAL["green"], PAL["card"]),
    }
    for b in boxes.values():
        parts.append(svg_card(b[:4], b[4], b[5], b[6], b[7]))
    bnames = list(boxes)
    for left, right in zip(bnames, bnames[1:]):
        lx, ly, lw, lh = boxes[left][:4]
        rx, ry, rw, rh = boxes[right][:4]
        parts.append(svg_arrow(lx + lw + 4, ly + lh // 2, rx - 8, ry + rh // 2))
    for c in [
        ((278, 292, 165, 58), "计划层决策", "批量 / 分区", PAL["blue"], PAL["blue_fill"]),
        ((498, 292, 165, 58), "运行层决策", "K_max", PAL["orange"], PAL["orange_fill"]),
        ((718, 292, 165, 58), "运行层决策", "routing policy", PAL["orange"], PAL["orange_fill"]),
        ((938, 292, 165, 58), "服务端决策", "micro-batch", PAL["orange"], PAL["orange_fill"]),
        ((1158, 292, 165, 58), "保护约束", "写回占比", PAL["green"], PAL["green_fill"]),
    ]:
        box, title, value, edge, fill = c
        parts.append(svg_decision(box, title, value, edge, fill))
    selector = (278, 420, 1052, 72)
    parts.append(svg_rect(*selector, PAL["yellow_fill"], PAL["yellow"], r=10, sw=2))
    parts.append(svg_text(804, 449, "策略控制器：计划层 + 运行层 + 服务端批处理", 15, "700", PAL["yellow"], "middle"))
    parts.append(svg_text(804, 476, "执行前定数据批次；运行中调门控/路由；服务端形成 micro-batch", 13, fill=PAL["muted"], anchor="middle"))
    parts.append(svg_polyline([(1467, 258), (1467, 456), (1330, 456)]))
    parts.append(svg_text(1364, 456, "反馈", 11, fill=PAL["dash"]))
    for x in [361, 581, 801, 1021, 1241]:
        parts.append(svg_polyline([(x, 420), (x, 362)]))
    parts.append(svg_text(50, 576, "图注：不重切数据库侧已物化批次；动态 batch 位于模型服务侧，请求队列按等待时间、token/shape 预算形成推理 micro-batch。", 12, fill=PAL["muted"]))
    parts.append("</svg>")
    FLOW_SVG.write_text("\n".join(parts), encoding="utf-8")


def draw_table_png():
    w, h = 1300, 520
    img = Image.new("RGB", (w, h), PAL["bg"])
    d = ImageDraw.Draw(img)
    d.text((48, 32), "信号触发的候选策略规则表", font=F["title"], fill=PAL["ink"])
    d.text((50, 70), "规则表表达待实验验证的候选动作；运行链路由闭环图单独说明。", font=F["sub"], fill=PAL["muted"])
    x, y = 70, 130
    col_w = [370, 390, 370]
    headers = [("观测信号", PAL["purple"], PAL["purple_fill"]), ("下一步动作", PAL["blue"], PAL["blue_fill"]), ("保护约束", PAL["green"], PAL["green_fill"])]
    cx = x
    boxes = {}
    for i, (label, edge, fill) in enumerate(headers):
        rrect(d, (cx, y, cx + col_w[i], y + 48), fill, edge, radius=8, width=2)
        center_text(d, (cx, y, cx + col_w[i], y + 48), label, F["card"], edge)
        boxes[f"header_{i}"] = (cx, y, col_w[i], 48)
        cx += col_w[i] + 18
    rows = [
        ("GPU 未饱和，调用次数多", "增大计划批量 / 合并小分区", "P99 不明显上升"),
        ("模型服务队列积压高", "降低 K_max 或 least-queued routing", "吞吐不明显下降"),
        ("写回占比高", "调整写回路径 / 工程 baseline", "端到端收益仍保留"),
        ("过滤场景：选择率低", "验证提前过滤 / 小批量", "质量指标不下降"),
        ("生成场景：token 长尾明显", "验证 token-aware 分发", "长请求不阻塞短请求"),
    ]
    yy = y + 68
    for r, row in enumerate(rows):
        bg = "#FFFFFF" if r % 2 == 0 else "#F1F5F9"
        cx = x
        for i, txt in enumerate(row):
            rrect(d, (cx, yy, cx + col_w[i], yy + 44), bg, PAL["edge"], radius=6, width=1)
            center_text(d, (cx, yy, cx + col_w[i], yy + 44), txt, F["body"], PAL["ink"])
            boxes[f"row_{r}_{i}"] = (cx, yy, col_w[i], 44)
            cx += col_w[i] + 18
        yy += 56
    d.text((72, 464), "图注：每条规则必须对应可观测信号、可执行配置和防止副作用的保护约束；本表是候选策略，不代表已证明结论。", font=F["cap"], fill=PAL["muted"])
    img.save(TABLE_PNG)
    return img, boxes


def draw_table_svg():
    w, h = 1300, 520
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}">',
        f'<rect width="{w}" height="{h}" fill="{PAL["bg"]}"/>',
        svg_text(48, 64, "信号触发的候选策略规则表", 28, "700"),
        svg_text(50, 92, "规则表表达待实验验证的候选动作；运行链路由闭环图单独说明。", 15, fill=PAL["muted"]),
    ]
    x, y = 70, 130
    col_w = [370, 390, 370]
    headers = [("观测信号", PAL["purple"], PAL["purple_fill"]), ("下一步动作", PAL["blue"], PAL["blue_fill"]), ("保护约束", PAL["green"], PAL["green_fill"])]
    cx = x
    for i, (label, edge, fill) in enumerate(headers):
        parts.append(svg_rect(cx, y, col_w[i], 48, fill, edge, r=8, sw=2))
        parts.append(svg_center(cx, y, col_w[i], 48, label, 15, "700", edge))
        cx += col_w[i] + 18
    rows = [
        ("GPU 未饱和，调用次数多", "增大计划批量 / 合并小分区", "P99 不明显上升"),
        ("模型服务队列积压高", "降低 K_max 或 least-queued routing", "吞吐不明显下降"),
        ("写回占比高", "调整写回路径 / 工程 baseline", "端到端收益仍保留"),
        ("过滤场景：选择率低", "验证提前过滤 / 小批量", "质量指标不下降"),
        ("生成场景：token 长尾明显", "验证 token-aware 分发", "长请求不阻塞短请求"),
    ]
    yy = y + 68
    for r, row in enumerate(rows):
        bg = "#FFFFFF" if r % 2 == 0 else "#F1F5F9"
        cx = x
        for i, txt in enumerate(row):
            parts.append(svg_rect(cx, yy, col_w[i], 44, bg, PAL["edge"], r=6, sw=1))
            parts.append(svg_center(cx, yy, col_w[i], 44, txt, 13))
            cx += col_w[i] + 18
        yy += 56
    parts.append(svg_text(72, 478, "图注：每条规则必须对应可观测信号、可执行配置和防止副作用的保护约束；本表是候选策略，不代表已证明结论。", 12, fill=PAL["muted"]))
    parts.append("</svg>")
    TABLE_SVG.write_text("\n".join(parts), encoding="utf-8")


def check_forbidden(svg_path):
    forbidden = [
        "RC1", "RC2", "RC3", "BL1", "BL2", "边界确认", "联合决策面", "Workload 入口", "图形借鉴", " vs ",
        "Observed state", "Next action", "Guardrail", "feedback", "guardrail:", "RecordBatch",
        "Submit gate", "Endpoint queues", "GPU service", "Results / sink", "E2E metrics",
        "state:", "decision",
    ]
    text = svg_path.read_text(encoding="utf-8")
    hits = [term for term in forbidden if term in text]
    print(f"  forbidden_terms({svg_path.name}): {hits if hits else 'none'} {'PASS' if not hits else 'FAIL'}")
    return not hits


if __name__ == "__main__":
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    flow_img, flow_boxes, arrows = draw_flow_png()
    draw_flow_svg()
    table_img, table_boxes = draw_table_png()
    draw_table_svg()
    print(f"FLOW PNG: {FLOW_PNG}")
    print(f"FLOW SVG: {FLOW_SVG}")
    print(f"TABLE PNG: {TABLE_PNG}")
    print(f"TABLE SVG: {TABLE_SVG}")
    ok = True
    ok = border_check(flow_img, flow_boxes, 1600, 620) and ok
    for i, (x1, y1, x2, y2) in enumerate(arrows, start=1):
        arrow_ok = 0 <= x1 <= 1600 and 0 <= x2 <= 1600 and 0 <= y1 <= 620 and 0 <= y2 <= 620
        print(f"  flow_arrow_{i}: {'PASS' if arrow_ok else 'FAIL'}")
        ok = arrow_ok and ok
    ok = border_check(table_img, table_boxes, 1300, 520) and ok
    ok = check_forbidden(FLOW_SVG) and ok
    ok = check_forbidden(TABLE_SVG) and ok
    if not ok:
        raise SystemExit("Self-check failed.")
    print("Self-check complete.")
