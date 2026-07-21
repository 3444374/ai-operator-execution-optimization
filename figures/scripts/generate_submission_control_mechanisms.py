"""Generate polished submission-control mechanism figures.

These are schematic mechanism figures for research content two. They explain
submission control as three upstream decisions: when to flush, how many requests
may be in flight, and where each request is routed.
"""

from __future__ import annotations

from pathlib import Path
from xml.sax.saxutils import escape as xe

from PIL import Image, ImageDraw, ImageFont


OUT_DIR = Path(__file__).resolve().parents[1] / "architecture"
W = 1180

PAL = {
    "bg": "#FFFFFF",
    "ink": "#172033",
    "muted": "#536276",
    "line": "#334155",
    "soft_line": "#D9E0EA",
    "panel": "#F8FAFC",
    "card": "#FFFFFF",
    "blue": "#2F6FEB",
    "blue_soft": "#EAF2FF",
    "orange": "#D97706",
    "orange_soft": "#FFF7ED",
    "green": "#16A34A",
    "green_soft": "#ECFDF3",
    "red": "#DC2626",
    "red_soft": "#FEF2F2",
    "purple": "#6D5BD0",
    "purple_soft": "#F4F3FF",
    "gray": "#94A3B8",
    "gray_soft": "#F1F5F9",
    "route": "#64748B",
}


def rgb(c: str) -> tuple[int, int, int]:
    return tuple(int(c[i : i + 2], 16) for i in (1, 3, 5))


def font(size: int, bold: bool = False):
    paths = [
        r"C:\Windows\Fonts\msyhbd.ttc" if bold else r"C:\Windows\Fonts\msyh.ttc",
        r"C:\Windows\Fonts\simhei.ttf",
        r"C:\Windows\Fonts\arial.ttf",
    ]
    for path in paths:
        if Path(path).exists():
            return ImageFont.truetype(path, size=size)
    return ImageFont.load_default()


F = {
    "title": font(27, True),
    "sub": font(14),
    "panel": font(17, True),
    "card": font(14, True),
    "body": font(12),
    "small": font(10),
    "note": font(11),
}


def text_size(draw: ImageDraw.ImageDraw, text: str, fnt) -> tuple[int, int]:
    box = draw.textbbox((0, 0), text, font=fnt)
    return box[2] - box[0], box[3] - box[1]


def ct(draw: ImageDraw.ImageDraw, x: float, y: float, text: str, fnt, fill: str):
    tw, _ = text_size(draw, text, fnt)
    draw.text((x - tw / 2, y), text, font=fnt, fill=rgb(fill))


def rect(draw: ImageDraw.ImageDraw, box, fill: str, outline: str | None = None, width: int = 1, radius: int = 8):
    draw.rounded_rectangle(
        box,
        radius=radius,
        fill=rgb(fill),
        outline=rgb(outline) if outline else None,
        width=width,
    )


def line_arrow(draw: ImageDraw.ImageDraw, start, end, color: str = PAL["line"], width: int = 3):
    x1, y1 = start
    x2, y2 = end
    draw.line((x1, y1, x2, y2), fill=rgb(color), width=width)
    direction = 1 if x2 >= x1 else -1
    draw.polygon([(x2, y2), (x2 - direction * 11, y2 - 6), (x2 - direction * 11, y2 + 6)], fill=rgb(color))


def title(draw: ImageDraw.ImageDraw, title_text: str, sub_text: str):
    ct(draw, W / 2, 16, title_text, F["title"], PAL["ink"])
    ct(draw, W / 2, 52, sub_text, F["sub"], PAL["muted"])


def panel(draw: ImageDraw.ImageDraw, x, y, w, h, label, accent):
    rect(draw, (x, y, x + w, y + h), PAL["panel"], PAL["soft_line"], 1, 10)
    rect(draw, (x + 20, y + 20, x + 25, y + h - 20), accent, radius=2)
    draw.text((x + 38, y + 18), label, font=F["panel"], fill=rgb(PAL["ink"]))


def card(draw, x, y, x2, y2, label, accent, soft=None):
    w = x2 - x
    rect(draw, (x, y, x2, y2), PAL["card"], accent, 2, 8)
    if soft:
        rect(draw, (x + 12, y + 12, x2 - 12, y + 36), soft, radius=5)
    draw.text((x + 18, y + 14), label, font=F["card"], fill=rgb(accent))


def pill(draw, x, y, w, h, label, fill, text="#FFFFFF", outline=None):
    rect(draw, (x, y, x + w, y + h), fill, outline, 1, 5)
    ct(draw, x + w / 2, y + 5, label, F["small"], text)


def slots(draw, x, y, w, count, active, color, inactive=PAL["gray_soft"], slot_w=18, slot_h=18, gap=8):
    total = count * slot_w + (count - 1) * gap
    start = x + (w - total) / 2
    for i in range(count):
        fill = color if i < active else inactive
        outline = None if i < active else PAL["gray"]
        rect(draw, (start + i * (slot_w + gap), y, start + i * (slot_w + gap) + slot_w, y + slot_h), fill, outline, 1, 4)


def caption(draw, y, text):
    draw.text((48, y), text, font=F["body"], fill=rgb(PAL["muted"]))


def svg_text(x, y, text, size=12, fill=PAL["ink"], bold=False, anchor="start"):
    weight = "700" if bold else "400"
    return (
        f'<text x="{x}" y="{y}" font-family="Microsoft YaHei,SimHei,Arial,sans-serif" '
        f'font-size="{size}" font-weight="{weight}" fill="{fill}" text-anchor="{anchor}">{xe(text)}</text>'
    )


def svg_rect(x, y, w, h, fill, stroke=None, sw=1, rx=8):
    s = f' stroke="{stroke}" stroke-width="{sw}"' if stroke else ""
    return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" ry="{rx}" fill="{fill}"{s}/>'


def svg_arrow(x1, y1, x2, y2, color=PAL["line"], sw=3):
    direction = 1 if x2 >= x1 else -1
    return (
        f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{sw}"/>'
        f'<polygon points="{x2},{y2} {x2 - direction * 11},{y2 - 6} {x2 - direction * 11},{y2 + 6}" fill="{color}"/>'
    )


def svg_slots(x, y, w, count, active, color, inactive=PAL["gray_soft"], slot_w=18, slot_h=18, gap=8):
    total = count * slot_w + (count - 1) * gap
    start = x + (w - total) / 2
    out = []
    for i in range(count):
        fill = color if i < active else inactive
        stroke = None if i < active else PAL["gray"]
        out.append(svg_rect(start + i * (slot_w + gap), y, slot_w, slot_h, fill, stroke, 1, 4))
    return out


def svg_base(height, body):
    return "\n".join(
        [
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{height}" viewBox="0 0 {W} {height}">',
            f'<rect width="{W}" height="{height}" fill="{PAL["bg"]}"/>',
            *body,
            "</svg>",
        ]
    )


def svg_title(title_text, sub_text):
    return [
        svg_text(W / 2, 44, title_text, 27, PAL["ink"], True, "middle"),
        svg_text(W / 2, 68, sub_text, 14, PAL["muted"], False, "middle"),
    ]


def svg_panel(x, y, w, h, label, accent):
    return [
        svg_rect(x, y, w, h, PAL["panel"], PAL["soft_line"], 1, 10),
        svg_rect(x + 20, y + 20, 5, h - 40, accent, rx=2),
        svg_text(x + 38, y + 39, label, 17, PAL["ink"], True),
    ]


def svg_card(x, y, w, h, label, accent, soft=None):
    out = [svg_rect(x, y, w, h, PAL["card"], accent, 2, 8)]
    if soft:
        out.append(svg_rect(x + 12, y + 12, w - 24, 24, soft, rx=5))
    out.append(svg_text(x + 18, y + 32, label, 14, accent, True))
    return out


def svg_pill(x, y, w, h, label, fill, text="#FFFFFF", stroke=None):
    return [
        svg_rect(x, y, w, h, fill, stroke, 1, 5),
        svg_text(x + w / 2, y + 16, label, 10, text, False, "middle"),
    ]


def write_svg(path: Path, height: int, body: list[str]):
    path.write_text(svg_base(height, body), encoding="utf-8")


def validate_svg(path: Path):
    text = path.read_text(encoding="utf-8")
    forbidden = ["RC1", "RC2", "RC3", "BL1", "BL2", "Phase", "边界确认", "Workload 入口", "效果：", "鍊", "绛", "鎻", "鈫", "�"]
    for token in forbidden:
        if token in text:
            raise RuntimeError(f"{path.name} contains forbidden token: {token}")


def figure_queue_adaptive():
    height = 500
    img = Image.new("RGB", (W, height), rgb(PAL["bg"]))
    d = ImageDraw.Draw(img)
    title(d, "提交控制策略一：队列自适应提交", "提交时机：根据服务入口压力调节 flush 节奏")
    lx, rx, py, pw, ph = 48, 618, 94, 514, 320
    panel(d, lx, py, pw, ph, "固定节奏：提交与服务状态脱节", PAL["gray"])
    panel(d, rx, py, pw, ph, "闭环提交：观测队列后再发送", PAL["blue"])
    line_arrow(d, (lx + pw + 18, 254), (rx - 18, 254))

    actor = (88, 158, 240, 285)
    card(d, *actor, "Ray actor", PAL["blue"])
    d.text((106, 196), "定时或按行数触发", font=F["small"], fill=rgb(PAL["ink"]))
    d.text((106, 220), "不读取下游状态", font=F["small"], fill=rgb(PAL["red"]))
    pill(d, 106, 244, 108, 26, "固定 flush", PAL["gray_soft"], PAL["muted"], PAL["gray"])
    svc = (314, 142, 520, 300)
    card(d, *svc, "服务入口队列", PAL["red"])
    d.text((336, 184), "空闲时仍等待", font=F["small"], fill=rgb(PAL["green"]))
    d.text((336, 230), "拥塞时继续塞入", font=F["small"], fill=rgb(PAL["red"]))
    slots(d, 332, 250, 150, 6, 6, PAL["red"], slot_w=16, slot_h=14, gap=7)
    line_arrow(d, (240, 220), (314, 220))
    d.text((92, 344), "风险：空闲阶段吞吐不足，拥塞阶段尾延迟放大", font=F["note"], fill=rgb(PAL["red"]))

    actor = (658, 148, 818, 300)
    card(d, *actor, "Ray actor", PAL["blue"])
    pill(d, 676, 188, 124, 26, "读取 queue", PAL["blue_soft"], PAL["blue"], PAL["blue"])
    pill(d, 676, 228, 124, 26, "空闲：提前/增量", PAL["green"])
    pill(d, 676, 264, 124, 26, "拥塞：暂缓/减量", PAL["orange"])
    svc = (884, 142, 1102, 300)
    card(d, *svc, "服务入口队列", PAL["green"])
    pill(d, 910, 182, 166, 24, "running / waiting / wait", PAL["blue_soft"], PAL["blue"], PAL["blue"])
    slots(d, 910, 228, 150, 6, 4, PAL["green"], slot_w=18, slot_h=18, gap=8)
    slots(d, 920, 264, 92, 3, 3, PAL["orange"], slot_w=18, slot_h=14, gap=8)
    line_arrow(d, (818, 222), (884, 222))
    line_arrow(d, (884, 284), (818, 284), PAL["green"], 2)
    d.text((662, 344), "验证：queue wait、P95/P99、tokens/s、端到端耗时", font=F["note"], fill=rgb(PAL["green"]))

    caption(d, 456, "图注建议：队列自适应提交把固定 flush 改为状态反馈机制；该图说明候选机制，最终需通过队列压力和尾延迟消融验证。")
    img.save(OUT_DIR / "submission_control_queue_adaptive_mechanism.png")

    body = [
        *svg_title("提交控制策略一：队列自适应提交", "提交时机：根据服务入口压力调节 flush 节奏"),
        *svg_panel(lx, py, pw, ph, "固定节奏：提交与服务状态脱节", PAL["gray"]),
        *svg_panel(rx, py, pw, ph, "闭环提交：观测队列后再发送", PAL["blue"]),
        svg_arrow(lx + pw + 18, 254, rx - 18, 254),
        *svg_card(88, 158, 152, 127, "Ray actor", PAL["blue"]),
        svg_text(106, 208, "定时或按行数触发", 10),
        svg_text(106, 232, "不读取下游状态", 10, PAL["red"]),
        *svg_pill(106, 244, 108, 26, "固定 flush", PAL["gray_soft"], PAL["muted"], PAL["gray"]),
        *svg_card(314, 142, 206, 158, "服务入口队列", PAL["red"]),
        svg_text(336, 196, "空闲时仍等待", 10, PAL["green"]),
        svg_text(336, 242, "拥塞时继续塞入", 10, PAL["red"]),
        *svg_slots(332, 250, 150, 6, 6, PAL["red"], slot_w=16, slot_h=14, gap=7),
        svg_arrow(240, 220, 314, 220),
        svg_text(92, 357, "风险：空闲阶段吞吐不足，拥塞阶段尾延迟放大", 11, PAL["red"]),
        *svg_card(658, 148, 160, 152, "Ray actor", PAL["blue"]),
        *svg_pill(676, 188, 124, 26, "读取 queue", PAL["blue_soft"], PAL["blue"], PAL["blue"]),
        *svg_pill(676, 228, 124, 26, "空闲：提前/增量", PAL["green"]),
        *svg_pill(676, 264, 124, 26, "拥塞：暂缓/减量", PAL["orange"]),
        *svg_card(884, 142, 218, 158, "服务入口队列", PAL["green"]),
        *svg_pill(910, 182, 166, 24, "running / waiting / wait", PAL["blue_soft"], PAL["blue"], PAL["blue"]),
        *svg_slots(910, 228, 150, 6, 4, PAL["green"], slot_w=18, slot_h=18, gap=8),
        *svg_slots(920, 264, 92, 3, 3, PAL["orange"], slot_w=18, slot_h=14, gap=8),
        svg_arrow(818, 222, 884, 222),
        svg_arrow(884, 284, 818, 284, PAL["green"], 2),
        svg_text(662, 357, "验证：queue wait、P95/P99、tokens/s、端到端耗时", 11, PAL["green"]),
        svg_text(48, 471, "图注建议：队列自适应提交把固定 flush 改为状态反馈机制；该图说明候选机制，最终需通过队列压力和尾延迟消融验证。", 12, PAL["muted"]),
    ]
    svg = OUT_DIR / "submission_control_queue_adaptive_mechanism.svg"
    write_svg(svg, height, body)
    validate_svg(svg)


def figure_kmax():
    height = 500
    img = Image.new("RGB", (W, height), rgb(PAL["bg"]))
    d = ImageDraw.Draw(img)
    title(d, "提交控制策略二：在途上限准入控制", "提交数量：用 K_max 限制同时在途请求数")
    lx, rx, py, pw, ph = 48, 618, 94, 514, 320
    panel(d, lx, py, pw, ph, "无准入上限：请求一次性涌入", PAL["gray"])
    panel(d, rx, py, pw, ph, "有界准入：按槽位轮转提交", PAL["blue"])
    line_arrow(d, (lx + pw + 18, 254), (rx - 18, 254))

    actor = (92, 166, 240, 235)
    card(d, *actor, "Ray actor", PAL["blue"])
    d.text((110, 205), "生成 16 个请求", font=F["small"], fill=rgb(PAL["ink"]))
    svc = (318, 142, 522, 300)
    card(d, *svc, "vLLM 服务入口", PAL["red"])
    slots(d, 338, 190, 164, 8, 8, PAL["red"], slot_w=18, slot_h=18, gap=7)
    d.text((338, 226), "in-flight 被填满", font=F["small"], fill=rgb(PAL["red"]))
    slots(d, 338, 252, 142, 6, 6, PAL["orange"], slot_w=18, slot_h=14, gap=7)
    d.text((338, 280), "等待队列继续增长", font=F["small"], fill=rgb(PAL["orange"]))
    line_arrow(d, (240, 202), (318, 202))
    d.text((92, 344), "风险：后台批量请求占满服务入口，前台小任务排队不可控", font=F["note"], fill=rgb(PAL["red"]))

    actor = (662, 166, 812, 235)
    card(d, *actor, "Ray actor", PAL["blue"])
    pill(d, 680, 204, 112, 24, "max_inflight = K", PAL["blue"])
    svc = (886, 136, 1102, 310)
    card(d, *svc, "vLLM 服务入口", PAL["green"])
    pill(d, 928, 178, 132, 24, "K_max 准入门控", PAL["green"])
    slots(d, 908, 224, 172, 8, 4, PAL["green"], slot_w=18, slot_h=18, gap=8)
    d.text((914, 258), "只允许 K 个请求在途", font=F["small"], fill=rgb(PAL["green"]))
    slots(d, 928, 282, 104, 4, 0, PAL["blue"], inactive=PAL["blue_soft"], slot_w=18, slot_h=14, gap=8)
    line_arrow(d, (812, 202), (886, 202))
    d.text((662, 344), "验证：K_max sweep、前后台干扰、吞吐与尾延迟折中", font=F["note"], fill=rgb(PAL["green"]))

    caption(d, 456, "图注建议：在途上限准入控制把提交控制从“发完即算”改为有界窗口；K_max 需与上游 batch 形态联合消融。")
    img.save(OUT_DIR / "submission_control_kmax_admission_mechanism.png")

    body = [
        *svg_title("提交控制策略二：在途上限准入控制", "提交数量：用 K_max 限制同时在途请求数"),
        *svg_panel(lx, py, pw, ph, "无准入上限：请求一次性涌入", PAL["gray"]),
        *svg_panel(rx, py, pw, ph, "有界准入：按槽位轮转提交", PAL["blue"]),
        svg_arrow(lx + pw + 18, 254, rx - 18, 254),
        *svg_card(92, 166, 148, 69, "Ray actor", PAL["blue"]),
        svg_text(110, 217, "生成 16 个请求", 10),
        *svg_card(318, 142, 204, 158, "vLLM 服务入口", PAL["red"]),
        *svg_slots(338, 190, 164, 8, 8, PAL["red"], slot_w=18, slot_h=18, gap=7),
        svg_text(338, 238, "in-flight 被填满", 10, PAL["red"]),
        *svg_slots(338, 252, 142, 6, 6, PAL["orange"], slot_w=18, slot_h=14, gap=7),
        svg_text(338, 292, "等待队列继续增长", 10, PAL["orange"]),
        svg_arrow(240, 202, 318, 202),
        svg_text(92, 357, "风险：后台批量请求占满服务入口，前台小任务排队不可控", 11, PAL["red"]),
        *svg_card(662, 166, 150, 69, "Ray actor", PAL["blue"]),
        *svg_pill(680, 204, 112, 24, "max_inflight = K", PAL["blue"]),
        *svg_card(886, 136, 216, 174, "vLLM 服务入口", PAL["green"]),
        *svg_pill(928, 178, 132, 24, "K_max 准入门控", PAL["green"]),
        *svg_slots(908, 224, 172, 8, 4, PAL["green"], slot_w=18, slot_h=18, gap=8),
        svg_text(914, 270, "只允许 K 个请求在途", 10, PAL["green"]),
        *svg_slots(928, 282, 104, 4, 0, PAL["blue"], inactive=PAL["blue_soft"], slot_w=18, slot_h=14, gap=8),
        svg_arrow(812, 202, 886, 202),
        svg_text(662, 357, "验证：K_max sweep、前后台干扰、吞吐与尾延迟折中", 11, PAL["green"]),
        svg_text(48, 471, "图注建议：在途上限准入控制把提交控制从“发完即算”改为有界窗口；K_max 需与上游 batch 形态联合消融。", 12, PAL["muted"]),
    ]
    svg = OUT_DIR / "submission_control_kmax_admission_mechanism.svg"
    write_svg(svg, height, body)
    validate_svg(svg)


def figure_pool_routing():
    height = 520
    img = Image.new("RGB", (W, height), rgb(PAL["bg"]))
    d = ImageDraw.Draw(img)
    title(d, "提交控制策略三：分池路由", "提交去向：按请求形态选择 actor pool 与提交参数")
    lx, rx, py, pw, ph = 48, 618, 94, 514, 336
    panel(d, lx, py, pw, ph, "单一入口：不同请求混在一起", PAL["gray"])
    panel(d, rx, py, pw, ph, "分池入口：按形态选择策略", PAL["blue"])
    line_arrow(d, (lx + pw + 18, 264), (rx - 18, 264))

    reqs = [("短", PAL["blue"]), ("长", PAL["orange"]), ("sysA", PAL["purple"]), ("短", PAL["blue"]), ("长", PAL["orange"]), ("sysA", PAL["purple"]), ("短", PAL["blue"]), ("长", PAL["orange"]), ("sysB", PAL["green"])]
    for i, (label, color) in enumerate(reqs):
        col, row = i % 3, i // 3
        pill(d, 92 + col * 62, 154 + row * 34, 48, 24, label, color)
    pool = (322, 144, 522, 276)
    card(d, *pool, "同构 Actor Pool", PAL["gray"])
    d.text((342, 188), "所有请求共用参数", font=F["small"], fill=rgb(PAL["ink"]))
    d.text((342, 212), "K_max / flush 均相同", font=F["small"], fill=rgb(PAL["ink"]))
    d.text((342, 238), "短长请求相互影响", font=F["small"], fill=rgb(PAL["red"]))
    line_arrow(d, (270, 196), (322, 196))
    d.text((92, 362), "风险：一种策略同时服务低延迟、长尾吞吐和前缀局部性需求", font=F["note"], fill=rgb(PAL["red"]))

    router = (654, 170, 812, 286)
    card(d, *router, "请求形态判别", PAL["blue"])
    d.text((674, 212), "token 量 / prefix", font=F["small"], fill=rgb(PAL["ink"]))
    d.text((674, 236), "到达压力 / 任务类型", font=F["small"], fill=rgb(PAL["ink"]))
    pools = [
        ("短请求池", "低延迟优先", "较大 K_max，快速 flush", PAL["blue"], 130),
        ("长请求池", "吞吐优先", "较小 K_max，控制尾部", PAL["orange"], 236),
        ("前缀相似池", "共享 system prompt", "集中提交，观察缓存命中", PAL["purple"], 342),
    ]
    hub_x = 842
    hub_y1 = pools[0][4] + 39
    hub_y2 = pools[-1][4] + 39
    router_cy = router[1] + (router[3] - router[1]) / 2
    d.line((router[2], router_cy, hub_x, router_cy), fill=rgb(PAL["route"]), width=2)
    d.line((hub_x, hub_y1, hub_x, hub_y2), fill=rgb(PAL["route"]), width=2)
    rect(d, (hub_x - 3, router_cy - 3, hub_x + 3, router_cy + 3), PAL["route"], radius=2)
    for name, desc, cfg, color, y in pools:
        box = (880, y, 1102, y + 78)
        rect(d, box, PAL["card"], PAL["soft_line"], 1, 8)
        rect(d, (box[0] + 10, box[1] + 12, box[0] + 15, box[3] - 12), color, radius=3)
        d.text((902, y + 38), desc, font=F["small"], fill=rgb(PAL["ink"]))
        d.text((902, y + 60), cfg, font=F["small"], fill=rgb(color))
        d.text((902, y + 14), name, font=F["card"], fill=rgb(color))
        line_arrow(d, (hub_x, y + 39), (880, y + 39), PAL["route"], 2)
    d.text((662, 452), "验证：分组尾延迟、队列均衡、prefix locality 与吞吐", font=F["note"], fill=rgb(PAL["green"]))

    caption(d, 486, "图注建议：分池路由把“发给谁”纳入提交控制；箭头表示从形态判别器到目标 pool 的明确路由关系。")
    img.save(OUT_DIR / "submission_control_pool_routing_mechanism.png")

    body = [
        *svg_title("提交控制策略三：分池路由", "提交去向：按请求形态选择 actor pool 与提交参数"),
        *svg_panel(lx, py, pw, ph, "单一入口：不同请求混在一起", PAL["gray"]),
        *svg_panel(rx, py, pw, ph, "分池入口：按形态选择策略", PAL["blue"]),
        svg_arrow(lx + pw + 18, 264, rx - 18, 264),
        *svg_card(322, 144, 200, 132, "同构 Actor Pool", PAL["gray"]),
        svg_text(342, 200, "所有请求共用参数", 10),
        svg_text(342, 224, "K_max / flush 均相同", 10),
        svg_text(342, 250, "短长请求相互影响", 10, PAL["red"]),
        svg_arrow(270, 196, 322, 196),
        svg_text(92, 375, "风险：一种策略同时服务低延迟、长尾吞吐和前缀局部性需求", 11, PAL["red"]),
        *svg_card(654, 170, 158, 116, "请求形态判别", PAL["blue"]),
        svg_text(674, 224, "token 量 / prefix", 10),
        svg_text(674, 248, "到达压力 / 任务类型", 10),
        svg_text(662, 465, "验证：分组尾延迟、队列均衡、prefix locality 与吞吐", 11, PAL["green"]),
        svg_text(48, 501, "图注建议：分池路由把“发给谁”纳入提交控制；箭头表示从形态判别器到目标 pool 的明确路由关系。", 12, PAL["muted"]),
    ]
    for i, (label, color) in enumerate(reqs):
        col, row = i % 3, i // 3
        body.extend(svg_pill(92 + col * 62, 154 + row * 34, 48, 24, label, color))
    for name, desc, cfg, color, y in pools:
        body.append(svg_rect(880, y, 222, 78, PAL["card"], PAL["soft_line"], 1, 8))
        body.append(svg_rect(890, y + 12, 5, 54, color, rx=3))
        body.append(svg_text(902, y + 32, name, 14, color, True))
        body.append(svg_text(902, y + 50, desc, 10))
        body.append(svg_text(902, y + 72, cfg, 10, color))
    hub_x = 842
    hub_y1 = pools[0][4] + 39
    hub_y2 = pools[-1][4] + 39
    body.extend(
        [
            f'<line x1="812" y1="228" x2="{hub_x}" y2="228" stroke="{PAL["route"]}" stroke-width="2"/>',
            f'<line x1="{hub_x}" y1="{hub_y1}" x2="{hub_x}" y2="{hub_y2}" stroke="{PAL["route"]}" stroke-width="2"/>',
            svg_rect(hub_x - 3, 225, 6, 6, PAL["route"], rx=2),
        ]
    )
    for name, desc, cfg, color, y in pools:
        body.append(svg_arrow(hub_x, y + 39, 880, y + 39, PAL["route"], 2))
    svg = OUT_DIR / "submission_control_pool_routing_mechanism.svg"
    write_svg(svg, height, body)
    validate_svg(svg)


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    figure_queue_adaptive()
    figure_kmax()
    figure_pool_routing()
    print("Generated polished submission-control mechanism figures:")
    for name in [
        "submission_control_queue_adaptive_mechanism",
        "submission_control_kmax_admission_mechanism",
        "submission_control_pool_routing_mechanism",
    ]:
        print(f"  figures/architecture/{name}.png")
        print(f"  figures/architecture/{name}.svg")


if __name__ == "__main__":
    main()
