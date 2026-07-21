"""Generate RC2 submission-control strategy mechanism figures.

Three concept diagrams (概念图) for the submission-control candidate mechanisms.
Style matches RC1 mechanism figures — left/right comparison, shared palette.
"""

from pathlib import Path
from xml.sax.saxutils import escape as xe
from PIL import Image, ImageDraw, ImageFont

OUT_DIR = Path(__file__).resolve().parents[1] / "architecture"

# ── Palette ──────────────────────────────────────────────────
P = {
    "bg":         "#F8FAFC",
    "ink":        "#172033",
    "muted":      "#475569",
    "line":       "#334155",
    "blue":       "#2F6FEB",   "blue_fill":   "#E7F0FF",
    "orange":     "#F97316",   "orange_fill": "#FFF1E6",
    "green":      "#16A34A",   "green_fill":  "#F0FDF4",
    "red":        "#DC2626",   "red_fill":    "#FEF2F2",
    "gray":       "#94A3B8",   "gray_fill":   "#F1F5F9",
    "white":      "#FFFFFF",
    "purple":     "#7C3AED",   "purple_fill": "#F5F3FF",
}

# ── Shared layout constants ──────────────────────────────────
LX, RX = 45, 610          # left / right panel x
LW = RW = 495              # panel width
GAP_Y = 234                # center arrow y

# ── Font helpers ────────────────────────────────────────────
def _rgb(h: str):
    return tuple(int(h[i:i+2], 16) for i in (1, 3, 5))

def _font(size: int, bold: bool = False):
    for p in [
        r"C:\Windows\Fonts\msyhbd.ttc" if bold else r"C:\Windows\Fonts\msyh.ttc",
        r"C:\Windows\Fonts\simhei.ttf",
        r"C:\Windows\Fonts\arial.ttf",
    ]:
        if Path(p).exists():
            return ImageFont.truetype(p, size=size)
    return ImageFont.load_default()

FT = {  # font table
    "title":   _font(26, True),
    "sub":     _font(14),
    "section": _font(16, True),
    "label":   _font(12),
    "small":   _font(10),
    "note":    _font(11),
}

# PIL text width
def _tw(draw, text, fnt):
    b = draw.textbbox((0, 0), text, font=fnt)
    return b[2] - b[0]

def _ct(draw, x, y, text, fnt, fill):
    w = _tw(draw, text, fnt)
    draw.text((x - w // 2, y), text, font=fnt, fill=fill)

# PIL rounded rect
def _rr(draw, box, fill=None, outline=None, width=1, radius=8):
    draw.rounded_rectangle(box, radius=radius, fill=fill, outline=outline, width=width)

# PIL arrow
def _arr(draw, x1, y1, x2, y2, color=None, w=3):
    color = color or _rgb(P["line"])
    draw.line((x1, y1, x2, y2), fill=color, width=w)
    d = 1 if x2 >= x1 else -1
    draw.polygon([(x2, y2), (x2 - d * 10, y2 - 6), (x2 - d * 10, y2 + 6)], fill=color)

# ── SVG helpers ─────────────────────────────────────────────
def _st(x, y, text, size=12, color="#172033", bold=False, anchor="start"):
    w = "bold" if bold else "normal"
    return f'<text x="{x}" y="{y}" font-family="Microsoft YaHei,SimHei,Arial,sans-serif" font-size="{size}" font-weight="{w}" fill="{color}" text-anchor="{anchor}">{xe(text)}</text>'

def _sr(x, y, w, h, fill, stroke=None, sw=1, rx=8):
    s = f' stroke="{stroke}" stroke-width="{sw}"' if stroke else ""
    return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" ry="{rx}" fill="{fill}"{s}/>'

def _sa(x1, y1, x2, y2, color="#334155", sw=3):
    d = 1 if x2 >= x1 else -1
    return (f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{sw}"/>'
            f'<polygon points="{x2},{y2} {x2-d*10},{y2-6} {x2-d*10},{y2+6}" fill="{color}"/>')

def _write_svg(path, width, height, body):
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        f'<rect width="{width}" height="{height}" fill="{P["bg"]}"/>',
    ] + body + ["</svg>"]
    path.write_text("\n".join(lines), encoding="utf-8")

def _validate(svg_path):
    txt = svg_path.read_text(encoding="utf-8")
    for t in ["RC1", "RC2", "RC3", "BL1", "BL2", "边界确认", "Workload 入口"]:
        if t in txt:
            raise RuntimeError(f"{svg_path.name} contains forbidden token: {t}")

# PIL title block
def _title(draw, w, title, subtitle):
    _ct(draw, w // 2, 12, title, FT["title"], _rgb(P["ink"]))
    _ct(draw, w // 2, 45, subtitle, FT["sub"], _rgb(P["muted"]))

# ── SVG baseline-offset map ──────────────────────────────────
# PIL draw.text y = top-of-text; SVG text y = baseline.
# Offset = font_size + 2–4 (CJK ascent)
BO = {26: 28, 16: 18, 14: 16, 12: 14, 11: 13, 10: 12}


# ══════════════════════════════════════════════════════════════
# FIGURE 1: K_max Admission Control
# ══════════════════════════════════════════════════════════════
def figure1_kmax():
    W, H = 1150, 470
    PT, PB = 80, 378  # panel top/bottom
    img = Image.new("RGB", (W, H), _rgb(P["bg"]))
    d = ImageDraw.Draw(img)
    _title(d, W, "提交控制策略一：K_max 准入控制",
           "候选机制：限制同时在飞请求数上界，防止单任务独占模型服务资源")

    # ── LEFT: no admission control ──
    _rr(d, (LX, PT, LX + LW, PB), fill=_rgb(P["red_fill"]), radius=10)
    d.text((LX + 22, PT + 16), "无准入控制：所有请求同时进入", font=FT["section"], fill=_rgb(P["ink"]))

    # Actor
    al = (LX + 28, 128, LX + 168, 190)
    _rr(d, al, fill=_rgb(P["white"]), outline=_rgb(P["blue"]), width=2, radius=8)
    _ct(d, al[0] + 70, 144, "Ray Actor", FT["label"], _rgb(P["blue"]))
    d.text((al[0] + 15, 166), "生成 16 请求", font=FT["small"], fill=_rgb(P["ink"]))
    d.text((al[0] + 15, 182), "全部立即发送", font=FT["small"], fill=_rgb(P["ink"]))

    _arr(d, al[2] + 6, 159, al[2] + 52, 159)

    # vLLM
    vl = (al[2] + 58, 128, LX + LW - 22, 268)
    _rr(d, vl, fill=_rgb(P["white"]), outline=_rgb(P["red"]), width=2, radius=8)
    vlcx = (vl[0] + vl[2]) // 2
    _ct(d, vlcx, 144, "vLLM 模型服务", FT["label"], _rgb(P["red"]))

    sw, sg, ss = 20, 6, 26  # slot width, gap, step
    for i in range(8):
        sx = vl[0] + 14 + i * ss
        _rr(d, (sx, 168, sx + sw, 188), fill=_rgb(P["red"]), radius=4)
    d.text((vl[0] + 14, 196), "in-flight 满载 (8/8)", font=FT["small"], fill=_rgb(P["red"]))
    for i in range(5):
        sx = vl[0] + 14 + i * ss
        _rr(d, (sx, 212, sx + sw, 228), fill=_rgb(P["orange"]), radius=4)
    d.text((vl[0] + 14, 236), "队列积压 5+", font=FT["small"], fill=_rgb(P["orange"]))

    # Problem labels
    d.text((LX + 28, 320), "问题：in-flight 窗口被单任务填满，其他任务饥饿", font=FT["note"], fill=_rgb(P["red"]))
    d.text((LX + 28, 342), "无上界 → 尾部膨胀，共享场景下公平性丧失", font=FT["note"], fill=_rgb(P["red"]))

    _arr(d, LX + LW + 15, GAP_Y, RX - 25, GAP_Y)

    # ── RIGHT: K_max = 4 ──
    _rr(d, (RX, PT, RX + RW, PB), fill=_rgb(P["green_fill"]), radius=10)
    d.text((RX + 22, PT + 16), "K_max = 4 准入控制：槽位轮转", font=FT["section"], fill=_rgb(P["ink"]))

    # Actor (wider for "max_inflight = 4" text)
    ar = (RX + 20, 128, RX + 162, 190)
    _rr(d, ar, fill=_rgb(P["white"]), outline=_rgb(P["blue"]), width=2, radius=8)
    arcx = (ar[0] + ar[2]) // 2
    _ct(d, arcx, 144, "Ray Actor", FT["label"], _rgb(P["blue"]))
    pill_w1 = ar[2] - ar[0] - 28
    _rr(d, (ar[0] + 14, 164, ar[2] - 14, 184), fill=_rgb(P["blue"]), radius=4)
    _ct(d, arcx, 168, "max_inflight = 4", FT["small"], _rgb(P["white"]))

    _arr(d, ar[2] + 6, 159, ar[2] + 46, 159)

    # vLLM with gate
    vr = (ar[2] + 52, 128, RX + RW - 22, 288)
    _rr(d, vr, fill=_rgb(P["white"]), outline=_rgb(P["green"]), width=2, radius=8)
    vrcx = (vr[0] + vr[2]) // 2
    _ct(d, vrcx, 144, "vLLM 模型服务", FT["label"], _rgb(P["green"]))

    # Gate pill
    gate_w1 = 128
    _rr(d, (vrcx - gate_w1 // 2, 164, vrcx + gate_w1 // 2, 186),
         fill=_rgb(P["green"]), radius=5)
    _ct(d, vrcx, 169, "K_max = 4  准入门控", FT["small"], _rgb(P["white"]))

    # Slots: 8 total, centered in box
    tsw = 8 * ss - sg  # total slot span
    s0 = vrcx - tsw // 2
    for i in range(4):
        sx = s0 + i * ss
        _rr(d, (sx, 200, sx + sw, 220), fill=_rgb(P["green"]), radius=4)
    for i in range(4, 8):
        sx = s0 + i * ss
        _rr(d, (sx, 200, sx + sw, 220), fill=_rgb(P["gray_fill"]), outline=_rgb(P["gray"]), width=1, radius=4)
    d.text((vr[0] + 14, 228), "4 在飞 + 4 空闲", font=FT["small"], fill=_rgb(P["green"]))

    for i in range(4):
        sx = s0 + i * ss + 18
        _rr(d, (sx, 244, sx + sw, 258), fill=_rgb(P["blue_fill"]), outline=_rgb(P["blue"]), width=1, radius=4)
    d.text((vr[0] + 14, 264), "剩余请求排队轮转", font=FT["small"], fill=_rgb(P["blue"]))

    # Benefit
    d.text((RX + 28, 320), "效果：预留槽位 → 多任务不饥饿", font=FT["note"], fill=_rgb(P["green"]))
    d.text((RX + 28, 342), "上界约束使尾部可控；K_max 与 batch 形状需耦合配置", font=FT["note"], fill=_rgb(P["green"]))

    caption = ("图注建议：该图说明 K_max 如何通过限制同时在飞请求数来控制单任务的资源占用；"
               "最优 K_max 与 batch 形状耦合，需通过 b18/b24 消融实验确定。")
    d.text((45, H - 34), caption, font=FT["label"], fill=_rgb(P["muted"]))
    img.save(OUT_DIR / "submission_control_kmax_admission_mechanism.png")

    # ── SVG ──
    body = [
        _st(W // 2, 12 + BO[26], "提交控制策略一：K_max 准入控制", 26, P["ink"], True, "middle"),
        _st(W // 2, 45 + BO[14], "候选机制：限制同时在飞请求数上界，防止单任务独占模型服务资源", 14, P["muted"], False, "middle"),
        _sr(LX, PT, LW, PB - PT, P["red_fill"], rx=10),
        _sr(RX, PT, RW, PB - PT, P["green_fill"], rx=10),
        _st(LX + 22, PT + 16 + BO[16], "无准入控制：所有请求同时进入", 16, P["ink"], True),
        _st(RX + 22, PT + 16 + BO[16], "K_max = 4 准入控制：槽位轮转", 16, P["ink"], True),
        # Left actor
        _sr(*al[:2], al[2] - al[0], al[3] - al[1], P["white"], P["blue"], 2, 8),
        _st(al[0] + 70, 144 + BO[12], "Ray Actor", 12, P["blue"], False, "middle"),
        _st(al[0] + 15, 166 + BO[10], "生成 16 请求", 10, P["ink"]),
        _st(al[0] + 15, 182 + BO[10], "全部立即发送", 10, P["ink"]),
        _sa(al[2] + 6, 159, al[2] + 52, 159),
        # Left vLLM
        _sr(*vl[:2], vl[2] - vl[0], vl[3] - vl[1], P["white"], P["red"], 2, 8),
        _st(vlcx, 144 + BO[12], "vLLM 模型服务", 12, P["red"], False, "middle"),
    ]
    for i in range(8):
        sx = vl[0] + 14 + i * ss
        body.append(_sr(sx, 168, sw, 20, P["red"], rx=4))
    body.append(_st(vl[0] + 14, 196 + BO[10], "in-flight 满载 (8/8)", 10, P["red"]))
    for i in range(5):
        sx = vl[0] + 14 + i * ss
        body.append(_sr(sx, 212, sw, 16, P["orange"], rx=4))
    body.append(_st(vl[0] + 14, 236 + BO[10], "队列积压 5+", 10, P["orange"]))
    body.extend([
        _st(LX + 28, 320 + BO[11], "问题：in-flight 窗口被单任务填满，其他任务饥饿", 11, P["red"]),
        _st(LX + 28, 342 + BO[11], "无上界 → 尾部膨胀，共享场景下公平性丧失", 11, P["red"]),
        _sa(LX + LW + 15, GAP_Y, RX - 25, GAP_Y),
        # Right actor
        _sr(*ar[:2], ar[2] - ar[0], ar[3] - ar[1], P["white"], P["blue"], 2, 8),
        _st(arcx, 144 + BO[12], "Ray Actor", 12, P["blue"], False, "middle"),
        _sr(ar[0] + 14, 164, pill_w1, 20, P["blue"], rx=4),
        _st(arcx, 168 + BO[10], "max_inflight = 4", 10, P["white"], False, "middle"),
        _sa(ar[2] + 6, 159, ar[2] + 46, 159),
        # Right vLLM
        _sr(*vr[:2], vr[2] - vr[0], vr[3] - vr[1], P["white"], P["green"], 2, 8),
        _st(vrcx, 144 + BO[12], "vLLM 模型服务", 12, P["green"], False, "middle"),
        _sr(vrcx - gate_w1 // 2, 164, gate_w1, 22, P["green"], rx=5),
        _st(vrcx, 169 + BO[10], "K_max = 4  准入门控", 10, P["white"], False, "middle"),
    ])
    for i in range(4):
        sx = s0 + i * ss
        body.append(_sr(sx, 200, sw, 20, P["green"], rx=4))
    for i in range(4, 8):
        sx = s0 + i * ss
        body.append(_sr(sx, 200, sw, 20, P["gray_fill"], P["gray"], 1, 4))
    body.append(_st(vr[0] + 14, 228 + BO[10], "4 在飞 + 4 空闲", 10, P["green"]))
    for i in range(4):
        sx = s0 + i * ss + 18
        body.append(_sr(sx, 244, sw, 14, P["blue_fill"], P["blue"], 1, 4))
    body.append(_st(vr[0] + 14, 264 + BO[10], "剩余请求排队轮转", 10, P["blue"]))
    body.extend([
        _st(RX + 28, 320 + BO[11], "效果：预留槽位 → 多任务不饥饿", 11, P["green"]),
        _st(RX + 28, 342 + BO[11], "上界约束使尾部可控；K_max 与 batch 形状需耦合配置", 11, P["green"]),
        _st(45, H - 34 + BO[12], caption, 12, P["muted"]),
    ])
    svg_path = OUT_DIR / "submission_control_kmax_admission_mechanism.svg"
    _write_svg(svg_path, W, H, body)
    _validate(svg_path)


# ══════════════════════════════════════════════════════════════
# FIGURE 2: Queue-Adaptive Submission
# ══════════════════════════════════════════════════════════════
def figure2_queue_adaptive():
    W, H = 1150, 510
    PT, PB = 80, 418  # panel top/bottom
    img = Image.new("RGB", (W, H), _rgb(P["bg"]))
    d = ImageDraw.Draw(img)
    _title(d, W, "提交控制策略二：队列自适应提交",
           "候选机制：Ray actor 感知 vLLM 队列状态，动态调节请求发送节奏")

    # ── LEFT: fixed interval ──
    _rr(d, (LX, PT, LX + LW, PB), fill=_rgb(P["red_fill"]), radius=10)
    d.text((LX + 22, PT + 16), "固定节奏提交：无视服务状态", font=FT["section"], fill=_rgb(P["ink"]))

    # Actor — tall enough
    al = (LX + 28, 128, LX + 178, 298)
    _rr(d, al, fill=_rgb(P["white"]), outline=_rgb(P["blue"]), width=2, radius=8)
    alcx = (al[0] + al[2]) // 2
    _ct(d, alcx, 144, "Ray Actor", FT["label"], _rgb(P["blue"]))
    d.text((al[0] + 14, 172), "每 200ms 发送一批", font=FT["small"], fill=_rgb(P["ink"]))
    d.text((al[0] + 14, 194), "或每 N 行触发 flush", font=FT["small"], fill=_rgb(P["ink"]))
    d.text((al[0] + 14, 220), "→ 不关心下游状态", font=FT["small"], fill=_rgb(P["red"]))
    _rr(d, (al[0] + 14, 246, al[2] - 14, 282), fill=_rgb(P["gray_fill"]), outline=_rgb(P["gray"]), width=1, radius=6)
    _ct(d, alcx, 258, "定时器 / 行计数器", FT["small"], _rgb(P["muted"]))

    _arr(d, al[2] + 8, 213, al[2] + 54, 213)

    # vLLM states
    vl = (al[2] + 60, 128, LX + LW - 22, 298)
    _rr(d, vl, fill=_rgb(P["white"]), outline=_rgb(P["red"]), width=1, radius=8)
    # State 1
    _rr(d, (vl[0] + 10, 142, vl[2] - 10, 208), fill=_rgb(P["green_fill"]), outline=_rgb(P["green"]), width=1, radius=6)
    d.text((vl[0] + 20, 154), "队列空闲时", font=FT["note"], fill=_rgb(P["green"]))
    d.text((vl[0] + 20, 178), "→ 浪费吞吐机会", font=FT["small"], fill=_rgb(P["green"]))
    # State 2
    _rr(d, (vl[0] + 10, 218, vl[2] - 10, 286), fill=_rgb(P["red_fill"]), outline=_rgb(P["red"]), width=1, radius=6)
    d.text((vl[0] + 20, 230), "队列积压时", font=FT["note"], fill=_rgb(P["red"]))
    d.text((vl[0] + 20, 254), "→ 继续塞入，加剧拥塞", font=FT["small"], fill=_rgb(P["red"]))
    for i in range(6):
        qx = vl[0] + 20 + i * 22
        _rr(d, (qx, 268, qx + 18, 282), fill=_rgb(P["red"]), radius=3)

    d.text((LX + 28, 356), "问题：提交节奏与模型服务状态解耦", font=FT["note"], fill=_rgb(P["red"]))
    d.text((LX + 28, 380), "→ 空闲时吞吐不足，拥塞时加剧尾部延迟", font=FT["note"], fill=_rgb(P["red"]))

    _arr(d, LX + LW + 15, GAP_Y, RX - 25, GAP_Y)

    # ── RIGHT: queue-adaptive ──
    _rr(d, (RX, PT, RX + RW, PB), fill=_rgb(P["green_fill"]), radius=10)
    d.text((RX + 22, PT + 16), "队列自适应提交：闭环反馈", font=FT["section"], fill=_rgb(P["ink"]))

    # Actor
    ar = (RX + 20, 128, RX + 165, 298)
    _rr(d, ar, fill=_rgb(P["white"]), outline=_rgb(P["blue"]), width=2, radius=8)
    arcx = (ar[0] + ar[2]) // 2
    _ct(d, arcx, 144, "Ray Actor", FT["label"], _rgb(P["blue"]))
    pw = ar[2] - ar[0] - 24
    _rr(d, (ar[0] + 12, 170, ar[2] - 12, 196), fill=_rgb(P["blue_fill"]), outline=_rgb(P["blue"]), width=1, radius=5)
    d.text((ar[0] + 18, 176), "① 轮询 vLLM 队列状态", font=FT["small"], fill=_rgb(P["ink"]))
    _rr(d, (ar[0] + 12, 210, ar[2] - 12, 236), fill=_rgb(P["green"]), radius=5)
    _ct(d, arcx, 216, "空闲 → 加大发送", FT["small"], _rgb(P["white"]))
    _rr(d, (ar[0] + 12, 250, ar[2] - 12, 276), fill=_rgb(P["orange"]), radius=5)
    _ct(d, arcx, 256, "积压 → 暂缓发送", FT["small"], _rgb(P["white"]))

    _arr(d, ar[2] + 8, 213, ar[2] + 46, 213)

    # vLLM + feedback inline
    vr = (ar[2] + 52, 128, RX + RW - 22, 298)
    _rr(d, vr, fill=_rgb(P["white"]), outline=_rgb(P["green"]), width=2, radius=8)
    vrcx = (vr[0] + vr[2]) // 2
    _ct(d, vrcx, 144, "vLLM 模型服务", FT["label"], _rgb(P["green"]))

    _rr(d, (vr[0] + 10, 164, vr[2] - 10, 186), fill=_rgb(P["blue_fill"]), outline=_rgb(P["blue"]), width=1, radius=5)
    d.text((vr[0] + 18, 170), "队列可观测：running=3  waiting=1", font=FT["small"], fill=_rgb(P["blue"]))

    for i in range(4):
        sx = vr[0] + 18 + i * 28
        _rr(d, (sx, 200, sx + 22, 218), fill=_rgb(P["green"]), radius=4)
    for i in range(3):
        sx = vr[0] + 18 + i * 20
        _rr(d, (sx, 232, sx + 16, 246), fill=_rgb(P["orange"]), radius=3)
    d.text((vr[0] + 18, 256), "running + waiting 实时可见", font=FT["small"], fill=_rgb(P["ink"]))
    # Feedback signal inside vLLM box
    d.text((vr[0] + 18, 276), "←──────────── ▲ 反馈信号", font=FT["small"], fill=_rgb(P["green"]))

    # Feedback arrow from vLLM LEFT edge back to actor RIGHT edge
    _arr(d, vr[0], 283, ar[2] + 6, 283, _rgb(P["green"]), 2)

    d.text((RX + 28, 356), "效果：提交节奏与模型服务状态闭环", font=FT["note"], fill=_rgb(P["green"]))
    d.text((RX + 28, 380), "→ 空闲自填充，拥塞自退避 | 当前：二段式控制器", font=FT["note"], fill=_rgb(P["green"]))

    caption = ("图注建议：该图说明队列自适应提交的闭环逻辑——actor 轮询 vLLM 队列状态后动态调节发送量；"
               "当前实现（二段式 min/max K_max + 阈值）效果不如静态 K_max=8，详见 b24/b25。")
    d.text((45, H - 54), caption, font=FT["label"], fill=_rgb(P["muted"]))
    d.text((45, H - 32), "实验来源：b24_local_vllm_interference_sweep_small_job · b25_local_vllm_interference_sweep_bulk_tradeoff",
           font=FT["small"], fill=_rgb(P["muted"]))
    img.save(OUT_DIR / "submission_control_queue_adaptive_mechanism.png")

    # ── SVG ──
    body = [
        _st(W // 2, 12 + BO[26], "提交控制策略二：队列自适应提交", 26, P["ink"], True, "middle"),
        _st(W // 2, 45 + BO[14], "候选机制：Ray actor 感知 vLLM 队列状态，动态调节请求发送节奏", 14, P["muted"], False, "middle"),
        _sr(LX, PT, LW, PB - PT, P["red_fill"], rx=10),
        _sr(RX, PT, RW, PB - PT, P["green_fill"], rx=10),
        _st(LX + 22, PT + 16 + BO[16], "固定节奏提交：无视服务状态", 16, P["ink"], True),
        _st(RX + 22, PT + 16 + BO[16], "队列自适应提交：闭环反馈", 16, P["ink"], True),
        # Left actor
        _sr(*al[:2], al[2] - al[0], al[3] - al[1], P["white"], P["blue"], 2, 8),
        _st(alcx, 144 + BO[12], "Ray Actor", 12, P["blue"], False, "middle"),
        _st(al[0] + 14, 172 + BO[10], "每 200ms 发送一批", 10, P["ink"]),
        _st(al[0] + 14, 194 + BO[10], "或每 N 行触发 flush", 10, P["ink"]),
        _st(al[0] + 14, 220 + BO[10], "→ 不关心下游状态", 10, P["red"]),
        _sr(al[0] + 14, 246, al[2] - al[0] - 28, 36, P["gray_fill"], P["gray"], 1, 6),
        _st(alcx, 258 + BO[10], "定时器 / 行计数器", 10, P["muted"], False, "middle"),
        _sa(al[2] + 8, 213, al[2] + 54, 213),
        # Left vLLM
        _sr(*vl[:2], vl[2] - vl[0], vl[3] - vl[1], P["white"], P["red"], 1, 8),
        _sr(vl[0] + 10, 142, vl[2] - vl[0] - 20, 66, P["green_fill"], P["green"], 1, 6),
        _st(vl[0] + 20, 154 + BO[11], "队列空闲时", 11, P["green"]),
        _st(vl[0] + 20, 178 + BO[10], "→ 浪费吞吐机会", 10, P["green"]),
        _sr(vl[0] + 10, 218, vl[2] - vl[0] - 20, 68, P["red_fill"], P["red"], 1, 6),
        _st(vl[0] + 20, 230 + BO[11], "队列积压时", 11, P["red"]),
        _st(vl[0] + 20, 254 + BO[10], "→ 继续塞入，加剧拥塞", 10, P["red"]),
    ]
    for i in range(6):
        qx = vl[0] + 20 + i * 22
        body.append(_sr(qx, 268, 18, 14, P["red"], rx=3))
    body.extend([
        _st(LX + 28, 356 + BO[11], "问题：提交节奏与模型服务状态解耦", 11, P["red"]),
        _st(LX + 28, 380 + BO[11], "→ 空闲时吞吐不足，拥塞时加剧尾部延迟", 11, P["red"]),
        _sa(LX + LW + 15, GAP_Y, RX - 25, GAP_Y),
        # Right actor
        _sr(*ar[:2], ar[2] - ar[0], ar[3] - ar[1], P["white"], P["blue"], 2, 8),
        _st(arcx, 144 + BO[12], "Ray Actor", 12, P["blue"], False, "middle"),
        _sr(ar[0] + 12, 170, pw, 26, P["blue_fill"], P["blue"], 1, 5),
        _st(ar[0] + 18, 176 + BO[10], "① 轮询 vLLM 队列状态", 10, P["ink"]),
        _sr(ar[0] + 12, 210, pw, 26, P["green"], rx=5),
        _st(arcx, 216 + BO[10], "空闲 → 加大发送", 10, P["white"], False, "middle"),
        _sr(ar[0] + 12, 250, pw, 26, P["orange"], rx=5),
        _st(arcx, 256 + BO[10], "积压 → 暂缓发送", 10, P["white"], False, "middle"),
        _sa(ar[2] + 8, 213, ar[2] + 46, 213),
        # Right vLLM
        _sr(*vr[:2], vr[2] - vr[0], vr[3] - vr[1], P["white"], P["green"], 2, 8),
        _st(vrcx, 144 + BO[12], "vLLM 模型服务", 12, P["green"], False, "middle"),
        _sr(vr[0] + 10, 164, vr[2] - vr[0] - 20, 22, P["blue_fill"], P["blue"], 1, 5),
        _st(vr[0] + 18, 170 + BO[10], "队列可观测：running=3  waiting=1", 10, P["blue"]),
    ])
    for i in range(4):
        sx = vr[0] + 18 + i * 28
        body.append(_sr(sx, 200, 22, 18, P["green"], rx=4))
    for i in range(3):
        sx = vr[0] + 18 + i * 20
        body.append(_sr(sx, 232, 16, 14, P["orange"], rx=3))
    body.extend([
        _st(vr[0] + 18, 256 + BO[10], "running + waiting 实时可见", 10, P["ink"]),
        _st(vr[0] + 18, 276 + BO[10], "←──────────── ▲ 反馈信号", 10, P["green"]),
        _sa(vr[0], 283, ar[2] + 6, 283, P["green"], 2),
        _st(RX + 28, 356 + BO[11], "效果：提交节奏与模型服务状态闭环", 11, P["green"]),
        _st(RX + 28, 380 + BO[11], "→ 空闲自填充，拥塞自退避 | 当前：二段式控制器", 11, P["green"]),
        _st(45, H - 54 + BO[12], caption, 12, P["muted"]),
        _st(45, H - 32 + BO[10], "实验来源：b24_local_vllm_interference_sweep_small_job · b25_local_vllm_interference_sweep_bulk_tradeoff", 10, P["muted"]),
    ])
    svg_path = OUT_DIR / "submission_control_queue_adaptive_mechanism.svg"
    _write_svg(svg_path, W, H, body)
    _validate(svg_path)


# ══════════════════════════════════════════════════════════════
# FIGURE 3: Actor Pool Routing
# ══════════════════════════════════════════════════════════════
def figure3_pool_routing():
    W, H = 1150, 530
    PT, PB = 80, 438  # panel top/bottom
    img = Image.new("RGB", (W, H), _rgb(P["bg"]))
    d = ImageDraw.Draw(img)
    _title(d, W, "提交控制策略三：Actor Pool 分池路由",
           "候选机制：按请求特征分类，路由到异构 actor pool，各池独立调度")

    # ── LEFT: single homogeneous pool ──
    _rr(d, (LX, PT, LX + LW, PB), fill=_rgb(P["red_fill"]), radius=10)
    d.text((LX + 22, PT + 16), "单一同构 Pool：请求混在一起", font=FT["section"], fill=_rgb(P["ink"]))

    # Mixed request pills
    mix = [("短", P["blue"]), ("长", P["orange"]), ("sysA", P["purple"]),
           ("短", P["blue"]), ("长", P["orange"]), ("sysA", P["purple"]),
           ("短", P["blue"]), ("长", P["orange"]), ("sysB", P["green"])]
    for idx, (label, color) in enumerate(mix):
        col, row = idx % 3, idx // 3
        rx = LX + 35 + col * 58
        ry = 135 + row * 28
        _rr(d, (rx, ry, rx + 48, ry + 20), fill=_rgb(color), radius=5)
        _ct(d, rx + 24, ry + 4, label, FT["small"], _rgb(P["white"]))

    arr_from = LX + 35 + 3 * 58
    _arr(d, arr_from, 175, arr_from + 35, 175)

    # Single pool
    sp = (arr_from + 40, 135, LX + LW - 20, 260)
    _rr(d, sp, fill=_rgb(P["white"]), outline=_rgb(P["gray"]), width=2, radius=8)
    spcx = (sp[0] + sp[2]) // 2
    _ct(d, spcx, 152, "同构 Actor Pool", FT["label"], _rgb(P["muted"]))
    d.text((sp[0] + 14, 180), "所有请求同一策略：", font=FT["small"], fill=_rgb(P["ink"]))
    d.text((sp[0] + 14, 200), "• 相同 K_max", font=FT["small"], fill=_rgb(P["ink"]))
    d.text((sp[0] + 14, 218), "• 相同 flush 节奏", font=FT["small"], fill=_rgb(P["ink"]))
    d.text((sp[0] + 14, 236), "→ 短请求被长请求拖累", font=FT["small"], fill=_rgb(P["red"]))

    d.text((LX + 28, 330), "问题：不同特征请求共享同一策略，无法差异化优化", font=FT["note"], fill=_rgb(P["red"]))
    d.text((LX + 28, 356), "短请求等长 batch；共享 prefix 丢失缓存命中机会", font=FT["note"], fill=_rgb(P["red"]))

    _arr(d, LX + LW + 15, GAP_Y, RX - 25, GAP_Y)

    # ── RIGHT: multi-pool routing ──
    _rr(d, (RX, PT, RX + RW, PB), fill=_rgb(P["green_fill"]), radius=10)
    d.text((RX + 22, PT + 16), "异构 Actor Pool：按特征分池", font=FT["section"], fill=_rgb(P["ink"]))

    # Classifier — wide enough for text
    cls = (RX + 20, 133, RX + 195, 218)
    _rr(d, cls, fill=_rgb(P["white"]), outline=_rgb(P["blue"]), width=2, radius=8)
    clscx = (cls[0] + cls[2]) // 2
    _ct(d, clscx, 150, "请求分类器", FT["label"], _rgb(P["blue"]))
    d.text((cls[0] + 14, 176), "按 token 量 + prefix 分组", font=FT["small"], fill=_rgb(P["ink"]))
    d.text((cls[0] + 14, 196), "→ 路由到对应 actor pool", font=FT["small"], fill=_rgb(P["ink"]))

    # Three pools — y positions
    pool_x = cls[2] + 30
    pool_w = RX + RW - 22 - pool_x
    pool_h = 84
    pool_ys = [138, 228, 318]  # evenly spaced, with gap

    # Arrows from classifier to each pool TITLE
    for idx, py in enumerate(pool_ys):
        target_y = py + 20  # pool title y
        _arr(d, cls[2] + 4, target_y - 2, pool_x - 4, target_y - 8,
             _rgb([P["blue"], P["orange"], P["purple"]][idx]))

    pools = [
        ("快速 Pool", "短 token 请求", "K_max 较大 · flush 节奏快", P["blue"]),
        ("慢速 Pool", "长 token 请求", "K_max 较小 · flush 节奏慢", P["orange"]),
        ("亲和 Pool", "共享 prefix 请求", "按前缀聚合提交 · 利用 KV-cache", P["purple"]),
    ]
    for idx, (name, desc, strategy, color) in enumerate(pools):
        py = pool_ys[idx]
        _rr(d, (pool_x, py, pool_x + pool_w, py + pool_h), fill=_rgb(P["white"]), outline=_rgb(color), width=2, radius=8)
        _rr(d, (pool_x + 5, py + 5, pool_x + 10, py + pool_h - 5), fill=_rgb(color), radius=3)
        d.text((pool_x + 22, py + 10), name, font=FT["section"], fill=_rgb(color))
        d.text((pool_x + 22, py + 38), desc, font=FT["small"], fill=_rgb(P["ink"]))
        d.text((pool_x + 22, py + 58), strategy, font=FT["small"], fill=_rgb(color))

    # Benefits below all pools
    by_ = pool_ys[-1] + pool_h + 14
    d.text((RX + 28, by_), "效果：不同特征匹配不同策略，去中心化协调", font=FT["note"], fill=_rgb(P["green"]))
    d.text((RX + 28, by_ + 22), "→ 短请求低延迟 + 长请求高吞吐 + 缓存高命中", font=FT["note"], fill=_rgb(P["green"]))

    caption = ("图注建议：该图说明 actor pool 分池路由的核心概念——将请求按特征分流到异构 pool 以匹配差异化策略；"
               "截至当前，该方案为设计提案，尚无实验数据支撑。")
    d.text((45, H - 54), caption, font=FT["label"], fill=_rgb(P["muted"]))
    d.text((45, H - 32), "当前状态：方案设计阶段，尚未进入实验验证。实验将在前两个提交控制机制收敛后启动。",
           font=FT["small"], fill=_rgb(P["muted"]))
    img.save(OUT_DIR / "submission_control_pool_routing_mechanism.png")

    # ── SVG ──
    body = [
        _st(W // 2, 12 + BO[26], "提交控制策略三：Actor Pool 分池路由", 26, P["ink"], True, "middle"),
        _st(W // 2, 45 + BO[14], "候选机制：按请求特征分类，路由到异构 actor pool，各池独立调度", 14, P["muted"], False, "middle"),
        _sr(LX, PT, LW, PB - PT, P["red_fill"], rx=10),
        _sr(RX, PT, RW, PB - PT, P["green_fill"], rx=10),
        _st(LX + 22, PT + 16 + BO[16], "单一同构 Pool：请求混在一起", 16, P["ink"], True),
        _st(RX + 22, PT + 16 + BO[16], "异构 Actor Pool：按特征分池", 16, P["ink"], True),
    ]
    for idx, (label, color) in enumerate(mix):
        col, row = idx % 3, idx // 3
        rx = LX + 35 + col * 58
        ry = 135 + row * 28
        body.append(_sr(rx, ry, 48, 20, color, rx=5))
        body.append(_st(rx + 24, ry + 14 + BO[10], label, 10, P["white"], False, "middle"))
    body.append(_sa(arr_from, 175, arr_from + 35, 175))
    body.extend([
        _sr(*sp[:2], sp[2] - sp[0], sp[3] - sp[1], P["white"], P["gray"], 2, 8),
        _st(spcx, 152 + BO[12], "同构 Actor Pool", 12, P["muted"], False, "middle"),
        _st(sp[0] + 14, 180 + BO[10], "所有请求同一策略：", 10, P["ink"]),
        _st(sp[0] + 14, 200 + BO[10], "• 相同 K_max", 10, P["ink"]),
        _st(sp[0] + 14, 218 + BO[10], "• 相同 flush 节奏", 10, P["ink"]),
        _st(sp[0] + 14, 236 + BO[10], "→ 短请求被长请求拖累", 10, P["red"]),
        _st(LX + 28, 330 + BO[11], "问题：不同特征请求共享同一策略，无法差异化优化", 11, P["red"]),
        _st(LX + 28, 356 + BO[11], "短请求等长 batch；共享 prefix 丢失缓存命中机会", 11, P["red"]),
        _sa(LX + LW + 15, GAP_Y, RX - 25, GAP_Y),
        # Right classifier
        _sr(*cls[:2], cls[2] - cls[0], cls[3] - cls[1], P["white"], P["blue"], 2, 8),
        _st(clscx, 150 + BO[12], "请求分类器", 12, P["blue"], False, "middle"),
        _st(cls[0] + 14, 176 + BO[10], "按 token 量 + prefix 分组", 10, P["ink"]),
        _st(cls[0] + 14, 196 + BO[10], "→ 路由到对应 actor pool", 10, P["ink"]),
    ])
    # Arrows
    for idx, py in enumerate(pool_ys):
        target_y = py + 20
        body.append(_sa(cls[2] + 4, target_y - 2, pool_x - 4, target_y - 8,
                       [P["blue"], P["orange"], P["purple"]][idx]))
    # Three pools
    for idx, (name, desc, strategy, color) in enumerate(pools):
        py = pool_ys[idx]
        body.extend([
            _sr(pool_x, py, pool_w, pool_h, P["white"], color, 2, 8),
            _sr(pool_x + 5, py + 5, 5, pool_h - 10, color, rx=3),
            _st(pool_x + 22, py + 10 + BO[16], name, 16, color, True),
            _st(pool_x + 22, py + 38 + BO[10], desc, 10, P["ink"]),
            _st(pool_x + 22, py + 58 + BO[10], strategy, 10, color),
        ])
    body.extend([
        _st(RX + 28, by_ + BO[11], "效果：不同特征匹配不同策略，去中心化协调", 11, P["green"]),
        _st(RX + 28, by_ + 22 + BO[11], "→ 短请求低延迟 + 长请求高吞吐 + 缓存高命中", 11, P["green"]),
        _st(45, H - 54 + BO[12], caption, 12, P["muted"]),
        _st(45, H - 32 + BO[10], "当前状态：方案设计阶段，尚未进入实验验证。实验将在前两个提交控制机制收敛后启动。", 10, P["muted"]),
    ])
    svg_path = OUT_DIR / "submission_control_pool_routing_mechanism.svg"
    _write_svg(svg_path, W, H, body)
    _validate(svg_path)


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    figure1_kmax()
    figure2_queue_adaptive()
    figure3_pool_routing()
    print("Done. Generated:")
    for n in ["submission_control_kmax_admission_mechanism",
              "submission_control_queue_adaptive_mechanism",
              "submission_control_pool_routing_mechanism"]:
        print(f"  figures/architecture/{n}.png")
        print(f"  figures/architecture/{n}.svg")


if __name__ == "__main__":
    main()
