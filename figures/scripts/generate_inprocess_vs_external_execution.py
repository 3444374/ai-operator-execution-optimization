"""
概念澄清图 — 进程内 UDF vs 进程外执行链路：「内部」到底指什么
解答两个困惑：
  1. 进程内 UDF 是什么（推理 = 数据库进程里的函数调用，数据不出进程）
  2. 两种「内部」的区别（物理位置 vs 架构边界，二者正交）
核心视觉：虚线框 = OS 进程边界
生成 PNG + SVG
"""
from PIL import Image, ImageDraw, ImageFont
import os

# ── Canvas ──────────────────────────────────────────────
W, H = 1400, 1150
img = Image.new("RGB", (W, H), "#FFFFFF")
draw = ImageDraw.Draw(img)

# ── Fonts ───────────────────────────────────────────────
def load_font(size, bold=False):
    candidates = [
        f"C:/Windows/Fonts/msyh{'bd' if bold else ''}.ttc",
        "C:/Windows/Fonts/msyh.ttc",
        "C:/Windows/Fonts/simhei.ttf",
    ]
    for p in candidates:
        if os.path.exists(p):
            return ImageFont.truetype(p, size)
    return ImageFont.load_default()

font_h1    = load_font(26, bold=True)
font_h2    = load_font(18, bold=True)
font_h3    = load_font(16, bold=True)
font_card  = load_font(14, bold=True)
font_body  = load_font(12)
font_small = load_font(11)
font_anno  = load_font(12, bold=True)
font_src   = load_font(10)

# ── Colors (项目四色体系 + 语义) ─────────────────────────
C_DARK   = "#1E293B"
C_GRAY   = "#94A3B8"
C_LIGHT  = "#F1F5F9"
C_WHITE  = "#FFFFFF"
C_DB     = "#475569"   # 数据库 / 进程边界（灰蓝中性）
C_INPROC = "#7C3AED"   # 进程内 UDF 路线（紫，数据库内执行对照路线）
C_SCHED  = "#F97316"   # 调度层（橙，执行调度）
C_INFER  = "#2563EB"   # 推理服务（蓝）
C_TOPIC  = "#16A34A"   # 本课题高亮（绿）
C_RED    = "#DC2626"   # 关键区别强调
C_GREEN_BG = "#F0FDF4"
C_RED_BG   = "#FEF2F2"
C_PURPLE_BG = "#FAF5FF"

MARGIN = 40

def cx(text, font):
    """text width"""
    b = draw.textbbox((0, 0), text, font=font)
    return b[2] - b[0]

def text_center(x, y, text, font, fill):
    w = cx(text, font)
    draw.text((x - w // 2, y), text, fill=fill, font=font)

# ══════════════════════════════════════════════════════════
# 标题
# ══════════════════════════════════════════════════════════
title = "进程内 UDF  vs  进程外执行链路：「内部」到底指什么"
text_center(W // 2, 24, title, font_h1, C_DARK)
subtitle = "虚线框 = OS 进程边界。推理算在框内还是框外，与 GPU 在不在公司内部是两件事"
text_center(W // 2, 62, subtitle, font_small, C_GRAY)

# ══════════════════════════════════════════════════════════
# ① 上半：进程内 UDF
# ══════════════════════════════════════════════════════════
SEC1_TITLE_Y = 100
draw.text((MARGIN, SEC1_TITLE_Y), "① 进程内 UDF：推理是数据库进程里的一个函数调用", fill=C_INPROC, font=font_h2)
ex1 = "对照路线：PostgresML (pgml.embed) · GaussML"
draw.text((W - MARGIN - cx(ex1, font_small), SEC1_TITLE_Y + 4), ex1, fill=C_GRAY, font=font_small)

# 进程边界大虚线框（横跨全宽，包住所有卡片）
BOX1 = [40, 140, 1360, 400]
def dashed_rect(xy, color, dash=8, gap=5, width=2):
    x1, y1, x2, y2 = xy
    # top
    x = x1
    while x < x2:
        xe = min(x + dash, x2)
        draw.line([(x, y1), (xe, y1)], fill=color, width=width)
        x = xe + gap
    # bottom
    x = x1
    while x < x2:
        xe = min(x + dash, x2)
        draw.line([(x, y2), (xe, y2)], fill=color, width=width)
        x = xe + gap
    # left
    y = y1
    while y < y2:
        ye = min(y + dash, y2)
        draw.line([(x1, y), (x1, ye)], fill=color, width=width)
        y = ye + gap
    # right
    y = y1
    while y < y2:
        ye = min(y + dash, y2)
        draw.line([(x2, y), (x2, ye)], fill=color, width=width)
        y = ye + gap

dashed_rect(BOX1, C_INPROC)
box1_label = "数据库进程（单个 OS 进程）—— 虚线 = 进程边界"
text_center((BOX1[0] + BOX1[2]) // 2, BOX1[1] + 14, box1_label, font_anno, C_INPROC)

# 三个卡片（都在框内）
CARD1_Y = 200
CARD1_H = 130
cards1 = [
    (80,  "SQL 执行器",
     "SELECT pgml.embed(text)\nFROM documents\n（数据在进程内读取）", C_DB),
    (530, "UDF 调用（进程内函数）",
     "像调用 SUM() / COUNT() 一样\n直接函数调用\n无序列化 · 无网络往返", C_INPROC),
    (980, "模型推理",
     "模型驻留在数据库进程内\nGPU 通过进程内绑定访问\n（如 PyTorch / ONNX 进程内加载）", C_INFER),
]
for i, (sx, ttl, detail, color) in enumerate(cards1):
    cw = 340
    draw.rounded_rectangle([sx, CARD1_Y, sx + cw, CARD1_Y + CARD1_H], radius=8,
                           fill=C_WHITE, outline=color, width=2)
    text_center(sx + cw // 2, CARD1_Y + 10, ttl, font_card, color)
    draw.line([(sx + 12, CARD1_Y + 36), (sx + cw - 12, CARD1_Y + 36)], fill="#E2E8F0", width=1)
    dy = CARD1_Y + 44
    for line in detail.split("\n"):
        text_center(sx + cw // 2, dy, line, font_body, C_DARK)
        dy += 18
    # 箭头到下一张（框内）
    if i < len(cards1) - 1:
        ax1 = sx + cw + 6
        ax2 = cards1[i + 1][0] - 6
        my = CARD1_Y + CARD1_H // 2
        draw.line([(ax1, my), (ax2, my)], fill=C_GRAY, width=2)
        draw.polygon([(ax2, my), (ax2 - 8, my - 5), (ax2 - 8, my + 5)], fill=C_GRAY)

# 进程内 callout（框内底部）
co1 = "✓ 数据从不穿过进程边界 —— 模型对 SQL 引擎而言就是一个内置函数"
text_center((BOX1[0] + BOX1[2]) // 2, 348, co1, font_anno, C_TOPIC)

# ══════════════════════════════════════════════════════════
# ② 下半：进程外执行链路（U 形数据流：出框 → 调度 → 推理 → 回框写回）
# ══════════════════════════════════════════════════════════
SEC2_TITLE_Y = 432
draw.text((MARGIN, SEC2_TITLE_Y), "② 进程外执行链路：数据离开数据库进程，经调度到达推理服务，再写回", fill=C_SCHED, font=font_h2)
ex2 = "本课题路线：Cortex / 达梦+Ray+公司内部 GPU / pgai+OpenAI"
draw.text((W - MARGIN - cx(ex2, font_small), SEC2_TITLE_Y + 4), ex2, fill=C_GRAY, font=font_small)

# 数据库进程框（小，只包左边）
BOX2 = [40, 475, 370, 745]
dashed_rect(BOX2, C_DB)
text_center((BOX2[0] + BOX2[2]) // 2, BOX2[1] + 12, "数据库进程", font_anno, C_DB)

# 框内两卡片：SQL 执行器（上）+ 结果写回（下）
def small_card(x, y, w, h, ttl, detail, color):
    draw.rounded_rectangle([x, y, x + w, y + h], radius=8, fill=C_WHITE, outline=color, width=2)
    text_center(x + w // 2, y + 8, ttl, font_card, color)
    draw.line([(x + 10, y + 30), (x + w - 10, y + 30)], fill="#E2E8F0", width=1)
    dy = y + 36
    for line in detail.split("\n"):
        text_center(x + w // 2, dy, line, font_small, C_DARK)
        dy += 15

SQL_X, SQL_Y, SQL_W, SQL_H = 60, 515, 290, 75
WB_X, WB_Y, WB_W, WB_H = 60, 645, 290, 75
small_card(SQL_X, SQL_Y, SQL_W, SQL_H, "SQL 执行器",
           "AI_COMPLETE(prompt)\n读取表数据", C_DB)
small_card(WB_X, WB_Y, WB_W, WB_H, "结果写回",
           "COPY / INSERT 写回\n向量索引（可选）", C_DB)

# 进程外卡片
SCHED_X, SCHED_Y, SCHED_W, SCHED_H = 470, 545, 280, 125
INFER_X, INFER_Y, INFER_W, INFER_H = 800, 545, 300, 125
draw.rounded_rectangle([SCHED_X, SCHED_Y, SCHED_X + SCHED_W, SCHED_Y + SCHED_H],
                       radius=8, fill=C_WHITE, outline=C_SCHED, width=2)
text_center(SCHED_X + SCHED_W // 2, SCHED_Y + 10, "调度层（上游）", font_card, C_SCHED)
draw.line([(SCHED_X + 12, SCHED_Y + 36), (SCHED_X + SCHED_W - 12, SCHED_Y + 36)], fill="#E2E8F0", width=1)
for i, line in enumerate(["Ray / Daft", "batch · 路由 · 并发控制", "（本课题优化对象）"]):
    text_center(SCHED_X + SCHED_W // 2, SCHED_Y + 46 + i * 20, line, font_body, C_DARK)

draw.rounded_rectangle([INFER_X, INFER_Y, INFER_X + INFER_W, INFER_Y + INFER_H],
                       radius=8, fill=C_WHITE, outline=C_INFER, width=2)
text_center(INFER_X + INFER_W // 2, INFER_Y + 10, "推理服务", font_card, C_INFER)
draw.line([(INFER_X + 12, INFER_Y + 36), (INFER_X + INFER_W - 12, INFER_Y + 36)], fill="#E2E8F0", width=1)
for i, line in enumerate(["vLLM · GPU", "（可在公司内部 或 公网）", "与在哪无关——只要在框外"]):
    text_center(INFER_X + INFER_W // 2, INFER_Y + 46 + i * 20, line, font_body, C_DARK)

# U 形箭头：出框 → 调度 → 推理 → 回框
def arrow(x1, y1, x2, y2, color, label=None, label_above=True):
    draw.line([(x1, y1), (x2, y2)], fill=color, width=2)
    import math
    ang = math.atan2(y2 - y1, x2 - x1)
    s = 9
    p1 = (x2 - s * math.cos(ang - 0.4), y2 - s * math.sin(ang - 0.4))
    p2 = (x2 - s * math.cos(ang + 0.4), y2 - s * math.sin(ang + 0.4))
    draw.polygon([(x2, y2), p1, p2], fill=color)
    if label:
        mx, my = (x1 + x2) // 2, (y1 + y2) // 2
        lw = cx(label, font_small)
        ly = my - 16 if label_above else my + 4
        draw.text((mx - lw // 2, ly), label, fill=color, font=font_small)

# 出：SQL执行器右 → 调度层左（穿过 x=370 边界）
arrow(SQL_X + SQL_W, SQL_Y + SQL_H // 2 - 5,
      SCHED_X, SCHED_Y + 25, C_RED, "Arrow 序列化 · 出进程", label_above=True)
# 中：调度 → 推理（水平）
arrow(SCHED_X + SCHED_W, SCHED_Y + SCHED_H // 2,
      INFER_X, INFER_Y + INFER_H // 2, C_GRAY, "数据 + batch", label_above=True)
# 回：推理服务左下 → 结果写回右（穿过 x=370 边界）
arrow(INFER_X, INFER_Y + SCHED_H - 20,
      WB_X + WB_W, WB_Y + WB_H // 2 + 5, C_RED, "结果 · 回进程写回", label_above=False)

# 进程外 callout
co2 = "✗ 数据必须穿过进程边界（虚线框）两次：出去推理 + 回来写回 —— 这就是「进程外」"
draw.rounded_rectangle([MARGIN, 760, W - MARGIN, 790], radius=6, fill=C_RED_BG, outline=C_RED, width=1)
text_center(W // 2, 769, co2, font_anno, C_RED)

# ══════════════════════════════════════════════════════════
# ③ 底部：两个维度正交矩阵
# ══════════════════════════════════════════════════════════
SEC3_TITLE_Y = 812
text_center(W // 2, SEC3_TITLE_Y, "③ 两个「内部」是正交的维度——别混为一谈", font_h3, C_DARK)

# 矩阵几何
MAT_W = 940
MAT_X = (W - MAT_W) // 2
ROW_LBL_W = 150
COL_LBL_H = 56
CELL_H = 78
CELL_W = (MAT_W - ROW_LBL_W) // 2
MAT_TOP = 845
# 列标题（跨两列，居中）
col_hdr_y = MAT_TOP
text_center(MAT_X + ROW_LBL_W + CELL_W, col_hdr_y + 6, "架构边界：推理在哪算 →", font_anno, C_DARK)
draw.text((MAT_X + ROW_LBL_W + 6, col_hdr_y + 26), "进程内（DB 进程里算）", fill=C_INPROC, font=font_small)
draw.text((MAT_X + ROW_LBL_W + CELL_W + 6, col_hdr_y + 26), "进程外（DB 进程外算）", fill=C_SCHED, font=font_small)

# 行标题
text_center(MAT_X + ROW_LBL_W // 2, MAT_TOP + COL_LBL_H + 6, "物理位置：", font_anno, C_DARK)
text_center(MAT_X + ROW_LBL_W // 2, MAT_TOP + COL_LBL_H + 24, "GPU / 资源在哪 ↓", font_small, C_GRAY)

GRID_TOP = MAT_TOP + COL_LBL_H
rows = [("公司内部 GPU", C_DB), ("公网 / 第三方", C_GRAY)]
cells = [
    # [行0列0 进程内+公司内, 行0列1 进程外+公司内], [行1列0, 行1列1]
    [("PostgresML", "模型驻留 DB 进程", "公司 GPU", C_PURPLE_BG, C_INPROC, False),
     ("★ 本课题", "达梦 + Ray + 公司 GPU", "Cortex 也是这一类", C_GREEN_BG, C_TOPIC, True)],
    [("基本不成立", "若调公网 API", "推理已在进程外", C_LIGHT, C_GRAY, False),
     ("pgai + OpenAI", "数据出进程调公网 API", "vLLM 在外部集群", "#EFF6FF", C_INFER, False)],
]
for r, (rname, rcol) in enumerate(rows):
    ry = GRID_TOP + r * CELL_H
    # 行标题格
    draw.rectangle([MAT_X, ry, MAT_X + ROW_LBL_W, ry + CELL_H], fill=C_LIGHT)
    text_center(MAT_X + ROW_LBL_W // 2, ry + CELL_H // 2 - 8, rname, font_card, rcol)
    for c in range(2):
        cx0 = MAT_X + ROW_LBL_W + c * CELL_W
        ttl, l1, l2, bg, bcol, highlight = cells[r][c]
        draw.rectangle([cx0, ry, cx0 + CELL_W, ry + CELL_H], fill=bg, outline=bcol,
                       width=3 if highlight else 1)
        text_center(cx0 + CELL_W // 2, ry + 8, ttl, font_card, bcol)
        text_center(cx0 + CELL_W // 2, ry + 30, l1, font_small, C_DARK)
        text_center(cx0 + CELL_W // 2, ry + 46, l2, font_small, C_DARK)

# 矩阵结论
conc = "结论：达梦 + 公司内部 GPU 落在「进程外 · 公司内」格——属于本课题研究对象，与 GPU 在不在公司内部无关"
text_center(W // 2, GRID_TOP + 2 * CELL_H + 14, conc, font_anno, C_TOPIC)

# 来源
src = ("来源：PostgresML 官方文档（进程内模型加载）· Cortex AISQL (SIGMOD 2026, arXiv:2511.07663) "
       "· pgai GitHub · 本课题定位见 AGENTS.md §1-§2")
draw.text((MARGIN, H - 26), src, fill=C_GRAY, font=font_src)

# ── Save PNG ────────────────────────────────────────────
out_dir = "figures/architecture"
os.makedirs(out_dir, exist_ok=True)
out_name = "inprocess_udf_vs_external_execution"
png_path = os.path.join(out_dir, out_name + ".png")
img.save(png_path, "PNG")
img2x = img.resize((W * 2, H * 2), Image.LANCZOS)
img2x.save(os.path.join(out_dir, out_name + "@2x.png"), "PNG")
print("PNG saved:", png_path)

# ══════════════════════════════════════════════════════════
# SVG generation
# ══════════════════════════════════════════════════════════
def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

S = []
S.append(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}">')
S.append('<style>text{font-family:"Microsoft YaHei","SimHei",Arial,sans-serif}'
         '.h1{font-size:26px;font-weight:bold;fill:#1E293B}'
         '.sub{font-size:11px;fill:#94A3B8}'
         '.h2{font-size:18px;font-weight:bold}'
         '.h3{font-size:16px;font-weight:bold;fill:#1E293B}'
         '.card{font-size:14px;font-weight:bold}'
         '.body{font-size:12px;fill:#1E293B}'
         '.small{font-size:11px;fill:#1E293B}'
         '.anno{font-size:12px;font-weight:bold}'
         '.src{font-size:10px;fill:#94A3B8}'
         '</style>')
S.append(f'<rect width="{W}" height="{H}" fill="white"/>')

def tcenter(x, y, text, cls, fill):
    S.append(f'<text x="{x}" y="{y}" text-anchor="middle" class="{cls}" fill="{fill}">{esc(text)}</text>')

# title
tcenter(W // 2, 44, title, "h1", "#1E293B")
tcenter(W // 2, 70, subtitle, "sub", "#94A3B8")

# ①
S.append(f'<text x="{MARGIN}" y="{SEC1_TITLE_Y+15}" class="h2" fill="{C_INPROC}">{esc("① 进程内 UDF：推理是数据库进程里的一个函数调用")}</text>')
S.append(f'<text x="{W-MARGIN}" y="{SEC1_TITLE_Y+19}" text-anchor="end" class="sub">{esc(ex1)}</text>')
# dashed box1
S.append(f'<rect x="{BOX1[0]}" y="{BOX1[1]}" width="{BOX1[2]-BOX1[0]}" height="{BOX1[3]-BOX1[1]}" '
         f'fill="none" stroke="{C_INPROC}" stroke-width="2" stroke-dasharray="8,5"/>')
tcenter((BOX1[0]+BOX1[2])//2, BOX1[1]+28, box1_label, "anno", C_INPROC)
for i,(sx,ttl,detail,color) in enumerate(cards1):
    cw=340
    S.append(f'<rect x="{sx}" y="{CARD1_Y}" width="{cw}" height="{CARD1_H}" rx="8" fill="white" stroke="{color}" stroke-width="2"/>')
    tcenter(sx+cw//2, CARD1_Y+24, ttl, "card", color)
    S.append(f'<line x1="{sx+12}" y1="{CARD1_Y+36}" x2="{sx+cw-12}" y2="{CARD1_Y+36}" stroke="#E2E8F0"/>')
    dy=CARD1_Y+58
    for line in detail.split("\n"):
        tcenter(sx+cw//2, dy, line, "body", C_DARK); dy+=18
    if i<len(cards1)-1:
        ax1=sx+cw+6; ax2=cards1[i+1][0]-6; my=CARD1_Y+CARD1_H//2
        S.append(f'<line x1="{ax1}" y1="{my}" x2="{ax2}" y2="{my}" stroke="{C_GRAY}" stroke-width="2" marker-end="url(#ag)"/>')
tcenter((BOX1[0]+BOX1[2])//2, 360, co1, "anno", C_TOPIC)
S.append(f'<defs><marker id="ag" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto"><polygon points="0,0 10,4 0,8" fill="{C_GRAY}"/></marker></defs>')
S.append(f'<defs><marker id="ar" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto"><polygon points="0,0 10,4 0,8" fill="{C_RED}"/></marker></defs>')

# ②
S.append(f'<text x="{MARGIN}" y="{SEC2_TITLE_Y+15}" class="h2" fill="{C_SCHED}">{esc("② 进程外执行链路：数据离开数据库进程，经调度到达推理服务，再写回")}</text>')
S.append(f'<text x="{W-MARGIN}" y="{SEC2_TITLE_Y+19}" text-anchor="end" class="sub">{esc(ex2)}</text>')
S.append(f'<rect x="{BOX2[0]}" y="{BOX2[1]}" width="{BOX2[2]-BOX2[0]}" height="{BOX2[3]-BOX2[1]}" fill="none" stroke="{C_DB}" stroke-width="2" stroke-dasharray="8,5"/>')
tcenter((BOX2[0]+BOX2[2])//2, BOX2[1]+26, "数据库进程", "anno", C_DB)
def svg_small_card(x,y,w,h,ttl,detail,color):
    S.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8" fill="white" stroke="{color}" stroke-width="2"/>')
    tcenter(x+w//2,y+22,ttl,"card",color)
    S.append(f'<line x1="{x+10}" y1="{y+30}" x2="{x+w-10}" y2="{y+30}" stroke="#E2E8F0"/>')
    dy=y+46
    for line in detail.split("\n"):
        tcenter(x+w//2,dy,line,"small",C_DARK); dy+=15
svg_small_card(SQL_X,SQL_Y,SQL_W,SQL_H,"SQL 执行器","AI_COMPLETE(prompt)\n读取表数据",C_DB)
svg_small_card(WB_X,WB_Y,WB_W,WB_H,"结果写回","COPY / INSERT 写回\n向量索引（可选）",C_DB)
S.append(f'<rect x="{SCHED_X}" y="{SCHED_Y}" width="{SCHED_W}" height="{SCHED_H}" rx="8" fill="white" stroke="{C_SCHED}" stroke-width="2"/>')
tcenter(SCHED_X+SCHED_W//2,SCHED_Y+24,"调度层（上游）","card",C_SCHED)
S.append(f'<line x1="{SCHED_X+12}" y1="{SCHED_Y+36}" x2="{SCHED_X+SCHED_W-12}" y2="{SCHED_Y+36}" stroke="#E2E8F0"/>')
for i,line in enumerate(["Ray / Daft","batch · 路由 · 并发控制","（本课题优化对象）"]):
    tcenter(SCHED_X+SCHED_W//2,SCHED_Y+58+i*20,line,"body",C_DARK)
S.append(f'<rect x="{INFER_X}" y="{INFER_Y}" width="{INFER_W}" height="{INFER_H}" rx="8" fill="white" stroke="{C_INFER}" stroke-width="2"/>')
tcenter(INFER_X+INFER_W//2,INFER_Y+24,"推理服务","card",C_INFER)
S.append(f'<line x1="{INFER_X+12}" y1="{INFER_Y+36}" x2="{INFER_X+INFER_W-12}" y2="{INFER_Y+36}" stroke="#E2E8F0"/>')
for i,line in enumerate(["vLLM · GPU","（可在公司内部 或 公网）","与在哪无关——只要在框外"]):
    tcenter(INFER_X+INFER_W//2,INFER_Y+58+i*20,line,"body",C_DARK)
# arrows
def svg_arrow(x1,y1,x2,y2,color,label,label_above=True):
    S.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="2" marker-end="url(#ar)"/>')
    if label:
        mx,my=(x1+x2)//2,(y1+y2)//2
        ly=my-6 if label_above else my+14
        S.append(f'<text x="{mx}" y="{ly}" text-anchor="middle" class="small" fill="{color}">{esc(label)}</text>')
svg_arrow(SQL_X+SQL_W, SQL_Y+SQL_H//2-5, SCHED_X, SCHED_Y+25, C_RED, "Arrow 序列化 · 出进程", True)
svg_arrow(SCHED_X+SCHED_W, SCHED_Y+SCHED_H//2, INFER_X, INFER_Y+INFER_H//2, C_GRAY, "数据 + batch", True)
svg_arrow(INFER_X, INFER_Y+SCHED_H-20, WB_X+WB_W, WB_Y+WB_H//2+5, C_RED, "结果 · 回进程写回", False)
S.append(f'<rect x="{MARGIN}" y="760" width="{W-2*MARGIN}" height="30" rx="6" fill="{C_RED_BG}" stroke="{C_RED}"/>')
tcenter(W//2,780,co2,"anno",C_RED)

# ③ matrix
tcenter(W//2, SEC3_TITLE_Y+14, "③ 两个「内部」是正交的维度——别混为一谈", "h3", C_DARK)
tcenter(MAT_X+ROW_LBL_W+CELL_W, MAT_TOP+14, "架构边界：推理在哪算 →", "anno", C_DARK)
S.append(f'<text x="{MAT_X+ROW_LBL_W+6}" y="{MAT_TOP+38}" class="small" fill="{C_INPROC}">进程内（DB 进程里算）</text>')
S.append(f'<text x="{MAT_X+ROW_LBL_W+CELL_W+6}" y="{MAT_TOP+38}" class="small" fill="{C_SCHED}">进程外（DB 进程外算）</text>')
tcenter(MAT_X+ROW_LBL_W//2, MAT_TOP+COL_LBL_H+16, "物理位置：", "anno", C_DARK)
tcenter(MAT_X+ROW_LBL_W//2, MAT_TOP+COL_LBL_H+34, "GPU / 资源在哪 ↓", "small", C_GRAY)
for r,(rname,rcol) in enumerate(rows):
    ry=GRID_TOP+r*CELL_H
    S.append(f'<rect x="{MAT_X}" y="{ry}" width="{ROW_LBL_W}" height="{CELL_H}" fill="{C_LIGHT}"/>')
    tcenter(MAT_X+ROW_LBL_W//2, ry+CELL_H//2+5, rname, "card", rcol)
    for c in range(2):
        cx0=MAT_X+ROW_LBL_W+c*CELL_W
        ttl,l1,l2,bg,bcol,highlight=cells[r][c]
        sw = 3 if highlight else 1
        S.append(f'<rect x="{cx0}" y="{ry}" width="{CELL_W}" height="{CELL_H}" fill="{bg}" stroke="{bcol}" stroke-width="{sw}"/>')
        tcenter(cx0+CELL_W//2, ry+20, ttl, "card", bcol)
        tcenter(cx0+CELL_W//2, ry+42, l1, "small", C_DARK)
        tcenter(cx0+CELL_W//2, ry+58, l2, "small", C_DARK)
tcenter(W//2, GRID_TOP+2*CELL_H+24, conc, "anno", C_TOPIC)
S.append(f'<text x="{MARGIN}" y="{H-14}" class="src">{esc(src)}</text>')
S.append('</svg>')
svg_path = os.path.join(out_dir, out_name + ".svg")
with open(svg_path, "w", encoding="utf-8") as f:
    f.write("\n".join(S))
print("SVG saved:", svg_path)

# ══════════════════════════════════════════════════════════
# 程序化像素自检（figures/AGENTS.md §11.6）
# ══════════════════════════════════════════════════════════
print("\n=== 程序化自检 ===")
px = img.load()
def color_present(target, tol=60):
    tr, tg, tb = int(target[1:3], 16), int(target[3:5], 16), int(target[5:7], 16)
    for y in range(0, H, 3):
        for x in range(0, W, 3):
            r, g, b = px[x, y][:3]
            if abs(r-tr) < tol and abs(g-tg) < tol and abs(b-tb) < tol:
                return True
    return False
for name, col in [("进程内紫", C_INPROC), ("调度橙", C_SCHED), ("推理蓝", C_INFER),
                  ("课题绿", C_TOPIC), ("强调红", C_RED), ("数据库灰蓝", C_DB)]:
    print(f"  color {name} {col}: {'OK present' if color_present(col) else 'MISSING'}")
