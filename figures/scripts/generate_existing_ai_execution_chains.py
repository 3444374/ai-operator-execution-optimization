"""
Slide 4 背景图 — 单行横版 AI 算子执行路径（数据离开数据库后）
生成 PNG + SVG
"""
from PIL import Image, ImageDraw, ImageFont
import os

# ── Canvas ──────────────────────────────────────────────
W, H = 1300, 410
img = Image.new("RGB", (W, H), "#FFFFFF")
draw = ImageDraw.Draw(img)

# ── Fonts ───────────────────────────────────────────────
def load_font(size, bold=False):
    candidates = [
        f"C:/Windows/Fonts/msyh{'' if not bold else 'bd'}.ttc",
        f"C:/Windows/Fonts/msyh.ttc",
    ]
    for p in candidates:
        if os.path.exists(p):
            return ImageFont.truetype(p, size)
    return ImageFont.load_default()

font_title  = load_font(22, bold=True)
font_stage  = load_font(14, bold=True)
font_detail = load_font(11)
font_anno   = load_font(12)
font_small  = load_font(10)
font_gap_h  = load_font(13, bold=True)
font_gap_b  = load_font(11)
font_src    = load_font(9)

# ── Colors ───────────────────────────────────────────────
C_DB     = "#475569"
C_WORKER = "#2563EB"
C_API    = "#EA580C"
C_BOUND  = "#DC2626"
C_GRAY   = "#94A3B8"
C_LIGHT  = "#F1F5F9"
C_DARK   = "#1E293B"
C_WHITE  = "#FFFFFF"
C_RED_BG = "#FEF2F2"

# ── Layout ───────────────────────────────────────────────
TITLE_Y = 25
SUBTITLE_Y = 55
PIPELINE_Y = 110
STAGE_W = 200
STAGE_H = 95
STAGE_GAP = 30
ARROW_W = 30
MARGIN = 40

# Center the boundary so left and right areas are equal
# L_AREA = BOUNDARY_X - STAGE_GAP - MARGIN
# R_AREA = W - MARGIN - BOUNDARY_X - STAGE_GAP - ARROW_W
# L_AREA == R_AREA => BOUNDARY_X = (W - ARROW_W) // 2
BOUNDARY_X = (W - ARROW_W) // 2  # = 635

# Compute stage positions (4 stages: 2 left of boundary, 2 right)
L_STAGES = 2
R_STAGES = 2
L_START = MARGIN
L_AREA = BOUNDARY_X - STAGE_GAP - L_START
R_START = BOUNDARY_X + STAGE_GAP + ARROW_W
R_AREA = W - MARGIN - R_START

# Adjust stage widths to fit available space evenly
L_STAGE_W = (L_AREA - (L_STAGES - 1) * (ARROW_W + STAGE_GAP) - STAGE_GAP) // L_STAGES
R_STAGE_W = (R_AREA - (R_STAGES - 1) * (ARROW_W + STAGE_GAP) - STAGE_GAP) // R_STAGES

stages = [
    # (x, title, detail, color)
    (L_START, "① AI SQL 查询",
     "AI_COMPLETE · AI_EMBED 等算子\n进入查询计划\n表数据读取，准备发送",
     C_DB),
    (L_START + L_STAGE_W + STAGE_GAP + ARROW_W, "② 中间件 / 队列",
     "pgai: Python Worker 轮询\nBigQuery: Dremel 编排\nOceanBase: 内置 HTTP",
     C_WORKER),
    (R_START, "③ 外部模型服务调用",
     "Vertex AI · OpenAI · DeepSeek\n阿里云 DashScope\nvLLM / ONNX Runtime",
     C_API),
    (R_START + R_STAGE_W + STAGE_GAP + ARROW_W, "④ 结果写回",
     "JSON 解析 → 数据库列\nCOPY / INSERT 写回\n向量索引（可选）",
     C_DB),
]

# ── Helpers ─────────────────────────────────────────────
def draw_stage_box(x, y, w, h, title, detail, color):
    r = 8
    draw.rounded_rectangle([x, y, x+w, y+h], radius=r, fill=C_WHITE, outline=color, width=2)
    # Title — centered
    bbox = draw.textbbox((0, 0), title, font=font_stage)
    tw = bbox[2] - bbox[0]
    draw.text((x + (w - tw)//2, y + 8), title, fill=color, font=font_stage)
    # Separator
    draw.line([(x + 12, y + 34), (x + w - 12, y + 34)], fill="#E2E8F0", width=1)
    # Detail lines — center-aligned
    lines = detail.split("\n")
    dy = y + 40
    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=font_detail)
        lw = bbox[2] - bbox[0]
        draw.text((x + (w - lw)//2, dy), line, fill=C_DARK, font=font_detail)
        dy += 16

def draw_arrow_right(x1, x2, mid_y):
    draw.line([(x1, mid_y), (x2, mid_y)], fill=C_GRAY, width=2)
    pts = [(x2, mid_y), (x2 - 8, mid_y - 5), (x2 - 8, mid_y + 5)]
    draw.polygon(pts, fill=C_GRAY)

# ── Title ───────────────────────────────────────────────
title = "AI 算子执行链路：\"上游\"在哪里"
bbox = draw.textbbox((0, 0), title, font=font_title)
draw.text(((W - bbox[2] + bbox[0])//2, TITLE_Y), title, fill=C_DARK, font=font_title)

subtitle = "从数据读取到模型推理之间，存在一个\"决策环节\"——本课题称为上游调度"
bbox2 = draw.textbbox((0, 0), subtitle, font=font_small)
draw.text(((W - bbox2[2] + bbox2[0])//2, SUBTITLE_Y), subtitle, fill=C_GRAY, font=font_small)

# ── Upstream/Downstream Boundary ───────────────────────────
pipe_top = PIPELINE_Y - 15
pipe_bot = PIPELINE_Y + STAGE_H + 15
dy = pipe_top
while dy < pipe_bot:
    end = min(dy + 8, pipe_bot)
    draw.line([(BOUNDARY_X, dy), (BOUNDARY_X, end)], fill=C_BOUND, width=2)
    dy = end + 5
C_UP = "#2563EB"
draw.text((BOUNDARY_X - 52, pipe_top - 18), "上游", fill=C_UP, font=font_anno)
draw.text((BOUNDARY_X + 12, pipe_top - 18), "下游", fill=C_API, font=font_anno)

# ── Draw stages + arrows ────────────────────────────────
mid_y = PIPELINE_Y + STAGE_H // 2
for i, (sx, title, detail, color) in enumerate(stages):
    w = L_STAGE_W if i < 2 else R_STAGE_W
    draw_stage_box(sx, PIPELINE_Y, w, STAGE_H, title, detail, color)
    if i < len(stages) - 1:
        ax1 = sx + w + 6
        ax2 = stages[i+1][0] - 6
        draw_arrow_right(ax1, ax2, mid_y)

# ── System-specific detail row ─────────────────────────
sys_y = PIPELINE_Y + STAGE_H + 30
detail_color = "#1E40AF"
draw.text((MARGIN, sys_y), "pgai:", fill=C_WORKER, font=font_stage)
draw.text((MARGIN + 48, sys_y), "触发器→队列→外部 Python Worker 轮询→OpenAI/Voyage API→COPY 写回", fill=detail_color, font=font_detail)
draw.text((MARGIN, sys_y + 20), "BigQuery:", fill=detail_color, font=font_stage)
draw.text((MARGIN + 70, sys_y + 20), "Dremel 数据 Workers → 推理 Workers 攒批(默认128行)→Vertex AI(Gemini) API→JSON 解析→结果集", fill=detail_color, font=font_detail)
draw.text((MARGIN, sys_y + 40), "OceanBase:", fill=detail_color, font=font_stage)
draw.text((MARGIN + 82, sys_y + 40), "OBServer 端点查找→构造 JSON→HTTPS POST→阿里云/DeepSeek/OpenAI→JSON 解析→结果列", fill=detail_color, font=font_detail)

# ── Common Gap ──────────────────────────────────────────
gap_y = sys_y + 72
gap_h = 42
draw.rounded_rectangle([MARGIN - 6, gap_y, W - MARGIN + 6, gap_y + gap_h],
                       radius=8, fill=C_RED_BG, outline=C_BOUND, width=2)
gap_text = ("共性：三种系统的\"决策环节\"中，batch 粒度、提交节奏、并发控制——均由各厂商内部实现，"
            "不是独立的系统优化目标。本课题把这个地带称为上游调度：数据组织策略 + 提交控制策略。")
draw.text((MARGIN + 10, gap_y + 16), gap_text, fill=C_DARK, font=font_gap_b)
src = "来源：pgai GitHub 源码（2026.2 归档）· BigQuery ML.GENERATE_TEXT 官方文档 · OceanBase DBMS_AI_SERVICE 4.3.3+ 文档"
draw.text((MARGIN + 4, gap_y + gap_h + 10), src, fill=C_GRAY, font=font_src)

# ── Save PNG ────────────────────────────────────────────
out_dir = "figures/architecture"
os.makedirs(out_dir, exist_ok=True)
img.save(os.path.join(out_dir, "existing_ai_operator_execution_chains.png"), "PNG")
img2x = img.resize((W*2, H*2), Image.LANCZOS)
img2x.save(os.path.join(out_dir, "existing_ai_operator_execution_chains@2x.png"), "PNG")
print("PNG saved.")

# ══════════════════════════════════════════════════════════
# SVG generation
# ══════════════════════════════════════════════════════════
def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

svg_lines = []
svg_lines.append(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}">')
svg_lines.append('<style>')
svg_lines.append('  text { font-family: "Microsoft YaHei", "SimHei", Arial, sans-serif; }')
svg_lines.append('  .title { font-size: 22px; font-weight: bold; fill: #1E293B; }')
svg_lines.append('  .subtitle { font-size: 10px; fill: #94A3B8; }')
svg_lines.append('  .stage-title { font-size: 14px; font-weight: bold; }')
svg_lines.append('  .stage-detail { font-size: 11px; fill: #1E293B; }')
svg_lines.append('  .anno { font-size: 12px; }')
svg_lines.append('  .sys-label { font-size: 14px; font-weight: bold; }')
svg_lines.append('  .sys-detail { font-size: 11px; fill: #1E40AF; }')
svg_lines.append('  .gap-title { font-size: 13px; font-weight: bold; fill: #DC2626; }')
svg_lines.append('  .gap-text { font-size: 11px; fill: #1E293B; }')
svg_lines.append('  .source { font-size: 9px; fill: #94A3B8; }')
svg_lines.append('</style>')

# Background
svg_lines.append(f'<rect width="{W}" height="{H}" fill="white"/>')

# Title
svg_lines.append(f'<text x="{W//2}" y="{TITLE_Y + 18}" text-anchor="middle" class="title">{esc(title)}</text>')
svg_lines.append(f'<text x="{W//2}" y="{SUBTITLE_Y + 10}" text-anchor="middle" class="subtitle">{esc(subtitle)}</text>')

# DB boundary dashed line
svg_lines.append(f'<line x1="{BOUNDARY_X}" y1="{pipe_top}" x2="{BOUNDARY_X}" y2="{pipe_bot}" stroke="#DC2626" stroke-width="2" stroke-dasharray="8,5"/>')
svg_lines.append(f'<text x="{BOUNDARY_X - 52}" y="{pipe_top - 4}" class="anno" fill="#2563EB">上游</text>')
svg_lines.append(f'<text x="{BOUNDARY_X + 12}" y="{pipe_top - 4}" class="anno" fill="#EA580C">下游</text>')

# Stages as SVG rects
for i, (sx, s_title, s_detail, color) in enumerate(stages):
    sw = L_STAGE_W if i < 2 else R_STAGE_W
    sy = PIPELINE_Y
    sh = STAGE_H
    r = 8
    svg_lines.append(f'<rect x="{sx}" y="{sy}" width="{sw}" height="{sh}" rx="{r}" fill="white" stroke="{color}" stroke-width="2"/>')
    # Title
    svg_lines.append(f'<text x="{sx + sw//2}" y="{sy + 26}" text-anchor="middle" class="stage-title" fill="{color}">{esc(s_title)}</text>')
    # Separator
    svg_lines.append(f'<line x1="{sx + 12}" y1="{sy + 34}" x2="{sx + sw - 12}" y2="{sy + 34}" stroke="#E2E8F0" stroke-width="1"/>')
    # Detail lines - center aligned
    dlines = s_detail.split("\n")
    dy = sy + 56
    for dl in dlines:
        svg_lines.append(f'<text x="{sx + sw//2}" y="{dy}" text-anchor="middle" class="stage-detail">{esc(dl)}</text>')
        dy += 16

    # Arrow to next
    if i < len(stages) - 1:
        ax1 = sx + sw + 6
        ax2 = stages[i+1][0] - 6
        ay = mid_y
        svg_lines.append(f'<defs><marker id="arrow{i}" markerWidth="10" markerHeight="8" refX="9" refY="4" orient="auto"><polygon points="0,0 10,4 0,8" fill="#94A3B8"/></marker></defs>')
        svg_lines.append(f'<line x1="{ax1}" y1="{ay}" x2="{ax2}" y2="{ay}" stroke="#94A3B8" stroke-width="2" marker-end="url(#arrow{i})"/>')

# System-specific detail row
svg_lines.append(f'<text x="{MARGIN}" y="{sys_y + 10}" class="sys-label" fill="#2563EB">pgai:</text>')
svg_lines.append(f'<text x="{MARGIN + 48}" y="{sys_y + 10}" class="sys-detail">触发器→队列→外部 Python Worker 轮询→OpenAI/Voyage API→COPY 写回</text>')
svg_lines.append(f'<text x="{MARGIN}" y="{sys_y + 30}" class="sys-label" fill="{detail_color}">BigQuery:</text>')
svg_lines.append(f'<text x="{MARGIN + 70}" y="{sys_y + 30}" class="sys-detail">Dremel 数据 Workers → 推理 Workers 攒批(默认128行)→Vertex AI(Gemini) API→JSON 解析→结果集</text>')
svg_lines.append(f'<text x="{MARGIN}" y="{sys_y + 50}" class="sys-label" fill="{detail_color}">OceanBase:</text>')
svg_lines.append(f'<text x="{MARGIN + 82}" y="{sys_y + 50}" class="sys-detail">OBServer 端点查找→构造 JSON→HTTPS POST→阿里云/DeepSeek/OpenAI→JSON 解析→结果列</text>')

# Gap bar
svg_lines.append(f'<rect x="{MARGIN - 6}" y="{gap_y}" width="{W - 2*MARGIN + 12}" height="{gap_h}" rx="8" fill="#FEF2F2" stroke="#DC2626" stroke-width="2"/>')
svg_lines.append(f'<text x="{MARGIN + 10}" y="{gap_y + 26}" class="gap-title">共性</text>')
svg_lines.append(f'<text x="{MARGIN + 10}" y="{gap_y + 26}" class="gap-text" dx="40">三种系统的"决策环节"中，batch 粒度、提交节奏、并发控制——均由各厂商内部实现，不是独立的系统优化目标。本课题把这个环节称为上游调度：数据组织策略 + 提交控制策略。</text>')
svg_lines.append(f'<text x="{MARGIN + 4}" y="{gap_y + gap_h + 10}" class="source">{esc(src)}</text>')

svg_lines.append('</svg>')

svg_path = os.path.join(out_dir, "existing_ai_operator_execution_chains.svg")
with open(svg_path, "w", encoding="utf-8") as f:
    f.write("\n".join(svg_lines))
print(f"SVG saved: {svg_path}")
print("Done.")
