"""
完整执行框架图 —— 按最新调研（strategy_design_implementation_reference.md §8, 2026-07-23）
主链路：PostgreSQL → Daft 引擎层 → 研究内容一(数据组织) → 研究内容二(调度提交) → vLLM(黑盒) → 写回
重点：研究内容一/二；Daft=数据引擎，Ray=调度载体；vLLM 黑盒只画边界+Prometheus；每策略标注文献来源
反馈环走顶部弧线（vLLM→研究二 Admission），写回走底部弧线（vLLM→PG）
"""
from PIL import Image, ImageDraw, ImageFont
import os

W, H = 1620, 955
img = Image.new("RGB", (W, H), "#FFFFFF")
draw = ImageDraw.Draw(img)

def lf(s, b=False):
    for p in (f"C:/Windows/Fonts/msyh{'bd' if b else ''}.ttc", "C:/Windows/Fonts/msyh.ttc", "C:/Windows/Fonts/simhei.ttf"):
        if os.path.exists(p):
            return ImageFont.truetype(p, s)
    return ImageFont.load_default()

f_h1=lf(22,True); f_h2=lf(14,True); f_h3=lf(12,True); f_body=lf(11)
f_small=lf(10); f_tiny=lf(9); f_anno=lf(11,True); f_src=lf(9)

C_DARK="#1E293B"; C_GRAY="#64748B"; C_BORDER="#E2E8F0"; C_LIGHT="#F8FAFC"; C_WHITE="#FFFFFF"
C_STORE="#475569"; C_ENGINE="#2563EB"; C_RC1="#16A34A"; C_RC2="#EA580C"; C_VLLM="#7C3AED"; C_RED="#DC2626"

def cw(t,f):
    b=draw.textbbox((0,0),t,font=f); return b[2]-b[0]
def tc(x,y,t,f=None,fill=C_DARK,**kw):
    font=kw.get('font',f); draw.text((x-cw(t,font)//2,y),t,fill=fill,font=font)
def dt(x,y,t,fill=C_DARK,font=f_small):
    draw.text((x,y),t,fill=fill,font=font)

def panel(x,y,w,h,title,color,sub=""):
    draw.rounded_rectangle([x,y,x+w,y+h],radius=10,fill=C_WHITE,outline=color,width=3)
    draw.rounded_rectangle([x,y,x+w,y+40],radius=10,fill=color)
    draw.rectangle([x+2,y+20,x+w-2,y+40],fill=color)
    tc(x+w//2,y+7,title,f_h2,C_WHITE)
    if sub:
        tc(x+w//2,y+44,sub,f_small,color)
    draw.line([(x+12,y+62),(x+w-12,y+62)],fill=C_BORDER)
    return y+72

def bullet(x,y,w,text,lit="",font=f_small,col=C_DARK,litcol=C_GRAY):
    dt(x,y,"•",fill=col,font=font)
    dt(x+12,y,text,fill=col,font=font)
    if lit:
        dt(x+12,y+13,lit,fill=litcol,font=f_tiny)
        return y+30
    return y+22

def harrow(x1,x2,y,color=C_GRAY,w=2,label=None,laby=-14):
    draw.line([(x1,y),(x2,y)],fill=color,width=w)
    draw.polygon([(x2,y),(x2-8,y-5),(x2-8,y+5)],fill=color)
    if label:
        tc((x1+x2)//2,y+laby,label,f_small,color)

# ── 标题 ──
tc(W//2,14,"数据库 AI 算子外部执行链路 · 完整执行框架",font=f_h1,fill=C_DARK)
tc(W//2,46,"按 strategy_design_implementation_reference §8（2026-07-23）—— 重点：研究内容一/二；Daft=数据引擎，Ray=调度载体；vLLM 黑盒不展开",font=f_small,fill=C_GRAY)

# ── Workload Profiler 顶带（单行，压缩）──
wp_y=74; wp_h=44
draw.rounded_rectangle([40,wp_y,W-40,wp_y+wp_h],radius=8,fill=C_LIGHT,outline=C_BORDER)
dt(72,wp_y+6,"执行前 · Workload Profiler",fill=C_DARK,font=f_h3)
dt(72,wp_y+25,"算子类型(AI_COMPLETE/EMBED/CLASSIFY) · 行数估计 · token 长度分布 · prefix 结构 · 输出大小 → 给研究内容一选策略、研究内容二选初始 K_max",fill=C_DARK,font=f_small)

# ── 主链路 5 框 ──
ML_Y=158; ML_H=380
xs=[(60,200),(290,260),(580,340),(950,340),(1320,240)]

# f0 PostgreSQL
x,w=xs[0]; cy=panel(x,ML_Y,w,ML_H,"PostgreSQL",C_STORE,"存储层")
for t in ["documents 表","（数据源）","","写回目标：","  document_embeddings","  pgvector","","工程 baseline：","  COPY + deferred index"]:
    if t.startswith("（") or t.startswith("  ") or t.startswith("工程"):
        dt(x+12,cy,t,fill=C_GRAY,font=f_small)
    elif t:
        dt(x+12,cy,t,fill=C_DARK,font=f_small)
    cy+=20

# f1 Daft 数据引擎
x,w=xs[1]; cy=panel(x,ML_Y,w,ML_H,"Daft 数据引擎",C_ENGINE,"引擎层")
dt(x+12,cy,"DataFrame（df[\"prompt\"]）",fill=C_DARK,font=f_small); cy+=20
dt(x+12,cy,"into_batches / repartition",fill=C_DARK,font=f_small); cy+=20
dt(x+12,cy,"@daft.cls GPU UDF",fill=C_DARK,font=f_small); cy+=24
draw.line([(x+12,cy),(x+w-12,cy)],fill=C_BORDER); cy+=10
dt(x+12,cy,"多模态复用：",fill=C_RC1,font=f_small); cy+=18
dt(x+12,cy,"  df[\"prompt\"] → df[\"image\"]",fill=C_DARK,font=f_small); cy+=18
dt(x+12,cy,"  token_budget → frame_budget",fill=C_DARK,font=f_small); cy+=20
draw.line([(x+12,cy),(x+w-12,cy)],fill=C_BORDER); cy+=10
dt(x+12,cy,"Arrow 作为对照/回退",fill=C_GRAY,font=f_small); cy+=18
dt(x+12,cy,"DataOrganizer 抽象",fill=C_GRAY,font=f_small); cy+=18
dt(x+12,cy,"隔离引擎细节",fill=C_GRAY,font=f_small); cy+=18

# f2 研究内容一（重点·绿）
x,w=xs[2]; cy=panel(x,ML_Y,w,ML_H,"研究内容一 · 数据组织策略",C_RC1,"DataOrganizer.organize()")
dt(x+12,cy,"三种 batching 策略：",fill=C_RC1,font=f_anno); cy+=22
cy=bullet(x+12,cy,w,"token-budget","← vLLM max_num_batched_tokens",col=C_DARK)
cy=bullet(x+12,cy,w,"length-align","← 减少 generation straggler / padding",col=C_DARK)
cy=bullet(x+12,cy,w,"prefix-aware","→ 提高 vLLM APC（prefix cache）命中率",col=C_DARK)
cy+=6
draw.line([(x+12,cy),(x+w-12,cy)],fill=C_BORDER); cy+=10
dt(x+12,cy,"输出 BatchRequest：",fill=C_RC1,font=f_anno); cy+=20
dt(x+12,cy,"  prompt_tokens_sum / min / p95",fill=C_DARK,font=f_small); cy+=18
dt(x+12,cy,"  prefix_key / row_count",fill=C_DARK,font=f_small); cy+=18
dt(x+12,cy,"  token_count_source",fill=C_DARK,font=f_small); cy+=20
draw.line([(x+12,cy),(x+w-12,cy)],fill=C_BORDER); cy+=10
dt(x+12,cy,"策略层不依赖引擎：",fill=C_GRAY,font=f_small); cy+=18
dt(x+12,cy,"只依赖 BatchRequest 元数据，",fill=C_GRAY,font=f_small); cy+=18
dt(x+12,cy,"不调用 daft.* / pyarrow.*",fill=C_GRAY,font=f_small); cy+=18

# f3 研究内容二（重点·橙）
x,w=xs[3]; cy=panel(x,ML_Y,w,ML_H,"研究内容二 · 调度与提交控制",C_RC2,"Ray actor 异构 pool + 去中心化 async loop")
dt(x+12,cy,"① Admission Controller",fill=C_RC2,font=f_anno); cy+=20
dt(x+12,cy,"输入：vLLM Prometheus 指标",fill=C_DARK,font=f_small); cy+=17
dt(x+12,cy,"AIMD K_max（加性增/乘性减）",fill=C_DARK,font=f_small); cy+=17
dt(x+12,cy,"← Clipper NSDI'17 + CONCUR 2025",fill=C_GRAY,font=f_tiny); cy+=15
dt(x+12,cy,"现状双态(:512)作对照，验证后换 AIMD",fill=C_GRAY,font=f_tiny); cy+=18
draw.line([(x+12,cy),(x+w-12,cy)],fill=C_BORDER); cy+=10
dt(x+12,cy,"② Router",fill=C_RC2,font=f_anno); cy+=20
dt(x+12,cy,"prefix-aware ← SGLang RadixAttn",fill=C_DARK,font=f_small); cy+=17
dt(x+12,cy,"token-aware ← Parrot OSDI'24",fill=C_DARK,font=f_small); cy+=17
dt(x+12,cy,"算子类型感知路由",fill=C_DARK,font=f_small); cy+=18
draw.line([(x+12,cy),(x+w-12,cy)],fill=C_BORDER); cy+=10
dt(x+12,cy,"（可选）RequestPool",fill=C_GRAY,font=f_small); cy+=17
dt(x+12,cy,"跨查询按 token budget 合并",fill=C_GRAY,font=f_small); cy+=18

# f4 vLLM（黑盒·紫）
x,w=xs[4]; cy=panel(x,ML_Y,w,ML_H,"vLLM 推理引擎",C_VLLM,"部署平台 · 不修改")
dt(x+12,cy,"黑盒 —— 内部不展开：",fill=C_VLLM,font=f_anno); cy+=20
dt(x+12,cy,"  continuous batching",fill=C_GRAY,font=f_small); cy+=17
dt(x+12,cy,"  APC / PagedAttention",fill=C_GRAY,font=f_small); cy+=17
dt(x+12,cy,"  （本课题不研究）",fill=C_GRAY,font=f_tiny); cy+=22
draw.line([(x+12,cy),(x+w-12,cy)],fill=C_BORDER); cy+=10
dt(x+12,cy,"对外输出 Prometheus：",fill=C_VLLM,font=f_anno); cy+=20
dt(x+12,cy,"  num_requests_running",fill=C_DARK,font=f_small); cy+=17
dt(x+12,cy,"  num_requests_waiting",fill=C_DARK,font=f_small); cy+=17
dt(x+12,cy,"  gpu_cache_usage_perc",fill=C_DARK,font=f_small); cy+=22
draw.line([(x+12,cy),(x+w-12,cy)],fill=C_BORDER); cy+=10
dt(x+12,cy,"GPU forward",fill=C_DARK,font=f_small); cy+=18

# ── 主链路水平箭头 ──
my=ML_Y+ML_H//2
for i in range(4):
    x1=xs[i][0]+xs[i][1]; x2=xs[i+1][0]
    lab={0:"Arrow 读出",1:"organize",2:"submit",3:"HTTP POST"}[i]
    harrow(x1+4,x2-4,my,label=lab)

# ── 反馈环：vLLM → 研究内容二（顶部弧线，虚线红）──
fb_top=ML_Y-10
x3c=xs[3][0]+xs[3][1]//2
x4c=xs[4][0]+xs[4][1]//2
# 竖 f4 上
yy=ML_Y
while yy>fb_top:
    ye=max(yy-4,fb_top); draw.line([(x4c,yy),(x4c,ye)],fill=C_RED,width=2); yy=ye-3
# 横 f4→f3
xx=x4c
while xx>x3c:
    xe=max(xx-5,x3c); draw.line([(xx,fb_top),(xe,fb_top)],fill=C_RED,width=2); xx=xe-4
# 竖 f3 下
yy=fb_top
while yy<ML_Y-6:
    ye=min(yy+4,ML_Y-6); draw.line([(x3c,yy),(x3c,ye)],fill=C_RED,width=2); yy=ye+3
draw.polygon([(x3c,ML_Y),(x3c-5,ML_Y-8),(x3c+5,ML_Y-8)],fill=C_RED)
tc((x3c+x4c)//2,fb_top-17,"Prometheus 指标反馈 → AIMD 调 K_max",font=f_small,fill=C_RED)

# ── 写回弧线：f4 底部 → f0 底部（灰蓝）──
wb_y=ML_Y+ML_H+16
x0c=xs[0][0]+xs[0][1]//2
draw.line([(x4c,ML_Y+ML_H),(x4c,wb_y),(x0c,wb_y),(x0c,ML_Y+ML_H)],fill=C_STORE,width=2)
draw.polygon([(x0c,ML_Y+ML_H),(x0c-5,ML_Y+ML_H-8),(x0c+5,ML_Y+ML_H-8)],fill=C_STORE)
tc((x0c+x4c)//2,wb_y-15,"写回：fan-in → COPY + deferred index → PostgreSQL/pgvector",font=f_small,fill=C_STORE)

# ── Workload Profiler 向下箭头 ──
px=(xs[1][0]+xs[1][1]//2 + xs[2][0]+xs[2][1]//2)//2
draw.line([(px,wp_y+wp_h),(px,ML_Y-4)],fill=C_GRAY,width=2)
draw.polygon([(px,ML_Y+2),(px-5,ML_Y-6),(px+5,ML_Y-6)],fill=C_GRAY)

# ── Guardrail 底带 ──
gb_y=wb_y+26
draw.rounded_rectangle([40,gb_y,W-40,gb_y+50],radius=8,fill="#FEF2F2",outline=C_RED)
dt(72,gb_y+6,"端到端验证 Guardrail",fill=C_RED,font=f_h3)
dt(72,gb_y+25,"writeback ratio（写回是否吞噬上游收益）· P99/TTFT/TPOT · throughput · GPU utilization · 只在端到端指标上变好才算策略成功",fill=C_DARK,font=f_small)

# ── 8 prompt 实例带：实例化研究内容一的 token-budget 切分 ──
ib_y=gb_y+56
ib_h=250
draw.rounded_rectangle([40,ib_y,W-40,ib_y+ib_h],radius=10,fill=C_LIGHT,outline=C_BORDER)
dt(60,ib_y+8,"实例：8 个 prompt 流经本框架",fill=C_DARK,font=f_h2)
dt(60,ib_y+32,"橙=prefix A / 蓝=prefix B，条宽 ∝ token 量 —— 研究内容一 token-budget（预算 300）把 8 行切成 4 个 batch（3/2/2/1 行）",fill=C_GRAY,font=f_small)

C_PA="#F97316"; C_PB="#2563EB"
PROMPTS8=[("P0",50,"A"),("P1",180,"A"),("P2",60,"B"),("P3",200,"B"),("P4",40,"A"),("P5",90,"B"),("P6",70,"A"),("P7",150,"B")]
BATCHES8=[[("P0",50,"A"),("P1",180,"A"),("P2",60,"B")],[("P3",200,"B"),("P4",40,"A")],[("P5",90,"B"),("P6",70,"A")],[("P7",150,"B")]]

SC=0.7
bar_y=ib_y+62
dt(60,bar_y+2,"8 prompts:",fill=C_DARK,font=f_small)
bx=200
for pid,tok,pfx in PROMPTS8:
    bw=tok*SC; col=C_PA if pfx=="A" else C_PB
    draw.rounded_rectangle([bx,bar_y,bx+bw,bar_y+22],radius=3,fill=col,outline=C_WHITE,width=1)
    if bw>22: tc(bx+bw//2,bar_y+4,pid,font=f_tiny,fill=C_WHITE)
    bx+=bw

bc_y=ib_y+110
dt(60,bc_y+2,"token-budget →",fill=C_RC1,font=f_small)
bx=200; gap=10
for bi,b in enumerate(BATCHES8):
    bw=sum(t for _,t,_ in b)*SC
    draw.rounded_rectangle([bx,bc_y,bx+bw,bc_y+22],radius=4,fill=C_WHITE,outline=C_RC1,width=2)
    ix=bx+3
    for pid,tok,pfx in b:
        ibw=tok*SC; col=C_PA if pfx=="A" else C_PB
        draw.rectangle([ix,bc_y+3,ix+ibw,bc_y+19],fill=col)
        ix+=ibw
    tot=sum(t for _,t,_ in b)
    tc(bx+bw//2,bc_y+26,f"batch{bi+1}: {len(b)}行/{tot}tok",font=f_tiny,fill=C_DARK)
    bx+=bw+gap

dt(60,ib_y+ib_h-26,"→ batch1 经研究内容二 Admission（AIMD K_max）+ Router 提交 vLLM；反馈环按 vLLM 指标动态调 K_max",fill=C_DARK,font=f_small)

dt(40,H-16,"来源：strategy_design_implementation_reference.md §6/§8（2026-07-23）· 文献：vLLM SOSP'23 · Clipper NSDI'17 · CONCUR 2025 · SGLang NeurIPS'24 · Parrot OSDI'24 · Daft 官方文档",fill=C_GRAY,font=f_src)

out_dir="figures/architecture"; os.makedirs(out_dir,exist_ok=True); name="execution_framework_overview"
img.save(os.path.join(out_dir,name+".png"),"PNG")
img.resize((W*2,H*2),Image.LANCZOS).save(os.path.join(out_dir,name+"@2x.png"),"PNG")
print("PNG saved.")
print("=== self-check ===")
px_=img.load()
def present(t,tol=70):
    tr,tg,tb=int(t[1:3],16),int(t[3:5],16),int(t[5:7],16)
    for y in range(0,H,3):
        for xx in range(0,W,3):
            r,g,b=px_[xx,y][:3]
            if abs(r-tr)<tol and abs(g-tg)<tol and abs(b-tb)<tol: return True
    return False
for nm,c in [("存储灰蓝",C_STORE),("引擎蓝",C_ENGINE),("研究一绿",C_RC1),("研究二橙",C_RC2),("vLLM紫",C_VLLM),("反馈红",C_RED)]:
    print(f"  {nm} {c}: {'OK' if present(c) else 'MISSING'}")
