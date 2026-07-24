"""
学习配图 v3 — 请求的完整旅程：形成 → 切分 → 提交控制 → vLLM 推理
四段：①数据进入 ②研究内容一·策略切分(token-budget) ③研究内容二·提交控制(动态调度) ④单batch进vLLM旅程
按项目真实实现画：organizers.py / submit_with_backpressure / adaptive_inflight_limit / model_backends
"""
from PIL import Image, ImageDraw, ImageFont
import os

W, H = 1520, 1270
img = Image.new("RGB", (W, H), "#FFFFFF")
draw = ImageDraw.Draw(img)

def lf(size, bold=False):
    for p in (f"C:/Windows/Fonts/msyh{'bd' if bold else ''}.ttc", "C:/Windows/Fonts/msyh.ttc", "C:/Windows/Fonts/simhei.ttf"):
        if os.path.exists(p):
            return ImageFont.truetype(p, size)
    return ImageFont.load_default()

f_h1=lf(22,True); f_h2=lf(15,True); f_body=lf(11); f_small=lf(10)
f_tiny=lf(9,True); f_anno=lf(11,True); f_code=lf(10); f_src=lf(9); f_big=lf(20,True)

C_DARK="#1E293B"; C_GRAY="#64748B"; C_BORDER="#E2E8F0"; C_LIGHT="#F8FAFC"; C_WHITE="#FFFFFF"
C_PA="#F97316"; C_PB="#2563EB"                      # prefix A/B
C_BATCH="#16A34A"; C_BATCH_BG="#F0FDF4"             # 研究内容一
C_SCHED="#EA580C"; C_SCHED_BG="#FFF7ED"             # 研究内容二（橙红，区别于 prefix 橙）
C_RED="#DC2626"; C_VLLM="#7C3AED"; C_VLLM_BG="#FAF5FF"

# (pid, tok, prefix, desc)
PROMPTS=[("P0",50,"A","Summarize ticket"),("P1",180,"A","Incident summary"),
         ("P2",60,"B","Extract contract"),("P3",200,"B","Research note"),
         ("P4",40,"A","Classify review"),("P5",90,"B","SQL explanation"),
         ("P6",70,"A","Short support"),("P7",150,"B","Postmortem")]
PCOL={"A":C_PA,"B":C_PB}
BATCHES=[[("P0",50,"A"),("P1",180,"A"),("P2",60,"B")],[("P3",200,"B"),("P4",40,"A")],
         [("P5",90,"B"),("P6",70,"A")],[("P7",150,"B")]]

def cw(t,f):
    b=draw.textbbox((0,0),t,font=f); return b[2]-b[0]
def tc(x,y,t,f=None,fill=None,**kw):
    font = kw.get('font', f)
    draw.text((x-cw(t,font)//2,y),t,fill=fill,font=font)
def dt(x,y,t,fill,font):
    draw.text((x,y),t,fill=fill,font=font)
def batch1_detail():
    ids=[b[0] for b in BATCHES[0]]
    return [p for p in PROMPTS if p[0] in ids]

# ── 标题 ──
tc(W//2,16,"一个请求的完整旅程：形成 → 组织 → 提交控制 → vLLM 推理",f_h1,C_DARK)
tc(W//2,50,"8 个 prompt（橙=prefix A / 蓝=prefix B）贯穿全程 —— 研究①组织数据  研究②控制提交  vLLM 内部不修改",f_small,C_GRAY)

# ══════════ 第一段：数据进入（80~270）══════════
d_x,d_y,d_w,d_h=30,90,470,180
draw.rounded_rectangle([d_x,d_y,d_x+d_w,d_y+d_h],radius=8,fill=C_LIGHT,outline=C_BORDER)
tc(d_x+d_w//2,d_y+10,"① documents 表（PostgreSQL，行式）",f_h2,C_DARK)
hy=d_y+38
draw.line([(d_x+10,hy+18),(d_x+d_w-10,hy+18)],fill=C_BORDER)
dt(d_x+12,hy,"doc_id",fill=C_GRAY,font=f_small); dt(d_x+58,hy,"text = 1 个完整 prompt",fill=C_GRAY,font=f_small)
dt(d_x+300,hy,"tok",fill=C_GRAY,font=f_small); dt(d_x+344,hy,"prefix",fill=C_GRAY,font=f_small)
for k,(pid,tok,pfx,desc) in enumerate(PROMPTS):
    ry=hy+24+k*15
    dt(d_x+14,ry,str(k),fill=C_DARK,font=f_small)
    dt(d_x+58,ry,f"{pid}  {desc}",fill=C_DARK,font=f_small)
    dt(d_x+304,ry,str(tok),fill=C_DARK,font=f_small)
    draw.ellipse([d_x+350,ry+2,d_x+360,ry+12],fill=PCOL[pfx])
    dt(d_x+366,ry,pfx,fill=C_DARK,font=f_small)
draw.line([(d_x+d_w+6,d_y+d_h//2),(d_x+d_w+30,d_y+d_h//2)],fill=C_GRAY,width=2)
draw.polygon([(d_x+d_w+30,d_y+d_h//2),(d_x+d_w+22,d_y+d_h//2-5),(d_x+d_w+22,d_y+d_h//2+5)],fill=C_GRAY)
tc(d_x+d_w+18,d_y+d_h//2-16,"Arrow 读出",f_small,C_GRAY)
a_x=d_x+d_w+40; a_w=430
draw.rounded_rectangle([a_x,d_y,a_x+a_w,d_y+d_h],radius=8,fill=C_LIGHT,outline=C_BORDER)
tc(a_x+a_w//2,d_y+10,"② Daft / Arrow（数据引擎层·列式）",f_h2,C_DARK)
ct=d_y+38; chh=128
cols=[("text 列",a_x+12,150),("prompt_tokens",a_x+170,120),("prefix_key",a_x+300,118)]
for cn,cx0,cw0 in cols:
    draw.rounded_rectangle([cx0,ct,cx0+cw0,ct+chh],radius=5,fill=C_WHITE,outline=C_BORDER)
    tc(cx0+cw0//2,ct+5,cn,f_small,C_GRAY)
    draw.line([(cx0+4,ct+20),(cx0+cw0-4,ct+20)],fill=C_BORDER)
for k,(pid,tok,pfx,desc) in enumerate(PROMPTS):
    ry=ct+26+k*12
    dt(cols[0][1]+8,ry,pid,fill=PCOL[pfx],font=f_tiny)
    tc(cols[1][1]+cols[1][2]//2,ry,str(tok),f_tiny,C_DARK)
    draw.ellipse([cols[2][1]+cols[2][2]//2-4,ry+1,cols[2][1]+cols[2][2]//2+4,ry+9],fill=PCOL[pfx])
tc(a_x+a_w//2,d_y+168,"Arrow=当前实现 · Daft=目标后端（into_batches / @daft.cls）",f_tiny,C_GRAY)
nx=a_x+a_w+30
draw.rounded_rectangle([nx,d_y,W-30,d_y+d_h],radius=8,fill="#FEF3C7",outline="#D97706")
tc((nx+W-30)//2,d_y+12,"策略的输入",f_h2,"#92400E")
for i,n in enumerate(["每行带两个元数据：","  • prompt_tokens（计算量）","  • prefix_key（共享前缀分组）","","研究内容一用它们决定","一个 batch 装哪几行、装多少"]):
    dt(nx+14,d_y+42+i*17,n,fill=C_DARK,font=f_small)

# ══════════ 第二段：研究内容一·策略切分（290~620）══════════
S2=290
draw.line([(30,S2),(W-30,S2)],fill=C_BORDER)
tc(W//2,S2+10,"研究内容一 · 数据组织策略：怎么决定一个 batch 装哪些行",f_h2,C_BATCH)
tc(W//2,S2+32,"token-budget：累计 token 达到预算（300）就切一批 —— 长 prompt 少装、短 prompt 多装，不再是固定行数",f_small,C_DARK)
SCALE=1.067; gap=24
bws=[(sum(t for _,t,_ in b)*SCALE+8) for b in BATCHES]
total_w=sum(bws)+gap*(len(BATCHES)-1); bx0=(W-total_w)//2
b_y=S2+80; b_h=58; cur=bx0
for bi,b in enumerate(BATCHES):
    bw=bws[bi]
    draw.rounded_rectangle([cur,b_y,cur+bw,b_y+b_h],radius=6,fill=C_BATCH_BG,outline=C_BATCH,width=2)
    tc(cur+bw//2,b_y+6,f"batch {bi+1}",f_tiny,C_BATCH)
    ix=cur+4; iy=b_y+22; ih=b_h-28
    for pid,tok,pfx in b:
        ibw=tok*SCALE
        draw.rounded_rectangle([ix,iy,ix+ibw,iy+ih],radius=3,fill=PCOL[pfx],outline=C_WHITE,width=1)
        if ibw>30: tc(ix+ibw//2,iy+ih//2-4,pid,f_tiny,C_WHITE)
        ix+=ibw
    tot=sum(t for _,t,_ in b)
    tc(cur+bw//2,b_y+b_h+6,f"{len(b)} 行 / {tot} tok",f_small,C_DARK)
    cur+=bw+gap
# 预算参考线
rx=bx0; rw=300*SCALE; ry=b_y-14
x=rx
while x<rx+rw:
    xe=min(x+4,rx+rw); draw.line([(x,ry),(xe,ry)],fill=C_RED,width=2); x=xe+4
dt(rx+rw+8,ry-5,"预算 300 tok（每个 batch 宽 ≤ 它）",fill=C_RED,font=f_small)
# 图例
lg=b_y+b_h+28
draw.ellipse([bx0,lg,bx0+12,lg+12],fill=C_PA); dt(bx0+18,lg,"prefix A",fill=C_DARK,font=f_small)
draw.ellipse([bx0+90,lg,bx0+102,lg+12],fill=C_PB); dt(bx0+108,lg,"prefix B",fill=C_DARK,font=f_small)
dt(bx0+190,lg,"条宽 ∝ token 量；同色相邻=同 prefix（注意 batch1 是 橙橙蓝 混杂）",fill=C_GRAY,font=f_small)
# 策略对比
cy=lg+30
draw.rounded_rectangle([30,cy,W-30,cy+96],radius=8,fill=C_LIGHT,outline=C_BORDER)
dt(44,cy+8,"同是这 8 行，三种策略切法不同：",fill=C_DARK,font=f_anno)
for i,(nm,desc,col) in enumerate([
    ("token-budget（上图）","按 token 累计切 → 行数随长度变（这里 3/2/2/1 行）；batch 内 prefix 可能混杂",C_BATCH),
    ("length-align","先按长度排序再按预算切 → 每个 batch 内长度更均匀，减少 GPU 等待与 padding",C_PA),
    ("prefix-aware","先按 prefix 分组再切 → 同 prefix 装一起，让 vLLM 复用 KV cache",C_PB)]):
    yy=cy+30+i*20
    draw.rectangle([44,yy+3,54,yy+13],fill=col)
    dt(62,yy,nm+"：",fill=col,font=f_small)
    dt(62+cw(nm+"：",f_small),yy,desc,fill=C_DARK,font=f_small)

# ══════════ 第三段：研究内容二·提交控制（动态调度，640~870）══════════
S3=640
draw.line([(30,S3),(W-30,S3)],fill=C_BORDER)
tc(W//2,S3+10,"研究内容二 · 提交控制（动态调度）：决定几个 batch 同时在途",f_h2,C_SCHED)
tc(W//2,S3+32,"Admission（AIMD K_max）+ Router（prefix/token-aware）；Ray actor pool；读 vLLM 指标反馈调并发",f_small,C_DARK)

# 反馈环（先画，在主体上方）
fb_y=S3+62
tc(W//2,fb_y-2,"⟲ 反馈：vLLM 指标 → AIMD 调 K_max —— 队列堆积/KV 满 → 乘性减；空闲 → 加性增（← Clipper+CONCUR）",f_anno,C_RED)
# 反馈弧线：从 vLLM 指标框顶部 → 上 → 左 → 闸门顶部
m_y=S3+92   # 主体顶
gate_cx=410
draw.line([(1170,m_y),(1170,fb_y+16),(gate_cx,fb_y+16),(gate_cx,m_y)],fill=C_RED,width=2)
draw.polygon([(gate_cx,m_y+6),(gate_cx-5,m_y),(gate_cx+5,m_y)],fill=C_RED)

# 主体三列 + 指标源
m_top=m_y; m_bot=S3+210
# pending
p_x,p_w=40,220
draw.rounded_rectangle([p_x,m_top,p_x+p_w,m_bot],radius=8,fill=C_LIGHT,outline=C_BORDER)
tc(p_x+p_w//2,m_top+6,"待提交 pending",f_small,C_GRAY)
for i in range(4):
    yy=m_top+28+i*22
    draw.rounded_rectangle([p_x+20,yy,p_x+p_w-20,yy+16],radius=3,fill=C_BATCH_BG,outline=C_BATCH)
    dt(p_x+30,yy+2,f"batch {5+i}",fill=C_BATCH,font=f_tiny)
# 放行箭头 pending→闸门
ayy=m_top+(m_bot-m_top)//2-8
draw.line([(p_x+p_w+4,ayy),(p_x+p_w+28,ayy)],fill=C_GRAY,width=2)
draw.polygon([(p_x+p_w+28,ayy),(p_x+p_w+20,ayy-5),(p_x+p_w+20,ayy+5)],fill=C_GRAY)
tc(p_x+p_w+16,ayy-16,"放行",f_small,C_GRAY)
# 闸门
g_x,g_w=300,240
draw.rounded_rectangle([g_x,m_top,g_x+g_w,m_bot],radius=8,fill=C_SCHED_BG,outline=C_SCHED,width=2)
tc(g_x+g_w//2,m_top+5,"研究内容二",f_small,C_SCHED)
tc(g_x+g_w//2,m_top+20,"Ray actor pool · 去中心化",f_tiny,C_GRAY)
draw.line([(g_x+16,m_top+34),(g_x+g_w-16,m_top+34)],fill=C_BORDER)
dt(g_x+16,m_top+40,"① Admission",fill=C_SCHED,font=f_anno)
dt(g_x+16,m_top+58,"AIMD K_max（加性增/乘性减）",fill=C_DARK,font=f_small)
dt(g_x+16,m_top+73,"← Clipper NSDI'17+CONCUR'25",fill=C_GRAY,font=f_tiny)
dt(g_x+16,m_top+89,"② Router",fill=C_SCHED,font=f_anno)
dt(g_x+16,m_top+107,"prefix/token-aware ← SGLang/Parrot",fill=C_DARK,font=f_small)
# 闸门→in-flight 箭头
draw.line([(g_x+g_w+4,ayy),(g_x+g_w+28,ayy)],fill=C_GRAY,width=2)
draw.polygon([(g_x+g_w+28,ayy),(g_x+g_w+20,ayy-5),(g_x+g_w+20,ayy+5)],fill=C_GRAY)
# in-flight
if_x,if_w=580,260
draw.rounded_rectangle([if_x,m_top,if_x+if_w,m_bot],radius=8,fill=C_LIGHT,outline=C_BORDER)
tc(if_x+if_w//2,m_top+6,"in-flight（已 POST，在路上）",f_small,C_GRAY)
for i,b in enumerate(BATCHES[:3]):
    xx=if_x+20+i*80
    draw.rounded_rectangle([xx,m_top+30,xx+70,m_top+74],radius=4,fill=C_BATCH_BG,outline=C_BATCH,width=2)
    tc(xx+35,m_top+34,f"batch{i+1}",f_tiny,C_BATCH)
    iy2=m_top+50
    ix=xx+4
    for pid,tok,pfx in b:
        ibw=max(8,min(60,tok*0.25))
        draw.rectangle([ix,iy2,ix+ibw,iy2+16],fill=PCOL[pfx])
        ix+=ibw+1
tc(if_x+if_w//2,m_top+88,"（最多 K_max 个同时）",f_small,C_GRAY)
# in-flight→vLLM 箭头
draw.line([(if_x+if_w+4,ayy),(if_x+if_w+40,ayy)],fill=C_VLLM,width=3)
draw.polygon([(if_x+if_w+48,ayy),(if_x+if_w+36,ayy-8),(if_x+if_w+36,ayy+8)],fill=C_VLLM)
tc(if_x+if_w+24,ayy-16,"→ vLLM",f_small,C_VLLM)
# vLLM 指标源（反馈来源）
vm_x=if_x+if_w+60; vm_w=W-30-vm_x
draw.rounded_rectangle([vm_x,m_top,vm_x+vm_w,m_bot],radius=8,fill=C_VLLM_BG,outline=C_VLLM,width=2)
tc(vm_x+vm_w//2,m_top+6,"vLLM 实时指标（Prometheus）",f_small,C_VLLM)
for i,(mk,ex) in enumerate([("vllm:num_requests_waiting","队列里等着的请求数"),("vllm:num_requests_running","正在 GPU 跑的请求数"),("vllm:kv_cache_usage_perc","KV cache 占用率")]):
    yy=m_top+24+i*22
    draw.rounded_rectangle([vm_x+14,yy,vm_x+vm_w-14,yy+18],radius=3,fill=C_WHITE,outline=C_BORDER)
    dt(vm_x+22,yy+2,mk,fill=C_VLLM,font=f_code)
    dt(vm_x+22,yy+11,ex,fill=C_GRAY,font=f_tiny)
tc(vm_x+vm_w//2,m_bot-14,"adaptive_inflight_limit 读这些指标",f_tiny,C_VLLM)

# 底注
dt(30,S3+224,"static（K_max 固定） vs AIMD（加性增/乘性减，:512 现状双态作对照）；Router：prefix-aware←SGLang · token-aware←Parrot",fill=C_DARK,font=f_small)

# ══════════ 第四段：单 batch 进 vLLM 旅程（890~1160）══════════
S4=890
draw.line([(30,S4),(W-30,S4)],fill=C_BORDER)
tc(W//2,S4+10,"单个 batch 怎么进 vLLM 推理引擎（以 batch1 为例）",f_h2,C_VLLM)

L_X=30; L_W=470; L_Y=S4+38
draw.rounded_rectangle([L_X,L_Y,L_X+L_W,L_Y+44],radius=6,fill=C_BATCH_BG,outline=C_BATCH,width=2)
tc(L_X+L_W//2,L_Y+6,"batch 1（3 行 Arrow 子表）",f_small,C_BATCH)
ix=L_X+L_W//2-90
for pid,tok,pfx in BATCHES[0]:
    ibw=max(28,tok*0.5)
    draw.rounded_rectangle([ix,L_Y+24,ix+ibw,L_Y+38],radius=3,fill=PCOL[pfx],outline=C_WHITE,width=1)
    tc(ix+ibw//2,L_Y+26,pid,f_tiny,C_WHITE); ix+=ibw+6
ay=L_Y+50
draw.line([(L_X+L_W//2,ay),(L_X+L_W//2,ay+22)],fill=C_GRAY,width=2)
draw.polygon([(L_X+L_W//2,ay+28),(L_X+L_W//2-5,ay+22),(L_X+L_W//2+5,ay+22)],fill=C_GRAY)
tc(L_X+L_W//2+8,ay+4,".column(\"text\").to_pylist()",f_code,C_GRAY)
ly=ay+34
draw.rounded_rectangle([L_X,ly,L_X+L_W,ly+74],radius=6,fill="#F1F5F9",outline=C_BORDER)
dt(L_X+12,ly+6,"prompts = [",fill=C_DARK,font=f_code)
for i,(pid,tok,pfx,desc) in enumerate(batch1_detail()):
    ry=ly+24+i*16
    draw.ellipse([L_X+22,ry+2,L_X+32,ry+12],fill=PCOL[pfx])
    dt(L_X+40,ry,f'"{pid} {desc}"',fill=C_DARK,font=f_code)
dt(L_X+12,ly+58,"]   ← 3 个 prompt 的字符串",fill=C_DARK,font=f_code)
ay2=ly+80
draw.line([(L_X+L_W//2,ay2),(L_X+L_W//2,ay2+18)],fill=C_GRAY,width=2)
draw.polygon([(L_X+L_W//2,ay2+24),(L_X+L_W//2-5,ay2+18),(L_X+L_W//2+5,ay2+18)],fill=C_GRAY)
tc(L_X+L_W//2+8,ay2+2,"打包 JSON",f_code,C_GRAY)
jy=ay2+30
draw.rounded_rectangle([L_X,jy,L_X+L_W,jy+118],radius=6,fill="#F1F5F9",outline=C_BORDER)
dt(L_X+12,jy+6,"POST /v1/completions",fill=C_VLLM,font=f_code)
dt(L_X+12,jy+24,"{",fill=C_DARK,font=f_code)
dt(L_X+20,jy+40,'"model": "qwen2.5-1.5b",',fill=C_DARK,font=f_code)
dt(L_X+20,jy+56,'"prompt": [',fill=C_DARK,font=f_code)
ix2=L_X+40
for pid,tok,pfx,desc in batch1_detail():
    draw.ellipse([ix2,jy+60,ix2+8,jy+68],fill=PCOL[pfx])
    dt(ix2+12,jy+58,pid,fill=C_DARK,font=f_code); ix2+=42
dt(L_X+20,jy+74,"],   ← 3 个 prompt 进 1 个 JSON",fill=C_DARK,font=f_code)
dt(L_X+20,jy+90,'"max_tokens": 128',fill=C_DARK,font=f_code)
dt(L_X+12,jy+104,"}",fill=C_DARK,font=f_code)
# POST 箭头
ar1=L_X+L_W+10; ar2=ar1+120; ary=jy+50
draw.line([(ar1,ary),(ar2,ary)],fill=C_VLLM,width=5)
draw.polygon([(ar2+8,ary),(ar2-4,ary-9),(ar2-4,ary+9)],fill=C_VLLM)
tc((ar1+ar2)//2,ary-22,"1 次 HTTP POST",fill=C_VLLM,font=f_anno)
tc((ar1+ar2)//2,ary+8,"(urllib 同步)",fill=C_GRAY,font=f_small)
# vLLM 内部
V_X=ar2+30; V_W=W-30-V_X; V_Y=S4+38; V_BOT=jy+118
draw.rounded_rectangle([V_X,V_Y,V_X+V_W,V_BOT],radius=10,fill=C_VLLM_BG,outline=C_VLLM,width=2)
tc(V_X+V_W//2,V_Y+8,"vLLM 推理引擎（本课题不修改）",f_h2,C_VLLM)
steps=[("①","接收 HTTP POST","解析出 3 个 prompt，各自作为一个生成任务"),
       ("②","进入调度队列","与其它在途请求一起排队"),
       ("③","Continuous Batching + APC","合并多请求上 GPU；APC 复用 prefix cache（prefix-aware 的目标）"),
       ("④","GPU 执行","Prefill + Decode，PagedAttention 管 KV cache"),
       ("⑤","返回 JSON","结果回 Ray worker → ray.get → 写回 PG")]
sy=V_Y+38; shh=(V_BOT-sy-50)//5
for num,nm,desc in steps:
    draw.ellipse([V_X+16,sy+4,V_X+36,sy+24],fill=C_VLLM)
    tc(V_X+26,sy+7,num,f_tiny,C_WHITE)
    dt(V_X+46,sy+4,nm,fill=C_DARK,font=f_body)
    dt(V_X+46,sy+20,desc,fill=C_GRAY,font=f_small)
    sy+=shh
    if num!="⑤": draw.line([(V_X+26,sy),(V_X+26,sy-4)],fill=C_VLLM,width=1)
tb_y=V_BOT-40
draw.rounded_rectangle([V_X+12,tb_y,V_X+V_W-12,V_BOT-6],radius=5,fill=C_WHITE,outline=C_VLLM)
dt(V_X+22,tb_y+6,"两层 batching 不同：",fill=C_RED,font=f_small)
dt(V_X+22,tb_y+22,"你的 batch（发送层）= 多个 prompt 打 1 个 POST；vLLM batch（执行层）= 多个 POST 在 GPU 合并",fill=C_DARK,font=f_small)

dt(30,H-18,"来源：建表 postgres_ai_operator_profile.py:56 · 切 batch organizers.py:238 · 提交控制 :363/:512 · 打包 JSON model_backends.py:71 · vLLM 旅程=vLLM 官方文档（合理推断）",fill=C_GRAY,font=f_src)

out_dir="figures/learning"; os.makedirs(out_dir,exist_ok=True); name="request_shape_evolution"
img.save(os.path.join(out_dir,name+".png"),"PNG")
img.resize((W*2,H*2),Image.LANCZOS).save(os.path.join(out_dir,name+"@2x.png"),"PNG")
print("PNG saved.")
print("=== self-check ===")
px=img.load()
def present(t,tol=70):
    tr,tg,tb=int(t[1:3],16),int(t[3:5],16),int(t[5:7],16)
    for y in range(0,H,3):
        for x in range(0,W,3):
            r,g,b=px[x,y][:3]
            if abs(r-tr)<tol and abs(g-tg)<tol and abs(b-tb)<tol: return True
    return False
for nm,c in [("prefix A",C_PA),("prefix B",C_PB),("batch 绿",C_BATCH),("调度橙红",C_SCHED),("预算红",C_RED),("vLLM 紫",C_VLLM)]:
    print(f"  {nm} {c}: {'OK' if present(c) else 'MISSING'}")
