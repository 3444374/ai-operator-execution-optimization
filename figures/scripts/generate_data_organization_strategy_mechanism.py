"""
Generate 3 RC1 strategy figures (PNG+SVG). Clean panels, no border boxes, no overlap.
"""
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
from xml.sax.saxutils import escape as xe
OUT_DIR = Path(__file__).resolve().parents[1] / "architecture"
PAL = {"bg":"#F8FAFC","ink":"#172033","mu":"#334155","b":"#2F6FEB","bf":"#E7F0FF",
       "o":"#F97316","of":"#FFF1E6","g":"#16A34A","gf":"#F0FDF4","r":"#DC2626",
       "gy":"#64748B","lg":"#94A3B8","ln":"#334155","ca":"#FFFFFF"}
def R(h):return tuple(int(h[i:i+2],16)for i in(1,3,5))
def F(s,b=False):
    for p in [r"C:\Windows\Fonts\msyhbd.ttc"if b else r"C:\Windows\Fonts\msyh.ttc",
              r"C:\Windows\Fonts\simhei.ttf",r"C:\Windows\Fonts\arial.ttf"]:
        if Path(p).exists():return ImageFont.truetype(p,size=s)
    return ImageFont.load_default()
I=R(PAL["ink"]);MU=R(PAL["mu"]);WHT=(255,255,255)
BL=R(PAL["b"]);BF=R(PAL["bf"]);OR=R(PAL["o"]);GR=R(PAL["g"]);RD=R(PAL["r"])
LG=R(PAL["lg"]);CA=R(PAL["ca"]);LN=R(PAL["ln"])
FT={"t":F(26,1),"s":F(14),"b":F(13),"sm":F(11),"c":F(12),"x":F(10),"lg":F(16,1)}
def TS(d,t,f):b=d.textbbox((0,0),t,font=f);return b[2]-b[0],b[3]-b[1]
def RR(d,b,fill=None,stroke=None,sw=1,r=8):d.rounded_rectangle(b,radius=r,fill=fill,outline=stroke,width=sw)
def SR(x,y,w,h,rr=None,fill=None,stroke=None,sw=1):
    s=f'<rect x="{x}" y="{y}" width="{w}" height="{h}"'
    if rr:s+=f' rx="{rr}" ry="{rr}"'
    if fill:s+=f' fill="{fill}"'
    if stroke:s+=f' stroke="{stroke}" stroke-width="{sw}"'
    s+='/>';return s
def ST(x,y,t,sz,cl,bold=False,anchor="start"):
    return f'<text x="{x}" y="{y}" font-family="Microsoft YaHei,SimHei,sans-serif" font-size="{sz}" font-weight="{"bold"if bold else"normal"}" fill="{cl}" text-anchor="{anchor}">{xe(t)}</text>'
def ARROW(d,x1,y1,x2,y2,color=LN,sw=3):
    d.line((x1,y1,x2,y2),fill=color,width=sw)
    dx=1 if x2>=x1 else-1
    d.polygon([(x2,y2),(x2-dx*10,y2-6),(x2-dx*10,y2+6)],fill=color)
def SAR(x1,y1,x2,y2,color="#334155",sw=2):
    dx=1 if x2>=x1 else-1
    return f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{sw}"/><polygon points="{x2},{y2} {x2-dx*10},{y2-6} {x2-dx*10},{y2+6}" fill="{color}"/>'
def BC(img,boxes,w,h):
    for n,(x1,y1,x2,y2) in boxes.items():
        if x1<0 or y1<0 or x2>w or y2>h:print(f"  BORDER {n}");return False
    return True
def CF(svg_path):
    for f in['RC1','RC2','RC3','BL1','BL2']:
        if f in svg_path.read_text('utf-8'):print(f"  FORBID {f}");return False
    return True
GM=40

# ===== FIG 1 =====
def fig1():
    W,H=1000,400;img=Image.new("RGB",(W,H),R(PAL["bg"]));D=ImageDraw.Draw(img)
    t="Token-budget: Count Tokens, Not Rows"
    tw,_=TS(D,t,FT["t"]);D.text(((W-tw)//2,8),t,fill=I,font=FT["t"])
    st="Fixed batch_size causes unpredictable token counts. Token-budget groups by total tokens."
    D.text(((W-TS(D,st,FT["s"])[0])//2,38),st,fill=MU,font=FT["s"])

    bh,bg2=20,7;rows=[(50,"50"),(200,"200"),(800,"800"),(150,"150")]
    bw=300;CL=420;TY=74;L=(W-2*CL-60)//2

    # LEFT
    RR(D,[L-5,TY-5,L+CL,TY+190],fill=R("#FEF2F2"))
    D.text((L+8,TY+2),"Fixed-Row Batching",fill=I,font=FT["b"])
    D.text((L+8,TY+22),"batch_size = 4",fill=MU,font=FT["sm"])
    by=TY+44
    for i,(tokens,label) in enumerate(rows):
        rw=int(bw*(tokens/800));y=by+i*(bh+bg2)
        RR(D,[L+10,y,L+10+rw,y+bh],fill=BL,r=4)
        D.text((L+10+rw+8,y+2),f"row{i+1}: {label} tk",fill=I,font=FT["x"])
    ry=by-3;rh=len(rows)*(bh+bg2)+6
    RR(D,[L+5,ry,L+15+bw,ry+rh],stroke=RD,sw=3)
    # Red text: ensure it's to the right of all row labels
    D.text((L+25+bw+80,ry+rh//2-14),"Waits for\nlongest row",fill=RD,font=FT["sm"])

    # Arrow
    ax=L+CL+15;ay=TY+105
    ARROW(D,ax-8,ay,ax+26,ay)

    # RIGHT
    rx=ax+34
    RR(D,[rx-5,TY-5,rx+CL,TY+190],fill=R("#F0FDF4"))
    D.text((rx+8,TY+2),"Token-Budget Batching",fill=I,font=FT["b"])
    D.text((rx+8,TY+22),"token_limit = 600",fill=MU,font=FT["sm"])
    bch=[(["row1(50)","row2(200)","row4(150)"],"total 400, OK",GR),
         (["row3(800)"],"800 > limit, alone",OR)]
    by2=TY+44;bw2=bw
    for rows2,note,nc in bch:
        n=len(rows2);bh2=n*(bh+bg2)+14
        RR(D,[rx+10,by2,rx+10+bw2,by2+bh2],fill=BF,stroke=BL,sw=2,r=6)
        for ri,rl in enumerate(rows2):
            rp=50/800 if"1"in rl else(200/800 if"2"in rl else(150/800 if"4"in rl else 1.0))
            rw2=int((bw2-30)*rp)
            RR(D,[rx+20,by2+8+ri*(bh+bg2),rx+20+rw2,by2+8+ri*(bh+bg2)+bh],fill=BL,r=4)
            D.text((rx+26,by2+8+ri*(bh+bg2)+2),rl,fill=WHT,font=FT["x"])
        nw,_=TS(D,note,FT["sm"])
        D.text((rx+10+bw2-nw-4,by2+bh2+4),note,fill=nc,font=FT["sm"])
        by2+=bh2+24

    cap="Key: Replace fixed row count with token accumulation. Short rows pack more per batch; long rows get dedicated batches."
    D.text((GM,H-32),cap,fill=MU,font=FT["c"])
    png=str(OUT_DIR/"rc1_token_budget.png");img.save(png,"PNG")

    svg=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
         f'<rect width="{W}" height="{H}" fill="{PAL["bg"]}"/>',ST((W-tw)//2,28,t,26,PAL["ink"],1)]
    for i,(tokens,label) in enumerate(rows):
        rw=int(bw*(tokens/800));y=by+i*(bh+bg2)
        svg.append(SR(L+10,y,rw,bh,4,PAL["b"]))
        svg.append(ST(L+10+rw+8,y+15,f"row{i+1}: {label} tk",10,PAL["ink"]))
    svg.append(SR(L+5,ry,bw+10,rh,8,stroke=PAL["r"],sw=3))
    svg.append(ST(L+25+bw+80,ry+rh//2,"Waits for longest row",11,PAL["r"]))
    svg.append(SAR(ax-8,ay,ax+26,ay))
    by2=TY+44
    for rows2,note,_ in bch:
        n=len(rows2);bh2=n*(bh+bg2)+14
        nc=PAL["g"]if"OK"in note else PAL["o"]
        svg.append(SR(rx+10,by2,bw2,bh2,6,PAL["bf"],PAL["b"],2))
        for ri,rl in enumerate(rows2):
            rp=50/800 if"1"in rl else(200/800 if"2"in rl else(150/800 if"4"in rl else 1.0))
            rw2=int((bw2-30)*rp)
            svg.append(SR(rx+20,by2+8+ri*(bh+bg2),rw2,bh,4,PAL["b"]))
            svg.append(ST(rx+26,by2+8+ri*(bh+bg2)+15,rl,10,"#FFFFFF"))
        nw,_=TS(D,note,FT["sm"])
        svg.append(ST(rx+10+bw2-nw-4,by2+bh2+20,note,11,nc))
        by2+=bh2+24
    svg.append(ST(GM,H-12,cap,12,PAL["mu"]));svg.append('</svg>')
    (OUT_DIR/"rc1_token_budget.svg").write_text('\n'.join(svg),encoding='utf-8')
    ok=BC(img,{"L":(L-5,TY-5,L+CL,TY+190),"R":(rx-5,TY-5,rx+CL,TY+190)},W,H) and CF(OUT_DIR/"rc1_token_budget.svg")
    print(f"rc1_token_budget {W}x{H} {'OK'if ok else 'FAILED'}")

# ===== FIG 2 =====
def fig2():
    W,H=780,340;img=Image.new("RGB",(W,H),R(PAL["bg"]));D=ImageDraw.Draw(img)
    t="Length-align: Sort by Token Length, Group Similar Rows"
    tw,_=TS(D,t,FT["t"]);D.text(((W-tw)//2,8),t,fill=I,font=FT["t"])
    st="Sort by token count -> group adjacent rows -> each batch has uniform computation"
    D.text(((W-TS(D,st,FT["s"])[0])//2,38),st,fill=MU,font=FT["s"])

    v=[0.12,0.95,0.35,0.72,0.18,0.88,0.42,0.62,0.28,0.78]
    bw,bgs=18,9;n=len(v);VB,VM=275,105;sp=sorted(v)

    # Center content: total width ≈ n*bw+(n-1)*bgs + arrow + n*bw+(n-1)*bgs = 261+50+261=572
    total_w=2*n*(bw+bgs)-2*bgs+80  # bars + arrow gap
    L=(W-total_w)//2

    TY=72;lbl_y=TY+28;rule_y=TY+48

    # LEFT
    D.text((L,TY),"Unsorted",fill=I,font=FT["b"])
    for i,p in enumerate(v):
        bh=int(VM*p);x=L+i*(bw+bgs)
        RR(D,[x,VB-bh,x+bw,VB],fill=LG,r=3)
        D.text((x+1,VB+4),str(int(p*1000)),fill=MU,font=FT["x"])

    # Arrow
    ax=L+n*(bw+bgs)+10;ay=VB-VM//2
    ARROW(D,ax-5,ay,ax+24,ay)

    # RIGHT
    sx=ax+40
    D.text((sx,TY),"Sorted + Grouped",fill=I,font=FT["b"])
    # Group labels + colored rules
    groups=[("Short (fast)",0,3,GR),("Mid",3,6,BL),("Long (slow)",6,10,OR)]
    for label,start,end,color in groups:
        gx=sx+start*(bw+bgs);gw=(end-start)*(bw+bgs)
        cx=gx+gw//2
        D.text((cx-22,lbl_y),label,fill=color,font=FT["sm"])
        D.line((gx,rule_y,gx+gw,rule_y),fill=color,width=3)
    # Bars
    for i,p in enumerate(sp):
        bh=int(VM*p);x=sx+i*(bw+bgs)
        RR(D,[x,VB-bh,x+bw,VB],fill=BL,r=3)
        D.text((x+1,VB+4),str(int(p*1000)),fill=BL,font=FT["x"])

    cap="Key: Sort by token length, then group. Each batch has similar computation, eliminating straggler delays."
    D.text((GM,H-32),cap,fill=MU,font=FT["c"])
    png=str(OUT_DIR/"rc1_length_align.png");img.save(png,"PNG")

    svg=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
         f'<rect width="{W}" height="{H}" fill="{PAL["bg"]}"/>',ST((W-tw)//2,28,t,26,PAL["ink"],1)]
    for i,p in enumerate(v):
        bh=int(VM*p);x=L+i*(bw+bgs)
        svg.append(SR(x,VB-bh,bw,bh,3,PAL["lg"]));svg.append(ST(x+1,VB+13,str(int(p*1000)),9,PAL["mu"]))
    svg.append(SAR(ax-5,ay,ax+24,ay))
    for i,p in enumerate(sp):
        bh=int(VM*p);x=sx+i*(bw+bgs)
        svg.append(SR(x,VB-bh,bw,bh,3,PAL["b"]));svg.append(ST(x+1,VB+13,str(int(p*1000)),9,PAL["b"]))
    for label,start,end,ch in [("Short(fast)",0,3,PAL["g"]),("Mid",3,6,PAL["b"]),("Long(slow)",6,10,PAL["o"])]:
        gx=sx+start*(bw+bgs);gw=(end-start)*(bw+bgs)
        svg.append(f'<line x1="{gx}" y1="{rule_y}" x2="{gx+gw}" y2="{rule_y}" stroke="{ch}" stroke-width="3"/>')
        svg.append(ST(gx+gw//2,lbl_y+14,label,11,ch,1,"middle"))
    svg.append(ST(GM,H-12,cap,12,PAL["mu"]));svg.append('</svg>')
    (OUT_DIR/"rc1_length_align.svg").write_text('\n'.join(svg),encoding='utf-8')
    ok=BC(img,{"content":(L-5,TY-5,sx+n*(bw+bgs),VB+16)},W,H) and CF(OUT_DIR/"rc1_length_align.svg")
    print(f"rc1_length_align {W}x{H} {'OK'if ok else 'FAILED'}")

# ===== FIG 3 =====
def fig3():
    W,H=920,370;img=Image.new("RGB",(W,H),R(PAL["bg"]));D=ImageDraw.Draw(img)
    t="Prefix-aware: Group Shared Prefixes, Reuse KV-cache"
    tw,_=TS(D,t,FT["t"]);D.text(((W-tw)//2,8),t,fill=I,font=FT["t"])
    st="Same system prompt rows merged. KV-cache computed once, shared by the group."
    D.text(((W-TS(D,st,FT["s"])[0])//2,38),st,fill=MU,font=FT["s"])

    pc=[BL,OR,GR];pch=[PAL["b"],PAL["o"],PAL["g"]]
    rds=[("sys A","Q1",0),("sys B","Q2",1),("sys A","Q3",0),
         ("sys C","Q4",2),("sys B","Q5",1),("sys A","Q6",0),
         ("sys A","Q7",0),("sys B","Q8",1)]
    bh,bg2=20,5;pr=0.45;rw=320;pw=int(rw*pr);sw=rw-pw;L=50;TY=78;nr=len(rds)

    D.text((L,TY-20),"Scattered Submission",fill=I,font=FT["b"])
    for i,(pf,sx2,ci) in enumerate(rds):
        y=TY+i*(bh+bg2)
        RR(D,[L,y,L+pw,y+bh],fill=pc[ci],r=4);RR(D,[L+pw,y,L+rw,y+bh],fill=LG,r=4)
        ptw,_=TS(D,pf,FT["x"]);D.text((L+(pw-ptw)//2,y+3),pf,fill=WHT,font=FT["x"])
        stw,_=TS(D,sx2,FT["x"]);D.text((L+pw+(sw-stw)//2,y+3),sx2,fill=WHT,font=FT["x"])
    re=TY+nr*(bh+bg2)-bg2
    warn="Same prefix computed repeatedly  X"
    ww,_=TS(D,warn,FT["sm"]);D.text((L+(rw-ww)//2,re+6),warn,fill=RD,font=FT["sm"])

    ax=L+rw+30;ay=TY+(re-TY)//2
    ARROW(D,ax-6,ay,ax+24,ay)

    rx=ax+45;D.text((rx,TY-20),"Grouped by Prefix",fill=I,font=FT["b"])
    gd=[("sys A  (3 rows)",BL,PAL["bf"],"Q1  Q3  Q6  Q7"),
        ("sys B  (2 rows)",OR,PAL["of"],"Q2  Q5  Q8"),
        ("sys C  (1 row)",GR,PAL["gf"],"Q4")]
    cw=380;ch=72;cg=5;nc=len(gd);ctot=nc*ch+(nc-1)*cg;cs=TY
    for gi,(gn,gc,bgh,qs) in enumerate(gd):
        cy=cs+gi*(ch+cg)
        RR(D,[rx,cy,rx+cw,cy+ch],fill=R(bgh),stroke=gc,sw=2);RR(D,[rx+2,cy+2,rx+6,cy+ch-4],fill=gc,r=3)
        D.text((rx+14,cy+10),gn,fill=gc,font=F(12,1))
        D.text((rx+14,cy+34),f"Queries: {qs}",fill=I,font=FT["sm"])
        kv="KV-cache shared";kvw,kvh=TS(D,kv,FT["x"])
        RR(D,[rx+14,cy+54,rx+14+kvw+10,cy+54+kvh+4],fill=GR,r=3)
        D.text((rx+19,cy+55),kv,fill=WHT,font=FT["x"])

    cap="Key: Same system prompt -> single KV-cache computation in vLLM. Merge shared-prefix rows into one submission."
    D.text((GM,H-32),cap,fill=MU,font=FT["c"])
    png=str(OUT_DIR/"rc1_prefix_aware.png");img.save(png,"PNG")

    svg=[f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">',
         f'<rect width="{W}" height="{H}" fill="{PAL["bg"]}"/>',ST((W-tw)//2,28,t,26,PAL["ink"],1)]
    for i,(pf,sx2,ci) in enumerate(rds):
        y=TY+i*(bh+bg2)
        svg.append(SR(L,y,pw,bh,4,pch[ci]));svg.append(SR(L+pw,y,sw,bh,4,PAL["lg"]))
        ptw,_=TS(D,pf,FT["x"]);svg.append(ST(L+(pw-ptw)//2,y+15,pf,10,"#FFFFFF"))
        stw,_=TS(D,sx2,FT["x"]);svg.append(ST(L+pw+(sw-stw)//2,y+15,sx2,10,"#FFFFFF"))
    svg.append(ST(L+(rw-ww)//2,re+20,warn,11,PAL["r"]))
    svg.append(SAR(ax-6,ay,ax+24,ay))
    for gi,(gn,gch,bgh,qs) in enumerate(gd):
        cy=cs+gi*(ch+cg)
        svg.append(SR(rx,cy,cw,ch,8,bgh,gch,2));svg.append(SR(rx+2,cy+2,6,ch-4,3,gch))
        svg.append(ST(rx+14,cy+26,gn,12,gch,1))
        svg.append(ST(rx+14,cy+50,f"Queries: {qs}",11,PAL["ink"]))
        kv="KV-cache shared";kfw=F(10);kvw,kvh=TS(D,kv,kfw)
        svg.append(SR(rx+14,cy+54,kvw+12,kvh+6,3,PAL["g"]))
        svg.append(ST(rx+20,cy+66,kv,10,"#FFFFFF"))
    svg.append(ST(GM,H-12,cap,12,PAL["mu"]));svg.append('</svg>')
    (OUT_DIR/"rc1_prefix_aware.svg").write_text('\n'.join(svg),encoding='utf-8')
    ok=BC(img,{"L":(L,TY-20,L+rw,re+30),"R":(rx,TY-20,rx+cw,cs+ctot+10)},W,H) and CF(OUT_DIR/"rc1_prefix_aware.svg")
    print(f"rc1_prefix_aware {W}x{H} {'OK'if ok else 'FAILED'}")
if __name__=="__main__":
    fig1();fig2();fig3()
    print("Done.")
