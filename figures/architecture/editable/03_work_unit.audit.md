# 03 Work-unit 构造与数据组织：可编辑重构审计

## 角色与口径

- 图类型：Solution Overview / 研究内容一机制图。
- 页面任务：解释 fixed rows/images 的盲点，并把 `Work Estimation → typed WorkDescriptor → Candidate Packing → BatchRequest → matched-work evaluation` 串成可验证链路。
- 证据边界：Packing 只表示互斥候选；策略价值依赖 serving regime，不声称任一策略全局最优。
- 参考：`figures/audit/reference_opening_editable_20260811/03_work_unit.png`。

## 可见元素 inventory

| ID | 近似区域 / bbox | 内容与视觉描述 | 介质 | 样式要点 | 状态 |
|---|---|---|---|---|---|
| C0 | 0,0,1600,900 | 16:9 白色画布 | native | 无渐变、无阴影 | accepted |
| T1 | 30,18,1540,58 | 蓝色页码 03；主标题 | native | 主标题 42 px；页码 36 px 白字蓝底 | accepted |
| P1 | 32,92,390,288 | fixed rows/images 盲点面板 | native + SVG | 分区标题 30 px、正文 20 px；warning 图标；盲点压为三条 | accepted |
| P2 | 470,92,520,288 | 分阶段 Work Estimation 面板 | native + SVG | 分区标题 30 px；每个 stage 只保留一行，标题 24 px、说明 20 px | accepted |
| P3 | 1035,92,535,288 | typed WorkDescriptor 面板 | native + SVG | 分区标题 30 px；字段压为四组 20 px；text/image 映射一行 | accepted |
| A1 | 422,236→470,236 | 盲点到估计的数据流 | native connector | 蓝色实线三角箭头 | accepted |
| A2 | 990,236→1035,236 | 估计到 descriptor 的数据流 | native connector | 蓝色实线三角箭头 | accepted |
| A3 | 1302,380→1302,420 | descriptor 到 packing | native connector | 蓝色实线三角箭头 | accepted |
| P4 | 100,420,1400,170 | Candidate Packing 容器 | native | 橙色细边；“候选/互斥、regime-dependent”明确可见 | accepted |
| C1-C4 | P4 内四列 | Sequential / Length-aligned / Best-fit / Locality-aware | native + 4 SVG | 橙色线性图标；等宽卡片；标题 24 px、短语 20 px | accepted |
| A4 | 525,590→525,640 | Packing 到 BatchRequest | native connector | 蓝色实线三角箭头 | accepted |
| P5 | 100,640,850,145 | BatchRequest 结构 | native | 分区标题 29 px；五个 20 px 原生字段框 | accepted |
| A5 | 950,708→1000,708 | BatchRequest 到 matched-work 评价 | native connector | 绿色实线三角箭头 | accepted |
| P6 | 1000,640,500,145 | 相同 work budget 下的评价 | native + SVG | 分区标题 29 px、三项正文 20 px；紫色提示 20 px | accepted |
| F1 | P6→P2/P4 | 评价反馈到估计与候选 | native connector | 灰色虚线绕外侧返回，不穿卡片或文字 | accepted |
| L1 | 115,820,1410,51 | 流程图例 | native | 20 px；蓝/橙/绿/灰，颜色与线型双重编码 | accepted |
| I1-I10 | 各面板卡片内 | warning、source-work、prepare、model/result、descriptor、sequential、length、best-fit、locality、target | 独立 SVG 对象 | 单色线性图标；独立移动/缩放/替换 | accepted |

## 箭头 inventory

| ID | 源 → 目标 | 路径与方向 | 语义 | 状态 |
|---|---|---|---|---|
| A1 | fixed rows/images 盲点 → Work Estimation | 水平蓝色实线 | 数据进入估计 | accepted |
| A2 | Work Estimation → WorkDescriptor | 水平蓝色实线 | 估计结果进入 typed contract | accepted |
| A3 | WorkDescriptor → Candidate Packing | 垂直蓝色实线 | descriptor 供 organizer 消费 | accepted |
| A4 | Candidate Packing → BatchRequest | 垂直蓝色实线 | 组织决策形成提交对象 | accepted |
| A5 | BatchRequest → matched-work evaluation | 水平绿色实线 | 完成候选比较与证据判断 | accepted |
| F1 | matched-work evaluation → Work Estimation / Candidate Packing | 灰色虚线回路 | 校准与 regime 反馈；不表示在线已胜出 | accepted |

## 技术验证与全尺寸视觉审计

- [x] `check_drawio.py` 通过：77 cells、69 vertices、6 edges、10 个 image/SVG cells、无 warning。
- [x] Draw.io CLI 不可用；额外维护与 Draw.io 同画布、同坐标、同文字的 `03_work_unit.svg`，文字保持 SVG `<text>`；本轮 PNG 由本机 headless Chrome 从该 SVG 直接渲染，并用实际 `STHeiti Medium` 字体独立计算文字边界。
- [x] PNG 为 1600×900，并已按原尺寸打开检查。
- [x] 无文字裁切、越界、边框接触或异常换行。当前正文、字段、图例和提示均不低于 20 px，未通过遮罩或覆盖层解决放大后的布局问题。
- [x] 所有连接器从明确边界出发并落到明确边界；灰色反馈线从底部和左侧外绕，不穿内容。
- [x] 10 个 icon 均为不同的独立 SVG / Draw.io image 对象；私有源文件位于 `assets/03_work_unit/`，无整图截图。
- [x] “候选 / 互斥 / regime-dependent / 相同 Work Budget”口径可见。
- [x] 不含 RC、BL、Phase、P0/P1/P2、“边界确认”、未解释的 `vs`；“候选 / 互斥”“regime-dependent”“仅在当前 serving regime 内比较”共同限制外推。

### 大字版压缩与移出细节

- fixed rows/images 盲点从四条压为三条，只保留 `相同行数 ≠ 相同 token/frame work`、budget/tail 与 balance/locality 冲突。
- Work Estimation 每个 stage 只保留一行：`Source/Input — tokens·bytes`、`Prepare — tokenize·decode/resize`、`Model/Result — output·service`。三行说明分别按标题长度设置起点，真实字体下保留 70/37/69 px 水平余量。
- WorkDescriptor 画面压为 `ID/SLO`、`Stage Work`、`Control`、`Confidence` 四组。完整合同仍包括 record/job ID、source/prepare/model/result work、primary/remaining work、locality key、arrival/deadline、uncertainty interval 与 calibration signature；completion 后按实际完成 work 更新 remaining 的说明移至讲稿/本审计。
- 四个 Packing 卡只保留策略名和一个短语，不在图内解释算法步骤。
- 评价压为 Balance、Locality、Latency 三项；endpoint/KV/workload 的完整 regime 定义移至讲稿，图内保留 18 px 的“仅在当前 serving regime 内比较”。

### PPT 与 A4 可读性审计（当前大字密度版）

| 使用场景 | 20 px 正文等效 | 24 px 卡片标题等效 | 29 px 分区标题等效 | 42 px 主标题等效 | 结论 |
|---|---:|---:|---:|---:|---|
| 16:9 PPT 全宽（13.33 in） | 12.0 pt | 14.4 pt | 17.4 pt | 25.2 pt | 通过；适合作为主图全宽展示 |
| A4 横向全宽（11.69 in） | 10.5 pt | 12.6 pt | 15.2 pt | 22.1 pt | 通过；文档中应使用横向整页或跨栏全宽 |

- Draw.io 与全部 SVG 可见文本扫描：正文、图例、紫色 regime 提示及 Sequential 图标内部的 `1/2/3` 均不低于 20 px；主 SVG 与 `assets/03_work_unit/*.svg` 全局扫描无 `<20 px` 文本。
- A4 纵向单栏会把 16:9 图压得过窄，不作为通过版式；若报告为纵向正文，应给此图单独横向页或旋转整页，不能缩成半页插图。

### 首次独立边界级复核（2026-08-11，18 px 版本历史记录）

本轮使用实际渲染字体 `STHeiti Medium` 计算可见文字边界，并对 SVG 全尺寸预览逐块复核；不是仅凭 `check_drawio.py` 判断。

精确修复：

1. 左上盲点面板的第一、三条原文在 18 px 真实字体下会分别超出预留文本宽度 46/45 px，已压为 `相同行数 ≠ 相同 token/frame work` 和 `balance 与 locality/cache 冲突`；修后余量为 31/53 px。第二条相对预留文本框余量 7 px，但距面板右边框仍有 23 px，不碰边。
2. Work Estimation 原先三个说明共用同一横坐标，Prepare 和 Model/Result 在真实字体下会超过预留宽度 33/26 px。现按行设置说明起点，并将 Model/Result 说明压为 `output · service`；三行均不碰卡片右边框，也不进入 A2 箭头区域。
3. WorkDescriptor 的 `Stage Work` 可见行由 `source · prepare · model · result` 压为 `source / prepare / model`，完整 result-work 字段仍保留在本审计和讲稿合同中；可见行修后余量 19 px。
4. WorkDescriptor 底部 image 映射由 `bytes / decode / tensor` 改为 `bytes / resize / tensor` 并左移，真实字体下距映射卡右边界 13 px；Text 与 Image 两组之间无重叠。
5. 评价区三行 HTML 的 line-height 由 1.55 调整为 1.25，文本框增至 72 px；镜像 SVG 的三条基线固定为 700/725/750，最后一行与紫色 regime 胶囊顶部保留 10 px，不再出现文字与边框近接。
6. 重新核对六条 connector：A1/A2 水平从源面板右边界到目标面板左边界；A3/A4 垂直向下；A5 水平向右；feedback 从评价区底边出发，经 `y=810` 和左侧 `x=70` 外绕后向上落到 Work Estimation 底边。箭头头部均朝向目标，灰虚线不穿文字、卡片或底部图例。
7. Draw.io 由当前对象清单重新生成：77 个 cell ID 全部唯一，无重复边框、重复路径、陈旧错误节点或遮罩节点；白色 `bg` 仅为画布背景，不覆盖任何后绘对象。

箭头视觉统一追加检查：

- A1–A5 主链路统一为 5 px stroke、10 px 长 × 12 px 宽的三角头；SVG 目标端统一保留 8 px 空隙，Draw.io 使用 `endSize=8` 和 `targetPerimeterSpacing=8`，短间距内不再使用偏大的 12×14 px 头部。
- A1/A2/A5 保持严格水平中心线；A3 的 Packing 入口改为 `entryX=0.8586`，与 WorkDescriptor 中心 `x=1302` 对齐；A4 的 Packing 出口改为 `exitX=0.3036`，与 BatchRequest 中心 `x=525` 对齐，避免 2–5 px 的隐性斜线。
- feedback 降为 2 px stroke、`7 7` dash、10×10 px 小头；Draw.io 使用 `endSize=5`。外围路径采用 10 px 圆角，从评价区底边外绕左侧再向上，不与主链路竞争视觉重量。
- 图例与图中语义一致：蓝/橙/绿主流为 4 px、`endSize=8`；灰反馈为 2.5 px、`7 7` dash、`endSize=5`。
- 全尺寸复核中，主箭头头部大小、端点留白和视觉重量一致；反馈线明显弱于主流，未发现箭头漂浮、反向、压边或短线大头问题。

边界复核结果：

- 顶部三面板、Packing、BatchRequest 与评价卡全部位于 1600×900 画布内，边框连续且未被后绘对象覆盖。
- Packing 四张候选卡均在橙色容器内；BatchRequest 五字段框均在蓝色容器内；评价区 target、三条指标和紫色胶囊均在绿色容器内。
- 灰虚线最低位于 `y=810`，底部图例位于 `y=837–867`；二者不重合。图例底部距页面下边界 33 px。
- 最窄的已核对文字水平余量为：blind 第二条 7 px（距面板边界 23 px）、Image 映射 13 px、Descriptor Stage Work 19 px、紫色图例胶囊文字 9 px；均无裁切或碰边。
- `xmllint --noout` 对 Draw.io、主 SVG 和 10 个独立 SVG 全部通过；`check_drawio.py` 通过；Draw.io 与全部 SVG 可见文字均 ≥18 px；PNG 为 1600×900。

### 全尺寸对照结论

- 保留了参考图的页码＋大标题、上三栏、橙色 packing 横带、下方 BatchRequest 与评价卡、底部图例的主要视觉层次。
- 当前项目口径替换了旧版“固定行 batch”单一问题：明确加入 fixed rows/images、分阶段 work、remaining work、calibration signature 与 text/image 映射。
- 四个 packing 策略等宽并列，作为候选而非先验排序；评价卡在同一 work budget、相同资源和具体 serving regime 下同时看 balance、locality、JCT/P99/tail。
- 蓝、橙、紫、绿四色分别承担 work、组织、regime 条件和评价；灰色虚线单独表达观测/校准反馈。
- 全尺寸未发现缺失图标、通用占位图标、裁切、重叠、连线漂浮或背景接缝。
- Sequential 图标的数字已由 14 px 调整为 18 px，并同步到独立 SVG、Draw.io 内嵌 SVG 与镜像 SVG；全尺寸预览中数字未裁切。

### 整体字体与版面密度提升（2026-08-11，当前版本）

- 按用户反馈对全图做 layout-only refinement，没有删减或改写可见内容：页码 34→36 px，主标题 40→42 px，三栏标题 27→30 px，Packing/BatchRequest/评价标题 26→29 px，卡片标题 22→24 px，全部正文、字段、徽标、提示和图例 18→20 px。
- Sequential 图标内部的 `1/2/3` 同步从 18→20 px，并更新独立 SVG、Draw.io 内嵌 payload 和主 SVG；全图不再保留 18 px 可见文字。
- 左上盲点正文利用面板左侧空白左移 14 px，并把文本框扩至 370 px；真实字体下最长第二行距面板右边框约 5 px，未裁切、未换行。
- Work Estimation 三行按标题实际宽度重新分配横向空间：Source/Input、Prepare、Model/Result 使用 24 px，说明使用 20 px；三行都保留卡片右侧余量。
- WorkDescriptor 图标缩至 72 px 并左移，四行合同的文本区扩至 420 px；四行真实宽度余量为 28–71 px。底部映射条扩至 515 px，Image 映射仍保留约 12 px 横向余量。
- Packing 顶部两枚徽标增高至 36 px，四张策略卡标题/短语提升到 24/20 px；中心标题保持在两枚徽标之间，未重叠。
- 右下评价区重新安排标题、target、三行指标和 regime 胶囊：指标基线为 700/724/748，胶囊位于 y=754–780，最后一行与胶囊无覆盖；提示文字横向余量约 95 px。
- 底部反馈标签与五项图例统一为 20 px，图例底边仍小于 y=870，距 900 px 页面边界至少 30 px。
- 1600×900 PNG 已从当前 SVG 重新渲染并以原始尺寸检查；视觉密度显著提高，同时保留原有主链路、箭头端点、反馈外围路线和单一干净边框结构。未发现新越界、边框重叠、遮罩或箭头穿字。

## 已知限制

- Draw.io CLI 在当前环境不可用，因此 PNG 不是 Draw.io CLI 的像素级导出；它由同几何 SVG 经本机 headless Chrome 渲染。XML 有效性、边界和图标对象由 `check_drawio.py` 单独验证。
- 未解决项：无。当前交付为 20 px 最小可见字号的整体大字密度版。
