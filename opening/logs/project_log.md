# 开题材料 project log

## 2026-08-17 P11 箭头整体调小

- 用户手动调整 drawio 后，所有箭头 endSize 统一调小，重新导出并固定权威源与图集副本。PPT 由用户自行更新。

## 2026-08-17 P11 标题条边缘突起修正

- 三个面板标题条改为比 panel 各大 1 px（x-1、y-1、w+2、h+1），完全覆盖 panel 描边，顶部与左右突起消失。同步刷新权威源与图集副本。PPT 由用户自行更新。

## 2026-08-17 P11 organizer→admission 箭头定形

- 对角箭头改为左边缘出发的“┐”形正交折线，线形完整、不再“只剩箭头没有线”或穿卡。重导 P11 PNG/SVG 并刷新图集副本。PPT 由用户自行更新。

## 2026-08-17 P11 清除 draw.io 缓存残影

- draw.io desktop 缓存把已删除的 estimate_icon 等旧元素渲染出来致画面有叠影；清缓存重新导出后残影消失。重导 P11 PNG/SVG 并刷新图集副本。PPT 由用户自行更新。

## 2026-08-17 P11 五处细节修正

- PostgreSQL 文字框加高；organizer→admission 改完整正交折线；灰色反馈箭头改从 routing 卡底部进入与蓝线分离；state_panel 标题居中。重导 P11 PNG/SVG 并刷新图集副本。PPT 由用户自行更新。

## 2026-08-17 P11 灰色反馈箭头避开 validation_note

- 灰色反馈箭头水平段由 y=620 移至 y=636，不再压住“同上限 frozen-static A/B”提示条两端。重导 P11 PNG/SVG 并刷新图集副本。PPT 由用户自行更新。

## 2026-08-17 P11 Daft 官方标识修复

- Daft 图标原是 draw.io 默认占位图（base64 被截断成空 href）；后处理在 Daft 位置插入项目资产官方黑/洋红标识并删除空壳占位图。重导 P11 PNG/SVG 并刷新图集副本。PPT 由用户自行更新。

## 2026-08-17 P11 箭头修正与 Daft 官方标识

- 灰色反馈箭头绕到卡片边缘中部，不再覆盖橙色卡片；organizer→admission 改为正交折线消除斜线；Daft 图标换为项目资产官方黑/洋红标识。重导 P11 PNG/SVG 并刷新图集副本。PPT 由用户自行更新。

## 2026-08-17 P11 Cost Estimator 横条上移

- Shared Cost Estimator 横条移至研究内容一/二 header 正下方、工作流卡片之上（370,250），解决竖箭头穿横条问题；下移两行卡与 validation_note、研究边界框加高。重导 P11 PNG/SVG 并刷新图集副本，icon 全部归位。PPT 由用户自行更新。

## 2026-08-17 P11 icon 渲染管线修复

- draw.io 导出 P11 时把 11 个自绘 SVG icon 合并进单一空 symbol 致全丢；正确管线为 draw.io CLI 导 SVG → Python 按坐标把 `<use>` 换成独立 `<image>`（不能用尺寸匹配）→ headless Chrome 渲染 PNG。11 个 icon 全部归位。
- 三处修订（两个研究内容标题 + Shared Cost Estimator 横条）一并产出最终 PNG/SVG，权威源与图集副本已同步。PPT 由用户自行更新。

## 2026-08-17 P11 系统架构图：研究内容命名与 Cost Estimator 共享化

- 研究内容一/二标题对齐冻结主线（分阶段 Work 表征与数据组织 / 固定容量下的状态感知调度）；
  Cost Estimator 拆为独立 Shared Cost Estimator 横条（共同使能），WorkDescriptor 卡还原为纯 work 描述。
- 搭建本机 drawio 渲染环境（playwright-core + Edge，`figures/scripts/render_drawio.js`），重导 P11 PNG/SVG
  并刷新图集副本；审计已同步。PPT 由用户自行更新。

## 2026-08-17 P08B 重新定位为“AI Work 需要分阶段描述”

- P08B 由平级图像动机页降为动机补充页，避免与 M1（13.9–31×）和 M2（近饱和区）重复。
  suptitle 改为“AI Work 需要分阶段描述”；panel b 改为“输入表示改变阶段执行效率”，panel c 改为
  “阶段供给不匹配导致欠供给或等待堆积”。仅改标题，数据与布局未变；重画 part2 并刷新图集副本，
  `figures/README.md`、`figures/opening_figure_set/README.md`、audit 契约已同步。PPT 由用户自行更新。

## 2026-08-17 P07B active-work 代价显式化

- P07B 容量曲线补入冻结正式数据的直接注释：65K→98K 吞吐仅 +2.3%，请求 P99
  由 36.8 s 升至 40.0 s（+8.9%），明确继续增大 active work 的尾延迟代价。
- 图中保持单一吞吐坐标轴，以独立数值框说明 P99，不使用双 Y 轴；合成源图、16:9 拆分图、
  图集 PNG/SVG 与审计已同步。PPT 由用户自行替换，本轮未修改 PPT 源稿。

## 2026-08-17 P08A 标题修订与绘图脚本字体修复

- P08A panel 标题改为“图像也是分阶段工作量：张数描述不了阶段压力”，对齐动机 M1 页“文本讲量、
  图像讲阶段”口径；仅改文案，数据与布局未变。PPT 由用户自行更新，本日志不改动 PPT 源稿。
- 修复绘图脚本 Windows 中文字体回退（补入 Microsoft YaHei/SimHei），重画 P08A/P08B 的
  PNG/SVG/PDF 并刷新图集中文名副本；`figures/README.md`、`figures/opening_figure_set/README.md`
  与 `figures/audit/opening_story_figures_contract_20260808.md` 已同步。

## 2026-08-12 P02 边界与箭头修订

- 将四处贴边英文混排标签改成等义紧凑中文，保持字号不变；左下“文本 / 提示词”和中间两张
  机制卡均恢复安全右内边距。
- 将短主流程箭头改为固定 12 px 头部、4 px 线宽，Draw.io 使用小号 `endSize`；权威图、图集
  P02 与 PPT v8 已同步更新并通过原尺寸、PPT 缩放和 overflow 检查。
- 补充蓝/橙/紫/青虚线/绿五类流及普通框/虚线框图例；两张上方机制卡内容组整体左移。工作流
  页不再写“本课题研究对象”，第三列改为“外部 AI 数据执行层”。

## 2026-08-12 开题本地产物完成审计

- 完成 20 页 PPT 的独立重建、overflow 检查、逐页渲染哈希比对和 1--20 页目视复核；未发现
  重叠、裁切或图文主张冲突。
- Claim Matrix 与 opening 入口已改为本地报告/PPT/图表审计完成；当前不缺阻塞开题的数据图。
  第 2、3 页背景结构图和第 4 页文献分层图仅为可选视觉增强。
- 飞书普通云文档仍等待用户对指定 URL 的明确覆盖授权；未同步 Wiki。

## 2026-08-12 图像四 Job 证据补归档

- 将已完成的图像原生 40/40 与 Project observe-only 24/24 正式结果目录补入 `main`，并在
  Claim Matrix 与报告中引用相同的 normalized Job slowdown、snapshot freshness/构建成本。
- 报告新增图像四 Job 跨模态证据段，明确只作路径内部归一化；Project snapshot 不驱动
  credit/路由，static/shared group JCT 差 0.98%，不得写成动态策略胜出。
- 继续遵守暂停状态：本轮未生成或覆盖 PPT、飞书、Wiki 与 DOCX。

## 2026-08-11 SAOR 主线从 dynamic K 收窄为 fixed-envelope active-set release

- capacity-only SAOR 未超过 K160/简单 threshold，故开题研究问题、技术路线和答辩第14页不再
  把在线 K 增压/减压写成主方法；dynamic K 标记 `parked-conditional`。
- 主动作改为固定总 K 下的 Job entitlement、idle borrowing、completion-time reclaim 和
  ordered release；vLLM 继续使用显式 FCFS 与 continuous batching。
- 补上现有 static/shared 证据缺少 global FIFO/no project Job scheduler 的边界。下一项实验
  必须同时比较 FIFO/static/DRR/VTC-style/SAOR；简单策略达到同一 Pareto 前沿即淘汰 SAOR。
- 同步更新本地开题报告、Claim Matrix、20 页内容大纲和飞书 Markdown 源稿；遵守暂停状态，
  未覆盖云端飞书、Wiki、PPT 或图表。

## 2026-08-10 二十页内容大纲收口

- 按用户当前优先级暂停报告扩写和 PPT 制作，只维护 `opening_defense_outline_20260808.md`。
- 20 页均补齐“本页回答、核心内容/证据、页面结论、转场”，并统一标题与页码；新增
  背景—动机—研究内容—实验对应表，以及 18--20 分钟主讲时间和 15 分钟删减顺序。
- 逐页主线保持为：AI-Native Data Infra 背景→相邻研究与交界空白→baseline/动机现象→
  两项研究内容与共同代价估计→文本/图像验证→预期贡献；本轮未制作报告、PPT 或云文档。

## 2026-08-10 状态感知与动态提交内容闭环

- 将开题报告研究问题从四项补全为五项，分别覆盖 Work Unit、跨层状态识别、同上限动态
  admission/routing、多 Job 协调和代价估计决策质量；容量扫描作为状态识别和动作范围标定。
- 技术路线明确区分“GPU 低利用率”与“有 ready work 但欠供给”：只有 ready work、状态
  freshness、active work、完成速率与服务压力共同满足条件时才逐档增压；无 ready work 保持，
  状态过期或签名不一致回退 frozen-static。
- 报告、20页答辩大纲与问答库同步注明当前正式路径仍使用 static K/work；已有 trace、
  observe-only snapshot、request-window/controller 原型和 shared credit，但动态 active-work
  执行器尚未进入正式主 runner。该闭环列为开题后首个核心工程与同上限 A/B，不写成已完成贡献。
- 本轮未新增实验、图片、PPT、飞书云文档或 Wiki。

## 2026-08-09 权威报告图表状态护栏

- A/C脚本已含正确新标签，但当前PNG/SVG仍为旧渲染；按暂停绘图要求不执行生成器。
- 权威报告明确标注A/C不得作为最终开题图引用，恢复后只按冻结图合同做标签级重绘。
- 未生成或覆盖图片、PPT、飞书或Wiki。

## 2026-08-09 历史PPT/飞书底稿状态护栏

- 旧v6设计和本地飞书底稿仍含被replacement取代的数据；按暂停PPT/云文档/Wiki要求，
  仅增加“历史、禁止引用/同步/作为生成输入”护栏，并指向当前权威大纲、Claim Matrix、
  本地报告和待画图合同。
- README、navigation和总纲入口同步改为fail-closed：revision 289与本地飞书Markdown均为
  历史发布面，恢复同步时必须从权威本地报告重新生成，不能直接覆盖。
- 未生成或覆盖任何PPT、飞书云文档或Wiki。

## 2026-08-09 四级 Claim 与内容大纲收口

- Claim Matrix新增四条`不能声称`实际表行，覆盖跨系统总排名、dynamic普遍胜出、
  organizer/65K全局最优和71.24s/11.06s误读；四级证据不再只停留在定义。
- 报告的数据组织表述改为“大KV池仍有约12%范围但locality破坏未放大”；总纲与答辩
  大纲同步修正图像/原生/Project eager/DuckDB各轨已完成状态和可排名边界。
- 未生成图、PPT、飞书或Wiki内容。

## 2026-08-09 开题证据完成审计

- 复核两组database-E2E replacement：SQuAD可作静态均匀控制地基；ShareGPT只保留
  correctness/sink/产品语义护栏，因C32 direct仅达已测峰值52.07%而不作性能排名。
- 将Claim Matrix“待冻结实验组”改为“已完成实验组与可排名边界”，并把实验状态、方法
  合同和问答统一到online/eager两种arrival regime；动态策略仍不得写成普遍胜出。
- 回读全部待画图权威输入，修正三份最终eager多Job CSV的SHA前缀；headline未改变。
- 本轮未新增实验、图片、PPT、飞书或Wiki内容。

## 2026-08-09 Project eager 多 Job 匹配重测

- 统一报告、Claim Matrix、开题报告、答辩大纲和问答口径：Project eager 的 full/half
  single、static/shared+long 共12 formal全部通过，arrival span为66.76µs。
- 冻结两个独立效应：quota-only使short JCT+59.00%；matched long competition使
  static/shared short JCT+58.77%/+28.90%。eager shared相对static改善short、long、
  aggregate和Jain，但online replay方向不同，因此结论是arrival-regime dependence、
  idle borrowing和SLO/fairness guard的必要性，不称dynamic普遍胜出。
- 本轮只整理数据和待画图合同，未生成图片、PPT、飞书或Wiki内容。

## 2026-08-09 多 Job 请求到达语义澄清

- 明确 5 s 仅统一 Job 级启动；项目逐请求 arrival replay，原生 graph 使用完整 manifest
  eager 执行，71.24 s 与 11.06 s 不可写成系统性能倍数。
- 答辩攻击面和 H 图合同新增该红线；只保留项目 matched-cap 因果 A/B 与各原生轨内部
  single→overlap 变化。未改图、PPT、飞书或 Wiki。

## 2026-08-09 图表数据冻结与待画清单收口

- 回读 A–F、H 和 database-E2E 附录的权威输入、formal 数、SHA 与 headline；G 无结果，
  明确不画。
- 现有图视觉无裁切/重叠，但 A 的峰值 active-work/区间标签和 C 的“近似中性”标签
  需要下次渲染修订。当前没有运行绘图脚本或覆盖任何图。
- 后续唯一绘图清单为 A/C 标签重绘 + F/H 首次生成；未改 PPT、飞书或 Wiki。

## 2026-08-09 多 Job 因果方向与零重叠边界

- 将正式 5 s 矩阵明确标为 `Short→Long`：证明后到 long 对已运行 short 的干扰；
  `Long→Short` 是独立的 foreground-arrival/SLO 反事实，留论文阶段。
- 旧 15 s Daft Native 因 short 在 long 到达前自然完成，不进入多 Job 干扰结论；
  当前结论只使用 measured overlap > 0 的数据。
- 本轮只修订本地口径和实验计划入口，未画图、未修改 PPT、未同步飞书或 Wiki。

## 2026-08-09 答辩攻击面审计闭合

- 将“最强四项事实”改为 Work Unit、状态感知、动态/多作业调度和算子代价估计
  四条等权证据链，并将旧 13.9× 口径修正为正式 CSV 的 14.3×。
- 补齐高风险问题：Daft Ray 的 vendor-owned provenance；统一 5 s offset 只支持系统内
  single→overlap；两 Job 足以闭合最小因果但不外推 4+/weighted/SLO；当前多 Job
  是文本 ShareGPT；K512 不是开题 blocker；serving 干扰实验不强制 sink。
- 明确已有早期同步等量 1/2/4-job 矩阵；4-job shared 聚合均值有吞吐/tail/JCT
  改善，但三次中有一次回退且无 short/long 错峰因果，因此只作高竞争诊断，
  不与当前 5 s 两 Job 实验互相替代。
- 攻击面表和红线同步增加 provenance、offset 归一化、两 Job 外推与文本→图像外推
  限制。本轮未绘图、未修改 PPT，也未覆盖云文档或 Wiki。

## 2026-08-09 开题报告证据口径与实现边界对齐

- 使用已冻结的 Claim Matrix、答辩内容大纲和正式结果审计本地开题报告；
  本轮未修改 PPT、未绘图、未覆盖飞书或 Wiki。
- 多 Job 段补入 project static/shared 与 Daft Native/Ray/Ray Data 的实际 overlap，
  并明确原 15 s Daft Native 无 overlap 数据不进入干扰结论。
- 新增四个等权部件的“设计依据—当前实现—尚未验证”表：Work Unit、
  状态感知、动态/多作业调度和算子代价估计不再以设计图代替实现状态。
- 移除容量结果的重复图引用，保留数值与边界；原生 Daft 的 KV 表述收紧为
  “KV max 接近 1”，避免把均值 0.75–0.80 误写为全程顶格。

## 2026-08-08 database-E2E replacement 取代 failed-feeding headline

- K128 replacement 的 24/24 cells、18 formal 全部门禁通过；开题当前数字只引用 `opening_database_e2e_text_refeed_20260808/`。
- SQuAD 三条静态路径近似中性。ShareGPT 后续 C32–C256 扫描证明旧 direct C32 只达已测峰值 52.07%，故 project/C32-direct=1.5457 降级为并发/执行结构诊断，不作方法排名；DuckDB cap 语义边界仍有效。
- DuckDB AI ShareGPT service throughput≈direct，但 4,921/6,144 行 fixed-cap 产品语义失败。开题报告、Claim Matrix、问答与总纲已改用新口径，并保留 MFU、GPU、能耗和服务状态。
- 当前仍暂停 PPT、云文档和 Wiki；后续只补原生单 job、原生两 job 观察和项目 static/shared 同上限 A/B。

## 2026-08-08 四条证据链与原生框架/多 Job 对照成为当前执行目标

- 本条取代同日早先“文本 Daft/Ray Data 正式矩阵留论文阶段”的范围判断。Work Unit、状态感知、动态调度与共同使能代价估计按同等证据标准组织。
- 当前补齐 ShareGPT Chat 原生单 job 矩阵，以及 Daft Native/Ray、Ray Data 原生两 job 错峰观察；项目另作 static-partition vs shared-work-credit 因果 A/B。
- DuckDB 保持 SQuAD/cap=64 有界输出产品轨；不扩大 workload、模型、数据库、weighted 或 offset 搜索追求正结果。
- 原生两 job runner 复用现有轻量 sampler 保存逐 GPU 与 vLLM gauge/latency 时序，让 JCT 现象能够对应到感知信号；该观测不等于框架内部 scheduler trace。
- 新增单 job 与原生多 job 的 AutoDL 模板；calibration selection/fingerprint、manifest 和 offset 未冻结时均拒绝 formal。
- 原生单 job matrix 同步保存 GPU 与 vLLM estimated-FLOPs 时序；正式对比将包含 MFU/能耗，不再遗漏该指标。

## 2026-08-08 开题实验收缩为三臂 replacement + 两作业错峰

- 开题只需讲清问题、设计依据、已有可行性与后续可证伪评价，不要求 proposed 在开题前完成论文级胜出。
- 复用现有状态差异、active-work、组织 regime、1/2/4-job、图像 matched-resource 与 cost decision-quality；剩余只完成文本三臂 replacement 和 short/long 两作业 staggered 两臂。
- phase-change、3:1 weighted、文本 Daft/Ray Data 正式矩阵、图像新策略和 cost held-out 统一留论文阶段；已撤回临时图像新策略代码。

## 2026-08-08 调度实验边界改为完整结果 gather

- 文本与图像方法主实验不再强制 sink；统一 source、完整结果语义、资源与 gather 边界即可回答组织/提交/多 job 的因果增量。
- 已启动的文本三臂统一 PostgreSQL sink 重跑继续完成，定位为 database-E2E/correctness 护栏；其 service/operator 分项与后续无 sink 方法消融分开解释。
- 图像 pgvector sink 仅作小规模 exactly-once/检索质量闭环，不进入 operator-E2E 性能主排名；未同步 Wiki 或云文档。

## 2026-08-08 暂停 PPT 成品，冻结内容大纲与证据合同

- 用户要求先不制作 PPT；新的 PPTX 排版、生成与云端同步全部暂停。
- 新增 `../opening_defense_outline_20260808.md`，逐项冻结主讲 take-away、所需数据、claim 边界、必要实验和结果图合同。
- 已移除仅用于临时版式 dry-run 的 v7 构建器；旧 headline 不会进入新成品。当前只继续 feeding 校准、replacement formal、实验报告和数据图。
- 用户明确不需要 Wiki 同步；当前也不覆盖普通飞书云文档。

## 2026-08-08 第一性原理叙事与 19 项内容骨架

- 动机页改为“等行数不等 work → WorkDescriptor”“同一静态上限不等状态 → fresh snapshot”“欠供给/安全区/过载 → bounded dynamic credit”三条因果链；图像阶段失衡补充 stage-aware work 的跨模态必要性。
- 算子代价估计进入主方案图，作为两项研究内容的共同使能部件，同时输出 stage/service/remaining work、SLO slack 与 uncertainty；独立 cost 页只展示 decision-regret 可行性，不扩张成第三项研究内容。
- 曾做过一次仅位于 `/private/tmp` 的临时版式 dry-run；它没有形成仓库 PPTX，也不作为当前交付。后续已按用户要求停止 PPT 成品工作。
- 用户明确不需要 Wiki 同步。

## 2026-08-08 v6 PowerPoint 应用级验收通过

- Microsoft PowerPoint 已真实打开 `opening_defense_20260807_v6.pptx`，识别为 28 页；
  首页与缩略图正常，未出现文件修复提示。
- WPS Office 启动后未出现文档窗口，不计为通过。PPT 本地内容与兼容性门禁已完成。
- 飞书用户明确批准后，线上 docx 已由 revision 277 覆盖并上传四张核心图。最终 revision
  289 的八章目录、关键数字、结论边界和四个带 caption 的图片块均已回读通过。
- 这是当时的 v6 状态；随后第一性原理复审已将 v6 降级，用户也明确免除 Wiki 同步，不能沿用“只余 Wiki”判断。

## 2026-08-07 开题本地材料冻结候选

- 两组统一文本三臂正式实验完成并归档；项目路径在 SQuAD/ShareGPT feeding 门均失败，
  已按负结果写入报告、PPT、Claim Matrix 和答辩问答，没有增加第二数据库或新 workload。
- 报告正文压缩为一张统一三臂表和四张核心图；四级 claim 明确区分已证明、条件性、
  待验证与不能声称。
- 从 v5 学校模板映射生成 28 页 v6，逐页备注完整；程序化渲染发现并修复 v5 bullet 残留、
  图片未替换和底图透出。最终空 placeholder=0，notes failure=0，画布 overflow test 通过。
- `opening/feishu/opening_report_wiki.md` 已与本地报告逐字一致。线上文档未覆盖：现有用户
  token 过期，授权请求被 Codex 额度门禁拒绝；恢复授权后必须 dry-run overwrite、插入四图并回读。
- 尚欠 WPS/PowerPoint 实际打开检查；因此当前称“本地冻结候选”，不称最终发布版。


## 2026-07-29 开题答辩 PPT v6 设计规划与反馈修订

- 完成 v5 的内容、视觉和架构图审阅，确认当前文件落后于 2026-07-29
  项目实验结论，不能直接作为当前汇报版本。
- 新增 `opening/slides/opening_defense_v6_design.md`：正文沿用 v5 四章节，
  重点讲动机、架构、策略与实验设计；大量正式结果移入备份或 speaker notes。
- official baseline 先展示同条件实验设计，只有通过输入/资源等价、计量口径、
  规模校准和正式重复门禁后，才向正文模块化插入结果。
- 设计要求架构图以 SVG 为权威源，并输出 EMF/高清 PNG；不使用普通文生图，
  不把 vLLM waiting 画成当前默认控制器的一阶反馈，不把写回画成独立贡献。
- 当前等待设计复审，未修改 v5、未生成 v6、未同步线上飞书。

## 2026-07-29 文献基线升级与研究问题收敛

- 开题 Top 15 重排为 15/15 严格 CCF-A 正式 research paper；PVLDB Tutorial、
  CIDR、Companion 和 arXiv 移入核心补充。
- 新增 VTC、Llumnix、LOTUS、Palimpzest、Abacus、SemBench、FairServe、
  DLPM、Autellix、Chiron 十篇权威精读；Top 15 PDF 与笔记均 15/15 齐全。
- 开题仍保留两项研究内容：数据组织；调度与提交控制。代价估计从补充讨论
  提升为共同使能组件，多模态仍为泛化验证。
- 研究问题收敛为最小饱和压力/瞬态 ramp、相同 work 数据组织、多 job
  shared-credit/fairness；同步改写报告和 PPT 源稿。
- 未同步 Wiki。

## 2026-07-24 文献精读语料迁出 opening/，opening 仅留开题精读清单 + Top15 拷贝

- 文献精读笔记（44 篇）、PDF（69 个）、清单与评估从 `opening/literature/` 迁至 `research/`（项目级文献目录）。理由：opening 是阶段性开题工作区，不该承载项目级长期文献资产；opening 自己的 README/navigation 也指向 research/ 查文献。
- `opening/literature/` 现仅保留：`reading_list.md`（开题精读优先级）+ `top15_reading_notes/`（开题要求精读的 15 篇笔记拷贝 + figs，自包含快照；权威版在 `research/reading_notes/`）。
- 索引/链接同步：`opening/README.md`、`opening/AGENTS.md`、`opening/navigation.md` 的 `literature/` 描述已更新；完整变更清单见根 `PROJECT_LOG.md` 2026-07-24 条目。

## 2026-07-20 飞书同步：新增 Daft 管线开销验证图

- 在开题报告 §4.2 可行性分析中新增图 4-7（arrow_postgres vs daft_postgres 阶段耗时对比）及配套段落。
- 论证目标：Daft 作为数据引擎不会引入可观测的数据管线开销（DB Read + Build/Organize < 0.1s），后续 AI_COMPLETE 及多模态泛化验证统一使用 Daft 具备可行性依据。
- 本地来源：`opening/report/opening_report.md`（已同步修改），图源：`figures/data/report_main/b26_arrow_vs_daft_stage_breakdown.png`。
- 飞书同步：使用 `str_replace`（markdown stdin 模式）在飞书 docx 中 §4.2 "Lance sink" 段落后插入新段落，随后通过 `block_move_after` 将 `docs +media-insert` 上传的图片移到图注之前。revision 更新到 214。
- 工具教训：`--content ./file.md` 在 str_replace markdown 模式下被当作字面文本而非文件内容，需改走 stdin (`--content -`)。

## 2026-07-15 开题报告移除 fake/CPU 主文证据

- 根据当前已经完成 pgai SQL 触发面集成和真实 GPU-backed `AI_EMBED` 完整链路复测的事实，更新 `opening/report/opening_report.md` 和 `opening/feishu/opening_report_wiki.md`。
- 删除 4.2 中历史 fake/CPU 预研图、表和相关表述，避免读者误解课题仍停留在 toy/fake benchmark 阶段。
- 4.2 可行性证据现在只保留 PG18.4 + pgvector 环境、GPU-backed `AI_EMBED` 链路和双 endpoint Ray 动机测试；调优变量依据改为文献机制 + 当前真实 GPU-backed 复测。
- 已覆盖同步新版开题报告飞书 docx，并重新插入 8 张正式 PNG；回读确认 revision 更新到 `72`，未检出 fake/CPU、图 4-7、表 4-4、Mermaid 旧图或本地 `figures/` 路径残留。

## 2026-07-15 开题报告飞书新版 docx 同步

- 使用 user 身份将 `opening/feishu/opening_report_wiki.md` 覆盖同步到新版开题报告飞书 docx：`https://my.feishu.cn/docx/CRgXdyTlToXpgjxo3otcf3kInGb`。
- 覆盖写入后飞书返回 `partial_success`，原因是 Markdown 中的本地图片路径不能直接导入为图片资源；随后逐张上传并插入 8 张 PNG：研究缺口图、总体研究框架图、三层上游执行策略图、运行时策略闭环图、粒度对比图、阶段时延图、endpoint 对比图、pgvector 写回对比图。
- 回读线上文档确认 revision 更新到 `51`，关键图注附近为真实飞书图片 URL；关键词检查未发现本地 `figures/` 路径和旧的“三岛/Killer/联合最优/边界确认/阶段画像/Ours-v0”等表述残留。

## 2026-07-15 策略设计与实现参考沉淀

- 新增 `experiments/plans/strategy_design_implementation_reference.md`，作为后续实验设计和系统实现参考，汇总 Ray、vLLM/Ray Serve/Triton、GPU 数据放置和 DB AI 算子文献机制如何支持本课题三层策略。
- 明确三层策略口径：计划层数据组织、运行层入口调度、服务端 dynamic / continuous micro-batching；该口径后续应同步到开题报告、PPT 和答辩讲解中。
- 进一步补充“系统优化蓝图”和“机制到实现任务优先级”，把文献机制拆成 Workload Profiler、Plan-time Data Organizer、Ray Admission Controller、Endpoint Router、Service-side Micro-batcher 和 E2E Guardrail 六个可实现模块。
- 已同步 `experiments/plans/README.md`、`experiments/plans/strategy_design_literature_basis.md` 和 `PROJECT_INDEX.md`。

## 2026-07-15 GPU 调度与数据放置补充调研

- 新增 `research/gpu_scheduler_data_placement_supplement_20260715.md`，用于说明策略控制器设计参考了 GPU / LLM 推理服务调度、Ray/Ray Data 异构数据管线、GPU 数据库算子与数据库 AI 算子等文献线索。
- 该文件当前作为“设计依据与后续精读清单”，不替代逐篇精读笔记；未下载或未逐篇核验的条目仍按待核验处理。
- 已同步 `opening/README.md`、`opening/literature/reading_list.md` 和 `PROJECT_INDEX.md` 入口。

## 2026-07-15 策略设计重新评判与三层收窄

- 将策略机制图从“全运行时控制器”收窄为 three-layer upstream execution strategy：计划层在执行前选择 `batch_size` / `partition_count` / `object_merge`，运行层调整 `K_max`、routing、backpressure，服务端用 dynamic / continuous batching 形成推理 `micro-batch`。
- 当前不采用“运行时重切数据库侧已物化 RecordBatch”作为主方案；动态 batch 借鉴 vLLM / Ray Serve / Triton 思路，放在模型服务侧尚未执行的请求队列中。
- 补充 Ray OSDI 2018 调度思想映射：task/actor、resource-aware scheduling、local/global scheduler、object store locality 和 actor pool 可迁移为 task 粒度、actor 池、资源约束、placement/locality、`K_max` 与 routing 等实验变量。
- 已重新生成 `figures/architecture/runtime_strategy_control_loop.png` / `.svg`，并同步图表审计与策略设计文档。

## 2026-07-13 项目级图资产目录迁移

- 根据后续 learning、中期汇报和毕业论文都会复用图表的要求，将正式图资产从 `opening/assets/` 和 `learning/figures/` 迁移到根目录 `figures/`。
- 新增 `figures/AGENTS.md`、`figures/README.md`、`figures/audit/figure_plan.md`、`figures/audit/experiment_charts_audit.md` 和 `figures/scripts/README.md`，明确项目级图资产库、正文主图、备份图、审计记录和绘图脚本的职责。
- 当前正式主图位于 `figures/architecture/` 和 `figures/data/report_main/`；补充说明图位于 `figures/data/backup/`；学习讲解专用图位于 `figures/learning/`。
- 同步更新 `opening/report/opening_report.md`、`opening/feishu/opening_report_wiki.md`、`learning/experiment_walkthrough.md`、`learning/README.md`、`opening/navigation.md`、`opening/assets/README.md`、`PROJECT_INDEX.md` 和根目录 `README.md` 中的图路径或入口说明。
- 删除旧的 `opening/assets/charts/`、`opening/assets/figures/`、`learning/figures/` 以及旧 ECharts 生成脚本和重复系统架构图副本。后续如需重生成实验图，应使用 `figures/scripts/` 中的 Python 脚本和原始 CSV。

## 2026-07-13 开题报告本地调整与旧 PPT 作废

- 根据项目当前主线，将 `opening/report/opening_report.md` 中研究内容一从“数据组织与批处理执行调度”收束为“数据组织与批处理构造”，减少与研究内容二“GPU 推理服务状态感知的 Ray 并行调度与反压控制”的重叠。
- 同步更新 `PROJECT_OUTLINE.md`、根目录 `README.md`、`PROJECT_INDEX.md`、`overview/current_direction_and_plan.md`、`opening/outline.md` 和图表选择说明中的研究内容名称。
- 根据用户要求，当前 `opening/slides/opening_ppt.md` 和 `opening/slides/opening_defense_20260712.pptx` 的内容和表现形式先作废；保留学校模板中的标题区、正文安全区、图表区和页脚等页面布局经验。
- 新增 `opening/slides/README.md` 记录 PPT 当前状态。`opening/ppt_rules.md` 保留版式规则，并明确旧版 PPT 不再作为正式汇报内容依据。
- 本轮只调整本地开题报告和相关入口文件；飞书正文与线上文档等用户过目本地报告后再同步。

## 2026-07-13 GPU-backed 动机实验图生成

- 使用 `D:/Tools/echarts` 中的 ECharts + sharp 生成第一批开题实验图，脚本为 `opening/assets/generate_echarts_experiment_charts.js`。
- 输出目录为 `opening/assets/charts/`，每张图同时生成 SVG 与 PNG，便于报告和 PPT 分别引用。
- 当前生成三张图：fine vs coalesced 端到端耗时对比、16K 行阶段拆分、双 endpoint 下 Python / Ray task / Ray actor 对比。
- 数据均来自 `motivation/results/gpu/` 的真实 GPU-backed CSV，排除 warm-up，仅使用 formal repeats 平均；PG18.4 连接验证、dry-run 和 smoke 结果不单独画图，后续保留为表格或文字说明。
- 运行时 `sharp` 报告 fontconfig cache 不可写，但 SVG 和 PNG 均已生成；暂不修改系统级字体缓存配置。

## 2026-07-13 系统架构图版式修订

- 根据用户反馈修订 `opening/assets/figures/system_architecture_ai_data_execution.svg` 与 PNG 预览。
- 主要调整：主链路箭头改为水平通道连接，取消斜向研究箭头；各阶段框内改用等宽小标签；标题居中；底部研究卡片增高，避免文字贴边或越界。
- 根据二次反馈继续修订：将黄框“观测与策略层”的标签改为七个等宽等距标签，避免不明确的三段式间距；增大底部研究卡片编号与标题的间距，避免编号圆点与标题文字重叠。
- 根据三次反馈修复主链路阶段编号位置：编号改为框内左上角编号槽，避免越出边框或遮挡 `GPU model service` 等较长标题；同时将黄色“观测与策略层”与下方四个执行阶段左右边界对齐，并调整下指箭头到对应阶段中心线。
- 同步更新生成脚本 `opening/assets/generate_system_architecture_figure.py` 和质检记录 `opening/assets/figures/system_architecture_ai_data_execution_audit.md`。
- 当前图稿用于人工确认系统架构图方向，尚未自动替换到开题报告正文或重新生成 PPTX。

## 2026-07-13 系统架构图初稿

- 使用 `figure-designer` 重新设计开题系统架构图，图类型定位为 Solution Overview / System Architecture。
- 新增可复现生成脚本 `opening/assets/generate_system_architecture_figure.py`。
- 生成 SVG 与 PNG：
  - `opening/assets/figures/system_architecture_ai_data_execution.svg`
  - `opening/assets/figures/system_architecture_ai_data_execution.png`
- 新增质检记录 `opening/assets/figures/system_architecture_ai_data_execution_audit.md`，检查向量格式、字体、颜色、图注和无装饰性图表元素。
- 当前版本为初稿，尚未替换进开题报告正文或重新生成 PPTX，先供人工查看和确认结构。

## 2026-07-13 开题报告主线调整

- 根据用户确认，将开题报告题目调整为“面向数据库驱动 AI 工作负载的分布式数据执行与存储协同优化研究”。
- 重写 `opening/report/opening_report.md` 的背景、研究目标、研究内容、总体框架和预期创新点：数据库 AI 算子降为 workload 入口和验证场景，Daft/Arrow、Ray、GPU 模型服务、Lance / pgvector / PostgreSQL sink 成为数据执行与存储协同的研究主体。
- 同步更新 `opening/feishu/opening_report_wiki.md`、`opening/slides/opening_ppt.md`、`opening/outline.md`、`opening/qa_bank.md`、`opening/README.md` 和 `opening/AGENTS.md`。
- 同步检查并修改项目级规划文档：`README.md`、`PROJECT_OUTLINE.md`、`PROJECT_INDEX.md`、`AGENTS.md`、`overview/current_direction_and_plan.md` 和 `motivation/plans/integration.md`。
- 尝试使用 user 身份覆盖写入开题飞书 wiki 时，`lark-cli` 因用户目录刷新锁文件权限返回 `Access is denied`；提升权限重试被自动审批拒绝。本地源稿已准备好，线上飞书 wiki 需要后续有权限后再同步。

## 2026-07-12

- 创建 `opening/` 开题工作区。
- 建立开题总体规则、PPT 制作规则、统一骨架、文献清单和答辩问答库。
- 当前汇报主线确定为：数据库 AI 算子的新执行链路问题 -> 阶段画像与初步瓶颈 -> 模型服务感知的 batch、调度、反压和写回优化。
## 2026-07-12 飞书同步目标

- 登记两个飞书同步目标：
  - 开题报告与开题汇报：`https://my.feishu.cn/wiki/GCxowlVJbinzgRkoHDmc06cSn9J?from=from_copylink`
  - 动机测试与可行性测试：`https://my.feishu.cn/wiki/R2MywYu12i2PtWk84Vzcbp9Lnme?from=from_copylink`
- 新增 `opening/feishu/README.md`，规定本地 Markdown 是源稿、飞书是发布面。
- 后续实际写入飞书时使用 `lark-doc`；如遇嵌入 Base 或飞书幻灯片，再分别使用 `lark-base` 和 `lark-slides`。

## 2026-07-12 默认参考 skill

- 在 `opening/AGENTS.md` 中登记开题工作默认参考 skill：`karpathy-guidelines`、`humanizer`、`academic-research-suite`、`deep-research`、`nature-academic-search`、`vibe-research-workflow`、`ppt-master`、`nature-paper2ppt`、`lark-doc`、`lark-base`、`lark-slides`。
- 在 `opening/literature/reading_list.md` 中明确：文献检索优先使用 `nature-academic-search`，系统综述和研究缺口判断按需使用 `deep-research` 和 `academic-research-suite`。
- 补充说明：skill 是方法参考，不是固定流程；后续执行要结合本项目真实阶段、已有实验、学校模板和导师要求灵活使用。

## 2026-07-12 ECharts 图表规则

- 新增 `opening/assets/echarts_rules.md`，记录 ECharts SSR 生成 SVG、sharp 转 PNG、PPT 用 PNG、Word / 报告用 SVG 的流程。
- 在 `opening/ppt_rules.md` 和 `opening/assets/README.md` 中登记 ECharts 图表生成与嵌入规则。
- 在 `opening/AGENTS.md` 中补充 `figure-designer` 和 `nature-figure` 作为图表设计和论文级图表审查的可参考 skill。

## 2026-07-12 开题导航规则

- 新增 `opening/navigation.md`，说明开题材料需要项目内容、实验结果、文献、PPT 素材和飞书同步信息时分别从哪里找。
- 在 `opening/README.md` 和 `opening/AGENTS.md` 中登记 `navigation.md` 为开题目录阅读入口。
- 明确报告、PPT、飞书版之间的关系：报告负责完整论证，PPT 负责现场讲解，飞书负责发布同步，三者必须保持同一口径。

## 2026-07-12 开题报告与 PPT 初稿

- 补全 `opening/report/opening_report.md`，形成开题报告初稿，覆盖研究背景、国内外现状、研究目标、关键问题、技术路线、初步实验、预期创新点、进度安排和预期成果。
- 补全 `opening/slides/opening_ppt.md`，形成 16 页 PPT 内容源稿，并为每页加入 `汇报讲稿` 和 `答辩备注`。
- 更新 `opening/feishu/progress_update.md`，同步当前已完成内容、真实 GPU-backed 实验事实、问题、下周计划和待确认事项。
- 更新 `opening/literature/reading_list.md`，补充 Snowflake AISQL、Ray、Daft、Arrow、Lance、pgai、pgvector、PostgresML、Spark SQL tuning 等候选资料分类和精读顺序。
- 扩展 `opening/qa_bank.md`，补充 `AI_EMBED` 优先级、PG18.4/PG18.3 边界、pgvector 写回边界、模型推理淹没外部链路、传统查询优化区别等答辩问题。
- 更新 `opening/README.md`，将报告、PPT、飞书、文献和问答状态从占位阶段同步为初稿阶段。

## 2026-07-12 去除个人规划口径

- 根据用户反馈，调整开题报告、PPT、文献清单和答辩问答中关于 `AI infra / inference infra` 的表述。
- 开题材料改为只从项目和论文角度说明研究价值：数据库 AI 算子中的模型服务调用、批处理推理、外部调度和结果写回问题。
- 避免把研究方向表述为个人未来规划或职业取向。

## 2026-07-12 调整开题题目口径

- 根据用户反馈，将开题题目从“面向数据库 AI 算子的外部执行链路优化研究”调整为“面向数据库 AI 算子的模型服务感知批处理执行与写回协同优化研究”。
- 调整 `opening/report/opening_report.md`、`opening/slides/opening_ppt.md`、`opening/outline.md`、`opening/README.md`、`opening/feishu/progress_update.md` 和 `opening/qa_bank.md`。
- 保留“外部链路 / 可控执行路径”作为系统边界解释，不再把它作为正式题目中的研究对象。

## 2026-07-12 正式材料术语降频

- 根据用户反馈，将正式报告和 PPT 源稿中的“外部链路 / 链路”进一步替换为“执行路径”“执行过程”“阶段画像”“跨系统执行过程”等更适合开题报告的表述。
- 保留少量“外部 worker”“外部执行”等技术事实表达，用于说明系统边界。

## 2026-07-12 开题报告正文去元话语

- 根据用户反馈，清理 `opening/report/opening_report.md` 中“当前最适合写入开题”“本文件用于承接学校模板”“草稿”等工作流提示。
- 将第 6 节改为正式报告口吻：直接陈述已完成实验、实验结果和适用边界。

## 2026-07-12 记录开题材料生成顺序

- 根据用户反馈，明确当前阶段不生成 DOCX，优先维护本地 Markdown 和飞书文档。
- 记录开题材料顺序：本地 Markdown -> 飞书文档补全 -> PPT -> PPT 同步飞书 -> 最终 DOCX 生成。
- 在 `opening/templates/README.md` 中记录：最终 DOCX 必须使用学校 Word 模板生成，继承章节样式、字体、行间距、图表标注和参考文献格式。

## 2026-07-12 继续完善报告与飞书源稿

- 补充 `opening/report/opening_report.md` 的进度安排细节、预期关键技术指标和主要参考文献初稿。
- 新增 `opening/feishu/opening_report_wiki.md`，作为“开题报告与开题汇报”飞书 wiki 的本地源稿。
- 更新 `opening/feishu/README.md`、`opening/README.md` 和 `opening/navigation.md`，登记飞书 wiki 源稿入口。
- 更新 `opening/feishu/progress_update.md`，同步当前“已按模板重排报告”和“已新增飞书 wiki 源稿”的进展。

## 2026-07-12 研究内容写法规则

- 根据用户提供的既往开题反馈经验，明确“研究内容”不能写成工程任务清单，应写成“问题、技术难点、拟采用方法、评估方式”的闭环。
- 重写 `opening/report/opening_report.md` 第 3.2 节，将原先的执行路径画像、batch 划分、endpoint routing、写回优化和多 workload 验证，改为可检验的研究问题。
- 同步更新 `opening/feishu/opening_report_wiki.md` 的研究内容口径，便于后续补全飞书文档。
- 更新 `opening/work_rules.md`，记录研究内容写作规则：工程步骤放在技术路线或进度安排里，研究内容必须说明为什么难、怎么解决、如何证明有效，并包含对比对象、评价指标、消融实验和适用边界。

## 2026-07-12 研究内容范围收敛

- 根据用户反馈，进一步区分“研究手段”和“研究内容”：阶段划分、执行画像和瓶颈归因保留为动机测试、方案设计和评价基础，不再作为独立研究内容或预期创新点。
- 将 `opening/report/opening_report.md` 第 3.2 节收敛为三个方法问题：模型服务感知的批处理执行调度、写回压力感知的结果汇聚与持久化协同、多类数据库 AI 算子的策略选择与适用边界。
- 使用 `humanizer` 检查并改写相关段落，减少机械模板句式和元话语，保留正式开题报告语气。
- 同步更新 `opening/feishu/opening_report_wiki.md` 和 `opening/report/opening_report.md` 的预期创新点。

## 2026-07-12 可行性分析表格化

- 根据用户反馈，重写 `opening/report/opening_report.md` 第 4.2 节，用表格说明已有实验链路、GPU-backed `AI_EMBED` 分阶段结果、双 endpoint Ray 动机测试和 fake/CPU 历史预研。
- 可行性分析明确区分正式 GPU-backed 实验事实、PG18.4 本地预演事实、fake/CPU 预研和不能声称的边界。
- 同步更新 `opening/feishu/opening_report_wiki.md` 的初步实验依据部分，补充关键结果表和预研结果使用方式。

## 2026-07-12 正文去除写作元话语

- 根据用户反馈，清理 `opening/report/opening_report.md` 研究内容中的“该部分不把目标写成”“不能写成”等写作提醒式表述。
- 将相关句子改为正式研究表述，直接说明研究对象、变量、评价方法和适用边界。
- 同步清理 `opening/feishu/opening_report_wiki.md` 中同类表达。

## 2026-07-12 开题与项目方向同步

- 根据用户反馈，明确开题材料不是独立展示稿，开题报告中收敛出的题目、研究内容和实验边界会反向影响项目方向与后续实验规划。
- 更新项目级 `README.md`、`PROJECT_INDEX.md` 和 `overview/current_direction_and_plan.md`，登记当前开题题目“面向数据库 AI 算子的模型服务感知批处理执行与写回协同优化研究”及三项研究内容。
- 更新 `opening/README.md` 和 `opening/work_rules.md`，记录修改开题题目、研究内容或实验边界时必须同步检查项目入口文档，避免 opening 与项目主线割裂。

## 2026-07-12 飞书开题 wiki 同步

- 使用 user 身份将 `opening/feishu/opening_report_wiki.md` 覆盖写入开题飞书 wiki：`https://my.feishu.cn/wiki/GCxowlVJbinzgRkoHDmc06cSn9J?from=from_copylink`。
- 飞书返回 `result=success`，文档 revision 更新为 `4`，5 个 Mermaid 图块成功解析为 whiteboard，无写入警告。
- 本次同步仍以本地 Markdown 为源稿；后续修改报告、PPT 或实验边界时，需要先改本地文件，再同步飞书。

## 2026-07-12 开题核心图规划

- 根据用户反馈，明确项目框架结构、总体流程和方向把控类大图必须保留，不能只依赖表格和文字说明。
- 新增 `opening/assets/figure_plan.md`，记录开题必须优先完成的三类图：课题总体研究框架图、端到端执行路径与阶段画像图、可行性实验关键结果图组。
- 在 `opening/report/opening_report.md` 第 3 节新增“总体研究框架”，用 Mermaid 描述三类 AI 算子场景、可观测批处理执行过程、三项研究内容和评价证据之间的关系。
- 同步更新 `opening/feishu/opening_report_wiki.md` 的研究内容部分，加入总体研究框架图源稿。
- 已重新同步飞书开题 wiki，飞书返回 `result=success`，文档 revision 更新为 `8`，6 个 Mermaid 图块成功解析为 whiteboard，无写入警告。

## 2026-07-12 开题与项目规划双向同步规则

- 根据用户反馈，进一步明确开题报告和项目规划不是单向关系：开题报告要基于当前项目进展与后续规划撰写；后续开题报告内容和方向调整时，项目整体规划、实验优先级和侧重点也要同步调整。
- 更新项目级 `README.md` 和 `PROJECT_INDEX.md`，明确开题报告、overview、motivation 计划和项目入口之间必须保持同一方向口径。
- 更新 `overview/current_direction_and_plan.md`，说明该文件与开题报告保持双向同步，不能长期描述两个不同方向。
- 更新 `opening/README.md` 和 `opening/work_rules.md`，将开题调整分为语言格式调整、方向内容调整、实验结论调整三类，并记录对应的同步检查范围。

## 2026-07-12 调整实验主线入口

- 根据用户反馈，降级 `feasibility/guide.md` 在项目索引中的地位：该文件只作为早期组件可行性验证指南，不再承担当前实验大纲职责。
- 重写 `PROJECT_INDEX.md` 第 3 节为“实验主线与证据入口在哪里”，将主入口调整为 `motivation/README.md`、`motivation/plans/workloads.md`、`motivation/plans/integration.md`、`motivation/results/README.md` 和 `motivation/results/gpu/README.md`。
- 更新 `motivation/README.md`，移除 GPU-backed 结果“待补”的过时表述。
- 更新 `feasibility/README.md` 和 `feasibility/guide.md`，明确 feasibility 只负责组件、环境和脚本可用性，不承载开题主线或 GPU-backed 性能结论。

## 2026-07-12 根目录项目总纲与日志

- 根据用户反馈，新增根目录 `PROJECT_OUTLINE.md`，汇总当前项目大纲、实验主线、关键证据、近期优先级和开题/项目双向同步规则，方便直接从根目录阅读和调整。
- 新增根目录 `PROJECT_LOG.md`，作为项目级简要操作日志，用于记录跨目录、影响项目方向或入口结构的调整。
- 更新根目录 `README.md` 和 `PROJECT_INDEX.md`，登记 `PROJECT_OUTLINE.md` 和 `PROJECT_LOG.md` 作为快速入口。

## 2026-07-12 开题报告与飞书内容复核

- 根据当前项目大纲、GPU-backed 动机结果和实验主线入口，复核 `opening/report/opening_report.md` 与 `opening/feishu/opening_report_wiki.md`。
- 确认开题报告整体方向合适：研究内容、技术路线和可行性分析均以 `motivation/results/gpu/` 的真实 GPU-backed 结果作为主证据，并区分 PG18.4、fake/CPU 和 PostgreSQL 18.3 内部平台边界。
- 清理 `opening/feishu/opening_report_wiki.md` 开头的本地源稿说明，避免飞书发布面出现工作流元话语。
- 在飞书后续计划中补充 PostgreSQL 18.3 内部平台复测安排。
- 修正 `motivation/results/README.md` 中 GPU-backed 结果入口的过时措辞。

## 2026-07-12 实验结论写作标准

- 根据用户反馈，将 `learning/AGENTS.md` 的实验讲解标准作为后续实验结论、数据分析、开题可行性分析和飞书实验摘要的写作参照。
- 更新 `PROJECT_OUTLINE.md`、`PROJECT_INDEX.md` 和 `opening/work_rules.md`，要求实验分析说明实验目的、链路流程、参数含义、数据来源、结果读法、不能证明内容、结论类型和下一步验证。
- 明确正式报告可以更凝练，但结论边界和分析精细程度不能低于学习材料中的标准。

## 2026-07-12 开题飞书按学校模板重排

- 根据用户要求，严格参照 `opening/templates/硕士生开题报告模板0604.docx` 的大标题与章节结构调整开题飞书 wiki。
- 将 `opening/feishu/opening_report_wiki.md` 从汇报式结构改为模板式报告结构，保留封面占位、`1. 课题背景、目的和意义` 到 `7. 主要参考文献` 的章节顺序，并移除“当前主线”“一句话说明”“初步实验依据”“后续计划”“需要确认”等模板外标题。
- 使用 user 身份覆盖写入开题飞书 wiki：`https://my.feishu.cn/wiki/GCxowlVJbinzgRkoHDmc06cSn9J?from=from_copylink`。飞书返回 `result=success`，文档 revision 更新为 `18`，6 个 Mermaid 图块解析为 whiteboard，无写入警告。
- 覆盖后重新拉取飞书目录，确认目录只包含模板章节和对应二级结构；关键词检查未发现旧的汇报型标题残留。

## 2026-07-12 动机测试飞书页补全

- 新增 `opening/feishu/motivation_feasibility_wiki.md`，作为“动机测试与可行性测试”飞书 wiki 的本地源稿。
- 源稿按 `learning/AGENTS.md` 的实验讲解标准组织：本页定位、当前结论摘要、真实 GPU-backed `AI_EMBED` 结果、多 endpoint Ray 动机测试、fake/CPU 预研使用边界、可行性验证边界、对开题方向的含义和下一步实验。
- 使用 user 身份覆盖写入飞书 wiki：`https://my.feishu.cn/wiki/R2MywYu12i2PtWk84Vzcbp9Lnme?from=from_copylink`。飞书返回 `result=success`，文档 revision 更新为 `10`，5 个 Mermaid 图块解析为 whiteboard，无写入警告。

## 2026-07-12 开题汇报 PPTX 生成

- 使用 `opening/templates/opening_ppt_template_version_v6_long_notes_source_checked.pptx` 作为学校 PPT 模板，基于 `opening/report/opening_report.md`、`opening/slides/opening_ppt.md` 和 GPU-backed 动机实验生成 16 页开题汇报 PPTX。
- 生成目录：`projects/opening_defense_20260712/`；最终交付文件：`opening/slides/opening_defense_20260712.pptx`。
- 质量检查：模板填充容量检查 `ok=294 warn=0 error=0`；PPTX 读回验证 `ok=181 warn=0 error=0`；`nature-paper2ppt` 审查结果为 `high=0, medium=0, low=26`，剩余 low 均为模板近似对齐提示。
- 已使用 user 身份将 PPTX 导入为飞书在线幻灯片：`https://my.feishu.cn/slides/NXsJsm2FRlZAAgdSfAmcqk9rnCg`，导入任务 `7661615330808482775` 返回 `job_status_label=success`。

## 2026-07-13 开题报告飞书图文同步

- 将 `opening/report/opening_report.md` 与 `opening/feishu/opening_report_wiki.md` 中的总体研究框架图和三张 GPU-backed 实验结果图，从 Mermaid 图替换为本地 PNG 图片引用。
- 使用 user 身份覆盖写入开题飞书 wiki：`https://my.feishu.cn/wiki/GCxowlVJbinzgRkoHDmc06cSn9J?from=from_copylink`，正文同步后 revision 更新到 `32`；由于飞书 Markdown 不直接导入本地图片路径，返回 `partial_success` 和本地图片资源警告。
- 随后使用 `docs +media-insert` 将 4 张 PNG 上传并插入对应图注前：`system_architecture_ai_data_execution.png`、`gpu_embed_fine_vs_coalesced_e2e_20260712.png`、`gpu_embed_16k_stage_breakdown_20260712.png`、`gpu_embed_multi_endpoint_operator_wall_20260712.png`。
- 回读飞书文档确认目录完整，图 3-1、图 4-2、图 4-3、图 4-4 均已成为真实图片块，文档 revision 更新到 `41`；关键词检查未发现本地 `assets` 路径残留。
- 更新 `opening/feishu/README.md`，记录本地 PNG 不能仅依赖 Markdown 覆盖导入，后续同步需配合 `docs +media-insert`。

## 2026-07-13 链路阶段时延图修订

- 根据用户反馈，将 `gpu_embed_stage_overview_20260712.svg` / `.png` 从按阶段分组的横向柱状图，改为“场景为纵轴、阶段为柱内颜色”的横向堆叠柱状图。
- 新图以 4K / 16K、single endpoint / dual endpoint 四个场景为纵坐标，柱内堆叠 DB fetch、Arrow batch build、Ray submit / scheduling residual、GPU model request wall、fan-in 和 sink writeback，并在柱尾标注端到端总时延。
- 调整纵坐标标签左对齐和绘图区起点，避免图形过度居中；保留小阶段色块但不强制标注数值，突出 GPU 请求墙钟时间和 PostgreSQL JSON text writeback 两个主阶段。
- 同步更新 `opening/report/opening_report.md`、`opening/feishu/opening_report_wiki.md` 和 `opening/assets/charts/experiment_charts_audit.md` 中对图 4-5 的说明。
- 重新覆盖写入开题飞书 wiki 并逐张上传图片块；回读确认图 4-5 已替换为新版横向堆叠柱状图，正文、图注和图中编码一致，文档 revision 更新到 `71`，未发现本地 `assets` 路径残留。

## 2026-07-13 Python 实验图表生成

- 根据用户要求，将真实 GPU-backed 实验数据图重新用 Python 生成，脚本放在 `opening/assets/charts/scripts/generate_gpu_experiment_charts.py`，输出目录为 `opening/assets/charts/python/`。
- 新增五张候选正式图：调用粒度对比、single / dual endpoint 执行方式双面板对比、Ray actor 单 / 双 endpoint 扩展对比、链路阶段绝对时延、链路阶段占比。
- 图表只使用 `motivation/results/gpu/ai_embed_chain_breakdown_20260712.csv` 和 `motivation/results/gpu/ai_embed_multi_endpoint_20260712.csv` 的 formal repeats 平均，排除 warm-up，不混入 fake/CPU 或连接验证结果。
- 已目检 PNG 输出，确认中文、坐标、图例和标签可读；其中执行方式对比图将 single endpoint 和 dual endpoint 放在同一图中，避免单独强调 Ray 更快而忽略适用条件。
- 更新 `opening/assets/README.md`、`opening/assets/charts/scripts/README.md`、`opening/assets/charts/experiment_charts_audit.md` 和 `learning/README.md`。本批 Python 图暂未替换报告、飞书或 PPT 正式引用，后续替换时需同步图注、正文解释和讲稿备注。

## 2026-07-13 全量有意义实验对比图

- 根据用户要求，新增 `opening/assets/charts/scripts/generate_all_meaningful_experiment_charts.py`，把项目中对研究有解释价值的实验数据都生成候选图。
- 输出目录为 `opening/assets/charts/all_meaningful/`，共生成 14 张 PNG / SVG 图，覆盖 CPU/GPU endpoint、PG18.4 fake-model、fake/CPU 历史预研和 feasibility 组件 benchmark。
- 处理规则：带 `phase` 的实验只取 formal；无 `phase` 的历史实验排除 `repeat=0`；summary、smoke、dry-run 和连接验证数据不画正式性能图。
- 抽查 CPU/GPU、writeback、backpressure 和 Ray small task 图，确认中文、坐标、图例和关键数值可读；Ray small task 图已改为 y 轴从 0 开始，避免夸大差异。
- 更新 `opening/assets/README.md`、`opening/assets/charts/scripts/README.md` 和 `opening/assets/charts/experiment_charts_audit.md`，明确这些图是候选图集，报告正文仍应优先使用 GPU-backed 主证据。
- 根据用户反馈，修正 `all_cpu_vs_gpu_endpoint_e2e_20260712` 右上角图例与最高柱标签的拥挤问题：将图例移到绘图区上方，并增加 y 轴顶部留白；重新生成全量图集并目检该图。
## 2026-07-13 开题动机图表筛选

- 根据用户要求，从已有系统架构图、真实 GPU-backed 实验图和全量候选实验图中筛选开题主线图组。
- 新增 `opening/assets/charts/selected_motivation_figures.md`，记录主线图、备用图、不建议进入主线的图和推荐讲解顺序。
- 当前主线建议为：系统架构图、真实 GPU-backed 链路阶段耗时、调用粒度对比、执行方式与模型端点数量对比、actor endpoint scaling / 写回约束。
- 明确 fake/CPU、PG18.4 fake 和 feasibility 组件 benchmark 只作为附录、答辩备用或研究设计来源说明，不能替代真实 GPU-backed 主证据。
- 根据用户补充要求，在 `opening/report/opening_report.md`、`opening/feishu/opening_report_wiki.md` 和 `opening/assets/charts/selected_motivation_figures.md` 中补充“三类 workload 为什么选”和“为什么调 batch / partition / task / actor / routing / backpressure / writeback”的依据说明，明确依据来自外部系统资料和项目实验信号，而不是主观选择。
- 进一步将图表使用策略调整为 A/B/C 三层：A 层为报告和 PPT 正文主线图，B 层为支撑场景选择和变量选择的数据图，C 层为表格或文字即可的数据；明确 workload matrix、granularity attribution、backpressure、writeback batching 和 Ray / Arrow fan-in 属于值得关注的支撑性图。
- 为方便查找，新增集中目录 `opening/assets/charts/selected/`，其中 `report_main/` 存放报告和 PPT 正文建议图，`ppt_backup/` 存放 PPT 备份和飞书补充图；同步将开题报告本地正文和飞书源稿的图片引用切换到 `selected/report_main/` 下的短文件名版本。
- 根据用户要求，记录后续图表资产清理规则：最终只保留 `selected/`、生成脚本、审计记录、图表选择说明和系统架构图；`python/`、`all_meaningful/` 和旧 ECharts 根目录图在报告、PPT、飞书均完成路径切换后可以删除。
# 2026-07-14 PG18.4 pgai-integrated GPU rerun figures and report update

- Generated report-main figures from `motivation/results/gpu/ai_embed_pgai_integrated_key_20260714.csv` with `figures/scripts/generate_pgai_integrated_gpu_rerun_charts.py`.
- Added `06_gpu_pgai_rerun_granularity_20260714`, `07_gpu_pgai_rerun_stage_writeback_20260714`, and `08_gpu_pgai_rerun_endpoint_comparison_20260714` under `figures/data/report_main/`.
- Updated `opening/report/opening_report.md` to cite the PG18.4 local rehearsal + CUDA endpoint rerun, replacing older 2026-07-12 GPU figures in the current report body.
- Revised the endpoint-comparison figure wording so the dual-endpoint result is presented as absolute E2E time (`3.62s -> 2.86s`) and stage movement, not as a visually emphasized percentage claim.
- Synchronized figure index/audit files. Boundary remains: local PostgreSQL 18.4 rehearsal, JSON text writeback, two local endpoint replicas on one RTX 5070, not PostgreSQL 18.3 or multi-GPU.
# 2026-07-14 pgvector(384) writeback comparison

- Completed the same-chain GPU-backed Ray actor writeback comparison for no writeback, JSON text, and pgvector `vector(384)`.
- Output CSV and report:
  `motivation/results/gpu/ai_embed_pgvector_writeback_20260714.csv` and
  `motivation/results/gpu/pgvector_writeback_20260714.md`.
- Generated figure:
  `figures/data/report_main/09_gpu_pgvector_writeback_comparison_20260714.png`.
- Updated `opening/report/opening_report.md` with the new figure, table, and boundary note. The result remains PG18.4 local rehearsal, not PostgreSQL 18.3 internal-platform performance.
- Feishu/wiki and PPT were not synchronized in this pass.

# 2026-07-15 research plan figure for opening report

- Refined `figures/architecture/cross_layer_method_framework.png` / `.svg` into a research-plan figure for Section 4.1.
- Inserted the figure into `opening/report/opening_report.md` and local Feishu source `opening/feishu/opening_report_wiki.md` as Figure 4-1; renumbered subsequent Section 4 figures.
- Updated the figure asset index and added an audit record. Online Feishu/wiki and PPT were not synchronized in this pass.

# 2026-07-15 research-plan figure drawing rules

- Synchronized the research-plan figure drawing cautions into `figures/AGENTS.md` and `opening/ppt_rules.md`.
- The rules now require concrete workload cards, explicit upstream-tuning labels, full border/overflow checks, and no visible `RC/BL`, unexplained `vs`, or vague `边界确认` labels in formal figures.

# 2026-07-15 opening report mainline adjusted to upstream tuning plus end-to-end evaluation

- Adjusted the opening-report mainline from “independent best vs joint optimal” to “upstream execution-path tuning plus end-to-end evaluation”.
- Updated the local report and Feishu source so the main route is now staged profiling, upstream execution-path tuning, writeback-inclusive full-chain validation, and multi-workload validation.
- Kept independent-best vs end-to-end configuration as an optional enhanced contrast when later experiments show clear cross-stage coupling.

# 2026-07-15 opening report architecture figures aligned with three-layer strategy

- Regenerated the opening-report architecture figures:
  `figures/architecture/system_architecture_ai_data_execution.*`,
  `figures/architecture/cross_layer_method_framework.*`, and
  `figures/architecture/runtime_strategy_control_loop.*`.
- Updated `opening/report/opening_report.md` and `opening/feishu/opening_report_wiki.md`: Figure 4-1 now states the three-layer upstream execution strategy, and Figure 4-2 now uses the runtime control-loop figure instead of the previous Mermaid chain sketch.
- The updated figures clarify that database-side batch/partition are primarily plan-time choices, while runtime optimization focuses on `K_max`, `routing policy`, backpressure, and service-side `micro-batch`; writeback remains an end-to-end guardrail and bottleneck test.

# 2026-07-15 opening report figure color semantics corrected

- Revised Figure 4-1 so the three-layer upstream strategy is drawn as three separate neutral cards: plan-time data organization, runtime admission/routing, and service-side batching.
- Revised Figure 3-1 bottom research-content cards to use neutral borders, avoiding a misleading one-to-one color mapping with the system pipeline stages above.
- Adjusted research content 2 wording to `运行层调度与服务端批处理`, matching the current plan that combines Ray-side admission/routing with model-service-side `micro-batch`.

# 2026-07-15 opening report figure wording conservatism pass

- Revised the research-gap figure so the bottom positioning matches the current plan: data organization, runtime scheduling plus service-side batching, and writeback bottleneck determination.
- Revised the strategy rule table title to `信号触发的候选策略规则表`, making clear that table entries are candidate triggers for later experiments rather than proven rules.

# 2026-07-15 opening report text aligned with revised strategy

- Updated `opening/report/opening_report.md` and `opening/feishu/opening_report_wiki.md` so the prose matches the current figures.
- The report now describes the method as plan-time data organization, runtime admission/routing, and service-side batching, with writeback used for bottleneck determination and end-to-end benefit checks.
- Removed stale mainline wording around GPU-only scheduling, writeback as an independent contribution, and mandatory independent-best vs joint-optimal comparison.
## 2026-07-20 开题 PPT v5 数据组织机制图增量更新

- 从 `opening/slides/opening_defense_20260720_v4.pptx` 拷贝生成 `opening/slides/opening_defense_20260720_v5.pptx`。
- 按用户要求未重跑 `opening/slides/build_ppt.py`，仅使用 `python-pptx` 对 v5 做增量修改。
- 在原第 14 页“研究内容一：按计算量组织数据，而非按行数”后插入三页机制图：token-budget、length-align、prefix-aware。
- 将原第 14 页 prefix-aware 表述从“让 vLLM 复用 KV-cache”收紧为“为 vLLM prefix caching 创造命中条件，后续用 APC 命中率和端到端效果验证”，避免提前声称未验证收益。
- 结构检查：v5 共 25 页；新增页 15-17；PPTX 几何检查未发现越界；当前环境缺少 `markitdown`、`soffice/libreoffice` 和 `pdftoppm`，未做真实渲染预览。
- 根据 PPT 预览反馈，替换第 16 页 length-align 机制图为字体修正版；该修正版去掉标题中的中英粗体混排，改为纯中文机制标签。
- 对 v5 执行整体结构与版式一致性检查，保持章节页 01/02/03/04 切换形式不变；仅修正新增第 15-17 页：文字从硬编码 `微软雅黑` 改回主题字体，机制图按原始比例等比居中，页码编号卡替换为与第 14/18 页一致的方形样式。
- 按用户反馈继续增量修订 v5：调整封面标题、技术关键词和报告人信息的垂直层次；将目录与章节切换页统一为“目录页 + 当前章节高亮”形式，去除项目符号导致的目录数字字体问题；将研究内容一/二标题与正文改为更学术、机制导向的表述，分别对应“面向计算量的数据组织策略”和“面向服务状态的调度与提交控制策略”。
- 按用户反馈修正 v5 的目录导航与底部注释条：纯目录页不再显示章节高亮竖线，章节目录页删除额外绘制的横线并保留模板横线层次；所有底部注释条统一贴近左下角，竖线与注释文字按视觉中心对齐，减少与正文内容重叠的风险。
- 继续修正底部注释条的竖线/文字对齐：将所有底部注释的竖线与文字框设置为相同 top 和 height，文字框垂直居中，段前段后与行距归一化，避免不同颜色/不同字体注释在 PowerPoint 渲染时出现基线错位。
- 按用户反馈新增 v5 第 22 页“后续工作：多模态泛化验证”，插入在“可行性与创新点”和“进度安排”之间。页面使用 `figures/ppt_cropped/b26_arrow_vs_daft_stage_breakdown.png`，说明 Daft 接入后的 DB 读取与组织开销仍小于 0.1s，从而支撑后续将同一套 Daft/Ray pipeline 从 prompt 列扩展到 image/frame 列；该页明确多模态是后续验证目标，不声称已经完成。
## 2026-08-07 开题叙事与证据边界冻结

- 新增 `opening/claim_matrix.md`，作为报告、PPT、答辩问答和开题实验准入的当前内部依据。
- 题目保持“数据库 AI 负载的执行优化与调度研究”。统一抽象为 Database、AI Data Execution Layer、Model Service / GPU Executor、Database / Vector Sink。
- 两项研究内容固定为 workload 感知的 work-unit 构造，以及容量感知的提交、路由和多 job 调度。代价估计仍是共同使能组件，state-aware 性能增量仍是待验证项。
- 开题前只补 SQuAD 均匀控制组和 ShareGPT controlled-skew 异质组三臂统一 database-E2E，完成后转入四图、报告/PPT 和答辩一致性审计。
# 2026-08-08 第一性原理复审：动机—挑战—方法—实验—图/PPT

- 新增 `opening/first_principles_reassessment_20260808.md`：开题前完成合格强静态 baseline 与必要性证据，开题后再完成同上限 static/dynamic、变化负载、多 job 和图像统一 formal。
- 动机改为一一对应：异质 work → work descriptor；运行状态变化 → state observer；欠供给/过载 → bounded dynamic control。现有动态负结果保留为强静态与信号选择教训，不写成 proposed 已胜出。
- 旧四张图存在同结论双 panel 重复、散点语义不直观和正文信息过载，降级为待替换底稿；新图只保留单一问题与自然标注。
- PPT 主讲由 28 页收敛为 19 页，明确增加“数据组织生成调度可消费的 work descriptor”桥接页，完整诊断和六 estimator 结果进入附录。
- Wiki 同步由用户明确豁免；最终只覆盖飞书云文档。
## 2026-08-08 四条证据链与 Daft/Ray Data 多 Job 对照

- Work Unit、状态感知、动态调度和共同使能代价估计改为同等严格的动机证据链，每条均需由实验现象导出设计字段/信号/动作。
- 当前实验增加 ShareGPT Chat 原生单 job 矩阵，以及 Daft Native/Ray、Ray Data 原生两 job 错峰观察；项目另作 static-partition vs shared-work 同上限 A/B。
- DuckDB 保持为 SQuAD/cap=64 有界输出产品轨，不与语义不兼容的 ShareGPT 框架轨混排。
- 同步更新 `opening/claim_matrix.md`、`opening/opening_defense_outline_20260808.md`、`experiments/plans/state_aware_work_unit_evaluation_20260808.md`、`PROJECT_OUTLINE.md` 和根规则的当前顺序。
# 2026-08-09 两作业证据与四部件实现边界审计

- 把 5s guaranteed-overlap 的原生观察和项目 static/shared A/B 从“待运行”更新为已完成，
  并明确零 overlap 只能在已同时到达但被框架串行化时证明 HOL，不能证明运行中干扰。
- 在权威答辩大纲和问答库中加入 Work Unit、状态感知、动态调度、代价估计的
  “动机证据—当前实现—可声称—不能声称”映射。
- 当前仅整理数据合同和待画图清单；按用户要求未生成图、未修改 PPT、未覆盖云文档、
  未同步 Wiki。

# 2026-08-09 开题材料冻结 readiness 收口

- 在 `opening/claim_matrix.md` 建立发布材料 readiness 表，分离内容/证据冻结与图、PPT、
  云文档发布冻结，防止旧图、旧 PPTX 或 revision 289 飞书文档被误作当前终稿。
- 当前开题实验与数据不再扩展；图 A/C、F/H、PPT、云文档和最终一致性审计在用户恢复
  相应工作后继续，Wiki 保持明确豁免。

# 2026-08-09 权威内容与冻结数据一致性审计

- 直接从六组核心冻结 CSV/JSON 回算报告、总纲、答辩大纲、QA 和 Claim Matrix 的 headline，
  数字与 Claim 等级一致，历史 45.7%、15 s 零重叠、6.4× 和 ShareGPT 154.57% 只保留在
  反驳/历史边界中。
- 修复 cost 合并 LOO JSON 的 6 处非 UTF-8 `§6` 字节并记录新 SHA；同时把实验报告标题
  明确为 320-run 基础矩阵加 429-formal 合并评估，并将误写的 CE0–CE6 更正为实际的
  CE0–CE5。未改变任何实验数值或结论。

# 2026-08-09 总目标完成条件审计

- 将叙事、两组 P0、四组核心证据、报告/总纲、四级 Claim/QA、PPT、云发布与最终冻结
  逐项映射到权威证据。实验和内容层已闭合；A/C/F/H、PPT、云发布及最终逐页审计仍按
  用户要求暂停，因此整体材料不能标为冻结。
- 同步 cost UTF-8 规范化后的图 E 输入 SHA；只更新数据合同，不重画图、不改 PPT、
  不同步飞书或 Wiki。

## 2026-08-12 20 页中文开题 PPT v7

- 在独立 worktree 与 `codex/opening-report-ppt` 分支完成 v7，没有切换主工作区 `main`。
- 第 5–19 页完整使用开题专用图集 14/14 张主讲图；第 2、3 页保留背景文字页并给出建议补图
  提示词，第 4 页文献分层图为可选增强。没有缺失的开题实验数据图。
- 完成 20/20 页渲染、0 空 placeholder、20/20 notes、模板保真 0 issue 和逐页视觉复核；
  图文比较范围与 Claim Matrix 一致。
- 更新 `opening/slides/README.md`、v7 QA、图文一致性审计、`opening/README.md`、
  `opening/claim_matrix.md`、`PROJECT_INDEX.md` 与根日志。中文 Markdown 报告和飞书云文档
  后续继续，Wiki 不同步。

## 2026-08-12 中文开题报告按学校模板收口

- 将 `opening/report/opening_report.md` 重组为学校模板要求的七部分，先定义两项研究内容与
  共同代价估计，再在研究方案中说明数据结构、固定上限调度实现、实验设计和前期可行性证据。
- 14/14 张主讲图和 2/2 张单作业补充图统一改从 `figures/opening_figure_set/` 引用，移除报告
  对旧 `report_main` 与旧 architecture PNG 的直接依赖。
- 根据 `origin/main` 最新实现更新边界：固定总上限的阶段感知有序释放已接入具名 Ray 协调器、
  配置和 active-set trace，但尚无正式 GPU 对照；SLO 债务和阶段队列输入仍待接线。
- 新增 `opening/report/opening_report_20260812_qa.md`，完成学校模板、31 条引用、图表、实现边界
  和 20 页 PPT 映射审计。当前没有开题论证所缺的数据图；第 2、3 页背景结构图仍是可选增强。
- 按用户要求不同步 Wiki；飞书普通云文档已完成旧 revision 读取、16 张图 URL 验证和覆盖
  dry-run，外部写入门禁要求对指定文档 URL 再次明确确认，因此尚未执行最终覆盖。
# 2026-08-12 开题第 2–4 页补图与 v8

- 完成三张 1600×900 可编辑概念图：数据库 AI 外部执行链路、传统/外部 AI 执行假设对照、
  相关工作分层；Draw.io、SVG、PNG、独立 icon 和审计均齐全。
- 图面按项目真实链路和开题听众口径重写，移除内部研究边界表达，相关工作只提出待系统验证的
  跨层闭环问题；全尺寸与 PPT 缩放均无越界、残层、重复边框或断线箭头。
- 将三图以 P02–P04 纳入专用图集，并从 v7 原位替换 PPT 第 2–4 页生成 v8；20/20 页渲染、
  overflow、notes、模板保真和 contact sheet 复核全部通过。
- 修订 P03 的流程表达：加大右侧步骤卡间距，确保五条蓝色向下连接都有线身；三条状态反馈支线
  独立带箭头，补充线型/边框图例并统一卡内文字居中。
# 2026-08-12 31 页对外开题答辩 PPT v9（历史初版）

- 新建 `codex/opening-ppt-template-v9` 分支并生成 `slides/opening_defense_20260812_v9.pptx`。
- 保留学校页眉、配色和身份识别，内容区不逐框仿制模板；按数据库成为 AI 任务入口、执行假设
  变化、四层代表工作、跨层研究空白、动机证据与方法对应重构为 31 页。
- 数据页补齐实验现象、系统含义和设计对应；关系页解释两项研究内容、代价估计和独立/联合
  验证关系；未来工作保持保守，不把未完成的动态比较写成既有收益。
- 清理正文和交付图中的内部代称及“门禁、冻结、正式点、失效边界”等项目管理表达；对外图在
  构建时单独生成并栅格化，权威源图不变。
- 31/31 页渲染、0 空占位符、31/31 讲稿与来源、画布溢出及关键页目视检查通过；详见
  `slides/opening_defense_20260812_v9_qa.md`。

## 2026-08-12 v9 叙事收敛与模板式结尾复核

- 在 `codex/opening-ppt-template-v9` 分支将 v9 从 31 页收敛为 26 页：三张逐篇论文截图合并为
  一张四层研究现状图，实验有效性并入验证方案，研究计划与预期贡献合并为学校模板的双栏结尾。
- 保留图像 baseline 与图像四作业两页，分别承担“路径/能力边界”和“跨模态并发干扰”两项不同
  证明义务；未增加新图，也未回退到旧版低质量图。
- 项目数据结构和机制采用英文专业名首次附中文解释的口径：WorkDescriptor、Runtime State
  Snapshot、Admission Control、Endpoint Routing、Work Credit、Idle Borrowing、Cost Estimator。
- 修正文本 baseline 的长标签裁切、图下最后一行裁切、图像阶段数值和图像 baseline 叙述口径；
  四张目录页分别高亮当前章节。
- 26/26 页逐页渲染、0 空占位符、26/26 notes 来源、画布溢出与逐页目视检查通过；本机已用
  Microsoft PowerPoint 成功打开，正式答辩前仍需在最终投影机器确认字体与比例。

## 2026-08-13 Claim Matrix 同步 bounded-ready 归因门

- 只更新 Claim Matrix 中“固定总并发上限下的状态感知有序释放”一项，不改报告正文、PPT 或
  飞书：证据等级仍为未证明；补入 bounded-ready $0.125W_e$ 双轮 development 结果和
  $0.25W_e$ bulk guard 拒绝。
- 下一步从“继续实现有界 guard”改为“先让 FIFO/DRR/VTC/strict-priority 使用相同 ready-window
  做归因门，通过后才 formal”。若简单 selector 已在同一 Pareto 前沿，则收敛为 observation
  contract 或淘汰复杂 selector；这收紧而非提升开题 claim。

# 2026-08-16 开题前部图资产叙事清理

- 仅修改图资产，不重新生成 PPT；P02 按用户要求保持不变。
- P03 改为通用外部 AI 算子六阶段执行链路，删除提前出现的项目 Work Unit、credit、状态反馈、
  准入/路由和 typed Ray actor 设计；P04 只作相关工作分层；P05 只作研究空白归纳。
- P06--P08 仅收束标题/标签措辞，实验数值、坐标、统计量和几何均未改变。
- 权威 SVG/PNG、Draw.io 源、开题专用图集副本与审计记录同步更新，并通过原尺寸目视复核。

# 2026-08-17 P05 图面可读性修订

- 仅调整 P05 图资产，不修改 PPT：跨栏请求/提交箭头改为固定尺寸的小箭头头和完整 60 px 线身，
  灰色反馈箭头同步采用更小固定头与清晰虚线。
- 标题、分区标题、卡片标题、正文/标签和结论整体放大为 42/30/25/20/26 px；同步调整文本框和
  结论框尺寸，原文与研究空白叙事保持不变。
- Draw.io、SVG、1600×900 PNG 和专用图集 P05 副本已同步并完成全尺寸目视检查。
