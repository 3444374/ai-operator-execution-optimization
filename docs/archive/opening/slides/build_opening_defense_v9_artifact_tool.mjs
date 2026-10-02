#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import { execFileSync, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(process.env.OPENING_PROJECT_ROOT || path.resolve(SCRIPT_DIR, "../.."));
const TMP = path.resolve(process.env.OPENING_PPT_V9_TMP || path.join(SCRIPT_DIR, ".v9_work"));
const SOURCE = path.join(ROOT, "opening/templates/模板.pptx");
const FINAL = path.join(SCRIPT_DIR, "opening_defense_20260812_v9.pptx");
const SKILL_DIR = process.env.PRESENTATIONS_SKILL_DIR
  || "/Users/junshun/.codex/plugins/cache/openai-primary-runtime/presentations/26.805.11740/skills/presentations";
const BUNDLED_NODE_MODULES = "/Users/junshun/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules";
const { ensureArtifactToolWorkspace, importArtifactTool, saveBlobToFile } =
  await import(path.join(SKILL_DIR, "container_tools/artifact_tool_utils.mjs"));

const FIG = (name) => path.join(ROOT, "figures/opening_figure_set/main_png", name);

const slides = [
  {
    source: 1,
    kind: "cover",
    texts: [
      "华中科技大学",
      "数据库 AI 负载的执行优化与调度研究",
      "报告人：____________\n指导教师：____________",
    ],
    notes: ["opening/report/opening_report.md", "opening/claim_matrix.md"],
  },
  {
    source: 2,
    kind: "agenda",
    active: "01",
    texts: ["提纲", "● 01 研究背景与研究空白\n○ 02 动机实验与设计要求\n○ 03 研究方案、策略关系与验证\n○ 04 研究基础、计划与预期贡献"],
    notes: ["opening/opening_defense_outline_20260808.md"],
  },
  {
    source: 5,
    kind: "wideFigure",
    section: "01",
    title: "研究背景｜数据库成为 AI 任务入口",
    body: "Snowflake Cortex AISQL、BigQuery ML/AI、Oracle AI Vector Search 与 PostgreSQL pgai/pgvector 已支持生成、嵌入、过滤和分类。\n图示说明：数据库负责发起任务与管理结果，模型调用却经过请求组织、数据准备、GPU 服务和写回。\n本页结论：数据库成为 AI 任务入口后，查询边界已经延伸到模型服务之前。\n下一问题：传统算子的执行假设能否直接覆盖这条链路？",
    image: FIG("P02_背景_数据库AI算子外部执行链路.png"),
    notes: ["opening/report/opening_report.md#1-选题背景与意义"],
  },
  {
    source: 5,
    kind: "wideFigure",
    section: "01",
    title: "研究背景｜AI 算子改变传统数据执行假设",
    body: "传统算子通常以行数、字节和本地资源估计成本；AI 算子还受 token/frame、CPU prepare、传输、模型队列、KV 与输出长度影响。\n图示说明：数据库只看到记录与结果，模型服务只看到已到达请求，中间阶段没有统一状态所有者。\n本页结论：固定行数、固定 batch 和固定并发不足以描述端到端执行。\n下一问题：相邻研究分别解决了哪些层，仍缺什么接口？",
    image: FIG("P03_背景_传统算子与外部AI执行假设.png"),
    notes: ["opening/report/opening_report.md#2-国内外研究现状"],
  },
  {
    source: 5,
    kind: "wideFigure",
    section: "01",
    title: "研究现状｜相邻层各自优化，跨层信息仍然分散",
    body: "数据库侧：LOTUS、Galois、GaussML 与 Cortex AISQL 优化算子语义、计划和调用成本；数据执行侧：Ray Data、Daft 与 NeuStream 提供异构流水线；模型服务侧：Orca、vLLM、Sarathi-Serve 与 VTC 优化批处理、KV 和服务内公平。\n代价研究：GRACEFUL、COSTREAM 与 Abacus 开始服务配置和计划选择。\n本页结论：现有工作各自解决一层，但数据库作业、分阶段工作量和模型运行状态仍没有共同的执行闭环。\n下一问题：这段跨层能力应由谁承担？",
    image: FIG("P04_相关工作_跨层执行闭环.png"),
    notes: ["research/top15_ranked_papers.md", "opening/report/opening_report.md#2-国内外研究现状", "research/reading_notes/vllm_sosp2023.md", "research/reading_notes/graceful_udf_cost_icde2025.md", "research/reading_notes/costream_icde2024.md", "research/reading_notes/abacus_pvldb2026.md"],
  },
  {
    source: 5,
    kind: "wideFigure",
    section: "01",
    title: "研究空白｜AI Data Execution Layer 缺少上游闭环",
    body: "AI Data Execution Layer（AI 数据执行层）需要把记录转换为可比较的分阶段工作量，观测数据源、准备、模型、结果阶段与服务状态，并在固定容量内控制准入、路由和多作业额度。\n图示说明：数据库提供作业、服务目标和结果语义；模型服务提供运行/等待请求、KV 与完成节奏；中间层连接两侧。\n研究边界：不修改数据库内核、vLLM 调度器、模型结构或 GPU 内核。\n下一问题：现有执行路径究竟会落入哪些供给和排队状态？",
    image: FIG("P05_研究空白_AI数据执行层.png"),
    notes: ["opening/claim_matrix.md#1-冻结题目与系统抽象", "opening/report/opening_report.md#3-研究目标与关键问题"],
  },
  {
    source: 2,
    kind: "agenda",
    active: "02",
    texts: ["提纲 · 动机实验", "○ 01 研究背景与研究空白\n● 02 动机实验与设计要求\n○ 03 研究方案、策略关系与验证\n○ 04 研究基础、计划与预期贡献"],
    notes: ["opening/opening_defense_outline_20260808.md#32-逐页结构"],
  },
  {
    source: 5,
    kind: "wideFigure",
    section: "02",
    title: "动机证据｜不同执行图形成不同服务状态",
    body: "实验现象：均匀负载下的三种固定配置表现接近；同一受控对话负载中，Daft 两条官方路径出现大量等待与高 KV 占用，Ray Data 当前官方路径则供给不足。\n系统含义：作业完成时间接近或等待队列为零，都不能单独说明运行状态健康；执行图决定请求到达模型服务的节奏。\n设计对应：运行时需联合观察工作速率、运行/等待请求、KV 占用、尾延迟与任务进度。\n比较原则：先验证数据读取、结果写回和输出质量，再观察各官方执行路径的外部服务压力。",
    image: FIG("P06_文本基线_执行路径与可比边界.png"),
    notes: ["experiments/results/opening_database_e2e_text_refeed_20260808/README.md", "experiments/results/opening_text_native_single_job_formal_20260808/README.md"],
  },
  {
    source: 5,
    kind: "wideFigure",
    section: "02",
    title: "动机证据｜行数和静态上限不能描述工作状态",
    body: "实验现象：固定 16 行时，一批输入的 token 工作量最小 474、最大 6,793，相差 14.3 倍；相同的每端点 65,536 在途 token 上限，在高负载与到达受限场景下呈现不同 GPU 活跃度。\n系统含义：容量扫描存在最小近饱和区，继续增加在途工作量的吞吐收益很小，但请求尾延迟继续上升；容量上限不是在线状态。\n设计对应：需要分阶段工作描述、新鲜状态快照与固定容量边界内的控制动作。",
    image: FIG("P07_动机证据_工作量运行状态与容量边界.png"),
    notes: ["experiments/results/dual_gpu_active_work_saturation_20260729/README.md", "opening/claim_matrix.md"],
  },
  {
    source: 5,
    kind: "wideFigure",
    section: "02",
    title: "动机证据｜图像任务暴露多阶段失配",
    body: "实验现象：实用批次下，CPU 解码与归一化时间约为 GPU 模型执行时间的 13.9–31.0 倍；普通内存、锁页内存和 GPU 常驻输入的传输代价也不同。\n系统含义：活动窗口从 16 增到 32 只有小幅收益，继续增到 64 出现等待或性能回退；图片数量不能代表真实工作量。\n设计对应：跨模态公共接口需要表达数据源、准备、模型和结果四个阶段。",
    image: FIG("P08_图像阶段_准备传输与GPU执行失配.png"),
    notes: ["motivation/results/gpu/image_clip_preprocess_variants_20260801/README.md", "motivation/results/gpu/image_clip_transfer_ceiling_20260803/README.md"],
  },
  {
    source: 5,
    kind: "wideFigure",
    section: "02",
    title: "动机证据｜四作业并发同时干扰短作业与长作业",
    body: "实验现象：Daft 本地执行、Daft 分布式执行和 Ray Data 中，短作业与三个长作业的完成时间均退化，但幅度和服务压力形态不同。\n系统含义：模型服务只看到请求，无法直接管理数据库作业的到达、活跃、排空和公平；只保护短作业也不足以描述系统。\n设计对应：需要每作业已完成/剩余工作量、Work Credit（工作额度）、隔离约束与 Jain fairness（Jain 公平指数）。\n比较方式：每条原生路径只与自身单作业隔离运行比较，不做跨框架绝对排名。",
    image: FIG("P09_文本多作业_原生路径并发干扰.png"),
    notes: ["experiments/results/opening_fourjob_interference_20260809/README.md"],
  },
  {
    source: 10,
    kind: "twoProblem",
    section: "02",
    title: "设计要求｜动机现象导出两项研究内容和共同支撑",
    intro: "动机实验不是结果堆叠：每个现象都对应一个设计对象、一个控制边界和一组后续对照实验。",
    leftLabel: "研究内容一",
    leftTitle: "工作单元与数据组织",
    left1: "同行数工作量相差 14.3×，图像又呈多阶段失配。",
    left2: "因此先构造分阶段工作描述，再比较工作量预算、均衡与局部性；固定调度，只改变组织。",
    rightLabel: "研究内容二",
    rightTitle: "状态感知提交与多作业调度",
    right1: "同一上限对应不同状态，多作业出现效率、隔离与公平冲突。",
    right2: "因此保持 GPU 资源和最大在途工作量不变，比较准入、路由、空闲份额借用、有序释放与公平保护；代价估计同时服务两项内容。",
    notes: ["opening/opening_defense_outline_20260808.md#第-10-页实验现象导出四项同等重要的设计要求"],
  },
  {
    source: 2,
    kind: "agenda",
    active: "03",
    texts: ["提纲 · 研究方案", "○ 01 研究背景与研究空白\n○ 02 动机实验与设计要求\n● 03 研究方案、策略关系与验证\n○ 04 研究基础、计划与预期贡献"],
    notes: ["opening/report/opening_report.md#4-研究内容与技术路线"],
  },
  {
    source: 5,
    kind: "wideFigure",
    section: "03",
    title: "总体方案｜前向工作描述与反向状态组成闭环",
    body: "前向链路：数据库/数据帧 → WorkDescriptor（分阶段工作描述）与 Cost Estimator（算子代价估计器）→ Data Organizer（数据组织器）→ 调度 → GPU 执行 → 结果写回。\n反向链路：Runtime State Snapshot（运行状态快照）把完成进度、队列与服务状态反馈给上游。\n本页结论：两项研究内容可独立消融，但共享 WorkDescriptor、状态快照和代价信号。\n下一步：先定义组织与调度共同消费的数据结构。",
    image: FIG("P11_系统架构_数据组织与状态调度闭环.png"),
    notes: ["opening/report/opening_report.md#41-总体系统架构", "figures/opening_figure_set/README.md"],
  },
  {
    source: 5,
    kind: "wideFigure",
    section: "03",
    title: "研究内容一｜WorkDescriptor 连接数据组织与调度",
    body: "WorkDescriptor（分阶段工作描述）包含记录/作业标识、数据源/准备/模型/结果工作量、局部性、到达时间/期限、不确定区间与校准签名。\n图示说明：文本映射输入/输出 token、前缀局部性与结果字节；图像映射编码字节、解码/缩放、张量/模型工作量与向量结果字节。\n本页结论：WorkDescriptor 是研究内容一的输出，也是研究内容二的输入。\n设计原则：先采用可解释的最小字段集，只有决策收益证明需要时才增加复杂度。",
    image: FIG("P12_研究内容一_WorkUnit与数据组织.png"),
    notes: ["opening/report/opening_report.md#411-workdescriptor", "code/src/planning/work.py", "code/src/modalities/image/contracts.py"],
  },
  {
    source: 5,
    kind: "wideFigure",
    section: "03",
    title: "研究内容一｜数据组织权衡工作量均衡与局部性",
    body: "候选策略包括固定行数、按 token/图像帧预算保序成批、长度对齐、最佳适配装箱与行数上限；一次消融中互斥比较。\n图示说明：低压力下五种策略近似中性；高 KV 压力下，重排/装箱会破坏前缀组，缓存命中率与吞吐同步下降。\n策略关系：工作量预算约束单元大小，均衡控制填充，局部性约束重排范围。\n本页结论：先独立搜索最小有效组织策略，再与调度策略拼接。",
    image: FIG("P13_数据组织_服务压力与局部性权衡.png"),
    notes: ["experiments/results/rc1_data_organization/README.md"],
  },
  {
    source: 5,
    kind: "wideFigure",
    section: "03",
    title: "研究内容二｜固定容量内动态释放工作额度",
    body: "离线按机器、模型、协议和负载测量最小近饱和容量；在线通过 Runtime State Snapshot（运行状态快照）观测每作业待准备、在途、剩余、已完成工作量，以及运行/等待请求与 KV cache 占用。\nAdmission Control（准入控制）限制总量，Endpoint Routing（端点路由）选择去向，Work Credit（工作额度）与 Idle Borrowing（空闲额度借用）分配多作业份额。\n状态过期、配置不匹配或额度账本异常时，退回先到先服务或 Fair Queue（公平队列），并保持总容量不变。\n本页结论：动态控制对象是释放顺序与份额，不是持续猜测新的总并发。",
    image: FIG("P14_研究内容二_状态感知提交与多作业调度.png"),
    notes: ["opening/report/opening_report.md#42-研究内容二容量感知的提交路由与多作业调度", "opening/claim_matrix.md"],
  },
  {
    source: 5,
    kind: "wideFigure",
    section: "03",
    title: "研究内容二｜共享额度权衡效率、隔离与公平",
    body: "实验现象：单作业全容量与四分之一容量控制组先分离额度损失；同一总容量下，共享额度相对静态分区提高作业组吞吐与 MFU（模型浮点运算利用率），但不同作业收益不均，Jain fairness（Jain 公平指数）下降。\n系统含义：提高资源利用并不保证每个作业同时改善，吞吐、隔离和公平是并列目标。\n设计对应：空闲额度借用提高资源利用；每作业保底/上限、工作量欠账和服务目标保护约束隔离。",
    image: FIG("P15_共享调度_效率隔离与公平权衡.png"),
    notes: ["experiments/results/opening_fourjob_interference_20260809/README.md", "opening/claim_matrix.md"],
  },
  {
    source: 5,
    kind: "wideFigure",
    section: "03",
    title: "共同支撑｜代价估计面向配置排序与决策损失",
    body: "实验现象：429 个重复观测覆盖 20 组负载与服务配置，每组比较四个在途工作量候选；混合模型排序准确率 0.808，平均决策损失 2.90%，最坏损失 14.72%。\n系统含义：单点执行时间误差更低，不保证能选对组织、容量或路由配置；当前结果只说明初步可行。\n设计对应：Cost Estimator（算子代价估计器）采用解析工作量特征、少量剖析校准和残差修正，同时报告相对排序准确率与平均、中位、最坏决策损失。",
    image: FIG("P16_代价估计_配置选择与决策质量.png"),
    notes: ["experiments/results/operator_cost_profile_dual4090_formal_v2_cache_on_20260807/README.md"],
  },
  {
    source: 5,
    kind: "wideFigure",
    section: "03",
    title: "跨模态验证｜图像基准先确认路径与能力边界",
    body: "实验现象：12 万条同资源重复实验中，本课题固定配置在两个 CPU 配置下均比 Ray Data 缩短作业完成时间，具体幅度随资源配置变化；1.2 万条实验只用于检查执行结构。\n系统含义：直接调用、框架原生路径和本课题方法承担不同角色，不能混为一个总排行榜；Daft 内置路径在 2 万条时出现对象存储空间不足。\n设计对应：先分别建立直接调用、框架原生与本课题方法的可比较设置，再验证状态感知方法的增量；无法完成规定规模或输出语义不一致的路径不报告性能数值。",
    image: FIG("P17_图像基线_执行路径与可比边界.png"),
    notes: ["experiments/results/image_ai_embed_operator_formal_20260803/README.md", "feasibility/results/vllm_clip_pooling_gate_20260804/README.md"],
  },
  {
    source: 5,
    kind: "wideFigure",
    section: "03",
    title: "跨模态验证｜图像四作业重现任务级干扰",
    body: "实验现象：Daft 内置 AI 函数、Ray Data 与本课题执行路径均出现作业级干扰，但短作业和长作业的退化形态不同。\n系统含义：图像同样需要每作业分阶段工作量、活跃/剩余状态、共享额度与隔离约束；现有状态数据尚未用于在线控制。\n设计对应：WorkDescriptor、运行状态快照与额度接口跨模态复用，只有数据源、准备和模型阶段的字段映射不同。\n比较方式：每条路径只与自身单作业隔离运行比较，不做跨框架绝对排名。",
    image: FIG("P18_图像多作业_并发干扰.png"),
    notes: ["experiments/results/opening_image_native_fourjob_formal_20260810/README.md", "experiments/results/opening_image_project_fourjob_observe_only_formal_20260810/README.md"],
  },
  {
    source: 10,
    kind: "twoProblem",
    section: "03",
    title: "验证方案｜先独立归因，再判断是否需要联合优化",
    intro: "所有策略在相同 GPU、模型、数据和最大在途工作量下比较；先保证结果正确、模型供给充分且重复稳定。",
    leftLabel: "独立归因",
    leftTitle: "组织与调度分开搜索",
    left1: "固定调度，只改变固定行数、工作量预算、均衡和局部性。",
    left2: "固定组织，只改变先到先服务、静态分区、公平队列、状态感知准入、路由和共享份额。",
    rightLabel: "联合判断",
    rightTitle: "拼接方案与联合搜索对照",
    right1: "统一报告正确吞吐、作业完成时间与尾延迟、模型计算利用率与能耗、局部性、隔离、公平以及状态—动作轨迹。",
    right2: "联合显著优于拼接才保留耦合；若简单策略处于同一效率—公平前沿，则采用简单方案。",
    notes: ["AGENTS.md#1-项目目标", "opening/report/opening_report.md#44-策略关系与联合验证"],
  },
  {
    source: 2,
    kind: "agenda",
    active: "04",
    texts: ["提纲 · 基础与计划", "○ 01 研究背景与研究空白\n○ 02 动机实验与设计要求\n○ 03 研究方案、策略关系与验证\n● 04 研究基础、计划与预期贡献"],
    notes: ["opening/report/opening_report.md#6-工作基础与进度安排"],
  },
  {
    source: 5,
    kind: "wideFigure",
    section: "04",
    title: "研究基础｜现有链路支撑后续动态验证",
    body: "已完成文本 vLLM、Daft、Ray 执行链路，WorkDescriptor、Work Credit、完成即归还和可追踪运行记录；图像 CLIP 流水线与同资源原生基准也已闭环。\n图示说明：已有结果承担动机、机制与可行性证据，不提前替代后续相同容量下的动态对照。\n本页结论：工程链路可运行，关键缺口是状态感知有序释放、未参与校准场景的代价验证与跨模态动态增量。\n下一步：按统一实验设置逐层把机制验证为可归因结论。",
    image: FIG("P19_研究基础与后续工作计划.png"),
    notes: ["opening/report/opening_report.md#6-工作基础与进度安排", "opening/claim_matrix.md"],
  },
  {
    source: 28,
    kind: "closingTwoColumn",
    section: "04",
    title: "后续工作与预期成果",
    leftTitle: "后续工作",
    leftBody: "• 完成相同资源、相同容量下的状态感知有序释放对照\n• 补充新场景的代价验证与图像动态实验\n• 比较分层拼接与联合搜索，完成系统复现和论文\n• 若简单策略达到同一前沿，则保留简单方案",
    rightTitle: "预期成果",
    rightBody: "• 分阶段工作描述及工作量、均衡、局部性联合约束\n• 容量内的提交、路由与多作业共享方法\n• 面向配置排序和决策损失的轻量代价估计\n• 跨文本与图像的可复现实验系统和运行轨迹",
    notes: ["opening/report/opening_report.md#62-后续进度安排", "opening/report/opening_report.md#7-预期成果创新点与风险控制", "opening/claim_matrix.md"],
  },
  {
    source: 29,
    kind: "thanks",
    title: "谢谢，请各位老师批评指正",
    subtitle: "数据库 AI 负载的执行优化与调度研究",
    meta: "报告人：____________\n指导教师：____________",
    notes: ["opening/report/opening_report.md"],
  },
];

function parseNdjson(value) {
  return String(value || "").split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
}

function recordsFor(records, slideNumber, kinds) {
  return records.filter((record) => record.slide === slideNumber && kinds.has(record.kind));
}

function runChecked(command, args, label) {
  const result = spawnSync(command, args, { stdio: "inherit", env: process.env });
  if (result.status !== 0) throw new Error(`${label} failed with status ${result.status}`);
}

function mapFor(sourceRecords) {
  const used = new Set(slides.map((slide) => slide.source));
  return {
    outputSlides: slides.map((spec, index) => ({
      outputSlide: index + 1,
      sourceSlide: spec.source,
      narrativeRole: spec.kind,
      reuseMode: "duplicate-slide",
      editTargets: recordsFor(sourceRecords, spec.source, new Set(["textbox", "shape", "image"])).map((record) => {
        const deleteSlideNumber = record.placeholder === "slideNumber" || (record.placeholder === true && /编号/.test(record.name || ""));
        return {
          shapeId: record.id,
          action: deleteSlideNumber ? "delete" : record.kind === "textbox" ? "rewrite" : record.kind === "image" && spec.image ? "replace" : "keep",
          reason: deleteSlideNumber ? "remove inherited slide-number placeholder" : `output slide ${index + 1}: template-preserving edit`,
        };
      }),
    })),
    omittedSourceSlides: Array.from({ length: 29 }, (_, index) => index + 1)
      .filter((sourceSlide) => !used.has(sourceSlide))
      .map((sourceSlide) => ({ sourceSlide, reason: "not required by the revised narrative" })),
  };
}

function sourceTextRecords(records, slideNumber) {
  return recordsFor(records, slideNumber, new Set(["textbox"]));
}

function byPosition(records) {
  return [...records].sort((a, b) => (a.bbox?.[1] ?? 0) - (b.bbox?.[1] ?? 0) || (a.bbox?.[0] ?? 0) - (b.bbox?.[0] ?? 0));
}

function setText(target, value, style = {}) {
  target.text = value;
  target.text.style = { ...target.text.style, ...style };
}

function setFrame(target, left, top, width, height) {
  target.position = { left, top, width, height };
}

const audienceFigureReplacements = [
  ["Direct static", "Direct Call"],
  ["Project Static", "本课题静态方法"],
  ["Project static", "本课题静态方法"],
  ["Project shared", "本课题共享额度"],
  ["Project 暂无", "本课题方法暂缺"],
  ["Project 机制 A/B", "本课题方法对照"],
  ["Project 内同一 Job", "本课题中同一作业"],
  [">Project<", ">本课题方法<"],
  ["配置上限 W65K", "每端点 65K token 上限"],
  ["3 次 formal", "3 次正式重复"],
  ["3次formal", "3 次正式重复"],
  ["n=3 formal", "n=3 正式重复"],
  ["同 Chat manifest", "同一受控对话负载"],
  ["vendor scheduler ownership", "框架原生调度"],
  ["产品 / database-E2E 轨：仅 SQuAD 可排名", "数据库端到端设置：SQuAD 只核对完成性与质量，不作细微性能排名"],
  ["官方 Chat graph 轨：服务状态与供给差异", "官方对话执行图轨：比较服务状态与供给差异"],
  ["统一 PostgreSQL source/sink", "统一 PostgreSQL 数据源与写回"],
  ["Project 暂无同一 2,048-row graph→gather 正式点，不混入排名", "本课题方法暂无同一 2,048 行执行图→结果汇聚正式点，不混入比较"],
  ["2,048-row graph→gather", "2,048 行执行图→结果汇聚"],
  ["four-job JCT ÷ 本 Job isolated-single JCT", "四作业完成时间 ÷ 本作业单独运行完成时间"],
  ["Short@0s，3×Long@5s", "短作业于 0 秒到达，3 个长作业于 5 秒到达"],
  ["Short@0s，3×Long@0.5s", "短作业于 0 秒到达，3 个长作业于 0.5 秒到达"],
  ["静态/共享是同一总上限下互斥A/B臂", "静态分区/共享额度是同一总上限下的互斥对照"],
  ["同上限 frozen-static A/B", "同总上限冻结静态对照"],
  ["同上限 A/B 验证", "同总上限对照验证"],
  ["异常 → frozen-static", "异常 → 冻结静态回退"],
  ["R1 pinned FP16", "锁页内存 FP16"],
  ["R2 pageable FP32", "普通内存 FP32"],
  ["120K formal cell", "12 万条正式对照"],
  ["panel a", "左图"],
  ["panel b", "右图"],
  ["formal", "正式重复"],
  ["AI workload", "AI 任务"],
  ["work unit", "工作单元"],
  ["Work Unit", "工作单元"],
  ["WorkDescriptor", "工作描述"],
  ["RuntimeStateSnapshot + Trace", "运行状态快照与过程记录"],
  ["RuntimeState", "运行状态"],
  ["Packing", "组织成批"],
  ["安全容量标定", "可用容量测量"],
  ["Admission / Routing", "准入与路由"],
  ["Shared credit · Idle borrowing · Fairness / SLO", "共享份额 · 空闲份额借用 · 公平与服务目标"],
  ["credit · idle borrowing", "共享份额 · 空闲份额借用"],
  ["fair queue · SLO guard", "公平队列 · 服务目标保护"],
  ["同总上限冻结静态对照", "保持总资源与最大在途工作量不变的固定配置对照"],
  ["同总上限", "相同资源与最大在途工作量"],
  ["同上限", "相同资源与最大在途工作量"],
  ["冻结强静态上限", "保持固定配置不变"],
  ["强静态基线", "固定配置基准"],
  ["异常 → 冻结静态回退", "状态异常时回到固定配置"],
  ["异常 → static 回退", "状态异常时回到固定配置"],
  ["配置排序与决策风险门禁", "配置排序与决策风险两项检查"],
  ["规模门禁", "规定任务规模"],
  ["正式重复", "重复实验"],
  ["正式点", "可比较实验结果"],
  ["文本 baseline 需要分轨比较，不能把不同语义与计时边界混成总排行榜", "文本基准需要分别比较：任务语义与计时口径不同，不能合并成总排行榜"],
  ["数据库端到端轨：仅 SQuAD 可在轨内比较", "数据库端到端设置：SQuAD 只核对完成性与质量，不作细微性能排名"],
  ["两 panel 使用相同双卡模型服务，但 workload、source/sink 与输出语义合同不同；只在 panel 内比较。", "左右两组使用相同双卡模型服务，但任务、数据读写和输出语义不同，因此分别比较。"],
  ["图像 baseline 数据结果：短规模诊断与同资源比较分开", "图像基准结果：小规模结构检查与同资源性能比较分开"],
  ["仅两条路径通过规定任务规模", "仅两条路径完成规定任务规模并保持输出语义一致"],
  ["仅比较通过规定任务规模的路径", "仅比较完成规定任务规模且输出语义一致的路径"],
  ["可在轨内比较", "可在相同口径内比较"],
  ["跨模态复用：工作描述 · Organizer · safe capacity · credit · routing · multi-job · trace", "文本与图像复用：工作描述、数据组织、容量测量、共享份额、路由、多作业调度与过程记录"],
  ["Organizer", "数据组织"],
  ["safe capacity", "容量测量"],
  ["multi-job", "多作业调度"],
  ["credit", "共享份额"],
  ["routing", "路由"],
  ["trace", "过程记录"],
  ["active work", "在途工作量"],
  ["MFU", "MFU"],
  ["SLO", "服务目标"],
  ["Job", "作业"],
  ["baseline", "基准"],
  ["regime", "运行条件"],
  ["门禁", "有效性检查"],
  ["冻结", "保持不变"],
  ["闭环", "完整流程"],
  ["强静态", "固定配置"],
  ["（容量参照）", ""],
  ["Work-unit", "工作单元"],
  ["Work Control · State", "工作组织与状态反馈"],
  ["State-aware Admission", "状态感知准入"],
  ["State-aware Endpoint Routing", "状态感知服务路由"],
  ["Shared Credit", "共享份额"],
  ["Fair Queue", "公平队列"],
  ["Admission", "准入"],
  ["Routing", "路由"],
  ["Work Credit", "工作份额"],
  ["Text Executor", "文本模型服务"],
  ["Image Executor", "图像模型服务"],
  ["Request Refill", "完成后补充请求"],
  ["Completion & Release", "请求完成并释放份额"],
  ["Runtime Snapshot", "运行状态快照"],
  ["bounded active work", "限制最大在途工作量"],
  ["fresh / stale fallback", "状态新鲜度检查与回退"],
  ["ready / queued", "待提交 / 已排队"],
  ["rate / queue age", "处理速率 / 排队时间"],
  ["per-job floor / cap", "每作业保底 / 上限"],
  ["work-fair deficit", "按工作量记录欠账"],
  ["idle borrowing", "空闲份额借用"],
  ["Completion + queue / service / job state", "完成进度、队列、服务与作业状态"],
  ["共同使能", "共同支撑"],
  ["Credit", "共享份额"],
  ["State-aware", "状态感知"],
  ["work", "工作量"],
];

async function audienceFigure(filePath) {
  if (!filePath.includes(`${path.sep}figures${path.sep}opening_figure_set${path.sep}main_png${path.sep}`)) {
    return { filePath, contentType: "image/png" };
  }
  const svgPath = filePath
    .replace(`${path.sep}main_png${path.sep}`, `${path.sep}main_svg${path.sep}`)
    .replace(/\.png$/i, ".svg");
  const originalSvg = await fs.readFile(svgPath, "utf8");
  let svg = originalSvg;
  for (const [from, to] of audienceFigureReplacements) svg = svg.split(from).join(to);
  if (svg === originalSvg) return { filePath, contentType: "image/png" };
  const outDir = path.join(TMP, "audience-figures");
  await fs.mkdir(outDir, { recursive: true });
  const outPath = path.join(outDir, path.basename(svgPath));
  await fs.writeFile(outPath, svg, "utf8");
  const pngPath = outPath.replace(/\.svg$/i, ".png");
  const sharpModule = await import(path.join(BUNDLED_NODE_MODULES, "sharp/lib/index.js"));
  const sharp = sharpModule.default || sharpModule;
  await sharp(Buffer.from(svg), { density: 180 }).png().toFile(pngPath);
  return { filePath: pngPath, contentType: "image/png" };
}

async function replaceImage(deck, slideNumber, records, filePath, position) {
  const imageRecord = recordsFor(records, slideNumber, new Set(["image"]))[0];
  if (!imageRecord) throw new Error(`slide ${slideNumber}: missing inherited image`);
  const image = deck.resolve(imageRecord.id);
  const prepared = await audienceFigure(filePath);
  const bytes = await fs.readFile(prepared.filePath);
  const blob = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
  image.delete();
  deck.slides.getItem(slideNumber - 1).images.add({
    blob,
    contentType: prepared.contentType,
    alt: path.basename(filePath),
    fit: "contain",
    position,
    crop: { left: 0, top: 0, right: 0, bottom: 0 },
  });
}

const TALK_POINTS = {
  agenda: "用四个问题定位汇报：为什么需要研究、实验发现了什么、准备怎样解决、如何验证并完成。",
  wideFigure: "先读图中的核心事实，再说明该事实改变了哪一个系统设计判断，最后用一句话承接下一页。",
  twoProblem: "左侧说明可独立归因的部分，右侧说明需要对照或联合判断的部分，强调二者的接口与边界。",
  closingTwoColumn: "左侧给出开题后的工作顺序与停止条件，右侧对应预期方法和可复现产出，回应开场提出的跨层执行问题。",
  thanks: "用一句话收束：本课题研究模型服务之前的 AI 数据执行层，使数据库 AI 任务可描述、可观测、可调度。",
  cover: "介绍课题题目和研究边界：数据库触发 AI 算子后、数据进入模型服务前的外部执行与调度。",
};

function notesText(index, spec) {
  const title = spec.title || spec.texts?.[1] || "开题汇报";
  return [
    `汇报讲稿：${TALK_POINTS[spec.kind] || TALK_POINTS.wideFigure} 本页主题为“${title}”。`,
    "",
    "答辩备注：实验数字只在相同任务语义、资源和计时口径内解读；尚未完成同条件对照的机制属于研究方案，不作为既有收益。",
    "",
    "[Sources]",
    ...(spec.notes || ["opening/report/opening_report.md"]).map((source) => `- ${source}`),
  ].join("\n");
}

function applyCover(deck, slideNumber, records, spec) {
  const texts = sourceTextRecords(records, slideNumber);
  const org = texts.find((record) => (record.bbox?.[0] ?? 0) > 900);
  const title = texts.find((record) => (record.bbox?.[2] ?? 0) > 800 && (record.bbox?.[1] ?? 0) > 330);
  const meta = texts.find((record) => record.id !== org?.id && record.id !== title?.id);
  setText(deck.resolve(org.id), spec.texts[0]);
  setText(deck.resolve(title.id), spec.texts[1]);
  setText(deck.resolve(meta.id), spec.texts[2]);
}

function applyAgenda(deck, slideNumber, records, spec) {
  const texts = byPosition(sourceTextRecords(records, slideNumber));
  setText(deck.resolve(texts[0].id), spec.texts[0]);
  const lines = spec.texts[1].split("\n").map((line) => line.replace(/^[●○]\s*/, ""));
  const active = lines.findIndex((line) => line.startsWith(spec.active || "01"));
  const slide = deck.slides.getItem(slideNumber - 1);
  deck.resolve(texts[1].id).delete();
  for (let index = 0; index < lines.length; index += 1) {
    const isActive = index === active;
    const bullet = slide.shapes.add({
      geometry: "textbox",
      position: { left: 102, top: 252 + index * 50, width: 42, height: 34 },
      fill: "none",
      line: { style: "solid", fill: "none", width: 0 },
    });
    setText(bullet, "●", { fontSize: 23, bold: isActive, color: isActive ? "#000000" : "#8F8F8F" });
    const item = slide.shapes.add({
      geometry: "textbox",
      position: { left: 156, top: 246 + index * 50, width: 780, height: 40 },
      fill: "none",
      line: { style: "solid", fill: "none", width: 0 },
    });
    setText(item, lines[index], { fontSize: 23, bold: isActive, color: isActive ? "#000000" : "#8F8F8F" });
  }
}

function applyWideFigure(deck, slideNumber, records, spec) {
  const texts = sourceTextRecords(records, slideNumber);
  const section = texts.find((record) => (record.bbox?.[0] ?? 0) < 100 && (record.bbox?.[1] ?? 0) < 180);
  const title = texts.find((record) => (record.bbox?.[0] ?? 0) > 100 && (record.bbox?.[1] ?? 0) < 180);
  const body = texts.find((record) => (record.bbox?.[1] ?? 0) > 500);
  setText(deck.resolve(section.id), spec.section);
  setText(deck.resolve(title.id), spec.title);
  const bodyShape = deck.resolve(body.id);
  setText(bodyShape, spec.body, { fontSize: 13, color: "#2E2E2E" });
  setFrame(bodyShape, 64, 555, 1152, 145);
}

function applyTwoProblem(deck, slideNumber, records, spec) {
  const texts = sourceTextRecords(records, slideNumber);
  const findAt = (x0, x1, y0, y1) => texts.find((r) => {
    const [x, y] = r.bbox || [0, 0];
    return x >= x0 && x < x1 && y >= y0 && y < y1;
  });
  const section = findAt(0, 120, 80, 180);
  const title = findAt(120, 1280, 80, 180);
  const intro = findAt(0, 1280, 180, 280);
  const leftLabel = findAt(80, 240, 280, 370);
  const leftTitle = findAt(240, 640, 280, 370);
  const left1 = findAt(0, 650, 370, 480);
  const left2 = findAt(0, 650, 480, 720);
  const rightLabel = findAt(700, 860, 280, 370);
  const rightTitle = findAt(860, 1280, 280, 370);
  const right1 = findAt(650, 1280, 370, 500);
  const right2 = findAt(650, 1280, 500, 720);
  if (!section || !title || !intro || !leftLabel || !leftTitle || !left1 || !left2 || !rightLabel || !rightTitle || !right1 || !right2) {
    throw new Error(`slide ${slideNumber}: could not resolve two-column frame`);
  }
  setText(deck.resolve(section.id), spec.section);
  setText(deck.resolve(title.id), spec.title);
  const slide = deck.slides.getItem(slideNumber - 1);
  for (const record of [intro, leftLabel, leftTitle, left1, left2, rightLabel, rightTitle, right1, right2]) deck.resolve(record.id).delete();
  const addTextBox = (name, value, position, style) => {
    const shape = slide.shapes.add({
      geometry: "textbox",
      name,
      position,
      fill: "none",
      line: { style: "solid", fill: "none", width: 0 },
    });
    setText(shape, value, style);
    return shape;
  };
  addTextBox("intro", spec.intro, { left: 72, top: 198, width: 1138, height: 58 }, { fontSize: 21, color: "#111111" });
  addTextBox("left-label", spec.leftLabel, { left: 102, top: 314, width: 125, height: 45 }, { fontSize: 21, bold: true, color: "#FFFFFF", alignment: "center" });
  addTextBox("left-title", spec.leftTitle, { left: 265, top: 308, width: 320, height: 70 }, { fontSize: 24, color: "#111111" });
  addTextBox("right-label", spec.rightLabel, { left: 747, top: 314, width: 125, height: 45 }, { fontSize: 21, bold: true, color: "#FFFFFF", alignment: "center" });
  addTextBox("right-title", spec.rightTitle, { left: 910, top: 308, width: 320, height: 70 }, { fontSize: 24, color: "#111111" });
  addTextBox("left-point-1", spec.left1, { left: 72, top: 400, width: 540, height: 92 }, { fontSize: 19, color: "#222222" });
  addTextBox("left-point-2", spec.left2, { left: 72, top: 525, width: 540, height: 120 }, { fontSize: 19, color: "#222222" });
  addTextBox("right-point-1", spec.right1, { left: 717, top: 400, width: 540, height: 92 }, { fontSize: 19, color: "#222222" });
  addTextBox("right-point-2", spec.right2, { left: 717, top: 525, width: 540, height: 120 }, { fontSize: 19, color: "#222222" });
}

function applyClosingTwoColumn(deck, slideNumber, records, spec) {
  const texts = sourceTextRecords(records, slideNumber);
  const byName = (name) => texts.find((record) => record.name === name);
  const section = byName("Rectangle 1");
  const title = byName("TextBox 2");
  const leftTitle = byName("矩形 31");
  const leftBody = byName("矩形 23");
  const rightTitle = byName("矩形 20");
  const rightBody = byName("矩形 8");
  if (!section || !title || !leftTitle || !leftBody || !rightTitle || !rightBody) {
    throw new Error(`slide ${slideNumber}: could not resolve closing two-column frame`);
  }
  setText(deck.resolve(section.id), spec.section);
  setText(deck.resolve(title.id), spec.title);
  setText(deck.resolve(leftTitle.id), spec.leftTitle);
  setText(deck.resolve(leftBody.id), spec.leftBody, { fontSize: 17.5, color: "#222222" });
  setText(deck.resolve(rightTitle.id), spec.rightTitle);
  setText(deck.resolve(rightBody.id), spec.rightBody, { fontSize: 17.5, color: "#222222" });
}

function applyThanks(deck, slideNumber, records, spec) {
  const texts = sourceTextRecords(records, slideNumber);
  const title = texts.find((record) => record.placeholder === "title");
  const subtitle = texts.find((record) => record.text === "存算分离架构下的高效向量存储引擎研究");
  const meta = texts.find((record) => record.name === "TextBox 2");
  if (!title || !subtitle || !meta) throw new Error(`slide ${slideNumber}: could not resolve thanks frame`);
  setText(deck.resolve(title.id), spec.title);
  setFrame(deck.resolve(title.id), 220, 246.6, 840, 90.67);
  setText(deck.resolve(subtitle.id), spec.subtitle);
  setText(deck.resolve(meta.id), spec.meta);
}

function deletePlaceholders(deck, slideNumber, records) {
  for (const record of recordsFor(records, slideNumber, new Set(["shape", "textbox"])).filter((record) => record.placeholder === "slideNumber" || (record.placeholder === true && /编号/.test(record.name || "")))) {
    deck.resolve(record.id).delete();
  }
}

function emptyPlaceholders(pptxPath) {
  const names = execFileSync("unzip", ["-Z1", pptxPath], { encoding: "utf8" })
    .split(/\r?\n/).filter((name) => /^ppt\/slides\/slide\d+\.xml$/.test(name));
  const failures = [];
  for (const name of names) {
    const xml = execFileSync("unzip", ["-p", pptxPath, name], { encoding: "utf8" });
    for (const match of xml.matchAll(/<p:sp\b[\s\S]*?<\/p:sp>/g)) {
      const shape = match[0];
      if (!shape.includes("<p:ph")) continue;
      const text = [...shape.matchAll(/<a:t>([\s\S]*?)<\/a:t>/g)].map((item) => item[1].trim()).join("");
      if (!text) failures.push(name);
    }
  }
  return [...new Set(failures)];
}

async function main() {
  await fs.mkdir(TMP, { recursive: true });
  await ensureArtifactToolWorkspace(TMP);
  const { FileBlob, PresentationFile } = await importArtifactTool(TMP);

  const sourceDeck = await PresentationFile.importPptx(await FileBlob.load(SOURCE));
  const sourceInspect = await sourceDeck.inspect({ kind: "slide,textbox,shape,image", include: "id,slide,name,title,text,textPreview,bbox,bboxUnit,isPlaceholder", maxChars: 1000000 });
  const sourceRecords = parseNdjson(sourceInspect.ndjson);
  const map = mapFor(sourceRecords);
  const mapPath = path.join(TMP, "template-frame-map.json");
  await fs.writeFile(mapPath, `${JSON.stringify(map, null, 2)}\n`, "utf8");
  await fs.writeFile(path.join(TMP, "template-audit.txt"), [
    "Source template: opening/templates/模板.pptx, 29 slides.",
    "Narrative learned from the template: repeated agenda定位; background narrows through multiple pages; every evidence page includes explanation and a takeaway; methods are decomposed before evaluation.",
    "Reused source frames: cover 1, agenda 2, wide figure 5, two-problem comparison 10, closing summary 28, thanks 29.",
    "Typography, school header/footer, logo, color and safe area remain inherited from source slides.",
  ].join("\n") + "\n", "utf8");
  await fs.writeFile(path.join(TMP, "deviation-log.txt"), [
    "The output narrative uses 26 slides after removing redundant literature illustrations and merging repeated validation/plan pages.",
    "Inherited figure and text frames are repositioned inside the source safe area to add figure captions and takeaways.",
    "The related-work landscape is consolidated into one layered evidence page; individual paper illustrations remain in notes and reading materials.",
    "All page-number placeholders are deleted because the source placeholders are empty and not visible in the template render.",
  ].join("\n") + "\n", "utf8");
  await fs.writeFile(path.join(TMP, "source-notes.txt"), "The local opening report, claim matrix, formal experiment reports and Top-15 reading notes are the content sources.\n", "utf8");

  const inspectScript = path.join(SKILL_DIR, "template_following_scripts/inspect_template_deck.mjs");
  const prepareScript = path.join(SKILL_DIR, "template_following_scripts/prepare_template_starter_deck.mjs");
  const inspectDir = path.join(TMP, "template-inspect-v9");
  runChecked(process.execPath, [inspectScript, "--workspace", TMP, "--pptx", SOURCE, "--out-dir", inspectDir, "--scale", "0.7"], "inspect template");
  const starter = path.join(TMP, "template-starter.pptx");
  runChecked(process.execPath, [prepareScript, "--workspace", TMP, "--pptx", SOURCE, "--map", mapPath, "--out", starter, "--inspect", path.join(inspectDir, "template-inspect.ndjson"), "--preview-dir", path.join(TMP, "starter-preview-v9"), "--layout-dir", path.join(TMP, "starter-layout-v9"), "--contact-sheet", path.join(TMP, "starter-contact-sheet-v9.png"), "--scale", "0.6"], "prepare starter");

  const deck = await PresentationFile.importPptx(await FileBlob.load(starter));
  const deckInspect = await deck.inspect({ kind: "slide,textbox,shape,image,notes", include: "id,slide,name,title,text,textPreview,bbox,bboxUnit,isPlaceholder", maxChars: 1000000 });
  const deckRecords = parseNdjson(deckInspect.ndjson);
  if (deck.slides.items.length !== slides.length) throw new Error(`expected ${slides.length} slides, got ${deck.slides.items.length}`);

  for (let index = 0; index < slides.length; index += 1) {
    const slideNumber = index + 1;
    const spec = slides[index];
    deletePlaceholders(deck, slideNumber, deckRecords);
    if (spec.kind === "cover") applyCover(deck, slideNumber, deckRecords, spec);
    else if (spec.kind === "agenda") applyAgenda(deck, slideNumber, deckRecords, spec);
    else if (spec.kind === "wideFigure") applyWideFigure(deck, slideNumber, deckRecords, spec);
    else if (spec.kind === "twoProblem") applyTwoProblem(deck, slideNumber, deckRecords, spec);
    else if (spec.kind === "closingTwoColumn") applyClosingTwoColumn(deck, slideNumber, deckRecords, spec);
    else if (spec.kind === "thanks") applyThanks(deck, slideNumber, deckRecords, spec);
    else throw new Error(`unknown kind ${spec.kind}`);
    if (spec.image) {
      const position = { left: 64, top: 190, width: 1152, height: 370 };
      await replaceImage(deck, slideNumber, deckRecords, spec.image, position);
    }
    const slide = deck.slides.getItem(index);
    slide.speakerNotes.textFrame.setText(notesText(index, spec));
    slide.speakerNotes.setVisible(true);
  }

  const output = await PresentationFile.exportPptx(deck);
  await output.save(FINAL);

  const renderDir = path.join(TMP, "final-render-v9");
  const layoutDir = path.join(TMP, "final-layout-v9");
  await fs.rm(renderDir, { recursive: true, force: true });
  await fs.rm(layoutDir, { recursive: true, force: true });
  await fs.mkdir(renderDir, { recursive: true });
  await fs.mkdir(layoutDir, { recursive: true });
  for (let index = 0; index < deck.slides.items.length; index += 1) {
    const slide = deck.slides.getItem(index);
    const stem = `slide-${String(index + 1).padStart(2, "0")}`;
    await saveBlobToFile(await deck.export({ slide, format: "png", scale: 1.35 }), path.join(renderDir, `${stem}.png`));
    await saveBlobToFile(await slide.export({ format: "layout" }), path.join(layoutDir, `${stem}.layout.json`));
  }
  await saveBlobToFile(await deck.export({ format: "png", montage: true, scale: 0.55 }), path.join(TMP, "final-montage-v9.png"));
  const finalInspect = await deck.inspect({ kind: "slide,textbox,image,notes", maxChars: 1000000 });
  await fs.writeFile(path.join(TMP, "final-inspect-v9.ndjson"), finalInspect.ndjson || "", "utf8");
  const finalRecords = parseNdjson(finalInspect.ndjson);
  const noteFailures = slides.map((_, index) => index + 1).filter((slideNumber) => {
    const notes = String(finalRecords.find((record) => record.kind === "notes" && record.slide === slideNumber)?.text || "");
    return !notes.includes("汇报讲稿：") || !notes.includes("答辩备注：") || !notes.includes("[Sources]");
  });
  const qa = {
    final: FINAL,
    slideCount: deck.slides.items.length,
    emptyPlaceholders: emptyPlaceholders(FINAL),
    noteFailures,
    renderDir,
    layoutDir,
    montage: path.join(TMP, "final-montage-v9.png"),
  };
  await fs.writeFile(path.join(TMP, "final-qa-v9.json"), `${JSON.stringify(qa, null, 2)}\n`, "utf8");
  if (qa.emptyPlaceholders.length || qa.noteFailures.length) throw new Error(`final QA failed: ${JSON.stringify(qa)}`);
  console.log(JSON.stringify(qa, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message || String(error));
  process.exit(1);
});
