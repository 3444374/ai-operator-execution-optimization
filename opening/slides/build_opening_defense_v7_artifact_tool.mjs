#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import { spawnSync, execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(SCRIPT_DIR, "../..");
const TMP = path.resolve(process.env.OPENING_PPT_TMP || "/private/tmp/opening-report-ppt-v7");
const SOURCE = path.join(SCRIPT_DIR, "opening_defense_20260807_v6.pptx");
const FINAL = path.join(SCRIPT_DIR, "opening_defense_20260812_v7.pptx");
const SKILL_DIR = process.env.PRESENTATIONS_SKILL_DIR ||
  "/Users/junshun/.codex/plugins/cache/openai-primary-runtime/presentations/26.805.11740/skills/presentations";
const { ensureArtifactToolWorkspace, importArtifactTool, saveBlobToFile } =
  await import(path.join(SKILL_DIR, "container_tools/artifact_tool_utils.mjs"));

// One inherited slide frame per output slide. The historical 28-page deck is used only as a
// school-template library; the narrative is the frozen 20-page opening outline.
const sourceByOutput = [1, 3, 3, 3, 14, 8, 8, 8, 8, 3, 14, 16, 8, 19, 8, 8, 8, 8, 8, 27];

const imageBySlide = {
  5: "figures/opening_figure_set/main_png/P05_研究空白_AI数据执行层.png",
  6: "figures/opening_figure_set/main_png/P06_文本基线_执行路径与可比边界.png",
  7: "figures/opening_figure_set/main_png/P07_动机证据_工作量运行状态与容量边界.png",
  8: "figures/opening_figure_set/main_png/P08_图像阶段_准备传输与GPU执行失配.png",
  9: "figures/opening_figure_set/main_png/P09_文本多作业_原生路径并发干扰.png",
  11: "figures/opening_figure_set/main_png/P11_系统架构_数据组织与状态调度闭环.png",
  12: "figures/opening_figure_set/main_png/P12_研究内容一_WorkUnit与数据组织.png",
  13: "figures/opening_figure_set/main_png/P13_数据组织_服务压力与局部性权衡.png",
  14: "figures/opening_figure_set/main_png/P14_研究内容二_状态感知提交与多作业调度.png",
  15: "figures/opening_figure_set/main_png/P15_共享调度_效率隔离与公平权衡.png",
  16: "figures/opening_figure_set/main_png/P16_代价估计_配置选择与决策质量.png",
  17: "figures/opening_figure_set/main_png/P17_图像基线_执行路径与可比边界.png",
  18: "figures/opening_figure_set/main_png/P18_图像多作业_并发干扰.png",
  19: "figures/opening_figure_set/main_png/P19_研究基础与后续工作计划.png",
};

const content = {
  1: ["数据库 AI 负载的执行优化与调度研究", "面向 AI Data Execution Layer 的 work-unit 构造与状态感知调度", "报告人：____________    指导老师：____________"],
  2: ["01", "AI 算子已经进入数据库工作流", "Snowflake Cortex AISQL、BigQuery ML/AI、Oracle AI Vector Search 与 pgai/pgvector 已把文本生成、向量计算与多模态分析接入数据工作流。", "数据库正在成为 AI workload 的数据入口和结果管理载体。"],
  3: ["01", "AI 算子形成新的外部执行链路", "数据库记录要经过请求组织、队列、CPU prepare、tensor transfer、模型执行与结果回收；这些阶段共同决定端到端性能。", "固定行数、固定 batch size 与固定并发都不足以单独描述 AI 执行。"],
  4: ["01", "代表工作推进了相邻层", "数据库 AI：LOTUS / Galois / GaussML / Cortex AISQL；推理服务：Orca / vLLM / Sarathi-Serve / VTC\n数据执行：Ray / Ray Data / Daft / NeuStream；代价决策：Learned Cost Models / GRACEFUL / COSTREAM / Abacus", "相邻层已有强工作，但数据库 Job、上游数据阶段和模型服务状态仍未形成统一闭环。"],
  5: ["01", "AI Data Infra 仍缺少上游执行闭环", "数据库知道 Job、SLO 与结果语义；模型服务知道 running、waiting、KV 与完成节奏。中间层需要统一 work、state、control 与 cost 接口。", "本课题研究 Database 与 Model Service 之间的 AI Data Execution Layer。"],
  6: ["01", "同一任务会落入不同服务状态", "左：统一 source/sink 的产品 database-E2E；右：同一 ShareGPT manifest 的官方 Chat graph。两条轨分别回答正确性边界与服务供给状态。", "现有路径的关键差异不仅是 JCT，还包括欠供给、有效供给与过量排队。"],
  7: ["01", "记录数和静态上限都不是运行状态", "同行数的 token work 可相差 14.3×；相同 W65K 在不同 offered load 下呈现不同 running/MFU；容量扫描存在最小近饱和区。", "需要可比较的 work、新鲜状态快照和保持总上限不变的份额控制。"],
  8: ["01", "图像把 work 扩展为多阶段", "CPU prepare、host-to-device transfer 与 model forward 的代价形态明显不同；继续增大 active window 还会带来等待与回退。", "跨模态接口应表达 source/prepare/model/result stage，而非单一图片数。"],
  9: ["01", "四 Job 并发同时影响 Short 与 Long", "每条原生路径内部比较 four-job JCT / isolated JCT。Short 与三个 Long Job 均受影响，但退化幅度和压力形态不同。", "作业到达、活跃、drain 与公平需要模型服务上游的任务级管理。"],
  10: ["02", "实验现象导出四项设计要求", "Work 表征 → staged WorkDescriptor；运行状态 → fresh stage/service/job snapshot\n多 Job 干扰 → 额度借用、完成回收与公平保护；配置风险 → profile + residual，以 ranking/regret 验收", "Work Unit、状态感知、动态调度与代价估计具有同等证据要求。"],
  11: ["02", "AI 数据执行层形成反馈闭环", "Organizer 决定一个 work unit 包含什么；Scheduler 决定何时、向哪里、以多少 active work 提交；代价估计同时服务两者。", "研究发生在数据库与模型服务之间，不修改模型内部 batching。"],
  12: ["02", "WorkDescriptor 连接组织与调度", "文本表达 prompt/output token 与 prefix locality；图像表达 encoded bytes、prepare、tensor 与 model work。共同字段包括 deadline、uncertainty 与 calibration signature。", "字段不是清单：每个字段都要对应组织、路由、准入或公平决策。"],
  13: ["02", "数据组织没有全局最优", "低压力下五种策略近似中性；高压力下重排策略的 cache hit 与吞吐同步下降。颜色对应组织策略，实线保持输入顺序，虚线改变排序或装箱。", "balance 与 locality 可联合约束，但实验臂的排序/装箱策略互斥比较。"],
  14: ["02", "总容量固定，份额随活跃集调整", "先离线标定当前配置的稳定总上限；运行时仅在活跃 Job 间借用空闲份额、完成即回收，并以 freshness、SLO debt 与公平 deficit 决定释放顺序。", "状态缺失或过期时回退全局 FIFO/DRR；简单策略足够好则不引入复杂控制。"],
  15: ["02", "共享调度存在效率—公平权衡", "Project 的 full/quarter 控制先分离额度损失，再比较同上限 static 与 shared。shared 提高组吞吐与 MFU，但各 Job 收益和 Jain fairness 并不总是同向。", "动态调度必须同时报告效率、隔离与公平，不能只看总吞吐。"],
  16: ["02", "代价估计要评价决策质量", "20 个 context 的完整点云展示每个估计器的选择损失；另报告配置 pairwise accuracy、平均与最坏 regret。", "逐行 MAE 更低不保证配置选择更好；代价估计以 ranking/regret 验收。"],
  17: ["03", "图像 baseline 分层解释能力边界", "Direct CLIP 是容量参照；Daft Built-in 与 Ray Data 是原生 baseline；Project 是冻结静态方法参考；vLLM pooling 当前 capability gate 未通过。", "12K 结构诊断与 120K matched-resource 正式比较必须分开解释。"],
  18: ["03", "图像四 Job 重现任务级干扰", "Daft Built-in、Ray Data 与 Project 分别按自身 isolated JCT 归一化。Project static/shared 只展示份额改变，不把 observe-only snapshot 写成动态收益。", "统一 work/state 接口具有跨模态价值，但阶段字段必须随模态变化。"],
  19: ["03", "研究基础支撑后续逐层验证", "已有基础覆盖执行闭环、WorkDescriptor、共享 credit 和正式 trace；后续按数据组织、状态与公平调度、代价估计和外部有效性逐层验证。", "每一步固定比较合同并保存状态—动作—结果 trace。"],
  20: ["预期贡献：可估计、可感知、可调度的 AI 数据执行方法", "■ 研究内容一：面向 AI workload 的分阶段 work-unit 构造与组织\n■ 研究内容二：固定总容量下的状态感知提交、路由与多作业协调\n■ 共同使能：以配置排序和决策损失验收的轻量代价估计\n■ 跨模态验证：文本 AI_COMPLETE 与图像 AI_EMBED/AI_CLASSIFY", "最终贡献是 AI-Native Data Infra 中一段可复现、可证伪的上游执行闭环。"],
};

const sourceNotes = {
  2: ["https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql", "https://cloud.google.com/bigquery/docs/generate-text", "https://docs.oracle.com/en/database/oracle/oracle-database/23/vecse/vector_embedding.html", "https://github.com/timescale/pgai", "https://github.com/pgvector/pgvector"],
  4: ["opening/literature/reading_list.md", "opening/report/opening_report.md#2-国内外研究现状"],
  5: ["figures/opening_figure_set/main_png/P05_研究空白_AI数据执行层.png", "opening/claim_matrix.md#1-冻结题目与系统抽象"],
  6: ["experiments/results/opening_database_e2e_text_refeed_20260808/README.md", "experiments/results/opening_text_native_single_job_formal_20260808/README.md"],
  7: ["experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_token_budget_vs_fixed_timeout300_20260719.csv", "experiments/results/dual_gpu_slo_ewma_flush_formal_20260729/README.md", "experiments/results/dual_gpu_active_work_saturation_20260729/README.md"],
  8: ["motivation/results/gpu/image_clip_preprocess_variants_20260801/README.md", "motivation/results/gpu/image_clip_transfer_ceiling_20260803/README.md"],
  9: ["experiments/results/opening_fourjob_interference_20260809/README.md"],
  11: ["figures/opening_figure_set/main_png/P11_系统架构_数据组织与状态调度闭环.png", "opening/report/opening_report.md#4-研究内容与技术路线"],
  12: ["figures/opening_figure_set/main_png/P12_研究内容一_WorkUnit与数据组织.png", "code/src/planning/work.py", "code/src/modalities/image/contracts.py"],
  13: ["experiments/results/rc1_data_organization/README.md"],
  14: ["figures/opening_figure_set/main_png/P14_研究内容二_状态感知提交与多作业调度.png", "opening/claim_matrix.md", "opening/report/opening_report.md#42-研究内容二容量感知的提交路由与多作业调度"],
  15: ["experiments/results/opening_fourjob_interference_20260809/README.md"],
  16: ["experiments/results/operator_cost_profile_dual4090_formal_v2_cache_on_20260807/README.md"],
  17: ["experiments/results/image_ai_embed_operator_formal_20260803/README.md", "feasibility/results/vllm_clip_pooling_gate_20260804/README.md"],
  18: ["experiments/results/opening_image_native_fourjob_formal_20260810/README.md", "experiments/results/opening_image_project_fourjob_observe_only_formal_20260810/README.md"],
  19: ["figures/opening_figure_set/main_png/P19_研究基础与后续工作计划.png", "opening/opening_defense_outline_20260808.md#第-19-页主实验用同上限强静态-ab-逐层验证"],
  20: ["opening/report/opening_report.md#7-预期成果创新点与风险控制"],
};

function parseNdjson(text) {
  return text.split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
}

function recordsFor(records, slide, kinds) {
  return records.filter((record) => record.slide === slide && kinds.has(record.kind));
}

function sortTextRecords(records) {
  return [...records].sort((a, b) => (a.bbox?.[1] ?? 0) - (b.bbox?.[1] ?? 0) || (a.bbox?.[0] ?? 0) - (b.bbox?.[0] ?? 0));
}

function notesFor(slide) {
  const title = content[slide][slide === 1 ? 0 : (slide === 20 ? 0 : 1)];
  const sources = sourceNotes[slide] || ["opening/report/opening_report.md"];
  const talk = {
    1: "数据库正在直接触发生成、嵌入和多模态分析。本课题研究这些数据进入模型服务前，如何被组织、估计和调度。",
    6: "先看左侧：统一数据库端到端合同下，三条静态路径接近，说明静态基线很强。再看右侧：同一 Chat 任务却落入过量排队或欠供给。",
    7: "这一页按 work、state、capacity 三层讲。行数隐藏工作量，配置上限不是运行状态，继续增压也不等于有效供给。",
    10: "四项设计要求权重相同；它们在系统结构上仍归入两项研究内容，代价估计是共同输入。",
    15: "先强调 full/quarter 只是控制额度损失，真正方法比较是同上限 static 与 shared。总效率提高不代表每个作业都改善。",
    16: "左侧回答是否排对配置，右侧展示 20 个场景的损失分布。中位数为零并不代表所有场景安全，因此保留最坏损失门。",
    20: "回到 AI-Native Data Infra：本课题不覆盖全部平台，而是把数据库任务到模型服务之间这一段做成可估计、可感知、可调度的闭环。",
  };
  return [
    `汇报讲稿：${talk[slide] || `本页回答“${title}”。先陈述现象或设计，再用底部结论收束并转入下一页。`}`,
    "",
    "答辩提示：数据图只在成立的比较合同内解读；现有 observe-only 或 preliminary 结果不替代后续同上限 A/B。",
    "",
    "[Sources]",
    ...sources.map((source) => `- ${source}`),
  ].join("\n");
}

function makeFrameMap(records) {
  const selected = new Set(sourceByOutput);
  return {
    outputSlides: sourceByOutput.map((sourceSlide, index) => {
      const outputSlide = index + 1;
      return {
        outputSlide,
        sourceSlide,
        narrativeRole: outputSlide <= 9 ? "motivation and evidence" : outputSlide <= 18 ? "method and validation" : "plan and contribution",
        reuseMode: "duplicate-slide",
        editTargets: recordsFor(records, sourceSlide, new Set(["textbox", "shape", "image"])).map((record) => ({
          shapeId: record.id,
          action: record.kind === "textbox" ? "rewrite" : record.kind === "image" && imageBySlide[outputSlide] ? "replace" : "keep",
          reason: `output slide ${outputSlide}: template-preserving content update`,
        })),
      };
    }),
    omittedSourceSlides: Array.from({ length: 28 }, (_, i) => i + 1).filter((n) => !selected.has(n)).map((sourceSlide) => ({ sourceSlide, reason: "not required by frozen 20-page narrative" })),
  };
}

function slidesFromPresentation(presentation) {
  if (Array.isArray(presentation.slides?.items)) return presentation.slides.items;
  return Array.from({ length: presentation.slides.count }, (_, index) => presentation.slides.getItem(index));
}

function checkProcess(result, label) {
  if (result.status !== 0) throw new Error(`${label} failed with status ${result.status}`);
}

function emptySlidePlaceholders(pptxPath) {
  const names = execFileSync("unzip", ["-Z1", pptxPath], { encoding: "utf8" }).split(/\r?\n/).filter((name) => /^ppt\/slides\/slide\d+\.xml$/.test(name));
  const failures = [];
  for (const name of names) {
    const xml = execFileSync("unzip", ["-p", pptxPath, name], { encoding: "utf8" });
    for (const match of xml.matchAll(/<p:sp\b[\s\S]*?<\/p:sp>/g)) {
      const shape = match[0];
      if (!shape.includes("<p:ph")) continue;
      const texts = [...shape.matchAll(/<a:t>([\s\S]*?)<\/a:t>/g)].map((item) => item[1].replace(/<[^>]+>/g, "").trim());
      if (!texts.some(Boolean)) failures.push(name);
    }
  }
  return [...new Set(failures)];
}

async function replaceInheritedImage(deck, slide, imageRecords, filePath, alt) {
  if (!imageRecords.length) throw new Error(`no inherited image frame for ${filePath}`);
  const bytes = await fs.readFile(filePath);
  const blob = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
  // Historical evidence slides contain a visible image plus a duplicated crop layer. Delete
  // both and add one replacement in the inherited outer frame; otherwise the old layer can
  // remain above the new chart or two copies can overlap.
  const frames = imageRecords.map((record) => ({ record, image: deck.resolve(record.id) }));
  const target = [...frames].sort((a, b) => (b.record.bbox?.[2] ?? 0) * (b.record.bbox?.[3] ?? 0) - (a.record.bbox?.[2] ?? 0) * (a.record.bbox?.[3] ?? 0))[0];
  const frame = target.image.frame;
  for (const { image } of frames) image.delete();
  slide.images.add({ blob, contentType: "image/png", alt, fit: "contain", position: frame, crop: { left: 0, top: 0, right: 0, bottom: 0 } });
}

async function main() {
  await fs.mkdir(TMP, { recursive: true });
  await ensureArtifactToolWorkspace(TMP);
  const { FileBlob, PresentationFile } = await importArtifactTool(TMP);
  const source = await PresentationFile.importPptx(await FileBlob.load(SOURCE));
  const sourceSnapshot = await source.inspect({ kind: "slide,textbox,shape,image,notes", include: "id,slide,name,title,text,textPreview,bbox,bboxUnit,isPlaceholder", maxChars: 1000000 });
  const sourceRecords = parseNdjson(sourceSnapshot.ndjson || "");
  const frameMap = makeFrameMap(sourceRecords);
  const mapPath = path.join(TMP, "template-frame-map.json");
  await fs.writeFile(mapPath, `${JSON.stringify(frameMap, null, 2)}\n`, "utf8");
  await fs.writeFile(path.join(TMP, "template-audit.txt"), [
    "Source: opening_defense_20260807_v6.pptx (28 slides).",
    "Output: frozen 20-page opening-defense narrative.",
    "Reusable patterns: cover 1; compact body 3; evidence image 8; architecture 14/16/19; schedule 26; conclusion 27.",
    "Typography, school marks, background, header and footer are inherited from the source deck.",
    "Every output slide is mapped to one source frame; inherited image objects are replaced in place.",
  ].join("\n") + "\n", "utf8");
  await fs.writeFile(path.join(TMP, "deviation-log.txt"), [
    "The source 28-page narrative is replaced by the frozen 20-page outline.",
    "Slides 2-4 and 10 use the compact body frame because no approved data figure is required.",
    "Slides 5-9 and 11-19 preserve evidence/architecture image frames and use every approved main figure from figures/opening_figure_set/main_png.",
    "Slide 20 reuses the source summary frame.",
  ].join("\n") + "\n", "utf8");
  await fs.writeFile(path.join(TMP, "00-scope.txt"), "Database-triggered AI data execution between database/dataframe and model service.\n", "utf8");
  await fs.writeFile(path.join(TMP, "01-research-canon.txt"), "Two research contents: work organization; state-aware admission/routing/multi-job. Cost estimation is shared. Image is multimodal validation.\n", "utf8");
  await fs.writeFile(path.join(TMP, "02-evidence-table.txt"), "Authoritative evidence: opening/claim_matrix.md and figures/opening_figure_set/main_png; underlying experiment sources remain in each slide note.\n", "utf8");
  await fs.writeFile(path.join(TMP, "03-argument-map.txt"), "Background -> gap -> baseline/motivation -> four requirements -> two contents/shared enabler -> validation -> plan/contributions.\n", "utf8");

  const starter = path.join(TMP, "template-starter.pptx");
  const inspectTemplate = path.join(SKILL_DIR, "template_following_scripts/inspect_template_deck.mjs");
  const prepare = path.join(SKILL_DIR, "template_following_scripts/prepare_template_starter_deck.mjs");
  const inspectDir = path.join(TMP, "template-inspect");
  const inspectPath = path.join(inspectDir, "template-inspect.ndjson");
  checkProcess(spawnSync(process.execPath, [inspectTemplate, "--workspace", TMP, "--pptx", SOURCE, "--out-dir", inspectDir, "--scale", "0.65"], { stdio: "inherit", env: process.env }), "inspect_template_deck");
  checkProcess(spawnSync(process.execPath, [prepare, "--workspace", TMP, "--pptx", SOURCE, "--map", mapPath, "--out", starter, "--inspect", inspectPath, "--preview-dir", path.join(TMP, "starter-preview"), "--layout-dir", path.join(TMP, "starter-layout"), "--contact-sheet", path.join(TMP, "starter-contact-sheet.png")], { stdio: "inherit", env: process.env }), "prepare_template_starter_deck");

  const deck = await PresentationFile.importPptx(await FileBlob.load(starter));
  const snapshot = await deck.inspect({ kind: "slide,textbox,shape,image,notes", include: "id,slide,name,title,text,textPreview,bbox,bboxUnit,isPlaceholder", maxChars: 1000000 });
  const records = parseNdjson(snapshot.ndjson || "");
  const slides = slidesFromPresentation(deck);
  if (slides.length !== 20) throw new Error(`expected 20 slides, found ${slides.length}`);

  for (let slideNumber = 1; slideNumber <= slides.length; slideNumber += 1) {
    const textRecords = sortTextRecords(recordsFor(records, slideNumber, new Set(["textbox"])));
    const replacements = content[slideNumber];
    if (textRecords.length !== replacements.length) throw new Error(`slide ${slideNumber}: expected ${replacements.length} textboxes, found ${textRecords.length}`);
    for (let index = 0; index < textRecords.length; index += 1) deck.resolve(textRecords[index].id).text = replacements[index];
    if (imageBySlide[slideNumber]) await replaceInheritedImage(deck, slides[slideNumber - 1], recordsFor(records, slideNumber, new Set(["image"])), path.join(ROOT, imageBySlide[slideNumber]), replacements[1]);
    slides[slideNumber - 1].speakerNotes.textFrame.setText(notesFor(slideNumber));
    slides[slideNumber - 1].speakerNotes.setVisible(true);
  }

  const out = await PresentationFile.exportPptx(deck);
  await out.save(FINAL);
  const renderDir = path.join(TMP, "final-render");
  const layoutDir = path.join(TMP, "final-layout");
  await fs.mkdir(renderDir, { recursive: true });
  await fs.mkdir(layoutDir, { recursive: true });
  for (let index = 0; index < slides.length; index += 1) {
    await saveBlobToFile(await deck.export({ slide: slides[index], format: "png", scale: 1.5 }), path.join(renderDir, `slide-${String(index + 1).padStart(2, "0")}.png`));
    await saveBlobToFile(await deck.export({ slide: slides[index], format: "layout" }), path.join(layoutDir, `slide-${String(index + 1).padStart(2, "0")}.layout.json`));
  }
  await saveBlobToFile(await deck.export({ format: "png", montage: true, scale: 0.65 }), path.join(TMP, "final-montage.png"));
  const finalSnapshot = await deck.inspect({ kind: "slide,textbox,image,notes", maxChars: 1000000 });
  await fs.writeFile(path.join(TMP, "final-inspect.ndjson"), finalSnapshot.ndjson || "", "utf8");
  const finalRecords = parseNdjson(finalSnapshot.ndjson || "");
  const noteFailures = slides.map((_, index) => ({ index: index + 1, text: String(finalRecords.find((item) => item.kind === "notes" && item.slide === index + 1)?.text || "") })).filter(({ text }) => !text.includes("汇报讲稿：") || !text.includes("答辩提示：") || !text.includes("[Sources]")).map(({ index }) => index);
  const qa = { final: FINAL, slideCount: slides.length, emptySlidePlaceholders: emptySlidePlaceholders(FINAL), noteFailures, renderDir, layoutDir, montage: path.join(TMP, "final-montage.png") };
  await fs.writeFile(path.join(TMP, "final-qa.json"), `${JSON.stringify(qa, null, 2)}\n`, "utf8");
  if (qa.emptySlidePlaceholders.length || qa.noteFailures.length) throw new Error(`final QA failed: ${JSON.stringify(qa)}`);
  console.log(JSON.stringify(qa, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message || String(error));
  process.exit(1);
});
