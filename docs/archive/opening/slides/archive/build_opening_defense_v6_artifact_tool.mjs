#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import { spawnSync, execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(SCRIPT_DIR, "../..");
const TMP = path.resolve(process.env.OPENING_PPT_TMP || "/private/tmp/opening-ppt-v6.VMV6DC");
const SOURCE = path.join(SCRIPT_DIR, "opening_defense_20260720_v5.pptx");
const FINAL = path.join(SCRIPT_DIR, "opening_defense_20260807_v6.pptx");
const HEADLINE = path.join(ROOT, "experiments/results/opening_database_e2e_text_20260807/raw/headline_summary.json");
const SKILL_DIR = process.env.PRESENTATIONS_SKILL_DIR ||
  "/Users/junshun/.codex/plugins/cache/openai-primary-runtime/presentations/26.805.11740/skills/presentations";
const UTILS = await import(path.join(SKILL_DIR, "container_tools/artifact_tool_utils.mjs"));

const { ensureArtifactToolWorkspace, importArtifactTool, saveBlobToFile } = UTILS;

const sourceByOutput = [
  1, 2, 3, 8, 8, 6, 3, 8, 8, 8, 8, 3, 13, 8, 3, 8, 8, 3, 8, 8, 3, 3, 23, 3, 8, 26, 27, 28,
];

const imageBySlide = {
  4: "figures/architecture/existing_ai_operator_execution_chains.png",
  5: "figures/architecture/research_gap_three_islands.png",
  8: "figures/data/report_main/opening_serving_capacity_frontier.png",
  9: "figures/data/report_main/opening_work_organization_regime.png",
  10: "figures/data/report_main/opening_image_matched_resource.png",
  11: "figures/data/report_main/opening_cost_model_decision_quality.png",
  14: "figures/architecture/system_architecture_ai_data_execution.png",
  16: "figures/architecture/data_organization_token_budget_mechanism.png",
  17: "figures/architecture/data_organization_prefix_aware_mechanism.png",
  19: "figures/architecture/runtime_strategy_control_loop.png",
  20: "figures/architecture/submission_control_kmax_admission_mechanism.png",
  25: "figures/architecture/cross_layer_method_framework.png",
};

const toc = ["目录", "01", "问题与缺口", "02", "证据与边界", "03", "研究方法", "04", "计划与风险"];

const content = {
  1: ["数据库 AI 负载的执行优化与调度研究", "AI 数据执行层：work-unit · cost · admission · routing · multi-job", "报告人：____________    指导老师：____________"],
  2: toc,
  3: ["01", "数据库正在成为 AI workload 入口", "■ Cortex AISQL、BigQuery 与 Oracle 已把生成、向量化和语义操作带入 SQL\n■ 数据库不再只管理结构化数据，还要触发模型调用并管理结果\n■ AI 算子成本同时受数据组织、模型服务和写回影响", "场景已经成立；研究问题是数据库如何有效驱动外部模型服务。"],
  4: ["01", "模型服务前后出现新的数据执行层", "数据库行需要先转换为计算量可控的请求，再经过准入、路由、模型执行和结果写回。", "行数不等于 work；数据库语义与 serving 状态需要在连接层汇合。"],
  5: ["01", "现有系统分别优化两端，连接层仍缺方法", "DB 内优化减少模型调用；serving 内优化已到达请求；数据框架提供机制。三者都没有完整覆盖数据库记录如何形成并提交模型请求。", "研究空白位于 Database 与 Model Service 之间，而不是再造一个 serving engine。"],
  6: toc,
  7: ["02", "统一文本三臂揭示静态路径边界", "", "同 source/sink 后不预设项目胜出；语义失败和 feeding 门必须与吞吐同表呈现。"],
  8: ["02", "先标定最小饱和点，再比较策略", "固定资源下，65K active work 已接近吞吐平台；继续增压主要增加尾延迟。", "65K 是当前签名下的最小饱和点，不是跨机器通用常数。"],
  9: ["02", "数据组织收益取决于 serving regime", "大 KV 池、低压力时策略近似中性；小 KV 池、KV 饱和时出现分化和排名反转。", "组织策略必须在明确的 endpoint/KV regime 下评价。"],
  10: ["02", "相同资源下，图像执行结构获得可重复收益", "matched-resource 两轮正式结果方向一致，主报告冻结约 13%–15% operator-JCT 改善。", "图像证据支持执行结构可行性，尚不支持状态感知增量。"],
  11: ["02", "代价估计已能辅助选择，但仍有最坏情形", "Hybrid pooled regret 1.67%，macro 2.90%，pairwise 0.808；max regret 14.72%。", "代价估计进入决策闭环，但最坏 regret 仍需压缩。"],
  12: ["02", "证据支持问题存在，不代表方法已经完成", "已证明：最小饱和点、regime dependence、图像静态结构收益\n条件性：代价估计配置选择价值\n待验证：state-aware 提交、路由和多作业增量\n不能声称：项目路径普遍胜出、某 organizer 全局最优", "所有后续方法都必须超过同资源、同上限的冻结强静态点。"],
  13: toc,
  14: ["03", "AI 数据执行层连接数据库与模型服务", "两项研究内容位于同一层：work-unit 构造；容量感知的提交、路由与多作业调度。", "模型服务保持黑盒；数据库和 sink 提供任务与正确性边界。"],
  15: ["03", "三个研究问题限定方法设计", "■ 最小饱和：多少 active work 足以达到容量平台？\n■ 相同 work：balance 与 locality 何时冲突？\n■ 多作业共享：怎样兼顾 work-conserving、JCT 与公平？", "每个方法组件必须对应一个可证伪问题，而不是因框架可用就加入。"],
  16: ["03", "研究内容一：按工作量构造 work-unit", "Cost Adapter 把数据库记录映射为 token/frame work；Organizer 在预算内形成 BatchRequest，并显式处理 oversize row。", "从固定行数转向计算量预算，但不预设某种 organizer 必然胜出。"],
  17: ["03", "数据组织同时处理 balance 与 locality", "length alignment 可降低 work 方差；prefix affinity 可保留缓存复用。重排序可能改善前者、破坏后者。", "候选信号必须通过跨 serving-regime 消融决定是否保留。"],
  18: ["03", "数据组织策略用强静态点和机制指标证伪", "固定 manifest 与服务配置，分别报告 packing、endpoint work skew、prefix group ratio、cache hit、TTFT、P99、吞吐和能耗。\n先做单因素消融，再检查跨 regime 排名是否稳定。", "差异不足 5% 仍是有效结果，不更换 workload 追求正数。"],
  19: ["03", "研究内容二：按容量提交、路由与协调", "控制 request/work credit 与 active work，在完成事件后连续补位；候选状态信号只驱动上游决策。", "目标是维持有效供给，不是无限压入请求。"],
  20: ["03", "request/work credit 在完成时精确释放", "共享 endpoint 上限支持 request-level replenishment、idle borrowing、routing 和多 job fair queue。", "request count 与 work credit 分开；credit 不在 submit 时提前释放。"],
  21: ["03", "状态感知策略必须超过同上限静态点", "输入信号：predicted work、active work、completion、running/waiting、KV、TTFT/ITL。\n评价顺序：吞吐 → tail/SLO → JCT/fairness → 失效边界。", "“动态”不是贡献；未过门时保留强静态点。"],
  22: ["03", "代价估计共同服务两项研究内容", "解析特征 + profile 校准 + residual correction，预测 work、service、remaining work 与 SLO slack。\n用 ranking、selection regret 和最坏 context 评价决策价值。", "代价估计是共同使能组件，不扩张为第三项研究内容。"],
  23: toc,
  24: ["04", "实验矩阵只回答可证伪问题", "■ 研究内容一、二分别与冻结静态点消融\n■ 独立最优拼接，再与小规模联合搜索对比\n■ 多 job 评价 JCT、tail、SLO 与公平性\n■ correctness、feeding-saturation、stability 任一不过即限制 claim", "正式 baseline 由被测系统拥有调度；项目只做统一 source/sink 与审计适配。"],
  25: ["04", "同一抽象跨文本与图像复用", "文本 adapter 输出 token work，图像 adapter 输出 frame/pixel/preprocess work；Organizer、Scheduler、Tracing 和配置逻辑保持一致。", "多模态用于检验抽象边界，不是第二套独立系统。"],
  26: ["04", "进度安排与停止规则", "■ 2026.08  冻结开题材料与统一实验合同\n■ 2026.09  work-unit 构造跨 workload/regime 消融\n■ 2026.10  state-aware 提交、路由与多 job 公平性\n■ 2026.11  代价模型 held-out 与两项策略耦合验证\n■ 2026.12+  外部有效性、论文图表与正文", "开题前不扩第二数据库和无关矩阵；后续实验必须对应核心 claim。"],
  27: ["预期创新、风险与降级路径", "■ 创新一：token/frame work-unit 构造与 balance/locality 边界\n■ 创新二：shared request/work credit 的提交、路由与多 job 调度\n■ 共同组件：面向 ranking/regret 的轻量代价估计\n■ 降级：动态不增益时收敛为最小饱和、regime 诊断和失效边界", "不改问题追结果；负结果同样用于限定方法适用范围。"],
  28: ["谢谢各位老师", "恳请批评指正"],
};

const pageRole = new Set([2, 6, 13, 23, 22]) ;

function parseNdjson(text) {
  return text.split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
}

function recordsFor(records, slide, kinds) {
  return records.filter((record) => record.slide === slide && kinds.has(record.kind));
}

function sortTextRecords(records) {
  return [...records].sort((a, b) => {
    const ay = a.bbox?.[1] ?? 0;
    const by = b.bbox?.[1] ?? 0;
    const ax = a.bbox?.[0] ?? 0;
    const bx = b.bbox?.[0] ?? 0;
    return ay - by || ax - bx;
  });
}

function notesFor(slide) {
  const title = content[slide][slide === 1 ? 0 : (content[slide].length === 2 ? 0 : 1)];
  const role = pageRole.has(slide) ? "可跳过" : "优先讲";
  const boundaries = {
    7: "SQuAD 项目臂未过 95% feeding 门；ShareGPT DuckDB AI 的 cap 语义失败不是基础设施失败。",
    8: "65K 只绑定当前机器、模型、协议和 workload。",
    9: "只声称 regime dependency，不声称 sequential 全局最优。",
    10: "headline 为 13%–15%；GPU busy 低，不声称 GPU-serving 优化。",
    11: "max regret 14.72% 为 marginal pass。",
    12: "state-aware 仍是待验证，不使用完成时。",
  };
  const sources = {
    7: "experiments/results/opening_database_e2e_text_20260807/README.md",
    8: "experiments/results/dual_gpu_active_work_saturation_20260729/README.md",
    9: "experiments/results/rc1_data_organization/README.md",
    10: "experiments/results/image_ai_embed_operator_formal_20260803/README.md",
    11: "experiments/results/operator_cost_profile_dual4090_formal_v2_cache_on_20260807/README.md",
    12: "opening/claim_matrix.md",
    14: "PROJECT_OUTLINE.md",
  };
  return [
    `页面角色：${role}`,
    "",
    `汇报讲稿：本页聚焦“${title}”。先给出证据或设计，再说明它如何收敛到下一页。`,
    "",
    `答辩备注：${boundaries[slide] || "只陈述本页证据等级；平台组件不是贡献，拟研究方法不得写成已有成果。"}`,
    "",
    "[Sources]",
    `- ${sources[slide] || "opening/report/opening_report.md"}`,
  ].join("\n");
}

function makeFrameMap(records) {
  const selected = new Set(sourceByOutput);
  const outputSlides = sourceByOutput.map((sourceSlide, index) => {
    const outputSlide = index + 1;
    const editTargets = recordsFor(records, sourceSlide, new Set(["textbox", "shape", "image"]))
      .map((record) => {
        let action = "keep";
        if (record.kind === "textbox") action = "rewrite";
        if (record.kind === "image") action = imageBySlide[outputSlide] ? "replace" : "keep";
        if (record.kind === "shape" && record.isPlaceholder && !String(record.text || "").trim()) action = "delete";
        return { shapeId: record.id, action, reason: `output slide ${outputSlide} template-preserving edit` };
      });
    return {
      outputSlide,
      sourceSlide,
      narrativeRole: outputSlide === 1 ? "opening thesis" : outputSlide === 28 ? "summary close" : "evidence and method body",
      reuseMode: "duplicate-slide",
      editTargets,
    };
  });
  const omittedSourceSlides = Array.from({ length: 28 }, (_, i) => i + 1)
    .filter((slide) => !selected.has(slide))
    .map((sourceSlide) => ({ sourceSlide, reason: "dense or outdated content pattern replaced by an existing simpler source layout" }));
  return { outputSlides, omittedSourceSlides };
}

function checkProcess(result, label) {
  if (result.status !== 0) {
    throw new Error(`${label} failed with status ${result.status}`);
  }
}

function slidesFromPresentation(presentation) {
  if (Array.isArray(presentation.slides?.items)) return presentation.slides.items;
  return Array.from({ length: presentation.slides.count }, (_, index) => presentation.slides.getItem(index));
}

function emptySlidePlaceholders(pptxPath) {
  const names = execFileSync("unzip", ["-Z1", pptxPath], { encoding: "utf8" })
    .split(/\r?\n/).filter((name) => /^ppt\/slides\/slide\d+\.xml$/.test(name));
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

async function main() {
  const headline = JSON.parse(await fs.readFile(HEADLINE, "utf8"));
  const squad = headline.workloads.squad_uniform;
  const sharegpt = headline.workloads.sharegpt_controlled_skew;
  content[7][2] = [
    `SQuAD correct rows/s：direct ${squad.direct_static_sharded.correct_rows_per_s_mean.toFixed(2)}｜DuckDB AI ${squad.duckdb_ai_static_sharded.correct_rows_per_s_mean.toFixed(2)}｜project ${squad.project_frozen_static.correct_rows_per_s_mean.toFixed(2)}`,
    `SQuAD feeding：project / direct = ${(100 * squad.project_frozen_static.feeding_service_tokens_ratio_vs_direct).toFixed(1)}%（未过 95% 门）`,
    `ShareGPT correct rows/s：direct ${sharegpt.direct_static_sharded.correct_rows_per_s_mean.toFixed(2)}｜DuckDB AI ${sharegpt.duckdb_ai_static_sharded.correct_rows_per_s_mean.toFixed(2)}｜project ${sharegpt.project_frozen_static.correct_rows_per_s_mean.toFixed(2)}`,
    `DuckDB AI ShareGPT：三次 formal 共 ${sharegpt.duckdb_ai_static_sharded.cap_semantic_failures_total}/6144 行 cap 语义失败；基础设施失败 0`,
  ].join("\n");
  await fs.mkdir(TMP, { recursive: true });
  await ensureArtifactToolWorkspace(TMP);
  const { FileBlob, PresentationFile } = await importArtifactTool(TMP);

  const source = await PresentationFile.importPptx(await FileBlob.load(SOURCE));
  const sourceSnapshot = await source.inspect({
    kind: "slide,textbox,shape,image,notes",
    include: "id,slide,name,title,text,textPreview,bbox,bboxUnit,isPlaceholder,placeholders",
    maxChars: 1000000,
  });
  const sourceRecords = parseNdjson(sourceSnapshot.ndjson || "");
  const map = makeFrameMap(sourceRecords);
  const mapPath = path.join(TMP, "template-frame-map.json");
  await fs.writeFile(mapPath, JSON.stringify(map, null, 2) + "\n", "utf8");
  await fs.writeFile(path.join(TMP, "template-audit.txt"), [
    "Source: opening_defense_20260720_v5.pptx (28 slides)",
    "Reusable patterns: cover 1; TOC 2/6/13/23; text body 3; image body 8; schedule 26; summary 27; close 28.",
    "Typography and chrome: preserve imported fonts, sizes, master, footer, school mark, title and claim rails.",
    "Insertion contract: duplicate mapped source slides; rewrite declared textboxes; replace declared image frames; no overlay deck.",
    "All source-slide renders and layouts were inspected under template-inspect/ before mapping.",
  ].join("\n") + "\n", "utf8");
  await fs.writeFile(path.join(TMP, "deviation-log.txt"), [
    "Slides 4-5, 8-11, 14, 16-17, 19-20, 25: replace inherited image content while preserving image frames.",
    "Slides 7, 12, 15, 18, 21-22, 24: reuse the source text-body pattern to reduce density.",
    "Repeated source slide 8 is intentional: it is the template's clearest title + evidence image + conclusion pattern.",
  ].join("\n") + "\n", "utf8");

  const starter = path.join(TMP, "template-starter.pptx");
  const prepare = path.join(SKILL_DIR, "template_following_scripts/prepare_template_starter_deck.mjs");
  checkProcess(spawnSync(process.execPath, [prepare,
    "--workspace", TMP,
    "--pptx", SOURCE,
    "--map", mapPath,
    "--out", starter,
    "--preview-dir", path.join(TMP, "template-starter-preview"),
    "--layout-dir", path.join(TMP, "template-starter-layout"),
    "--contact-sheet", path.join(TMP, "template-starter-contact-sheet.png"),
  ], { stdio: "inherit", env: process.env }), "prepare_template_starter_deck");

  const deck = await PresentationFile.importPptx(await FileBlob.load(starter));
  const snapshot = await deck.inspect({
    kind: "slide,textbox,shape,image,notes",
    include: "id,slide,name,title,text,textPreview,bbox,bboxUnit,isPlaceholder,placeholders",
    maxChars: 1000000,
  });
  const records = parseNdjson(snapshot.ndjson || "");
  const slides = slidesFromPresentation(deck);

  for (let slideNumber = 1; slideNumber <= slides.length; slideNumber += 1) {
    const textRecords = sortTextRecords(recordsFor(records, slideNumber, new Set(["textbox"])));
    const replacements = content[slideNumber];
    if (textRecords.length !== replacements.length) {
      throw new Error(`slide ${slideNumber}: expected ${replacements.length} textboxes, found ${textRecords.length}`);
    }
    for (let index = 0; index < textRecords.length; index += 1) {
      const record = textRecords[index];
      const target = deck.resolve(record.id);
      target.text = replacements[index];
    }
    if (imageBySlide[slideNumber]) {
      const images = recordsFor(records, slideNumber, new Set(["image"]));
      if (images.length !== 1) throw new Error(`slide ${slideNumber}: expected one inherited image, found ${images.length}`);
      const image = deck.resolve(images[0].id);
      const oldFrame = image.frame;
      const bytes = await fs.readFile(path.join(ROOT, imageBySlide[slideNumber]));
      const blob = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
      slides[slideNumber - 1].shapes.add({
        geometry: "rect",
        position: oldFrame,
        fill: "#FFFFFF",
        line: { style: "solid", fill: "none", width: 0 },
      });
      const replacement = slides[slideNumber - 1].images.add({
        blob,
        contentType: "image/png",
        alt: content[slideNumber][1],
        fit: "contain",
        position: oldFrame,
        crop: { left: 0, top: 0, right: 0, bottom: 0 },
      });
      replacement.frame = oldFrame;
    }
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
  await saveBlobToFile(await deck.export({ format: "webp", montage: true, scale: 0.8 }), path.join(TMP, "final-montage.webp"));
  const finalSnapshot = await deck.inspect({ kind: "slide,textbox,image,notes", maxChars: 1000000 });
  await fs.writeFile(path.join(TMP, "final-inspect.ndjson"), finalSnapshot.ndjson || "", "utf8");

  const emptyPlaceholders = emptySlidePlaceholders(FINAL);
  const finalRecords = parseNdjson(finalSnapshot.ndjson || "");
  const noteFailures = slides.map((_, index) => {
    const record = finalRecords.find((item) => item.kind === "notes" && item.slide === index + 1);
    return { index: index + 1, text: String(record?.text || record?.textPreview || "") };
  }).filter(({ text }) => !text.includes("汇报讲稿：") || !text.includes("答辩备注：") || !text.includes("[Sources]"))
    .map(({ index }) => index);
  const qa = {
    final: FINAL,
    slideCount: slides.length,
    sourceMap: sourceByOutput,
    emptySlidePlaceholders: emptyPlaceholders,
    noteFailures,
    headlineSummary: HEADLINE,
    renderDir,
    layoutDir,
    montage: path.join(TMP, "final-montage.webp"),
  };
  await fs.writeFile(path.join(TMP, "final-qa.json"), JSON.stringify(qa, null, 2) + "\n", "utf8");
  if (slides.length !== 28 || emptyPlaceholders.length || noteFailures.length) {
    throw new Error(`final QA failed: ${JSON.stringify(qa)}`);
  }
  console.log(JSON.stringify(qa, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message || String(error));
  process.exit(1);
});
