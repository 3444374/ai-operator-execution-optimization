#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(SCRIPT_DIR, "../..");
const TMP = path.resolve(process.env.OPENING_PPT_V8_TMP || "/private/tmp/opening-report-ppt-v8");
const SOURCE = path.join(SCRIPT_DIR, "opening_defense_20260812_v7.pptx");
const OUTPUT = path.join(SCRIPT_DIR, "opening_defense_20260812_v8.pptx");
const SKILL_DIR = process.env.PRESENTATIONS_SKILL_DIR
  || "/Users/junshun/.codex/plugins/cache/openai-primary-runtime/presentations/26.805.11740/skills/presentations";
const { ensureArtifactToolWorkspace, importArtifactTool, saveBlobToFile } =
  await import(path.join(SKILL_DIR, "container_tools/artifact_tool_utils.mjs"));

const FIGURES = [
  "figures/opening_figure_set/main_png/P02_背景_数据库AI算子外部执行链路.png",
  "figures/opening_figure_set/main_png/P03_背景_传统算子与外部AI执行假设.png",
  "figures/opening_figure_set/main_png/P04_相关工作_跨层执行闭环.png",
];

function parseNdjson(value) {
  return String(value || "").split("\n").filter(Boolean).map((line) => JSON.parse(line));
}

async function imageBytes(filePath) {
  const buffer = await fs.readFile(filePath);
  return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength);
}

async function main() {
  await fs.mkdir(TMP, { recursive: true });
  await ensureArtifactToolWorkspace(TMP);
  const { FileBlob, PresentationFile } = await importArtifactTool(TMP);
  const deck = await PresentationFile.importPptx(await FileBlob.load(SOURCE));
  const snapshot = await deck.inspect({ kind: "slide,textbox,shape", maxChars: 100000 });
  const records = parseNdjson(snapshot.ndjson);

  for (let offset = 0; offset < FIGURES.length; offset += 1) {
    const slideNumber = offset + 2;
    const targets = records.filter((record) =>
      record.slide === slideNumber
        && ["TextBox 4", "Rectangle 5", "TextBox 6"].includes(record.name),
    );
    if (targets.length !== 3) {
      throw new Error(`slide ${slideNumber}: expected 3 replaceable objects, found ${targets.length}`);
    }
    for (const target of targets) deck.resolve(target.id).delete();
    deck.slides.getItem(slideNumber - 1).images.add({
      blob: await imageBytes(path.join(ROOT, FIGURES[offset])),
      contentType: "image/png",
      alt: `开题第 ${slideNumber} 页背景结构图`,
      fit: "contain",
      position: { left: 160, top: 178, width: 960, height: 540 },
    });
  }

  const output = await PresentationFile.exportPptx(deck);
  await output.save(OUTPUT);

  const renderDir = path.join(TMP, "final-render");
  const layoutDir = path.join(TMP, "final-layout");
  await fs.mkdir(renderDir, { recursive: true });
  await fs.mkdir(layoutDir, { recursive: true });
  for (let index = 0; index < deck.slides.items.length; index += 1) {
    const slide = deck.slides.getItem(index);
    const stem = `slide-${String(index + 1).padStart(2, "0")}`;
    await saveBlobToFile(await deck.export({ slide, format: "png", scale: 1 }), path.join(renderDir, `${stem}.png`));
    await saveBlobToFile(await slide.export({ format: "layout" }), path.join(layoutDir, `${stem}.layout.json`));
  }
  await saveBlobToFile(await deck.export({ format: "png", montage: true, scale: 0.65 }), path.join(TMP, "final-montage.png"));

  const finalRecords = parseNdjson((await deck.inspect({ kind: "slide,textbox,image,notes", maxChars: 1000000 })).ndjson);
  const noteFailures = Array.from({ length: 20 }, (_, index) => index + 1).filter((slideNumber) => {
    const notes = String(finalRecords.find((record) => record.kind === "notes" && record.slide === slideNumber)?.text || "");
    return !notes.includes("汇报讲稿：") || !notes.includes("答辩提示：") || !notes.includes("[Sources]");
  });
  if (deck.slides.items.length !== 20 || noteFailures.length) {
    throw new Error(`final QA failed: slideCount=${deck.slides.items.length}, noteFailures=${noteFailures.join(",")}`);
  }
  console.log(JSON.stringify({ output: OUTPUT, slideCount: 20, noteFailures }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message || String(error));
  process.exit(1);
});
