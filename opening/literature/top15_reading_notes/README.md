# 开题精读 Top 15 — 自包含快照

本目录是**开题要求精读的 15 篇文献笔记拷贝**，用于开题答辩的自包含交付。

## 来源与选取链路（provenance）

本 15 篇并非独立挑选，而是从 `research/` 全量文献库**按学术标准排名选出的前 15**：

1. **候选池**：`research/ai_operator_literature_inventory.md`——项目全量文献库（**66 篇**，CCF-A 优先 + 权威系统论文 + 工程系统资料，v5 / 2026-07-24）。
2. **排名**：`research/top15_ranked_papers.md` 按学术研究标准（基础工作 / 核心技术 / 相关工作；CCF-A > 顶会 > arXiv）从 66 篇选前 15，每篇附入选理由。
3. **精读**：权威精读笔记在 `research/reading_notes/`（共 33 篇精读，含此 15 篇）；**本目录是这 15 篇笔记的拷贝快照**，供开题自包含交付。

> 链路：`research` 全量文献库（66）→ 学术排名（`top15_ranked_papers.md`）→ 前 15 → 开题精读快照（本目录）。

## 权威版本

单篇精读笔记的**权威版本**在 `research/reading_notes/`（项目级文献库）。本目录是其快照副本：

- 笔记内容以 `research/reading_notes/` 为准。
- 更新后需手动重新拷贝：`cp research/reading_notes/<x>.md opening/literature/top15_reading_notes/`。

## PDF 与配图

- 论文 PDF 全集在 `research/reference/`（笔记内"本地 PDF"指针已统一指向 `research/reference/<x>.pdf`）。
- 配图在同级 `figs/`（与本目录笔记的相对路径一致）。

## 15 篇清单

排序与入选理由见 `research/top15_ranked_papers.md`。本目录文件（按 #1–#15）：

1. `vllm_sosp2023.md` · 2. `orca_osdi2022.md` · 3. `ray_osdi2018.md` · 4. `clipper_nsdi2017.md` · 5. `sarathi_serve_osdi2024.md` · 6. `sglang_neurips2024.md` · 7. `distserve_osdi2024.md` · 8. `splitwise_isca2024.md` · 9. `concur_2025.md` · 10. `ray_data_streaming_batch_2025.md` · 11. `bucketserve_2025.md` · 12. `cortex_aisql_sigmod2026.md` · 13. `neurdb_cidr2025.md` · 14. `galois_sigmod2025.md` · 15. `db_perspective_llm_pvldb2025.md`
