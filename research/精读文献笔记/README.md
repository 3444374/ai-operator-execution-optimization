# research/精读文献笔记/ — 精读笔记权威库

本目录保存需要逐篇深入分析的论文笔记。每篇论文使用独立文件夹，主笔记、论文配图和后续补充材料放在一起；从 2026-08-21 起，本目录是项目精读笔记的唯一权威来源。

## 目录合同

```text
精读文献笔记/
├── README.md
├── paper_deep_reading_digest/     # 15 篇论文的紧凑方法速览
│   ├── paper_deep_reading_digest.tex
│   └── paper_deep_reading_digest.pdf  # 本地可再生成，不纳入 Git
└── <短名>_<会议年份>/
    ├── <短名>_<会议年份>.md
    └── figures/                    # 可选；有配图时才创建
```

- `<短名>_<会议年份>` 使用小写 snake_case；会议名采用正式常用缩写，例如 `lotus_pvldb2025`。
- 每个论文文件夹只保留一份同名主笔记，避免出现多个无法判断权威性的正文。
- `figures/` 是可选目录；不要为没有配图的论文创建空目录。

## 横向速览

`paper_deep_reading_digest/paper_deep_reading_digest.tex` 将最初 15 篇权威精读笔记压缩为一份 A4 LaTeX 文档。2026-08-27 新登记的 Kalypso 尚未并入该速览，因此不能把速览篇数当作本目录主笔记总数。每篇统一采用“背景与问题 → 核心思路 → 关键词解释 → 具体做法 → 实验结果 → 与本课题的区别”的总—分结构，方法部分为主体，目标篇幅为约半页至一页 A4；它只用于快速复习，不替代各论文目录中的主笔记。关键词只解释理解该篇方法所必需的缩写、系统术语和指标，不做无关的通用词典。

进入 `paper_deep_reading_digest/` 后，使用 VS Code LaTeX Workshop 选择 **XeLaTeX** 或包含 XeLaTeX 的 recipe；文档首行已声明 `% !TeX program = xelatex`，以 `ctexart` 处理中文。本机已将 MiKTeX 安装到 `D:\Programs\MiKTeX` 并完成实际编译、分页和视觉检查；当前 PDF 为 6 页。目录只长期保留同名 `.tex` 和 `.pdf`，`.aux/.log/.out/.synctex.gz` 等中间文件在确认编译成功后删除；PDF 按根目录 `.gitignore` 中的 `*.pdf` 规则不纳入版本控制。

## 当前内容

| 论文目录 | 内容 |
|---|---|
| `lotus_pvldb2025/` | LOTUS 精读主笔记；`figures/` 内含论文 Figure 1、4、6、7 的原图裁剪件 |
| `galois_sigmod2025/` | Galois 精读主笔记；`figures/` 内含论文 Figure 1–4、7–11 的 9 个原图裁剪件；Figure 1–2 按 motivation example、Figure 11 按 oracle 参考上界解读 |
| `palimpzest_cidr2025/` | Palimpzest 精读主笔记；`figures/` 内含附件 arXiv 版本全部 Figure 1–7 的 7 个原图裁剪件；方法、图号与实验仍按 2024 arXiv v2 解读，CIDR 2025 只作为后续正式发表信息 |
| `abacus_pvldb2026/` | Abacus 精读主笔记；`figures/` 内含正式 PVLDB 版全部 Figure 1–8 的 8 个原图裁剪件；Figure 1 按动机示例、Figure 6–8 按有限采样与经验约束满足证据解读 |
| `sema_vldb2026/` | Sema 精读主笔记；VLDB 2026 官方程序已确认录用，当前本地全文仍为 arXiv v1；`figures/` 内含该版本正文 Figure 1–8 的 8 个原图裁剪件，Figure 4–7 的查询数与实验结论均按 arXiv v1 解读 |
| `ayo_asplos2025/` | AYO 精读主笔记；`figures/` 内含 ASPLOS 正式版 Figure 1、3、4、5、6、7、8、9、10、11、12 的原图裁剪件 |
| `cortex_aisql_sigmod2026/` | Cortex AISQL 精读主笔记；`figures/` 内含论文 Figure 1、7、9、10、11、12 的原图裁剪件 |
| `ray_data_streaming_batch_nsdi2027/` | Ray Data Streaming Batch 精读主笔记；`figures/` 内含论文 Figure 2、4、5、6、7、9 的原图裁剪件；PDF 版本仍按 arXiv v5 记录，不由目录名推断正式 venue |
| `ray_osdi2018/` | Ray OSDI 2018 精读主笔记；`figures/` 内含论文 Figure 4、5、6、7、8、10、11、12、14 的 10 个原图裁剪件（Figure 10a/10b 分开） |
| `blendserve_asplos2026/` | BlendServe 精读主笔记；`figures/` 内含论文 Figure 1、2、3、4、5、6、7、9、10、11 的 10 个原图裁剪件；Figure 11 明确按 simulated backend 证据解读 |
| `relational_llm_queries_mlsys2025/` | *Optimizing LLM Queries in Relational Data Analytics Workloads* 精读主笔记；`figures/` 内含论文全部 Figure 1–6 的 6 个原图裁剪件；Figure 1 按 worst-case 构造、Figure 6 按有限 correctness workload 解读 |
| `vtc_osdi2024/` | VTC 精读主笔记；`figures/` 内含论文 Figure 1、2、3、4、6、8、9、10、12、15、16、19 的 12 个原图裁剪件；VTC 仍按引擎内多 client 调度相关工作解读 |
| `IMBridge_sigmod2024/` | IMBridge 精读主笔记；`figures/` 内含 SIGMOD-Companion ’24 论文全部 Figure 1–6 的 6 个原图裁剪件；本地文件名中的 `2026` 不作为发表年份，Figure 5/6 只按 demo 截图证据解读 |
| `dlpm_2025/` | DLPM/D²LPM 精读主笔记；当前正式状态仍为 arXiv:2501.14312v1；`figures/` 内含正文全部 Figure 1–12 的 12 个原图裁剪件，Figure 7 保留缺失点与 Long-Context worst-case，理论保证不外推到黑盒 endpoint 的端到端 Job 公平 |
| `parrot_osdi2024/` | Parrot 精读主笔记；按正式 OSDI 2024 proceedings PDF（正文 17 页，文件另含 1 页 USENIX 封面）核对；`figures/` 内含正文全部 Figure 1–19 的原图裁剪件，机制图与实验图均在相邻图注中说明读法和证据边界 |
| `kalypso_arxiv2026/` | Kalypso 精读主笔记；arXiv:2607.23815v2，关系查询计划感知的 LLM serving、跨算子流水执行与 KV-cache 生命周期管理；原稿已从下载目录原件逐字节恢复；按核心补充管理，不冒充正式 venue，不进入现有 Top 15、十五篇速览或开题正文 |

## 精读流程

1. 先在 `../ai_operator_literature_inventory.md` 核验题名、作者、正式轨道、年份和 DOI，并确认 `../reference/REFERENCE_INDEX.md` 中的 PDF/版本。
2. 在独立论文目录中创建同名主笔记；本目录不使用统一模板，也不维护阅读状态字段。
3. 精读至少覆盖：研究问题与假设、方法和系统边界、实验设计与 baseline、关键数据、局限/威胁、可迁移机制、与本项目的关系及不能声称的内容。
4. 论文原文事实尽量标注章节、页码、表号或图号；推断和项目判断必须与论文事实分开。
5. 完成后更新本目录计数、`../README.md`、`../../PROJECT_INDEX.md` 和 `../../PROJECT_LOG.md`；新增或替换 PDF 时再更新 `../reference/REFERENCE_INDEX.md`。进入开题 Top 15 时，是否更新开题快照由 `../../opening/literature/reading_list.md` 单独决定。

## 配图规则

- 文件名建议为 `fig<N>_<简短说明>.png`。
- 笔记中注明来源，例如“论文 Figure 4，第 9 页”；自行重绘时明确标注“根据论文重绘”，不要冒充论文原图。
- 配图只服务于理解关键机制或实验，不为装饰而收集。

## 与泛读库的关系

- `../reading_notes/` 是泛读、筛选和快速回顾库。
- 同一论文若已有泛读笔记，精读主笔记应链接它；泛读笔记也应补充精读入口。
- 两份笔记结论冲突时，以经过全文核验的精读笔记为准，并回写修正泛读摘要。
