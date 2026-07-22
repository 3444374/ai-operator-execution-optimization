# 知识库同步操作指南

> **读者**：Claude Code / Codex agent。当 AGENTS.md §11 触发提醒且用户确认后，读取本文件执行同步。
> **人读版**：`../ai-operator-wiki/my-notes/知识库同步规则.md`

---

## 执行同步

```bash
cd ../ai-operator-wiki && bash sync-wiki.sh
```

脚本机制：先 `rm -rf` 清空所有目标目录，再 `cp` 重新拷贝。新增、修改、删除全部自动处理。

同步完成后提醒用户：打开 Obsidian → `Cmd+P` → "Karpathy LLM Wiki: Ingest from folder" → 选 `raw/` → 插件增量更新 wiki。

---

## 自动同步范围（通配符，新增文件自动发现）

| 项目路径 | → | Wiki 路径 |
|---|---|---|
| `research/*.md` | → | `raw/references/` |
| `opening/literature/reading_notes/*.md` | → | `raw/papers/` |
| `opening/literature/reference/*.pdf` | → | `raw/papers/` |
| `experiments/plans/*.md` | → | `experiments/plans/` |

特殊路由：`literature_and_evidence_review.md` 和 `existing_ai_operator_execution_chains.md` 在脚本中自动从 `raw/references/` 移到 `raw/analysis/`。

## 手动映射（opening/literature/ 顶层文件）

| 项目路径 | → | Wiki 路径 |
|---|---|---|
| `opening/literature/direction_assessment_20260715.md` | → | `raw/analysis/` |
| `opening/literature/gpu_scheduler_data_placement_supplement_20260715.md` | → | `raw/analysis/` |
| `opening/literature/ai_operator_literature_inventory.md` | → | `raw/inventory/` |
| `opening/literature/reading_list.md` | → | `raw/inventory/` |

---

## 新增同步路径

当用户在项目中新建了知识目录或文件，且不在上述自动范围内：

1. 打开 `../ai-operator-wiki/sync-wiki.sh`
2. 在对应区块加一行 `cp`，例如：
   ```bash
   cp "$PROJECT/research/算子代价估计/"*.md raw/references/
   ```
3. 判断目标目录：
   - 技术参考类 → `raw/references/`
   - 分析/评估类 → `raw/analysis/`
   - 论文笔记类 → `raw/papers/`
   - 文献清单类 → `raw/inventory/`
4. 在项目 `PROJECT_LOG.md` 记录变更
5. `git commit` 更新后的脚本

---

## 不同步的内容

代码文件、CSV 原始数据、`AGENTS.md`/`README.md` 规则文件、`notes/` 沟通记录、`PROJECT_LOG.md`——这些永远不进知识库。

---

## 触发条件（来自 AGENTS.md §11）

- 完成论文精读笔记
- 更新 `research/knowledge_hub.md`
- 新增/修改 `experiments/plans/` 下的实验计划
- 新建知识目录或文件
