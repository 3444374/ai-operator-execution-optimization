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

## 触发条件

**显式触发**：用户在对话中说"记住""记下来""同步到知识库""加到 wiki"等表达——**立即执行同步**，不要等到会话结束。

**隐式触发**：会话中任何知识文件（`research/`、`opening/literature/`、`experiments/plans/` 下的 `.md`，或用户指定的新路径）被创建或修改——**会话结束前提醒一次**。

触发不依赖文件名或内容类型——只要知识目录下有变更，或者用户表达了记住意愿，就是触发条件。

### 典型场景："帮我记住这个"

```
用户: "vLLM 的 max_num_seqs 默认值是多少？"
Agent: "256。超过这个数的请求会排队。"
用户: "帮我记住这个。"

Agent 的操作序列：
1. 把知识点写入项目（新文件或追加到现有文件，视内容量决定）
2. 立即运行 cd ../ai-operator-wiki && bash sync-wiki.sh
3. 提醒：打开 Obsidian → Ingest
```

知识点该写到哪个文件，根据内容判断：
- 系统参数/机制细节 → `research/vllm_continuous_batching_reference.md` 或 `research/knowledge_hub.md` 对应章节
- 新发现的技术事实 → 新建 `research/` 下的文件，或在 `knowledge_hub.md` 新增条目
- 实验中的发现 → `experiments/plans/` 对应文件
- 属于全新方向 → 新建目录 + 告知用户已在 `sync-wiki.sh` 中加好映射
