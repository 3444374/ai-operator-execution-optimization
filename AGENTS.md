# AGENTS.md

本文件只保存**全项目长期规则**。会变化的实现状态、实验数字、运行命令和历史记录分别进入
`code/INFRA_STATUS.md`、`experiments/results/EXPERIMENT_EVIDENCE_REGISTRY.md`、平台 runbook 和
`PROJECT_LOG.md`，不在规则文件中复制。

## 1. 规则层级与读取顺序

处理任何文件时，按以下顺序读取：

1. 根 `AGENTS.md`；
2. 从根到目标文件所在目录，逐级读取沿途存在的 `AGENTS.md`；
3. 目标目录 `README.md`；
4. 任务对应的源码、计划、结果或 runbook。

子目录规则只增加该目录的职责、边界和验证要求。局部规则可以收紧操作，但不能放宽本文件的
研究范围、安全、证据和隐私要求。规则与事实冲突时，按“源码/原始结果 → 领域权威入口 →
`PROJECT_OUTLINE.md` → README/速览 → 历史计划”的顺序核对；先修错误的缓存文档，不把多个版本
长期并列为现役答案。

规则文件只写会改变 agent 行为的约束；目录内容、当前进度和文件清单写入 README 或索引。
Claude Code 通过根 `CLAUDE.md` 导入本文件，并遵循相同的逐级读取规则。

| 工作范围 | 追加规则入口 |
|---|---|
| 可复用代码、脚本、测试 | `code/AGENTS.md` |
| 环境、容器、跨机器运行 | `deploy/AGENTS.md`、`deploy/runtime/AGENTS.md` 与平台 runbook |
| 数据资产与导入 | `data/AGENTS.md` |
| 正式方法实验 | `experiments/AGENTS.md`，再读 `plans/` 或 `results/` 的局部规则 |
| 动机画像 | `motivation/AGENTS.md` 及目标子目录规则 |
| 可行性与 smoke | `feasibility/AGENTS.md` 及目标子目录规则 |
| 文献和知识文件 | `research/AGENTS.md` |
| 图资产 | `figures/AGENTS.md` |
| 开题与对外材料 | `opening/AGENTS.md` |
| 学习讲解 | `learning/AGENTS.md` |
| 快速方向卡片 | `overview/AGENTS.md` |
| 导师/企业沟通记录 | `notes/AGENTS.md` |
| 历史设计/工程归档 | `docs/AGENTS.md`、`code_doc/AGENTS.md`、`projects/AGENTS.md` |

继承示例：

| 目标文件 | 实际生效的项目规则 |
|---|---|
| `code/src/...` | 根 → `code/AGENTS.md` |
| `experiments/plans/...` | 根 → `experiments/AGENTS.md` → `experiments/plans/AGENTS.md` |
| `motivation/results/gpu/...` | 根 → `motivation/AGENTS.md` → `motivation/results/AGENTS.md` → `motivation/results/gpu/AGENTS.md` |
| `opening/report/...` | 根 → `opening/AGENTS.md` |
| `deploy/postgres18.4/...` | 根 → `deploy/AGENTS.md` → `deploy/postgres18.4/AGENTS.md` |

因此，任意子目录中的文档都先受本文件的证据和语言规则约束，再叠加本目录的格式/验证要求。

## 2. 项目范围

研究对象是：**PostgreSQL 内置 LOTUS AI 语义算子的外部分布式物理执行与调度优化**。

- 数据库拥有 SQL、ordinary child plan、snapshot、权限、query cancel/error/result lifecycle；
- 首版复用 LOTUS v1.2.4 `sem_map` 的 operator、prompt、output 和错误语义；
- Daft、Ray、vLLM、typed CLIP actor 等位于数据库进程外，作为受数据库管理的可替换 backend；
- 研究内容一是按 token/frame/阶段 work 与局部性组织数据；
- 研究内容二是在固定 request/work capacity 下控制提交、服务实例路由和单租户多 Job 调度；
- 算子代价估计是两项研究内容和数据库计划比较的共同支撑；
- 图像 `AI_EMBED/AI_CLASSIFY` 用于跨模态验证，文本 `AI_COMPLETE` 是首版主场景；
- PostgreSQL + pgvector 的 COPY + deferred index 是写回工程 baseline。

项目不以 PostgreSQL core fork、PL/Python 逐行 HTTP UDF、LOTUS DataConnector 外拉、修改 vLLM
continuous batching、修改 Ray scheduler、模型/kernel 优化、传统 GPU 查询算子或单纯产品集成为主线。
“数据库内置”只表示 SQL/planner/query lifecycle 属于数据库，不表示 payload 不会传到外部服务。

## 3. 当前资格顺序

当前实现按以下顺序推进：

1. 用真实、版本锁定的 LOTUS v1.2.4 `SemMapNode`/prompt/output/error 语义替换现有
   UDF/manifest-like `AI_COMPLETE` 入口；
2. 实现 PostgreSQL extension/planner-visible operator，验证 child plan、snapshot、cancel、
   error 和 result lifecycle；
3. 资格验证完成后，再恢复图像动态控制、HSE GPU 对照和其他策略扩展。

在前两步完成前，现有 profiler、manifest、Daft/Ray/static/SAOR 路径统一标为外部物理执行基座或
emulated operator contract；不扩 GPU 参数矩阵，不继续调 SAOR，也不把它们写成已实现数据库内算子。
当前状态只从 `PROJECT_OUTLINE.md`、`code/INFRA_STATUS.md` 和实验证据台账引用。

## 4. 工作方式与证据纪律

- 遵循 `karpathy-guidelines`：先写可验证目标，显式说明假设，做最小充分改动，验证后再扩展。
- 设计新系统机制前，先查 `research/knowledge_hub.md`、文献清单和
  `experiments/plans/baseline_reference.md`；新增候选记录到知识库，不把工程直觉伪装成研究空白。
- 代码、计划、结果和对外材料分层保存。历史文件可以保留原始叙事，但必须指向当前替代入口。
- 原始实验数据、失败运行和审计证据默认保留；移动或删除前先检查引用、唯一性与恢复路径。
- 结论标注来源类型：源码、原始实验、论文、官方文档、模拟、推断或待验证。microbenchmark、
  smoke、CPU/fake 与单次 GPU run 不能外推为完整系统结论。
- 当前代码和实验不支持的能力明确写 `pending` 或“尚未实现”，不以计划存在代替实现。

## 5. 环境与正式实验

涉及新机器/容器、GPU 切换、缺依赖、模型或数据下载、数据库导入、单/多 GPU 实验时，必须先读：

1. `deploy/runtime/AGENTS.md`；
2. `deploy/runtime/README.md`；
3. 目标平台 runbook，例如 `deploy/autodl/README.md`。

先按 runtime README 确认仓库外 `AI_OPERATOR_ENV_FILE`；文件不存在时先从模板创建并填写目标路径，
不得跳过到安装或实验。再按任务选择 capability groups，运行只读
`PYTHONPATH=code python code/scripts/environment/manage_environment.py check --groups <groups> --json-out <artifact-root>/preflight.json`
并保存机器报告；可复制的实例以 `deploy/runtime/README.md` 为准。随后再执行相互独立的安装、下载
和 importer。
driver 与 vLLM 环境保持隔离。batch/K/actor/active-work 配置绑定“机器 + 模型/版本 + 服务配置 +
协议 + workload 分布”校准签名；签名变化必须重新做 correctness、scale 和 saturation 校准，正式 run
期间不在线调参。

正式实验同时遵守 `experiments/AGENTS.md`、对应计划和以下全局要求：

- baseline 由被测系统拥有执行与调度；项目 adapter 只处理 source、sink、质量审计和统一指标；
- GPU-backed database-E2E 优先，CPU/fake 仅作调试、机制隔离或历史对照；
- 记录 upstream URL/commit、实现来源、scheduler owner、适配 diff、server/pgvector 版本、配置签名、
  warm-up、全部重复值、失败/重试和 exactly-once；
- 区分 source、organization、serialization/put、queue/admission、model、fan-in 与 writeback 阶段；
- 运行门禁、指标合同和报告模板以 `experiments/plans/baseline_reference.md`、
  `experiments/plans/reference/experiment_report_honesty_checklist.md` 和目标计划为准；根规则不缓存具体
  K/W、endpoint 拓扑或阈值。

## 6. 文档受众与对外表达

修改任何 Markdown、报告、PPT、图注或讲稿前，先判定受众：

- **读者型/对外文档**：根 README/overview、research 综述、learning、opening、图中可见文字、对外
  沟通稿，以及任何准备给导师、评审或非项目成员阅读的内容；
- **内部操作文档**：AGENTS、实验计划/原始结果、runbook、audit、日志和历史设计记录。

目录不能替代受众判断：`experiments/` 中的对外摘要仍按读者型文档处理，`opening/logs/` 的内部日志
仍可记录实际状态。受众不明确时默认按读者型文档处理。

所有文档都优先写具体对象、条件、动作和结果。内部操作文档只有在表示可检查的状态、实验合同或
历史原文时才使用项目管理词，并在首次出现处说明对象；不把抽象词堆叠成结论。

读者型/对外文档不得直接把“冻结、门禁、闭环、边界、约束、合同、产品轨、框架轨、正式点、
晋级、失效”等项目内部管理词当作普通叙述。改写成实际含义，例如：

- “冻结配置” → “实验开始前选定配置，运行期间保持不变”；
- “通过门禁” → “正确性、资源使用和重复实验满足预先规定条件后才纳入比较”；
- “形成闭环” → 写清“读取、执行、写回、结果核对”实际包含哪些步骤；
- “适用边界” → 写清在哪些 workload、资源和服务条件下有效或不再有效。

数学约束、事务边界和 API contract 等具有明确技术含义的术语可以使用，但首次出现时说明约束对象
和作用。文档任务的完成条件是：对本次修改的全部读者型文件搜索
`冻结|门禁|闭环|产品轨|框架轨|正式点|晋级|失效|RC[0-9]|BL[0-9]|Phase [0-9]|P0|P1|P2`，每个命中
都已改写，或属于已解释的技术术语；不能只检查目标目录的局部规则。

开题报告、论文、PPT、图表、答辩讲稿和外部同步稿还必须：

- 使用“PostgreSQL 内置 LOTUS AI 语义算子的外部分布式物理执行与调度优化”这一对象表述；
- 区分总动机、子问题、证据实验、研究内容和实现任务；兄弟项保持同一层级与粒度；
- 写出具体对象、条件和动作，不把“冻结、门禁、闭环、产品轨、框架轨、晋级”等内部管理词当成
  读者已知概念；
- 首次出现的英文缩写、内部数据结构和指标说明中文作用；
- 不使用 `RC1/RC2/RC3`、`BL1/BL2`、`Phase 0/1/2/3`、`P0/P1/P2` 等内部代号；
- 文献使用正式英文题名；系统名保留英文，中文解释研究问题、方法、证据和适用条件；
- 对初步结果使用“可行性依据/观察信号/待扩大验证”，不写成已经完成的贡献。

更具体的报告、PPT 和图形规则分别由 `opening/AGENTS.md` 与 `figures/AGENTS.md` 增补。

## 7. 目录与变更同步

项目内的 `research/`、实验计划/结果、总纲和各级 README 继续按现有权威关系作为知识来源。

| 变更 | 必须同步 |
|---|---|
| 方向、题目、研究内容 | `PROJECT_OUTLINE.md`、根入口、开题正文/材料、`PROJECT_LOG.md` |
| 实现状态或关键接口 | 源码/测试、`code/INFRA_STATUS.md`、证据台账、相关 README、`PROJECT_LOG.md` |
| 实验结论 | 原始结果报告、证据台账、`PROJECT_OUTLINE.md`、相关对外材料、`PROJECT_LOG.md` |
| 目录或关键入口 | 所在目录 README、`PROJECT_INDEX.md`、根 README、`PROJECT_LOG.md` |
| 规则 | 只改拥有该规则的最窄 `AGENTS.md`；全局行为才改本文件，并记入 `PROJECT_LOG.md` |
| 图表 | 图源、导出、`figures/README.md`、对应 audit；影响主线时同步开题/论文引用 |

不要机械修改表中所有文件；只有事实、入口或引用实际受影响时才更新。新增文件必须进入所在目录
README 或已有索引；一次性过程材料放 `tmp/`，不建立新的权威入口。

## 8. Git 与隐私

- commit message 不添加 `Co-Authored-By` 或任何 AI 署名；用户署名只属于项目开发者。
- API key、token、私钥、外部服务器 IP/host、非 localhost 用户名/口令和真实 runtime env 不进入 Git。
- 新连接串使用环境变量；示例只允许 localhost 公共默认值或明显占位符。
- evidence 中的命令、异常和 traceback 在落盘前使用 `code/src/baselines/common/redact.py` 脱敏。
- commit 前运行 `python code/scripts/environment/scan_git_secrets.py`；误报只以最小正则登记到
  `code/scripts/environment/secret_scan_baseline.txt`，真实泄漏立即轮换。
- `.gitignore` 已放行历史 `postgres:postgres@localhost` 公共本地默认；新增内容仍优先使用环境变量。

## 附：历史章节引用兼容

旧计划、结果 README 和项目日志保留了重构前的章节号。读取这些历史引用时按下表跳转；新文档
使用当前章节名称或目标领域规则，不再新增旧编号：

| 历史引用 | 当前入口 |
|---|---|
| 根 §1–3（方向、边界、状态） | 本文件 §2–3；当前事实再查 `PROJECT_OUTLINE.md` 和证据台账 |
| 根 §5、§7.5（实验与运行） | 本文件 §5、`experiments/AGENTS.md`、baseline reference 与目标计划 |
| 根 §6、§6.5（严谨性、文献） | 本文件 §4、§6 与 `research/AGENTS.md` |
| 根 §8（沟通） | 本文件 §2、§6 与 `notes/AGENTS.md` |
| 根 §9–10（变更同步、Git） | 本文件 §7–8 |
