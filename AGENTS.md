# AGENTS.md

本文件只保存**全项目长期规则**。会变化的实现状态、实验数字、运行命令和历史记录分别进入
`code/INFRA_STATUS.md`、`results/EXPERIMENT_EVIDENCE_REGISTRY.md`、平台 runbook 和
`PROJECT_LOG.md`，不在规则文件中复制。

## 1. 规则层级与读取顺序

从根到目标路径逐级加载 `AGENTS.md`；已提供或已读且未变化的内容直接复用。首次进入目录、
查找入口或职责不明时读对应 README，再按问题选择源码、计划、结果或 runbook 的相关部分。
局部文字修订只读受影响内容；接口、研究判断和环境操作再加载对应材料。

子目录只追加本地职责和验证要求，不放宽全项目研究、安全、证据与隐私要求。事实冲突按
“源码/原始结果 → 领域权威入口 → `PROJECT_OUTLINE.md` → README/速览 → 历史计划”核对，修正错误的缓存文档。
规则只记录会改变 agent 行为的要求；清单与状态写入 README 或事实入口。Claude Code 从
`CLAUDE.md` 导入本文件；两者共用规则与事实来源。

| 工作范围 | 追加规则入口 |
|---|---|
| 可复用代码、脚本、测试 | `code/AGENTS.md` |
| 环境、容器、跨机器运行 | `deploy/AGENTS.md`、`deploy/runtime/AGENTS.md` 与目标平台的 `AGENTS.md`/runbook |
| 数据资产与导入 | `data/AGENTS.md` |
| 方法实验的计划与设计 | `docs/plans/AGENTS.md` |
| 某次测试的记录与证据 | `results/AGENTS.md` 及目标记录目录 |
| 动机画像 | `motivation/AGENTS.md` 及目标子目录规则 |
| 可行性与 smoke | `feasibility/AGENTS.md` 及目标子目录规则 |
| 文献和知识文件 | `docs/research/AGENTS.md` |
| 图资产 | `docs/figures/AGENTS.md` |
| 开题与对外材料 | `docs/thesis/AGENTS.md` |
| 长期文档导航与历史归档 | `docs/AGENTS.md`、`docs/design/history/AGENTS.md`、`docs/thesis/archive/AGENTS.md` |

## 2. 项目范围

研究对象是：**PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化**。

- 系统名称为 **SemLoom**；DB-AIEL（Database-Aware AI Execution Layer）只表示架构层，不作为
  Python 类型、函数、包或实验身份的前缀；领域术语以根 `CONTEXT.md` 为准。
- 数据库拥有 SQL、ordinary child plan、snapshot、权限、query cancel/error/result lifecycle；
- 主要架构参照 Sema 一类数据库原生语义算子系统：语义算子进入 SQL、计划和执行生命周期；
- recording `SemMap/SemFilter` 只验证 PostgreSQL carrier 与生命周期；真实语义由数据库内版本化
  `SemanticPlanSpec` 定义，LOTUS v1.2.4 只作可选兼容 profile、算法参照和完整路径 baseline；
- Daft、Ray、vLLM、typed CLIP actor 等位于数据库进程外，作为受数据库管理的可替换 backend；
- 研究内容一是按 token/frame/阶段 work 与局部性组织数据；
- 研究内容二是在固定 request/work capacity 下控制提交、服务实例路由和单租户多 Job 调度；
- 算子代价估计是两项研究内容和数据库计划比较的共同支撑；
- 图像 `AI_EMBED/AI_CLASSIFY` 用于跨模态验证，文本 `AI_COMPLETE` 是首版主场景；
- PostgreSQL + pgvector 的 COPY + deferred index 是写回工程 baseline。

项目不以广泛 PostgreSQL fork、PL/Python 逐行 HTTP UDF、LOTUS DataConnector 外拉、修改 vLLM
continuous batching、修改 Ray scheduler、模型/kernel 优化、传统 GPU 查询算子或单纯产品集成为主线。
允许在 `REL_18_3` extension capability 与 carrier audit 证明目标 LOTUS/Cortex 类优化或稳定 node lifecycle
无法可靠表达后，维护受控的最小 core semantic patch；不得仅为“更原生”改 grammar、storage 或扩大 fork。
“数据库内置”只表示 SQL/planner/query lifecycle 属于数据库，不表示 payload 不会传到外部服务。

## 3. 当前资格顺序

区分独立研发、数据库接入和方法结论的前置条件，不把某一算子的质量实验设为所有工程工作的串行前置项：

1. 自有数据库载体锁定 `REL_18_3`。每个真实算子由数据库拥有 instruction、prompt/parser、
   model/generation 与 NULL/error/order policy，并先建立可验证的同步 reference 与中立 provider seam。
2. SemLoom 核心可在公开 sealed-task producer、fixture 或现有外部 workload 上独立开展行为表征、
   增量 session、数据组织、有界提交、多 Job 和路由研发；不等待 SemFilter 语义质量、真实成本校准、
   第二路径或完整 carrier audit。fixture/外部 producer 必须保留真实身份，不能据此声称已接入 PG。
3. PG port/wire 的多在途、accepted-prefix 与结果重排单独版本化，依赖对应算子的真实语义与同步基线。
   每条实际接入路径须通过 plan/task/result、snapshot/权限、取消/错误、关联/顺序及资源上限检查；
   公共层变化还须验证受影响的旧路径。通过后才纳入该路径的 PG 匹配端到端或 batch-placement 实验。
4. SemFilter reference/optimized 路径的质量与成本比较仍需要独立 reference 质量、matched calibration、
   显式近似授权、algorithm/model role、quality evidence 与 fallback。生成型 SemMap 或独立核心不借用、
   也不等待无关的 Filter 资格；自身实验仍满足适合任务的质量、资源和可比性要求。
5. carrier audit 随真实路径增量进行；已有同步证据不能代替新增 batch/reorder 或第二路径的检查。
   extension 能表达则保留，只有目标 identity/placement/lifecycle 出现已复现阻断才增加最小 core patch。
6. 当前参考公司 demo 的工程经验完成自有系统；未来可将自有算子语义、处理/优化方法和 SemLoom
   执行/调度能力移植到公司系统。算子策略移植与 provider 执行接入分别验证，不能以一次连通替代前者。
   公司接口可提前只读核对；最小 spike、正式移植和环境验证按授权分步推进，不作为自有主实现前置项。
   保持同一套方法与执行核心，代码复用和外部部署遵守本文件 Git/隐私规则。

上述允许独立研发不自动授权模型运行或正式实验；仍先有具体计划、环境、baseline、资源和停止条件。
既有 profiler/manifest、Daft/Ray/static/SAOR 结果保持外部执行或 emulated operator 身份，不自动恢复旧
GPU 矩阵、SAOR、图像动态/HSE。Kalypso-like lineage/KV 只有真实多阶段需求后才另行立项。
当前任务与实现状态只从主计划、`PROJECT_OUTLINE.md`、`code/INFRA_STATUS.md` 和证据台账引用。

## 4. 工作方式与证据纪律

- 按用户目标确定产物、影响结果的假设和完成条件；实现类任务完成修改、相关验证、失败修复及必要
  文档同步后再交付。用户只要求审计、计划或局部修订时，以该范围为完成条件。
- 在已有授权范围内持续完成准备、修改和相关验证；只有超出授权、涉及不可恢复变更，或缺少
  会改变结果的必要信息时再询问。不把阶段结束或通用 skill 的确认步骤当作重新索取授权的理由。
  真实模型/正式实验与公司材料操作仍分别遵守本文件“环境与正式实验”“Git 与隐私”的要求。
- 已确认只使用临时 fixture、无真实模型或外部服务访问的本地验证，可在当前任务内连续运行、修复
  本次改动引起的失败并重跑受影响项；无需逐步确认。失败输出保留，环境与正式实验要求仍适用。
- skill 按具体工作流、协议或工具需要选用；仅有相同关键词不触发加载。多流程 skill 只读取当前
  分支所需材料，已有用户选择与项目规则优先于通用模板。
- 设计新系统机制前，先查 `docs/research/knowledge_hub.md`、文献清单和
  `docs/plans/baseline_reference.md`；新增候选记录到知识库，不把工程直觉伪装成研究空白。
- 代码、计划、结果和对外材料分层保存。历史文件可以保留原始叙事，但必须指向当前替代入口。
- 原始实验数据、失败运行和审计证据默认保留；移动或删除前先检查引用、唯一性与恢复路径。
- 结论标注来源类型：源码、原始实验、论文、官方文档、模拟、推断或待验证。microbenchmark、
  smoke、CPU/fake 与单次 GPU run 不能外推为完整系统结论。
- 当前代码和实验不支持的能力明确写 `pending` 或“尚未实现”，不以计划存在代替实现。

## 5. 环境与正式实验

准备或执行新机器/容器配置、GPU 切换、缺依赖处理、模型或数据下载、数据库导入、单/多 GPU 实验时，
读取 `deploy/runtime/AGENTS.md`、`deploy/runtime/README.md` 和目标平台的规则/runbook；
仅修订文字或分析已有结果时，只读取受影响条款与证据，不触发环境操作。

按 runtime README 确认仓库外 `AI_OPERATOR_ENV_FILE`；缺失时从模板填写真实目标路径，再选择
capability groups 运行只读 `manage_environment.py check` 并保存机器报告。命令与参数以该 README
为准；根据报告分别执行已获授权的安装、下载或 importer，不跳过 preflight。
driver 与 vLLM 环境保持隔离。batch/K/actor/active-work 配置绑定“机器 + 模型/版本 + 服务配置 +
协议 + workload 分布”校准签名；签名变化必须重新做 correctness、scale 和 saturation 校准，正式 run
期间不在线调参。

正式实验同时遵守 `docs/plans/AGENTS.md`、对应计划和以下全局要求：

- baseline 由被测系统拥有执行与调度；SemLoom adapter 只处理 source、sink、质量审计和统一指标；
- GPU-backed database-E2E 优先，CPU/fake 仅作调试、机制隔离或历史对照；
- 记录 upstream URL/commit、实现来源、scheduler owner、适配 diff、server/pgvector 版本、配置签名、
  warm-up、全部重复值、失败/重试和 exactly-once；
- 区分 source、organization、serialization/put、queue/admission、model、fan-in 与 writeback 阶段；
- 运行条件、指标定义和报告模板以 `docs/plans/baseline_reference.md`、
  `docs/plans/reference/experiment_report_honesty_checklist.md` 和目标计划为准；根规则不缓存具体
  K/W、endpoint 拓扑或阈值。

## 6. 文字表达

- 所有新增或改写的文字统一禁用：`冻结|门禁|闭环|边界|约束|合同|产品轨|框架轨|正式点|晋级|失效`。
- 禁用内部代号：`RC[0-9]+|BL[0-9]+|Phase [0-9]+|P0|P1|P2`。用实际对象、条件、动作和结果表达。
- 交付前对本次新增或改写内容执行上述搜索，命中即改写；禁词表只在本节定义。
- 研究对象统一表述为“PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化”。
- 总动机、子问题、证据实验、研究内容和实现任务分清层次；同级内容保持相近粒度。
- 英文缩写、内部结构和指标首次出现时说明中文作用；文献使用正式英文题名，系统名保留英文。
- 初步结果写“可行性依据、观察信号或待扩大验证”，不写成已经完成的贡献。

报告、PPT 和图形的专项要求按任务读取 `docs/thesis/AGENTS.md` 与 `docs/figures/AGENTS.md`。

## 7. 目录与变更同步

项目内的 `docs/research/`、实验计划/结果、总纲和各级 README 继续按现有权威关系作为知识来源。

| 变更 | 必须同步 |
|---|---|
| 方向、题目、研究内容 | `PROJECT_OUTLINE.md`、根入口、开题正文/材料、`PROJECT_LOG.md` |
| 实现状态或关键接口 | 源码/测试、`code/INFRA_STATUS.md`、证据台账、相关 README、`PROJECT_LOG.md` |
| 实验结论 | 原始结果报告、证据台账、`PROJECT_OUTLINE.md`、相关对外材料、`PROJECT_LOG.md` |
| 目录或关键入口 | 所在目录 README、`PROJECT_INDEX.md`、根 README、`PROJECT_LOG.md` |
| 规则 | 只改拥有该规则的最窄 `AGENTS.md`；全局行为才改本文件，并记入 `PROJECT_LOG.md` |
| 图表 | 图源、导出、`docs/figures/README.md`、对应 audit；影响主线时同步开题/论文引用 |

### 7.1 文档生命周期

- 新建或替代前用 `rg` 查同主题内容，明确其唯一所属目录、用途及当前/历史状态。规则放能覆盖任务
  的最窄 `AGENTS.md`，事实放既有权威文件，清单放 README，临时材料放 `tmp/`。
- 新文档登记到已有 README/索引；仅新增局部行为要求时增设子目录规则，不另建平行规则或索引。
- 替代时让当前入口只指向新文档；旧文档开头标明历史状态和替代入口。更新现行引用，保留历史日志
  与原始证据引用。移动或删除前检查反向引用、唯一内容与恢复路径；只有内容重复或可重生成且无现行引用时才删除。
- 验收核对登记、替代关系与必要同步，执行 §6 禁词搜索、Markdown 本地链接检查和 `git diff --check`；
  准备进入 Git 时再运行隐私扫描。检查通过后才算完成；同步表只应用于实际受影响的事实与入口。

## 8. Git 与隐私

- commit message 不添加 `Co-Authored-By` 或任何 AI 署名；用户署名只属于项目开发者。
- API key、token、私钥、外部服务器 IP/host、非 localhost 用户名/口令和真实 runtime env 不进入 Git。
- 公司内网 fork 的复用/修改权限不自动包含外部发布或部署权限；公司源码、二进制、容器、测试数据
  和日志进入公开仓库或 AutoDL 等外部环境前，须有覆盖材料和目标环境的明确授权。未获授权时留在
  获批内网环境，公开实现依据公开接口/资料并保留可说明的来源。
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
| 根 §1–3（方向、范围、状态） | 本文件 §2–3；当前事实再查 `PROJECT_OUTLINE.md` 和证据台账 |
| 根 §5、§7.5（实验与运行） | 本文件 §5、`docs/plans/AGENTS.md`、baseline reference 与目标计划 |
| 根 §6、§6.5（严谨性、文献） | 本文件 §4、§6 与 `docs/research/AGENTS.md` |
| 根 §8（沟通） | 本文件 §2、§6；沟通带来的有效信息直接吸收进对应主题文档 |
| 根 §9–10（变更同步、Git） | 本文件 §7–8 |
