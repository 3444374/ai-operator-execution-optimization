# code/AGENTS.md

本目录用于可复用工程代码、实验驱动脚本和测试入口。进入本目录前先读根目录 `AGENTS.md`。

## 作用

- 存放可复用系统实现，而不是一次性 notebook 或临时 CSV。
- 承载 PostgreSQL connector、fake/real `AI_EMBED` pipeline、Ray/Daft/Lance 类外部执行链路、优化实现和测试入口。
- 为 `feasibility/` 和 `motivation/` 提供可复用脚本，但正式结果仍写入对应结果目录。

## 边界

- 不在 `code/` 堆临时 CSV、原始实验结果或一次性 notebook。
- 绘图、图表复现和素材筛选脚本归 `figures/scripts/`；`code/scripts/` 只放实验主体、服务启动、数据采集和 profiling 入口。
- 连接验证结果归 `feasibility/results/`。
- 系统画像、瓶颈定位和动机结果归 `motivation/results/`。

## 规则

- 所有代码应有可运行入口、配置、结果输出和最小验证。
- 不提前引入复杂框架；只有当重复逻辑稳定出现时才抽象。
- 每条真实运行 CSV 必须记录实际 `server_version` 和 `pgvector_version`，不能靠目录名推断平台。
- Python baseline、Ray task、Ray actor、模型服务等 baseline 要尽量共享同一数据读取和写回路径，避免 baseline 不可比。
- 完成代码实现或功能测试后，按根规则同步更新 `learning/` 中的学习讲解。

## 编码规范（2026-07-23 综合评估产出）

以下规则从项目文献精读笔记、Wiki 知识库和已有实验教训中提取，新代码必须遵守。

### 1. 保持简单（来源：Ray ConcurrencyCapBackpressurePolicy 废弃教训）

Ray Data 的 `ConcurrencyCapBackpressurePolicy` 用 ~400 行实现了 EWMA + deadband 自适应并发控制，性能反而不如简单方案，已被官方废弃。

**要求**：
- 自适应策略第一版 <100 行（不含 dataclass 和 abstract 定义）
- 优先用简单规则（静态阈值、固定步长），只在数据证明必要时再增加复杂度
- 不要提前做"可配置性"——先用硬编码值跑通，确认有效后再参数化

**文献依据**：`research/ray_actor_dynamic_batching_reference.md` §3.7

### 2. 每行是一个独立完整的请求（来源：vLLM Chunked Prefill 语义安全分析）

vLLM 的 `--enable-chunked-prefill` 在 token 级做数学等价拆分；手动将一份 prompt 拆成多条请求会导致 KV cache 隔离、上下文断裂。

**要求**：
- Token-budget 策略决定"多少行合并为一个 batch"——每行仍是独立完整的 vLLM 请求
- **禁止**在 Ray actor/Daft UDF 中自动拆分单行 prompt 内容为多条请求
- 超长单行（超过模型 context window）只能：预处理截断、独占一个 batch、或从数据集排除
- 代码中涉及文本拆分的地方必须注释：这是"行间合并"还是"行内拆分"——前者 OK，后者禁止

**文献依据**：`experiments/plans/data_organization_batching.md` §2.5.7；vLLM SOSP 2023 论文

### 3. 策略层不依赖引擎层（来源：DataOrganizer 抽象设计）

当前 `organizers.py` 通过 `ArrowOrganizer` / `DaftOrganizer` + `BatchRequest` 元数据实现了引擎隔离。

**要求**：
- 策略代码（admission、routing、request_pool）只依赖 `BatchRequest` 抽象（`prompt_tokens_sum`、`row_count`、`prefix_key` 等），不感知底层是 Arrow 还是 Daft
- 新增引擎后端时保留旧后端作为对照/回退（如 Daft 接入时保留 Arrow）
- 不在策略代码中直接调用 `daft.*` 或 `pyarrow.*` API

**文献依据**：`experiments/plans/strategy_design_implementation_reference.md` §4.2

### 4. 多模态复用文本代码路径（来源：策略泛化性验证设计）

Token-budget 和 frame-budget 本质都是"按计算量预算组织数据"——合并逻辑相同，仅计数函数不同。

**要求**：
- `token_budget` → `frame_budget` 仅替换计数函数：`_row_token_cost()` → `_row_frame_cost()`
- 分组策略（length-align、bin-packing、prefix-aware）的排序/合并逻辑不变
- 不为图像单独写一套 batch 构造代码

**文献依据**：`research/daft_ray_multimodal_reference.md`；Daft DataFrame 统一 API

### 5. 文献优先——新机制从精读笔记提取（来源：文献优先设计方法论）

项目已有 57 篇 CCF-A 文献清单 + 16 篇精读笔记。凭空设计的机制大概率重复已有工作或漏掉已知最优 practice。

**要求**：
- 新增策略机制前，先查 Wiki `文献地图` MOC 中对应研究岛的论文
- 代码注释标注每个非平凡设计决策的文献来源（如 `# Ref: Clipper NSDI 2017, AIMD adaptive batching`）
- 如果找不到文献依据，标注为"工程决策"并在实验报告中说明

**文献依据**：`research/README.md` §文献优先设计方法论；Wiki `设计方法论` MOC

### 6. 新实验指标完整性（来源：指标盲区审计）

所有 07-18/19 实验用 `rows/s` 作为主吞吐指标，但 AI_COMPLETE 中每行 token 量可差 13.9×。

**要求**：
- 所有新实验 CSV 必须包含 `tokens/s`（从 vLLM Prometheus `prompt_tokens_total` + `generation_tokens_total` 计算）
- 必须包含 `service_p99`（系统性 tail latency）
- 涉及 adaptive 的实验必须记录 inflight/queue 时间序列
- 涉及分组策略的实验必须记录 per-request e2e latency 分布
- 不重跑已有实验仅为了补指标——已有 CSV 中 vLLM Prometheus 数据可做事后计算

**文献依据**：`experiments/plans/experiment_status_and_gaps.md` §3

详细脚本说明见 `scripts/README.md`。依赖清单见 `requirements.txt`。
