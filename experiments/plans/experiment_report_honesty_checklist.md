# 实验报告与数据诚实性检查清单（写报告/数据/身份前必读必对照）

> **来源**：codex 第一轮审计 + 复审两轮反复指出的**共性**错误。每条都配"我犯过的反例"。写任何多卡/吞吐/身份/统计/证据结论前，**逐条对照**；违反就是反复犯的"口径不一致 / 身份凭印象 / 过强结论 / 统计错 / 证据状态声称错 / 不读文档"。
>
> 这不是新规则，是把 `AGENTS.md §7.5/§6/§5` + `bounded_output_duckdb_comparison_protocol_20260805.md` + `provenance.py::ComparisonRole` 已经写明、但我反复没做到的点，落成可勾选清单。

## 1. 吞吐口径（service tokens/s）—— 反复错的最多次

- **统一用 vLLM counter**：`Σ(vllm_prompt_tokens_delta + vllm_generation_tokens_delta) / group_service_wall_s`（gate 臂从 `gate.json::metrics`；lb_rr 从 `ttft_metrics.json` 两后端 Σ delta / shard wall）。
- **禁用**：
  - `total_tokens / max_jct`（高估 gate 臂——max_jct 是单 shard jct，非 group wall）；
  - 分片速率求和 `tok0/wall0 + tok1/wall1`（两 shard jct 不等时高估，等价于按各自更短耗时加权）；
  - duckdb `summary.total_tokens`（输入 token 估算，`output_tokens=0` 是"未暴露输出"，不是"输出为 0"——漏 generation/chat-template，~5% 低估）。
- **反例（我犯过）**：saturated 分片求和 → group（+0.8% 误为 +2.55%）；Phase 2 `total/max_jct`（误排 duckdb>project → group 下 bounded>project>duckdb）；lb_rr `total_tokens`（256: 47708 → ttft 50186）。

## 2. 身份 / provenance —— 必须读 Literal + 协议，不凭印象

- **读** `code/src/baselines/common/provenance.py::ComparisonRole` Literal，现有值（**7**）：`service_ceiling / direct_client_control / framework_native_baseline / database_product_native_baseline / project_scheduled_method / harness_pre_split_diagnostic / gateway_system_diagnostic`（后两个是 ramp 层 composed-system 角色，复审第三轮扩展）。
- **不存在的值（别写）**：`harness_sharded_diagnostic`、`project_method_diagnostic`——协议/旧文档有这些**描述**，但 Literal 里没有（harness 预切写 `harness_pre_split_diagnostic`，gateway 写 `gateway_system_diagnostic`）。要加新值先改 Literal + 测试。
- **读** `bounded_output_duckdb_comparison_protocol_20260805.md §2.6` 三轨：① 单 endpoint 产品语义轨 ② 双 endpoint 方法/control 轨 ③ **可选 gateway 完整系统轨**（DuckDB 单 BASE_URL 经第三方 gateway → 多 endpoint）。
- **harness 预切 ≠ gateway 轨**：2 个独立 DuckDB 进程（harness 预切 manifest）= `harness_pre_split_diagnostic`；1 个 DuckDB 进程经 nginx round-robin = `gateway_system_diagnostic`（gateway 完整系统轨，§2.6 line 112）。lb_rr 是后者。
- `scheduler_owner` 纳入**所有**调度方：lb_rr = `duckdb_ai_extension + nginx_round_robin + vllm`；2-shard duckdb = `experiment_harness + duckdb_ai_extension + vllm`。不要只写一个。
- ramp 层 identity sidecar：`comparison_role`（**STANDARD 主字段**）= 系统角色（harness_pre_split / gateway_system / project_scheduled / direct_client）；`component_comparison_role` = 单 shard 组件（database_product_native_baseline）；`formal_baseline_eligible=false`（ramp 层编排不进产品原生 formal）。**复审 #1：comparison_role 主字段必须 = 系统角色，不能留 component**（否则通用消费者读主字段误判产品原生）。
- **反例（已修）**：曾写 `project_method_diagnostic`（不在 Literal，应 `project_scheduled_method`）+ comparison_role 留 `database_product_native_baseline` 而系统角色只放侧字段 `system_comparison_role`/`ramp_layer_classification`（通用消费者读主字段仍误判 → 现主字段 = 系统角色，component 移 `component_comparison_role`）。

## 3. 结论强度（cache / finish / 统计 / 根因）—— 不过头

- **cache regime**：只有 cache 控制统一（同 warmup/reset 协议 + 随机化 scale 顺序 + ≥1w+3f）才声称"跨执行路径 regime / 干净的系统级发现"。lb_rr `warmup_per_cell=false` + 规模嵌套继承 + 三臂 warmup 方式不同 → 只能说"**本次 run 内**吞吐下降与 prefix-hit 下降**相关**"。
- **finish_reason**：requests.csv 字段**空** ≠ "已审计为非 length"。只能说"0 error、未观察到 max_tokens/truncation 错误"。
- **统计等价/优越**：n≥3 + 预注册 equivalence/superiority margin + TOST 才声称。"未检出差异"≠"证明等价"；p<0.05 不等于"优越"（尤其 vs harness diagnostic 非产品）。
- **根因**：per-run 证据未审计 / 无 service-counter → "疑似/推断"，不"已证"。
- **反例**：lb_rr "prefix-cache thrash 跨执行路径 regime / 干净系统级发现"（cache 不统一，撤回）；finish_reason=length=0（空≠审计）；"单入口固有极限"（lbrr64 per-run 未审计）。

## 4. 统计

- **sample stdev（n-1 分母）** 用于推断（n=3）。不用 population stdev（低估）。
- **p 值用对 reps**：不跨实验误用（rich 的 p 用到 saturated）。
- Welch t（不等方差）+ Welch-Satterthwaite df；reps 不重叠 → p 应小（方向自检）。
- **反例**：saturated population stdev（447/569/597）→ sample（548/698/731）；p=0.127（rich）误用 saturated → 0.0284。

## 5. 证据状态 —— 提交了才声称

- **raw git tracked** 才声称"可复现/可审计"。写报告前 `git ls-files <dir>` 确认。
- 引用文件前确认提交状态（`formal.log/sweep.log` 被 .gitignore；`ps8_collapse` 实际已跟踪——别说"未提交"）。
- raw 裁剪版（summary/gate/ttft/ramp_run/agg，**排 requests.csv**）提交到仓库 output_root + ramp_aggregate 重生成入仓库。
- nginx config / 实际运行 config 是 provenance，提交快照或 README 内联。
- **反例**：Phase 2 只 README（raw 在服务器）→ 不可复现；ADDENDUM 说 ps8_collapse 未提交（实已提交 114 files）。

## 6. 文档（写报告前必读，不凭记忆）

- `AGENTS.md §7.5`（实验执行与结果记录流程 7 步）/ `§6`（严谨性）/ `§5`（实验规则）/ `§6.5`（文献优先）。
- `experiments/plans/bounded_output_duckdb_comparison_protocol_20260805.md`（身份三轨 + gateway + ComparisonRole 语义 + 计时边界）。
- `code/src/baselines/common/provenance.py`（ComparisonRole Literal——写身份前 grep 这个文件）。
- `deploy/autodl/README.md`（runtime/profile/资产 + §9.1 calibration 模板 + §10.5 Ray + §2.3 paramiko）。
- `experiments/plans/baseline_reference.md`（数据库 AI 算子评价指标合同）。
- **不凭印象写身份/口径/角色/统计**——读 Literal/协议/§7.5 确认后再下笔。

## 7. Gate 性补充（复审第三轮：从"人工提醒"升级为可靠 gate）

下列四项不只人工对照，要在**代码/落盘层**强制（本仓库已部分实现，剩余标注）：

- **7.1 计时粒度不混名**（复审 #5）：`request-level` / `query_barrier` / `group wall` 是不同边界。DuckDB-ai `timing_granularity=query_barrier`，summary 的 `latency_p50/p95/p99` 全等于整条 SQL JCT，**不是 per-request E2E**——aggregator 输出 `timing_granularity` 字段，不把 query_barrier JCT 标成 `request_e2e`。（**待补**：aggregator 加 timing_granularity 透传。）
- **7.2 fail-closed 优先**（复审 #3/#5/#1）：缺 service counter（无 group gate.json + 无 ttft）→ `metric_unavailable`（不生成可排名数字）；缺 balance 指标（ttft_deltas 空）→ cell fail（不 passed）；缺 identity sidecar → aggregate 主角色 = null（不回退 product-native component role）。**已实现**。
- **7.3 同源文档传播**（复审 #4）：改一处结论必须全局 grep 同步——`experiments/results/multicard_*/README.md` + ADDENDUM + `PROJECT_INDEX.md` + `PROJECT_LOG.md` + `overview/`。本次教训：只改 lb_rr 正文，INDEX/LOG/其他 README 残留旧结论（harness_sharded_diagnostic/length=0/regime）。改前 grep，改后再 grep 残留=0。
- **7.4 证据运行身份**（复审 #6）：报告引的"可复现"必须落到**真实存在的文件**——actual_run_config.json（非 example，warmup_per_cell 实际值）+ commit hash + gateway version（nginx 1.18.0）+ 配置 sha256 + identity sidecar（system_comparison_role）。不能"未来代码已支持"当已闭环——历史 raw 无 sidecar 则 aggregate null，报告必须标"未机器闭环"。

## 写每个新报告/数据 cell 前的勾选流程

1. ☐ 吞吐：vLLM counter Σ(prompt+gen)/group_service_wall_s？（lb_rr: ttft 两后端）—— 非 total_tokens/max_jct/分片求和；缺 counter → metric_unavailable（不排名）。
2. ☐ 身份：ComparisonRole Literal 内的值（含 harness_pre_split/gateway_system）？协议 §2.6 哪轨？system_comparison_role（主）+ comparison_role（component）+ scheduler_owner 全？identity sidecar 真实存在（非"未来支持"）？
3. ☐ 结论：cache 统一才 regime？finish 空≠审计？n+TOST？根因有 service-counter 证据？
4. ☐ 统计：sample stdev(n-1)？p 用对 reps（不跨实验）？
5. ☐ 证据：raw `git ls-files` tracked + actual_run_config + gateway version/sha + identity sidecar 真实存在？
6. ☐ 计时粒度：timing_granularity 输出？query_barrier JCT ≠ request_e2e？
7. ☐ 同源传播：改后 grep 全局残留（README + INDEX + LOG + OUTLINE）= 0？
8. ☐ 文档：读了 §7.5/§6/协议/provenance/deploy + 本 checklist 才下笔？

> **登记**：本 checklist 是 `AGENTS.md §7.5`/`§6` + `bounded_output_duckdb_comparison_protocol_20260805.md` + `provenance.py::ComparisonRole` 的可勾选投影，登记于 `PROJECT_INDEX.md` 与 `experiments/plans/README.md`；写多卡/吞吐/身份/统计报告前强制对照（人工），代码层 fail-closed 由 aggregator/driver 强制（§7.2）。

