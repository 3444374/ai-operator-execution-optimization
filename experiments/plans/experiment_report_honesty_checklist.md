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

- **读** `code/src/baselines/common/provenance.py::ComparisonRole` Literal，现有值：`service_ceiling / direct_client_control / framework_native_baseline / database_product_native_baseline / project_scheduled_method`。
- **不存在的值（别写）**：`harness_sharded_diagnostic`、`gateway_system_diagnostic`、`project_method_diagnostic`——协议里有这些**描述**，但 ComparisonRole Literal 里没有；要加先改 Literal + 测试，不要偷偷写。
- **读** `bounded_output_duckdb_comparison_protocol_20260805.md §2.6` 三轨：① 单 endpoint 产品语义轨 ② 双 endpoint 方法/control 轨 ③ **可选 gateway 完整系统轨**（DuckDB 单 BASE_URL 经第三方 gateway → 多 endpoint）。
- **harness 预切 ≠ gateway 轨**：2 个独立 DuckDB 进程（harness 预切 manifest）是 `harness_sharded_diagnostic`；1 个 DuckDB 进程经 nginx round-robin 是 **gateway 完整系统轨**（§2.6 line 112）。lb_rr 是后者。
- `scheduler_owner` 纳入**所有**调度方：lb_rr = `duckdb_ai_extension + nginx_round_robin + vllm`；2-shard duckdb = `experiment_harness + duckdb_ai_extension + vllm`。不要只写一个。
- ramp 层 identity sidecar：`comparison_role` 用单 shard 角色（Literal 内）+ `ramp_layer_classification`（harness_pre_split / gateway_system）+ `formal_baseline_eligible=false`（ramp 层编排不进产品原生 formal）。
- **反例**：1cd52be 写 `project_method_diagnostic`（不在 Literal，应 `project_scheduled_method`）+ lb_rr 标 `harness_sharded_diagnostic`（lb_rr 是 gateway 轨）。

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

## 写每个新报告/数据 cell 前的勾选流程

1. ☐ 吞吐：vLLM counter Σ(prompt+gen) / group_service_wall_s？（lb_rr: ttft 两后端）—— 不是 total_tokens/max_jct/分片求和。
2. ☐ 身份：ComparisonRole Literal 内的值？协议 §2.6 哪轨？scheduler_owner 全（DuckDB+nginx+vLLM/harness）？
3. ☐ 结论：cache 统一才 regime？finish 空≠审计？n+TOST？根因有 service-counter 证据？
4. ☐ 统计：sample stdev(n-1)？p 用对 reps（不跨实验）？
5. ☐ 证据：raw `git ls-files` tracked？引用文件确认提交？
6. ☐ 文档：读了 §7.5/§6/协议/provenance/deploy 才下笔？
