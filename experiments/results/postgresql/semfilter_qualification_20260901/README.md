# 校准前小切片：秩检查与普通统计通过，reference 语义资格未通过

本文件是内部实验与修复记录，来源为源码、PG18.3 实测和真实模型响应。对应语义算子代价估计的
前置验证，不是整轮校准、质量基准测试或第二 physical path 的结果。

后续 prompt/model 诊断见[单一分类 prompt 对照](../semfilter_prompt_qualification_20260901/README.md)。
下文保留本轮原始结果；其中 12/27 表示 **9 个独立样例中 4 个符合预期、各重复三次**，不是
27 个独立样本的准确率。新实验没有修改本报告的原始证据或标签。

| 检查 | 结果 |
|---|---|
| calibration builder 可辨识性 | 已修复；完全共线、近似共线和组合病态均拒绝，10/10 合同测试通过 |
| PG18.3 普通谓词统计 | 无 AI 条件时 estimate 从 8 改为 64；actual 始终 64 |
| reference 输出资格 | choice 格式 30/30，但预先规定的语义预期只符合 12/27，未通过 |

因此整轮校准继续暂停。没有访问 held-out payload，没有拟合真实数据、生成新 calibration artifact，
也没有将 choice 接入生产 SQL、`SemanticPlanSpec` 或 wire v3。

## 1. 目的、配置与实验设计

唯一实施来源为[架构计划工作包五的小切片](../../../plans/postgresql_ai_semantic_operator_architecture_20260827.md)。
代码与请求前登记的样例/要求均绑定 `6c111b24`；原失败采集 `c77c1441` 和原始证据保持不变。

- 秩检查走公开 `build_reference_calibration()`，以旧提交与修复提交处理同一四行合成输入，不写 artifact。
- 统计验证使用新建 PG18.3 集群，只导入公开 manifest 的 `doc_id/split/cell`；不加载真实文本或调用模型。
- reference 使用原 Qwen2.5-1.5B-Instruct 文件、vLLM 0.25.1、单 RTX4090、BF16、TP1、eager、prefix cache
  off，context/token budget 4096、max sequences 1、GPU memory utilization 0.25。模型、PID/start-time、
  cmdline 与文件 SHA 在请求前后核对；软件环境未安装新依赖。
- 同一 instruction、prompt 与严格 C parser；两种配置仅相差 `structured_outputs.choice`。
  每配置各 10 条输入 ×3 重复，交错顺序 seed=20260901，另各 1 条预热并保存。9 条工程构造样例的预期
  标签在运行前登记，原失败输入只重放并检验格式，不事后猜语义标签。
- 通过要求预先定为 30/30 通过生产 C parser、27/27 满足预先规定的判断。三次重复不算三个独立语料样本。
  格式失败继续记录，不 trim、重试或改写 completion；不访问校准 held-out 部分。

这是一次小样本资格测试，不是人工标注语料上的泛化准确率、模型排名、吞吐或成本精度评价。
没有运行调度优化臂。依据为 [vLLM 0.25.1 structured outputs](https://docs.vllm.ai/en/v0.25.1/features/structured_outputs/)
和 [PostgreSQL 18 CREATE STATISTICS](https://www.postgresql.org/docs/18/sql-createstatistics.html)。

## 2. 可辨识性修复

旧 builder 的最小反例：

| calls | prompt tokens | output tokens | service ms |
|---:|---:|---:|---:|
| 90 | 360 | 180 | 316 |
| 60 | 1260 | 120 | 316 |
| 100 | 1100 | 200 | 420 |
| 15 | 270 | 30 | 82 |

`output_tokens=2*calls`，两个系数不能分别识别。旧实现仍产生 fixed=10、call=1、prompt=0.1、output=1，
且同关系的 held-out 误差为 0；预测恰好正确不代表参数可辨识。有限 Decimal 精度的正规方程消元留下
伪非零主元，不能用 `pivot == 0` 可靠识别秩亏。

`6c111b24` 先增加精确有理数消元与归一化主元检查。最终合成反例复核又证明，仅看单个主元会遗漏
多列共同退化；后续修复因此检查整个矩阵：每列先除以最大绝对值，精确形成 `G = XᵀX` 并求逆，
奇异则拒绝，`||G||∞ × ||G⁻¹||∞ ≥ 1e16` 也拒绝。Gram 的形成和求逆都用 Fraction，不让舍入误差
制造伪秩。该整体条件数上限独立于量纲与 held-out 误差，**不是 SVD condition number**。
有效 fixture 的原有系数、artifact schema/identity 和误差要求不变。

最初新旧公开接口对照见 [rank-repro.json](raw/rank-repro.json)；最终 10 项测试还覆盖单 token 扰动的
近似共线、组合病态、零/常量维度、量纲变化、行序变化和原有 held-out/identity/CLI 行为。
最终补充只使用合成数据，不重跑模型或 held-out。这里只拒绝不可靠拟合，没有自动降维。
最终提交为 `44f6632c`，组合病态的新旧对照见 [condition-repro.json](raw/final-rank/condition-repro.json)。

## 3. 普通 SQL 多列统计

实际导入计数为：总数 1216，warmup 64，cell=0 为 160，两者共同命中 64。
独立性估计 `1216 × (64/1216) × (160/1216) ≈ 8.42` 与 PG 原估计 8 相符。

```sql
EXPLAIN (ANALYZE, FORMAT JSON)
SELECT doc_id FROM calibration_inputs
WHERE split='warmup' AND cell=0;

CREATE STATISTICS calibration_split_cell (mcv, dependencies)
ON split, cell FROM calibration_inputs;
ANALYZE calibration_inputs;
```

同一普通查询再次执行，estimate=64、actual=64。[stats.json](raw/stats.json)保存两个完整 EXPLAIN。
没有 AI 条件，没有修改 core、semantic path cost 或把行数常量写入执行器。这证明本次采集表上的普通
统计可以修正该相关谓词估计，不外推为任意查询或未来 workload 都能精确估计。

## 4. Reference 格式与语义结果

| 配置 | 格式通过 / 30 | 预期判断符合 / 27 | output token 分布 | 资格 |
|---|---:|---:|---|---|
| 原 generation | 27 | 12 | 2 tokens ×30 | 未通过 |
| choice 候选 | 30 | 12 | 2 tokens ×27；4 tokens ×3 | 未通过 |

原失败输入三次都复现 3-byte 非法输出；choice 下三次均返回合法 `TRUE`。没有给这条原始输入补标签，
因此这里只能说格式问题得到控制，不能说模型语义判断已修复。

9 条预先构造样例在两种配置下三次结果都一致：

| 样例 | 预期 | 实际 |
|---|---|---|
| Python 加法函数 | TRUE | TRUE |
| 解释 SQL COUNT | TRUE | TRUE |
| 修复 JavaScript 加法 | TRUE | UNKNOWN |
| 番茄汤食谱 | FALSE | UNKNOWN |
| 山的短诗 | FALSE | UNKNOWN |
| 法国首都 | FALSE | UNKNOWN |
| `Please help me fix it.` | UNKNOWN | UNKNOWN |
| `Can you explain this?` | UNKNOWN | TRUE |
| 未说明要写什么 | UNKNOWN | UNKNOWN |

完整 62 次响应（含 2 次预热）见 [responses.jsonl](raw/responses.jsonl)，摘要见
[reference-summary.json](raw/reference-summary.json)。格式判断调用未修改生产 `sem_filter_exact_machine_methods`
的 C 函数，不是另写的 Python parser；[parser-controls.json](raw/parser-controls.json)记录合法标签、
3-byte 非法值、空串和多余空白的控制检查。

两个 [plans.json](raw/plans.json) 具有不同的 versioned generation profile 和完整候选 plan digest。
它们明确标记 `production_pg_plan=false`：本轮是直接 fixed-adapter/HTTP 资格测试，不是 choice 已进入
PostgreSQL SQL 执行链路的证据。由于语义资格未过，没有升级生产 schema-v2 plan 或 wire v3。

baseline 的输出 usage 固定为每调用 2 tokens，但该 profile 未通过语义资格；choice 还出现 4 tokens。
因此不能为了生成 artifact，把这组小样本直接改拟合成较简单模型。若未来合格 reference 的输出确实
固定，应该明确采用合并 call/output 成本的模型身份，并独立验证，而不是强行保留四个自由系数。

## 5. 回归、归档与资源清理

`6c111b24` 在 PostgreSQL 18.3 上通过 warning-free `-Werror` build、regression 1/1、TAP 437/437；
Python 合同为 PostgreSQL 45/45、gateway 5/5、calibration 9/9，总计 59/59。
干净构建 `.so` 与实际测试使用的已安装 `.so` 哈希一致，regression actual/expected 原始字节一致。
原有 Map、recording Filter、exact Filter、事务、权限、取消与恢复仍由完整 TAP 检验。

最终数值补充 `44f6632c` 又独立通过 PG18.3 warning-free `-Werror`、regression 1/1、TAP 437/437、
Python 60/60（45+5+10）；见 [最终资格](raw/final-rank/qualification.json)和
[补充清单](raw/final-rank/SHA256SUMS)。该补充没有真实模型调用，其 `model_calls=0` 不包括 TAP 中的
测试 provider，也不抹去上文已完成的 62 次模型请求。两次 PG 测试与对应源码分别绑定，不混用数量。

来源见 [qualification.json](raw/qualification.json)、[日志](raw/logs/)和 [SHA256SUMS](raw/SHA256SUMS)。
公开日志只做路径/敏感信息脱敏和行尾空白规范化；仓库外包
`semfilter_reference_qualification_c77c1441_20260901_r1` 保留原始日志、二进制、原始响应、服务身份和
停止后的 PGDATA。raw 中的脚本是本次一次性验证快照，不是新生产 CLI 或并列实施计划。
最终数值补充的仓库外包为 `semfilter_rank_condition_44f6632c_20260901`；
`raw/SHA256SUMS` 只覆盖原 `6c111b24` 子集，`raw/final-rank/SHA256SUMS` 覆盖补充子集，两者独立校验。

本次模型 endpoint、普通统计 PG 集群和 TAP 节点已停止；原失败校准包的完整 manifest 再次校验通过。
清理只针对本切片，不涉及其他历史 worktree 或 PostgreSQL 18.4。

## 6. 下一步

秩检查和普通统计两项已经解决；reference 还缺语义资格。接下来应单独诊断 prompt/instruction/model
组合为何在明确反例上仍输出 UNKNOWN，并用独立样例检验任何改进。不得放宽严格 parser、改写预期标签
来通过测试、偷偷换模型重跑原校准，或用这 9 条工程样例宣称真实语料准确率。三项全部具备资格前，
不恢复整轮采集，不发布真实 artifact，不开始第二 physical path。
