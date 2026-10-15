# Observability cores

This package owns passive measurement and stable profiling schemas. Measurement code must not take
over the execution policy it observes.

- `request_gateway.py` is the five-arm Job-labelled HTTP pass-through. It forwards each body exactly
  once with no admission limit, retry, cache, route choice, or payload rewrite, then records lifecycle
  clocks and endpoint-reported token usage for cross-framework tail/fairness metrics.
- `metrics/` contains reusable metric collectors and summaries.
- `profiling/` contains the PostgreSQL AI operator profiler's stable result schema and helpers.

The gateway is an observation boundary, not a baseline executor or scheduler. Any future queue,
backpressure, batching, or retry belongs in the measured system and must not be added here.

`process_resources/` provides Linux FD identity observations, explicit missing values, sampling windows,
stable baselines, operation/error capture and gzip JSONL persistence. A tick is a sequential observation
batch, not an atomic snapshot. PostgreSQL session attribution and threshold policies live under
`src/experiments/postgresql/`; the collector makes no qualification decision.

<a id="运行指标的读法"></a>

## 运行指标的读法

测量字段以[profiler schema](profiling/schema.py)和各采集器为准。报告同时解释请求形状、服务压力、
完整结果、资源与任务质量：prompt/output token、批次行数与字节说明工作量；排队、在途与缓存
说明服务状态；完整查询时间及首token、逐token等待说明用户实际等待；CPU、GPU和能耗说明资源使用。

子阶段计时必须说明起止事件和是否重叠，跨进程计时先核对时钟条件。请求级时延、分片结束时间和
SQL查询完成时间分别保存；采样窗口未捕获短任务时保留采样范围与实际请求记录。

生成任务用答案质量、非空输出、截断与失败解释正确吞吐；embedding任务说明向量维度、数值差异与
相应质量验证。未采集字段记录原因，重复与失败分别保留。研究中的指标选择依据见
[指标调研](../../../docs/research/evaluation_metrics_survey_20260731.md#观察变量选择)。

<a id="repeat-mean-confidence"></a>

## 重复均值的置信区间

`metrics.statistics.repeat_summary` 返回 `mean ± critical × sample_stdev / sqrt(n)`，
其中 `critical` 对应双侧 95% 区间的 `t(0.975, n-1)`。公式与分位点定义见
[NIST 均值置信区间](https://www.itl.nist.gov/div898/handbook/eda/section3/eda352.htm)和
[t 临界值表](https://www.itl.nist.gov/div898/handbook/eda/section3/eda3672.htm)。
无样本与单次重复继续分别标为 `unavailable:no_formal_repeats` 和 `single_repeat:no_interval`；
单次的上下界占位值不表示存在置信区间。

实现不新增运行依赖：df=1–30 逐项查表，另保留 40、60、120、1000、10000、1000000 六个锚点。
表值向上保留六位小数；缺档取较小 df，超过最大锚点继续取该有限 df 的值。
由于同一上尾分位点随 df 增加而下降，所得半宽相对精确 t 区间保守，所有正整数 df 的额外半宽
小于 1.1%。常量经本机既有 SciPy 1.13.1 `t.ppf(0.975, df)` 和 mpmath 1.3.0
45 位精度不完全 beta 函数反演交叉核对；二者仅用于离线验证，不是运行或测试依赖。
半宽先合并 `critical / sqrt(n)` 再乘样本标准差，避免可表示区间在中间乘除时发生可规避的
浮点溢出或下溢。

以下修前值来自 `277d8f4862c291bc2830cc6622f5a0b4f28b5c90` 的实际函数，误差均相对精确 t 半宽：

| 重复数 n | df | 修前临界值 | 修后保守临界值 | 高精度参考 | 修前半宽误差 |
|---:|---:|---:|---:|---:|---:|
| 3 | 2 | 4.303 | 4.302653 | 4.302652729749464 | +0.0081% |
| 12 | 11 | 2.131 | 2.200986 | 2.200985160091640 | −3.1797% |
| 32 | 31 | 1.96 | 2.042273 | 2.039513446396409 | −3.8986% |

n=3 只有旧表的取整差，未发生跨档；n=12 修后比精确半宽大约 0.000038%，n=32 大约 0.1353%。
原始时间、均值、中位数、标准差及独立的 nearest-rank P99 均不因本修复改变。

发布影响核查固定于上述提交：唯一生产调用者是
[`summarize_formal_repeats.py`](../../scripts/analysis/summarize_formal_repeats.py)。对 Git 对象中的
`experiments/`、`docs/`、`figures/` 及根现行报告搜索其 `ci95_lower`、`ci95_upper`、
`repeat_summaries` 输出字段，并只读扫描 2768 个压缩文件、15105 个归档成员
（4506690994 个解压字节），均无匹配且解压无错误。未确认任何已发布摘要由该调用者产生，
因此没有离线重算或改写历史摘要、原始观测；此结论不覆盖仓库外或后续生成的报告。
历史 `ci95_half_width` 同名统计不代表使用过本函数，例如 2026-07-25 的 flush 报告使用 n=5、
t=2.776，不属于本次跨档错误。

保守仅描述相对 t 临界值的取法；有效覆盖仍依赖独立重复及适用的均值 t 模型。
输入每轮 P99 时，此函数给出每轮 P99 的**均值**区间，不能充当总体 P99 分位数的置信区间，
也不证明尾部样本充分或独立。测试见
[`test_statistics.py`](../../tests/observability/test_statistics.py)，覆盖 df 档位、极小与大样本、
非法 df/非有限观测及实际 CSV 调用者的 warm-up 排除、分组计数和配对退化统计。
另覆盖接近浮点上限的可序列化区间，以及极小非正规数的非零区间。
