# 离线结果摘要入口

`summarize_offline_results.py` 只读调用方明确选定的紧凑文件，使用 Python 标准库；不查找运行目录、
不执行证据附带脚本、不启动服务或模型。库入口是
[`src.observability.metrics.offline_results`](../../src/observability/metrics/offline_results.py)。
它复用 [`statistics.latency_observations`](../../src/observability/metrics/statistics.py) 的中位数、
nearest-rank 分位数、小样本和不完整观测规则。此为已有结果的 CPU 复算，不增加任何 GPU 资格。

本次 r2 以 main `5f291d3d1ab4904a2a9a222b1f44e6f75012049f` 验证。该基底没有此 helper；
同一交付显式增加 a244 原版 `latency_observations` 及六项原回归。只插入这个函数，保留 main
已有 `percentile`、重复统计和其他函数，不复制另一套公式，也不依赖 a244 checkout 在运行时可见。

## CLI

```sh
PYTHONPATH=code python3 code/scripts/analysis/summarize_offline_results.py \
  --manifest /path/to/offline-inputs.json --output /path/to/new-summary.json
```

清单格式如下；相对路径默认相对清单目录解析，`--root` 可改基目录。
`--queries`、`--api-samples`、`--lifecycles`、`--results` 可覆盖对应路径，已有清单 SHA 仍会核验。
也可不传清单，显式传 `--schema`、`--comparison-scope` 和输入路径。
`--schema` 与清单冲突时拒绝。无 `--output` 时输出 JSON 到 stdout；文件输出采用独占创建，拒绝覆盖。

```json
{
  "schema": "semloom.offline_inputs.v1",
  "input_schema": "semloom.public_query_observations.v1",
  "comparison_scope": "records_e2e",
  "files": {
    "queries": {"path": "queries.jsonl.gz"},
    "api_samples": {"path": "api-latencies.jsonl.gz"},
    "lifecycles": {"path": "lifecycles.jsonl.gz"}
  },
  "metadata": {"evidence_kind": "retained observations", "historical_accounting": {}}
}
```

每个文件项可加 `sha256`。输入逐份记录 SHA-256 和字节数；SHA 不符、重复 query ID、孤立/重复
API 样本集合、批次/行数矛盾、负计时、重复 JSON key、未知版本会报错（CLI 退出码 2）。
每份文件压缩前及解压后的读取上限均为 32 MiB；没有递归、glob 或自动发现。
清单仅表达输入，不载入 Python module、命令或可执行 callback。

## 库 API

```python
from src.observability.metrics.offline_results import analyze, summarize, PUBLIC, FIFO

summary = analyze(manifest="/path/to/offline-inputs.json")
summary = analyze(input_schema=FIFO, comparison_scope="finite_chain_diagnostic",
                  results="/path/to/RESULTS.json")
# 内存 API 不读取文件，不修改传入对象：
summary = summarize(input_schema=PUBLIC, comparison_scope="records_e2e",
                    queries=query_rows, api_samples=sample_collections,
                    lifecycles=lifecycle_rows, metadata=historical_accounting)
```

`analyze` 返回一个字典（含 `inputs` 校验摘要）；`summarize` 返回同样指标但不含文件校验摘要。
必填 `comparison_scope` 是调用方提供的证据范围标签，各次调用只接纳一种输入格式。
数据集、查询、模型与配置名称仅作分组元数据，不决定成功、计时、质量或采样规则。
分组键是 configuration/task/phase/role；首次、复用、预热、资格各自保留，不自动筛掉任何阶段。

## 支持格式与最小范围

| 输入版本 | 输入角色和来源 | 支持 | 缺失/不支持 |
|---|---|---|---|
| `semloom.public_query_observations.v1` | `queries` JSONL(.gz)，可选 `api_samples`、`lifecycles`；a244 的 `native_baseline_q123_comparison_20261025` 保留包 | 查询提交至返回、query-ready、实际 API 单元样本、batch histogram、原质量/返回值、GPU 样本摘要、独立生命周期 | 不读取原 native trace；无 API 原值则不合并摘要分位数；缺 API 状态/时钟检查计数则 P99 为 NA |
| `semloom.fifo_summary.v1` | `results` JSON；已有 FIFO 小诊断 `queries[]`、`API.values`、`API_unit=single_generate` 格式，由调用方显式赋版本 | 现有两条冷调用及各六次 API 样本、中位/max、启动/关闭/owner 时钟、GPU 摘要 | 旧格式没有逐 API 时钟/身份检查计数，保留缺失；不推定为 0。未带 query ID 的旧数组仅用数组位置标识其摘要行 |
| 其他单查询 FIFO、index-matched 的嵌套 units、Core 未版本化 RESULTS/CSV | 尚未提供 adapter | 明确拒绝；不自动猜格式 | index-matched 无逐 API 原值，不能从每组 n/median/max 再合并 P99；可由 owner 后续提供必要紧凑字段 |
| token 资格 `phases[]` | 尚未提供 adapter | 明确拒绝 | 该诊断绕过 records Actor/IPC/parser/evaluator，不能纳入 records E2E 比较 |

两个版本描述已有文件布局，不声称旧文件本身已声明这些版本。清单格式也不等同于原运行 packet manifest。
版本目录字段固定属于格式协议，运行路径和分组名称均由输入提供。

## 摘要语义

- 输出版本 `semloom.offline_summary.v1`，分列 `n_query`、`model_API.n_successful_timed`、
  `GPU.n_samples` 和 `n_lifecycle`。一份 batch 间隔只产生一个 API 样本；不复制为行级尾延迟。
  批次含多少行由真实 histogram 校验。返回经验分位数不证明独立样本或总体尾部。
  FIFO 的 `actual_success` 是跨阶段模型调用总数，不能当输入行数；其输入行计数保持 null。
- 完整成功计时样本少于 100 时 P99 为 null，提供中位数、最大值及原因；失败、unknown、
  缺少状态/时钟证据同样不启用 P99。100 以上只允许已有函数的 nearest-rank 经验值。
  `n_retained_samples` 与分布中的 `completed_units` 只数保留的计时间隔，不证明其业务状态。
  `n_successful_timed` 另需明确的 successful_timed 和 succeeded 计数与样本对账；证据缺少时为 null，
  不从数组长度补出成功数。保留间隔的中位数/max 仍可描述已有数据，不能据此启用 P99。
  `status_counts` 不由时延推断；稀疏 Counter 只有在已知类别计数完整覆盖明确 attempted 时，
  才能把省略的类别解释为 0。整个 Counter 或必要计数缺少时保持 null。
- API 完整性要求 attempted/completed/timed/successful_timed 全部明确且匹配保留样本数、
  failed/unknown 为明确可核对的 0、missing_clock/unresolved_identity_records 为 0，以及批次计数可核对。
  缺任一必要计数，包括显式 null，均不启用 P99，并列入缺失字段。
  原定义中 completed=succeeded+failed，timed 包含所有有计时的可计数调用，因此 timed 可能包含 unknown；
  不要求 timed≤completed。已知计数需满足 completed/timed≤attempted、successful_timed≤completed/timed/succeeded、
  timed+missing_clock=attempted，以及稀疏状态总数=attempted。已知数值矛盾报 `invalid API accounting`
  （CLI 退出码 2），不静默改成 0 或丢掉矛盾项。合法的空样本仍需显式零计数，P99 保持 NA。
- `clocks.complete_query` 与 `clocks.query_ready` 分列。public 格式分别是提交至 API 返回与提交至结果就绪；
  FIFO 格式分别是完整冷 execute 调用返回（包括启动和关闭）与引擎就绪后的业务查询就绪。
  两个格式的起点不同，须使用各自 scope；没有重命名为同一性能试验。
  `lifecycles` 原样另列，不再叠加 startup/close 或在线探索，也不以并发区间之和计算关键路径。
- 质量按原查询保留原 evaluator、TP/FP/TN/FN/invalid、supplement 与返回结果，包括原 Top5/COUNT。
  没有新的数据集专用评价器。显式 `stage_counts.Map=0` 才设置 `zero_Map`；成功查询同时有
  `Map=0` 和 `result_rows=0` 才将 `empty_successor` 设为 true。成功状态、Map 计数或结果行数
  缺少任一项证据时为 null（NA），不能把未知变为 false，也不能只凭空结果推断空后继。
- GPU 均值沿用旧复算脚本的有效样本数加权公式，峰值为采样峰值；各指标有效数和尝试数分列。
  时间覆盖率缺证据为 NA，样本有效率、RSS 和逻辑字节均不升级为连续覆盖或物理 livebuffer。
- 这两种紧凑格式没有完整的封口 native FLOPs 与匹配时钟，MFU 及 API 窗口 MFU 明确 `unsupported`。
  即便已有 `finished/tokens` 相同或保存了 MFU 数字，也不追认其完整性，不把 close 尾增量搬回较早 API 窗口。
  ready/admit、consumer、kernel 时间缺少必要事件时为 NA；原 MFU/质量/结果文件不改写。
- `metadata` 原样保存历史账本，包括 failed/unknown/retry、未执行预留和非执行证明；它不与当前成功样本合并，
  不从 allocated 减 successful 推导退款、未执行或重试。缺失字段保持 null/NA，矛盾字段报错。
- FIFO 根节点已有的 actual_success/actual_failure/actual_unknown、重试、预热和历史预留等计数，
  即使未提供 manifest metadata，也保留在 `source_accounting.counters`。该对象明确属于整份输入摘要，
  没有各 query 归属证据时不分摊，组内不能归属的失败/unknown 仍为 null。源级 failure/unknown
  只要有已知正数就明确标示；两者都明确为零才标示没有这些事件，缺失不能默认 0。
  有源级失败/unknown 或缺失时保留间隔的描述统计，但 query 成功状态、成功计数和 P99 不获补证。

## 已审核 a244 模块盘点与本次依赖选择

| 既有位置 | 本次决定 |
|---|---|
| `metrics/statistics.py:latency_observations` | 直接调用，所有样本分布只走这一份定义；不重写分位数 |
| a244 `statistics.query_costs/reuse_payback` | 不加入 5f291 的依赖增量，也不调用；本次只分列时钟，不推导累计回本 |
| `metrics/mfu.py:estimate` 与 `mfu_trace.py` | 保留；缺开始/封口 counter、尾部归属和时钟时不调用，不添加新 MFU 计算器 |
| `experiments/postgresql/native_adapter_metrics.py` | 依赖具名 ready/caller 事件与哈希，当前摘要不够；不以旧 `sample_distribution` 的小 n 分位数绕过新入口的 P99 规则 |
| 保留包 `recompute.py` | 原入口包含固定配置/总数断言，仅作既有结果差分 oracle；不复制为通用调度或观测系统 |
| 私有各次 `analyze.py` | 有各自原路径/trace/账本依赖；本次不执行、不迁移、不改写原证据 |

## CPU 验证

```sh
PYTHONPATH=code python3 -m unittest discover \
  -s code/tests/observability -p test_offline_results.py -v
```

[`fixtures/offline_results`](../../tests/observability/fixtures/offline_results) 含两种真实保留布局的小样本，
对应原摘要值和来源文件 SHA。比较使用绝对及相对容差 `1e-12`；计数、状态和 null 完全一致。
测试另用明确 CPU 合成数据检查 0/1/2/99/100/101/217 样本、1/4/16 行 batch、单 batch 200 行、
40 次确定随机顺序置换、失败/unknown/预留分离、字段缺失、版本/hash 拒绝和输出防覆盖。
外部已保存的完整差分回执与原始 CPU 输出放在本次独立交付目录，不写回实验结果目录。
