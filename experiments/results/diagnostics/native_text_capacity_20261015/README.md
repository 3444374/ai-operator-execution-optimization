# 文本原生 API、容量与 Daft 清理检查

从 `integration/baselines@277d8f4862c291bc2830cc6622f5a0b4f28b5c90` 独立修改文本接入。
本项只修正 HTTP 活动请求与批次存储的单位，并合并 Daft 重复清理；不改厂商源码、方法输入／解析／usage、
调度归属或 R=1 默认值。图像、A 的 Q2、服务器和 GPU 不在本次运行范围。
主比较继续经过原LOTUS算子、提示／缓存／原SDK解析和后处理；独立API调用只辅助核查接入与隔离开销，
不替代LOTUS完整查询，不提前运行模型铺平Filter或多阶段依赖。

## 最小官方 API 与当前差异

| 路径 | 最小原生调用 | 接入增加的责任与决定 |
|---|---|---|
| Ray Data | `from_items → build_processor(HttpRequestProcessorConfig(...)) → iter_rows` | 仍是同一官方图。完整响应carrier保留原状态、字节、重复header、HTTP版本及transport错误，交原SDK解析；禁止重试／重启，保留出现身份、取消及退出确认。未增加请求池或`gather`。 |
| Daft | `from_pydict → func.batch → with_column/select → iter_rows` | 原生async UDF拥有执行；固定0.7.21仍用C−1校正其实际活动行为。loop内client及关闭责任保持；删除默认／计时两份清理。 |
| IMLane | 已有作者DBEnd桥，CPython／Ray两模式 | 不改实现。默认逐批`input.jsonl`／桥进程；可选session已存在。公共`_lotus_rows`每查询建executor并由本查询ExitStack关闭，常驻LM／runtime不意味着跨查询复用桥。 |
| 实验外围 | 原生API本身不要求账本、代理、进程采样或详细计时 | 这些仍属于已有实验层，不进入厂商调度。详细计时已可选，本轮不复制私有脚本，也不以脚本故障解释API复杂度。 |

单元素交付队列连接同步调用者的取消与背压，不是第二请求池，保留。LOTUS方法层和原生执行层的响应额度
分属不同边界，保留；未为删行数而移除错误、缺失／重复结果、完整消费或未知工作拒绝复用。
整批LOTUS formatter已在原生Map中使用；本轮未引入纯文本specialization，也不将外部CPU微测解释成模型收益。

LOTUS薄入口的`on_batch/on_prepared/on_response`默认均为`None`，无需Movie query runner、代理、账本或采样。
其首条SDK准备加尾行结构／UTF-8预检不等于把每条请求序列化两遍；完整请求仍在原生I/O之前准备。
独立审核确认，直接删掉该轻量预检会改变错误优先级、非法尾行前的观察可见性和后续批号，因此本轮不删。
SDK response replay继续调用原OpenAI／LiteLLM解释错误、usage和logprobs，不用自写JSON解析替代。

## 最小调用与计时

调用方已准备有限输入并拥有本地Ray runtime时，官方执行段仅需现有三步；这里的`prepared`来自原方法，
不重新实现formatter。`list`保证完整消费，执行计时包括图的实际应用、actor建立及消费，排除输入／方法准备与runtime启动。
独立调用者仍须在其`finally`关闭自己的迭代器／runtime；提前停止的完整处理见同文件现有测试。

```python
dataset = ray.data.from_items([
    {"row_id": call["row_id"], "payload": json.loads(call["payload"])}
    for call in prepared_calls])
processor = build_processor(HttpRequestProcessorConfig(
    url=endpoint, batch_size=4, concurrency=(1, 1), max_retries=0),
    preprocess=lambda row: {"payload": row["payload"]},
    postprocess=lambda row: {"row_id": row["row_id"], "response": row["http_response"]})
started = time.perf_counter()
result = processor(dataset)
iterator = iter(result.iter_rows())
try:
    rows = list(iterator)
finally:
    try:
        iterator.close()
    finally:
        executor = getattr(result, "_current_executor", None)
        if executor is not None:
            executor.shutdown(force=True)
execution_seconds = time.perf_counter() - started
```

需要LOTUS方法时，使用已有批次交接而不经过实验runner。LM、输入frame、model配置及Ray runtime的冷／常驻
准备由调用方明确放在计时外。`lm`已声明匹配model的API配置及零重试；
以下区间包含原LOTUS方法、执行、SDK解释和完整frame消费，不当作纯Ray执行时间。

```python
native = NativeCallExecutor("ray-data", options)
method = LotusNativeBatchExecutor(native, model, query_id="q", operator_id="map")
started = time.perf_counter()
try:
    with lotus.settings.context(lm=lm, enable_cache=False), lotus_executor(lm, method):
        result = frame.sem_map("Summarize {text}.", return_raw_outputs=True)
        rows = list(result.itertuples(index=False, name=None))
finally:
    native.close()
lotus_query_seconds = time.perf_counter() - started
```

第二个样例使用未改的既有LOTUS入口；本轮没有在Mac安装其固定SDK组，也不提供该完整查询的新计时。
实际运行的第一个样例及两个prepared图使用同一fixed Map完整payload；仅作零模型API核验，不提供LOTUS模型质量或性能结论。

## 两种容量

令A为HTTP stage actor数，B为导入时生效的异步批次数，R为每批行数，C为允许的活动HTTP请求数。
[固定Ray 2.56.1 HTTP stage](https://github.com/ray-project/ray/blob/ray-2.56.1/python/ray/llm/_internal/batch/stages/http_request_stage.py)
逐条await惰性的`session.post`；在本接入的actor `max_concurrency=1`下，上界为A×B。
HTTP guard因此由A×B×R≤C改为A×B≤C；`request_row_envelope=A×B×R`继续报告HTTP stage行包络。
[planner](https://github.com/ray-project/ray/blob/ray-2.56.1/python/ray/data/_internal/planner/plan_udf_map_op.py)
的B在导入时固定；版本、有效B及预／后处理所需CPU检查仍保留。上界不是实测峰值。

完整输入与结果另有存储边界：

- `NativeCallExecutor`沿用`max_batch_rows`／`max_batch_bytes`；改为逐项验证、累计payload并在首个越界项拒绝，
  同时构造原有身份lookup，省去先无界`tuple`及重复扫描。默认4096行／32MiB输入和32MiB累计已消费响应阈值保持。
- prepared字典图入口也有有限行数／payload参数，默认4096行／32MiB；固定Map调用方从原source额度S、
  实际有限行数N和空文本请求模板字节数O推导S+N×O，完整指令、model和generation字段均计入。
  source和payload使用同一UTF-8 JSON转义，原text编码已包含于S，不能直接把S用作payload额度。
- 每个NativeCall的完整响应（含metadata）有独立上限；prepared factory保留原1MiB响应限制。
  `batch_storage.response_allowance_sum_bytes`只展示所有单响应上限之和，未新增求和准入或结果调度。

有限N与单响应上限只给出逻辑响应总量级。厂商可以提前缓冲、编码可扩张并产生副本，累计消费阈值和A×B×R
均不保证全图同时驻留字节或RSS；本轮不提供硬内存保证，也不将本地消费退出等同于远端HTTP／模型已停。

## Daft 清理与回归边界

在导入的仓库真实`_daft_rows`上，修前已复现默认模式`ValueError`被iterator关闭`OSError`覆盖，
以及decorator建立失败后的本地`_DAFT_CONTROLS`登记残留；详细模式没有相同残留。原始失败保留在任务私有验证目录。
修复仅删除`detailed`／`cleanup_started`分支及标志，无条件调用已有`_observed_graph_cleanup`。
首执行错优先，全部已知关闭动作继续尝试，后续错误附注；正常EOF、提前close、取消、setup及观察失败分别检查。
本地控制对象残留不提供GPU泄漏结论。已启动失败批次的未知责任仍禁止executor复用。

## 验证记录

测试字节对应代码提交`f2749bf1`（在`a9c6df63`的Daft修补之上）；来源、各次日志摘要、版本和全部紧凑fixture
见[核验清单](verification.json)与[原始localhost观测归档](sdk-fixtures.jsonl.gz)。
本机独立macOS/ARM64 CPU环境，Ray2.56.1、Daft0.7.21、Arrow24.0.0、aiohttp3.14.5；
Ray8逻辑CPU、0GPU、128MiB object store。只安装自有venv依赖，未修改厂商源码或已有环境；没有服务器操作。

- 基础环境定向回归：120通过／19明确跳过。首次宽回归有1个旧测试helper导入路径错误；
  修正调用路径后仅重验该模块，12项仍按opt-in／缺SDK跳过，没有记为通过。变更后的query模块9项复验通过。
- 固定真实SDK：12个不同测试函数、13次执行全部通过。已有10项、两个prepared入口的1项测试／两个后端、
  fresh-import B核查与更新后的最小官方API各自保留运行身份；图入口调用20次，取消／超时可以在首次HTTP前停止。
- 通过用例共233次直接localhost POST。另一次完整Daft query尝试完成8次client→proxy和8次proxy→fixture POST，
  随后被Linux-only时钟域检查拒绝；Ray完整query尝试在构图前因旧测试临时目录已存在拒绝，POST为0。
  两次不计通过，测试移植草稿未保留到实现；没有伪造时钟域或新造runner。

| Ray HTTP stage设置 | 每格请求数 | 活动HTTP实测峰值 | 正常完整消费 |
|---|---:|---:|---|
| A1/B2/R1/C2 | 32 | 2 | 通过 |
| A1/B2/R4/C2 | 32 | 1 | 通过 |
| A2/B2/R4/C4 | 32 | 2 | 通过 |

峰值是localhost handler接收至完整响应写出的计数，不表示GPU并行。最小官方API使用相同fixed Map完整请求，
16行全部响应／usage及请求值核对通过；本次执行段1.289263125秒，runtime已启动、图actor仍由本次图创建。
这只是一次软件接线记录，不提供吞吐或冷热性能比较。

实库还覆盖完整429／gzip／重复header、响应超量、提前close、取消、超时、Daft并发和client退出。
Daft真实iterator关闭处注入的`OSError`在默认／计时两模式均作为次错保留，实际首错仍为`ValueError`，control登记为0，
失败executor拒绝复用。没有注入厂商请求执行或改变其调度。Ray shutdown留下的BufferedWriter ResourceWarning原样保留；
后续独立进程检查未发现匹配本任务独立venv／Ray根路径的遗留进程，检查范围不扩大为整个机器的泄漏证明。

本轮生产修改3文件、净增24行；其中删除一套重复清理分支及两个状态标志，清理逻辑净减10行。
其余增加的是既有行／字节上限的首越界拒绝与单位说明；没有新增调度器、请求池、结果求和准入或实验门槛。
两个独立审核阶段已完成，source/payload额度混同这一中途发现已修复。main和integration引用未改。

未验证项：原LOTUS固定SDK的完整方法／错误和usage差分本轮未重跑；新R配置的模型／GPU性能、全图RSS上限及远端停止确认均未建立。
Linux完整query观测的Mac可用性仍未实现。历史软件／模型资格保留原source身份，不借本轮API检查升级。
