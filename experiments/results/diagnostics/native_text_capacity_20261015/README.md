# 文本薄接入、原生容量与长期进程内连续 LOTUS 查询

从 `integration/baselines@277d8f4862c291bc2830cc6622f5a0b4f28b5c90` 独立修改文本接入。
本项只修正 HTTP 活动请求与批次存储的单位，并合并 Daft 重复清理；不改厂商源码、方法输入／解析／usage、
调度归属或 R=1 默认值。本报告前半部分记录首阶段CPU检查，后续薄观测与常驻验证单独列出；图像及其他owner的运行路径不在修改范围。
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

第二个样例使用未改的既有LOTUS入口；首阶段没有安装其固定SDK组，后续真实LOTUS验证见下方薄接入记录。
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

## 首阶段验证记录

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


<a id="thin-lotus"></a>

## LOTUS 薄观测与常驻边界

在 `0049b7d0` 上继续，`fad4761c` 补最小原生API对照，`21d87786` 只调整公共实验观测：
`performance` 不再登记完整wire响应及重复写出SDK响应JSON；`qualification` 保持原记录。
原SDK解析、完整ModelResponse摘要、usage、row/stage/call身份和错误／取消／退出责任保持，原LOTUS整批等待仍在。
没有新增执行器或facade；同一观测同时用于Ray Data、Daft与已有SemLoom接点。

`efa65c18` 再去掉常驻owner每次query开始前的强制连接池重置，并在公共代理使用 `DummyCookieJar`：
显式Cookie与Set-Cookie仍透传，代理不积累调用者状态。查询终态drain、binding复位、失败后poison及拒绝后续查询保持。
显式reset及fresh-connection诊断保留。原生图／actor／HTTP client仍按厂商批次生命周期创建和关闭；
仅延长 `NativeCallExecutor` 对象生命不会自动复用这些资源，因此没有扩大其scope。
统一的是长生命周期LOTUS进程连续提交独立查询的外部场景；各后端原生内部生命周期可以不同，其实际成本计入主查询。
原生每图重建不自动构成接入错误或失去主测资格；服务化可另列配置。
IMLane保留作者DBEnd桥及现有session选项：默认逐batch文件／Popen，可选session复用桥进程／context；
默认LOTUS装配仍由每query ExitStack关闭；`4271dc44`新增显式session配置的owner借用，见下方独立记录。
不强制作者桥跨query常驻，也不将其作为主测资格或新桥前置。

主时间为`full_query_seconds=(t_query_terminal_ns-query_preparation_started_ns)/1e9`，覆盖原LOTUS查询准备至完整结果消费，
包含提示、转换、图建立、响应恢复、后处理及查询内记录；`ready_query_seconds`另从`t_submit_ns`起算。
consumer EOF后的owner snapshot／trace写出、ExitStack文件flush、审计与官方quality评价在主时间外，
不得声称最终关闭全含在此区间。下次查询在这些动作完成后才开始；两次查询的间隔另列，不能从查询latency推导服务吞吐。
服务、owner启动另报。独立API或cProfile只定位组件，不从完整查询时间减去辅助时间。

### 真实 CPU 覆盖与成本

[薄接入核验清单](thin-verification.json)及[紧凑原始记录](thin-cpu-fixtures.jsonl.gz)保留来源与失败。
固定LOTUS1.2.4／LiteLLM1.95.0／OpenAI2.50.0已安装在本任务独立venv，23项preflight通过。
新增3个不同完整LOTUS测试函数、4次执行通过；Ray Data／Daft覆盖全未命中、混合命中、全命中，
重复输入出现身份、原缓存及usage保持；200／429核对原 `sem_map` 结果、异常类型／status及清理。
缓存三态只覆盖原LM与`LotusNativeBatchExecutor`的未缓存交接层，不代表cache-disabled实验观测器已支持通用混合缓存。
文本接入替换固定LOTUS的内部`_process_uncached_messages`，不称为全公开LOTUS插件；当前模型小轮仍明确关闭结果缓存。
已有SemLoom API的同一真实LOTUS路径也覆盖200／429。四次运行含reference／seed共120次直接fixture POST，均无模型。
既有SDK基线6项中3项是固定SDK差分、3项是dispatch／单元检查；不将其全部称为真实网络资格。
另有观测9项、供应商有效12项、公共网关／错误优先级／取消56项及既有Core三查询1项通过。
新Linux常驻LOTUS测试在Mac明确skip，不能计作Linux通过。

16行Daft完整 `sem_map` 的单次CPU组件观测中，重复raw记录从32条／46,202字节降为0；
图仍一次、HTTP仍16次、原请求准备及SDK解释仍16次，SDK回放client仍每批一个。
cProfile完整query的两次独立原值为0.444702042与0.463852958秒；包含诊断开销且非配对性能试验，不推导速度收益。
准备、SDK解释等inclusive阶段存在重叠，不相加为墙钟时间。

Ray辅助最小API及现有接入两图共32POST；当前R4接入4个session的建立／关闭总量约0.334／0.703毫秒，
16次HTTP阶段总量约374.714毫秒。Daft最小API16POST及现有24POST图均通过，后者一个loop内client建立／关闭约0.167／0.271毫秒。
这是各次CPU组件观测，不是冷热模型比较。Ray重复response decode及IMLane重复字节计量被定位，但涉及早期错误或未完成作者端到端资格，暂不硬删。
此前IMLane辅助检查只有已有Arrow HTTP UDF＋原SDK的正常、429、坏JSON三例共6POST；
后续`4271dc44`作者binary／Coordinator的独立20POST见下方，不借历史源码资格充当新源结果。

公共观测和常驻连接边界净增3行；`4271dc44`可选owner借用另净增10行。
连同首阶段共6个生产文件、增加79／删除42行，净增37行（不含测试和说明）。
删除一套重复清理及两个状态标志，并移除一次每query无条件reset；资格记录仍在使用，未用大块注释保留死代码。
默认R1、Daft C−1、单元素背压交付、完整消费与未知工作禁止复用均保持。

| 生产文件（相对最初`277d8f48`） | 实际修改 |
|---|---|
| [prepared_map.py](../../../../code/src/baselines/text/frameworks/prepared_map.py) | Ray活动HTTP按A×B，有限行／payload另计 |
| [native_calls.py](../../../../code/src/baselines/text/frameworks/native_calls.py) | 输入首越界即拒绝，Daft默认／详细模式统一清理 |
| [native_adapter_query.py](../../../../code/src/experiments/postgresql/native_adapter_query.py) | source额度加固定请求模板开销，避免混用payload单位 |
| [supplier_adapter_query.py](../../../../code/src/experiments/postgresql/supplier_adapter_query.py) | performance省重复SDK落盘；查询借用可选IMLane owner执行器 |
| [persistent_native_adapter_query.py](../../../../code/src/experiments/postgresql/persistent_native_adapter_query.py) | 移除健康查询强制池重置；可选桥在owner统一关闭 |
| [request_gateway.py](../../../../code/src/observability/request_gateway.py) | DummyCookieJar保留显式Cookie透传，连接池不积累Cookie |

### 新源 Linux 常驻检查与真实模型小轮

`1ba0fc1e` 的814文件闭包已完成实际Linux三臂各两次原LOTUS HTTP/1.1 fixture查询：
每臂8POST、合计24逻辑／48两跳、0模型；身份、usage、pool generation0、第二query复用及自然关闭通过。
此前`efa65c18`首轮Ray Data已完成两query／8POST及相同检查，最终测试误把空清理对象`{}`与`[]`比较而失败；
原run仍记失败并保留。`1ba0fc1e`只改该断言，不改生产字节。两轮实际fixture总量32逻辑／64两跳，均0模型。

真实模型来自新源`1ba0fc1e`，单RTX4090、Qwen2.5-7B-Instruct BF16、vLLM0.25.1，
max_model_len4096、max_num_seqs16、max_num_batched_tokens2048、GPU utilization0.8、TP/PP1、eager；
共同服务占8个声明CPU，driver各10个可用CPU、框架逻辑8CPU。三臂串行，同一API／engine跨臂保留。
LOTUS结果缓存及vLLM prefix cache关闭，首query保留、0warmup／0retry。
每臂一个CLI／owner连续两次原SemBench Movie Q2（relation2000，模型120次出现／119 distinct）。
[模型核验原值](model-verification.json)与[请求时长和SDK关联收据](model-request-receipts.jsonl.gz)保存全部六次结果，
完整HTTP正文、日志和关闭原件留在Git外。

| 配置 | owner启动秒 | 首次完整query秒 | 后续完整query秒 | 两query观察窗口calls/s | 两query间隔秒 |
|---|---:|---:|---:|---:|---:|
| LOTUS＋Ray Data | 19.545 | 23.056 | 10.040 | 7.027 | 1.059 |
| LOTUS＋Daft | 8.330 | 6.462 | 2.906 | 23.642 | 0.784 |
| LOTUS＋SemLoom | 23.943 | 5.838 | 3.873 | 23.074 | 0.690 |
| 原生LOTUS（另一服务scope） | 9.343 | 4.233 | 3.092 | 29.604 | 0.782 |

完整query使用上文preparation start→consumer EOF口径。观察窗口从首query开始到第二query EOF，
含两次query间的清理／审计／调用者记录，排除owner／服务启动；calls/s以240个真实模型调用为分子，
不是稳定服务吞吐。间隔不是纯清理时间。每query的120/full_query_seconds及ready API时间另存JSON。
前三行共享一个API／engine；原生LOTUS另开同配置服务，不是四臂同实例配对。

| 配置 | 固定请求执行参数 | 活动HTTP峰（首／后） | HTTP窗口平均在途（首／后） | 首HTTP前秒（首／后） | 末HTTP正文后至EOF秒（首／后） |
|---|---|---:|---:|---:|---:|
| Ray Data | C8、A2/B2/R1、source blocks2、8逻辑CPU | 4／4 | 3.032／3.608 | 17.100／5.150 | 0.987／0.645 |
| Daft | C8、原生max_concurrency7、8线程 | 8／8 | 6.507／6.878 | 3.137／0.196 | 0.055／0.056 |
| SemLoom | active8、held64、2worker×batch2、window2MiB/object8MiB | 8／8 | 5.023／5.607 | 1.954／0.186 | 0.050／0.045 |
| 原生LOTUS（另一服务scope） | 原LM max_batch_size8、cacheoff、performance | 8／8 | 6.462／6.664 | 0.895／0.158 | 0.045／0.041 |

平均在途为单代理dispatch→body-read区间积分除以首dispatch至末body-read窗口；完整query平均也在JSON中。
C是上限，实际并发未被强制相等。Ray的A2×B2工作点上界4与本轮峰一致；这不是各后端已充分调优的结论。
首HTTP前与末HTTP后的跨度来自同次运行的真实边界，不能归为纯调度／纯适配成本，也不从辅助API测量作减法。

每臂两query共享owner／LM；SemLoom execution／Ray runtime身份保持，查询后Core held/input/result/active及jobs归零。
原生图／actor／HTTP client仍按其各自生命周期执行，不因runtime UUID相同就声称actor或模型跨图常驻。
前三臂网关第二query均新建连接0、复用120，第一query新建分别4／8／8；其健康连接确实跨query保留。
文本模型权重在共同远端模型服务，HTTP worker创建与图像CLIP重新载入不是同一成本。

### 已有单代理 HTTP 分布

仅取owner `http-trace.jsonl` 的`upstream_dispatch_started_monotonic_ns`至
`upstream_response_body_read_completed_monotonic_ns`，同进程时钟内逐请求相减。
区间含连接池等待／建连、上游排队与处理、正文读取，不是纯模型计算；不混三跳、批API caller_response或重复计时。
以下合并本臂首次／后续各120，共240个真实HTTP出现；nearest-rank为`sorted[ceil(p*n)-1]`，逐query分布另存。

| 配置 | n | P50 ms | P95 ms | 经验P99 ms | 最大 ms |
|---|---:|---:|---:|---:|---:|
| Ray Data | 240 | 126.611 | 147.596 | 168.223 | 181.091 |
| Daft | 240 | 157.045 | 213.160 | 334.119 | 387.832 |
| SemLoom | 240 | 161.157 | 198.450 | 529.325 | 580.916 |
| 原生LOTUS（另一服务scope） | 240 | 163.804 | 226.169 | 243.967 | 257.260 |

这些只是此固定顺序小轮的经验请求分位数。每臂完整query仅2样本，查询P99和P99.99不可估计；
请求P99.99同样样本不足，不将nearest-rank退化成最大值冒充可信尾分位。
八份记录仍`performance_qualified=false`，不据此作稳态排名、加速比或调度归因。

八次query各120条Filter决定、LIMIT5结果及官方precision／recall／F1均与冻结原查询reference一致；
官方LIMIT5指标均1.0，逐120条的LIMIT前审计仍TP7／TN106／FP0／FN7，`quality_equivalence_proven=false`。
不能把返回5条正确说成120条分类全部正确。完整ModelResponse canonical digest逐条与捕获HTTP正文经固定SDK离线回放匹配，
960个response ID与row/stage/call ID完整关联；没有额外模型请求。模型资格全部200，不新增模型429／取消资格；
该轮原HTTP header／version未捕获，离线容器默认HTTP/1.1不冒充原始元数据。
三臂账本720模型／2160三跳，原生LOTUS另240／720，合计960模型／2880三跳；
两个scope服务API／engine、所有自有进程自然退出，GPU为空、共享lease释放，独立复核通过。

### 原生 LOTUS 补测与 IMLane 可选复用

用户新增原生LOTUS要求后，以同源`1ba0fc1e`、同Q2与服务参数、cacheoff／performance另开唯一原生臂，
同owner两query共240模型／720三跳；只使用原LM请求池及SDK，不为原生臂额外构造预期请求。
旧三臂不重跑，新scope的API／engine单独记录，不能与前三臂称为同实例配对。
[原生LOTUS核验原值](native-verification.json)与[240请求和SDK关联收据](native-request-receipts.jsonl.gz)
已独立审核通过；原LM、owner身份连续，第二query新建连接0／复用120、pool generation0。
完整query、资源及HTTP分布已并列于上表；仍有7FN，不升级质量或性能结论。

<a id="imlane-owner"></a>

`4271dc44`独立完成IMLane有限owner借用：仅显式`reuse_bridge=True`时持有既有`NativeCallExecutor`／DBEnd session，
每query仍有独立方法接点／身份，owner统一关闭；默认逐batch文件／Popen及原HTTP UDF生命周期保持。
没有新桥或改作者算法，原uncertain／poison／未知工作拒复用保留。15项受影响owner清理单测通过；
CPython／Ray各两健康query加一次完整429后禁止后query，共20fixture／0模型的作者scope实际通过，
进程／共享内存自然退出且lease释放，实际记录独立审核通过。
[作者fixture核验](imlane-fixture-verification.json)与[10份原始收据的脱敏归档](imlane-fixture-receipts.jsonl.gz)
保留20请求／40两跳、同桥身份和1→0创建／注册计数；10份收据不是请求数。
首个私有验证入口在HTTP前因禁止覆盖写入配置失败，
0fixture／0模型，原失败保留；仅改为新配置文件后重验，没有重跑旧模型。
随后以同源`4271dc44`的815文件闭包，通过已有公开`sembench_query_cli`完成两模式各同owner两次原Q2，
共480模型／1440三跳、全部200、0warmup／0retry；没有重跑前述四臂。
[IMLane模型原值](imlane-model-verification.json)与[480请求和SDK关联收据](imlane-model-request-receipts.jsonl.gz)
单独绑定新服务scope，两模式共享该API／engine，与前述四臂不是同实例配对。
两模式均显式`reuse_bridge=True`、query120秒／owner600秒；沿用原作者两worker、桥级原生线程池6与C8。
META的`native_executors=4`是作者实例额度，不是4个物理worker；6也不是每worker的HTTP并发。
本轮未改作者或桥源码，使用已固定的兼容构建。HTTP UDF仍逐调用建立client，不称所有层都常驻。

| 作者模式 | owner启动秒 | 首次完整query秒 | 后续完整query秒 | 两query窗口calls/s | 两query间隔秒 |
|---|---:|---:|---:|---:|---:|
| CPython | 8.517 | 7.906 | 7.506 | 14.922 | 0.672 |
| Ray | 8.563 | 8.128 | 7.677 | 14.438 | 0.818 |

| 作者模式 | 活动HTTP峰（首／后） | HTTP窗口平均在途（首／后） | 首HTTP前秒（首／后） | 末HTTP正文后至EOF秒（首／后） | P50／P95／经验P99／最大 ms（n240） |
|---|---:|---:|---:|---:|---:|
| CPython | 2／2 | 1.742／1.780 | 0.199／0.148 | 0.048／0.040 | 108.322／116.505／145.894／413.655 |
| Ray | 2／2 | 1.665／1.720 | 0.305／0.183 | 0.045／0.047 | 105.696／119.123／138.122／427.658 |

时间与HTTP分位沿用前述定义。owner启动不包含外部已准备的作者Coordinator／worker或模型服务；
首query内部的桥／context创建实际发生并计入主时间，不扣除。外部作者启动没有单独精确时长，不补造数字。
查询返回与owner退出各取既有报告，共每CLI三次边界观测；位于EOF后的记录成本进入两query观察窗口。
同桥PID／birth／start ticks、session root、namespace及native／LM身份连续；原桥报告创建／注册1→0，
session批序号1→2，每批120调用／120复制。没有虚构C++ context地址。
第二query网关新建0／复用120，第一query各新建2／复用118；批次排空后桥仍在，owner关闭后自然返回0、stdio关闭、无uncertain。
作者进程／共享内存、服务API／engine全部退出，GPU为空、lease释放；实际原件和派生指标均独立审核通过。

首个模型scope在发送前被私有身份检查拒绝，0模型／0HTTP，失败原件保留并完成退出。
检查错误地要求CPython worker与Coordinator同一可执行文件；固定作者[启动源码](https://github.com/IM-DM4AI/IMLane0/blob/072f8257db80569e42c9f5727d839587df529652/src/coordinator/coordinator.cpp#L35)
实际fork／exec独立`py_exec_instance`，
修正仅在私有helper核其原固定SHA、实际exe、父PID、argv、birth／钩子／namespace与CPU身份，不改生产接入。
这是测试器假设错误，不是原生API或接入失败；修正前未发模型，不增加480模型额度或重跑四臂。

本轮新增四query仍各TP7／TN106／FP0／FN7、官方LIMIT5指标1.0，`performance_qualified=false`；
加上前述四臂，累计12条完整Q2／1440模型／4320三跳。质量不足、三组服务scope、各臂n2和实际并发不同均保留，
不据本工作点宣称稳态排名或调度机制收益。

### 文档与实验能力边界

Daft官方说明类在worker上初始化并复用多行，初始化可以延后到查询执行；这支持worker内复用，
不能仅由class wrapper的寿命推断跨query模型常驻。[官方说明](https://docs.daft.ai/en/stable/custom-code/cls/)
用户引用的Ray `latest`页目前为2.59.0，`master`为3.0.0.dev0；本轮实库固定2.56.1，
不把在线新版本文档当成本轮已验证的Serve组合能力，也不新增服务化前置。
主实验只评价明确配置的完整LOTUS查询；生命周期设计收益可以是真实整体收益，调度归因仍需同生命周期的静态／新策略消融。
本轮两次连续查询不支持并发线上服务、故障恢复、稳态尾延迟或各后端全局最优参数结论。
