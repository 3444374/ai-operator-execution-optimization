# DuckDB＋作者IMLane的原生Movie Q3

这是项目的原生API工作负载绑定，使用作者修改版DuckDB、DBEnd-C、coordinator与CPython executor。
它不调用records协议或SemLoom执行／调度，不声称复现作者未公开的论文Q7 UDF。
数据、消息、解析及评价复用[中立Q3合同](../../src/baselines/text/sembench_movie_native.py)。

| 文件 | 职责 |
|---|---|
| [q3.cpp](q3.cpp) | 完整四列CSV→原生普通筛选→PROCESS或THREAD prediction→单次COUNT；保存完整EXPLAIN与结果，不在本地C++回调执行算法 |
| [q3_udf.py](q3_udf.py) | Arrow `c0..c3`→一次batch-provider调用→无NULL、同序、单chunk int8；保存逐occurrence原答复 |
| [model_provider.py](model_provider.py) | 一次作者batch→同批异步HTTP请求；原消息、单输出、原文／usage及失败回执，不拥有模型或引入SemLoom调度 |
| [model_service.py](model_service.py) | 独立单模型API进程及birth匹配的关闭；GET readiness／模型身份／上下文，外层仍核对GPU与SHM |
| [runtime.py](runtime.py) | 仅拥有作者coordinator／executor；复用已有纯进程归属与清理设施，不创建records客户端 |
| [build.py](build.py) | 隔离导出固定作者源码、记录版本／缺源恢复和便携配置，另行编译薄driver；不安装软件或改canonical前缀 |
| [missing_sources.json](missing_sources.json) | 三件必要构建源的准确官方提交、Git blob、SHA256；没有每批源码版本门槛 |

输入四VARCHAR依次为`occurrence_id/id/reviewId/reviewText`，作者转换器按位置命名`c0..c3`。
输出为TINYINT，SQL比较`=1`；不走尚未资格的位图Arrow Boolean转换。完整cohort先进入DuckDB；
作者planner将普通Filter放在prediction的child，运行时再以实际UDF回执确认只有eligible行。
COUNT与回执消费同一次模型执行，后置审计不会重跑AI图。

## 构建

固定作者来源为`IM-DM4AI/IMLane0@072f8257db80569e42c9f5727d839587df529652`。
原根`build*`规则误排除两个optimizer文件和mbedtls头。仅使用官方
`duckdb/duckdb@5ebd8ca697099859239ba85e0de07dac7e1b7c04`的三件准确字节恢复；
既有56份不同文件均保留，不把整个作者fork说成该上游树。来源与实测状态见
[准备报告](../../../experiments/results/diagnostics/imlane_duckdb_q3_20261106/README.md)。

先将manifest三件文件从该官方提交按相对路径放入仓外`UPSTREAM_SOURCE`，然后：

```sh
PYTHONPATH=code python code/integrations/imlane_duckdb/build.py prepare \
  --root "$IMLANE_AUTHOR_ROOT" --workspace "$DUCKDB_WORKSPACE" \
  --upstream-source "$UPSTREAM_SOURCE" --jobs 4
```

`prepare`只写新的私有源码副本和明确命令。复查后运行打印的configure／build命令；
数据盘、编译CPU／内存／时间与已有host租约按runtime runbook约束。
保留C++17、实际作者Python、fmt前缀和现有SDK依赖头目录；不通过安装普通pip DuckDB替代它。
无可核对release tag时LibraryVersion如实为unknown，不能填写论文0.10.1。

```sh
PYTHONPATH=code python code/integrations/imlane_duckdb/build.py driver \
  --root "$IMLANE_AUTHOR_ROOT" --duckdb-source "$DUCKDB_SOURCE" \
  --duckdb-build "$DUCKDB_BUILD" --output "$PRIVATE_BIN/q3"
```

## 零模型CPU检查

[实验入口](../../src/experiments/native_imlane_query.py)只允许明确的软件fixture。
在外部host租约、环境预检和资源supervisor内执行worker；不要在已有coordinator上注册／热替换函数。
作者默认端口7086必须空闲；省略executor／DuckDB线程配置时保留作者默认，不注入单线程限制。

```sh
PYTHONPATH=code python -m src.experiments.native_imlane_query prepare-fixture \
  --reviews-csv "$REVIEWS_CSV" --output "$PRIVATE_PACKET"

PYTHONPATH=code python -m src.experiments.native_imlane_query worker \
  --packet "$PRIVATE_PACKET" --output "$NEW_PRIVATE_RUN" \
  --author-root "$IMLANE_AUTHOR_ROOT" --method-python "$IMLANE_METHOD_PYTHON" \
  --driver "$PRIVATE_BIN/q3" --kind process
```

THREAD用新的coordinator／executor／输出目录独立检查。`audit_result(packet,query_json,receipts,checkout)`
在driver环境读取保存结果与回执并调用原评价。worker进程／namespace通过，不替代外部GPU、SHM和完整质量核查。
空结果、错误及清理失败保留原件；软件COUNT14只验证接线，不是模型质量。

准确798e的真实PROCESS／THREAD软件运行均已通过：2000→120、一次COUNT／batch-provider、
Arrow16.1往返及外层进程／SHM清理。作者默认4 executor共需4×128MiB Lane和8个控制队列；
实测队列逻辑8592B、各占3页，合计512MiB＋96KiB，不能套用只计Lane的512MiB预算。
DuckDB默认16线程仍在显式8核亲和性内，未注入单线程。完整来源和边界见上述准备报告，
特殊空选择／错误的真实二进制路径和模型服务生命周期仍未资格。

## 真实模型worker与独立HTTP对照

[模型worker](../../src/experiments/native_imlane_model_query.py)与原软件fixture入口分开，
前者明确拥有一个外部vLLM服务；作者coordinator／4个CPython executor不加载4份GPU模型。
数据库→executor仍用作者ArrowLane／共享内存；仅UDF→独立模型服务为loopback HTTP，SemLoom主线保持本地IPC。
论文Q7仅明确远程vLLM，未核实其准确HTTP客户端；本接线是项目baseline绑定，不冒充论文Q7 runner。

私有模型配置如下；用现有资产替换占位路径，模型port须空闲且与作者7086分开：

```json
{
  "schema": "semloom.imlane.duckdb.q3.model_run.v1",
  "provider_mode": "model",
  "reviews_csv": "<original-Reviews.csv>",
  "model_service": {
    "model_python": "<existing-vllm-python>",
    "model": "<local-Qwen2.5-7B-Instruct-directory>",
    "served_model_name": "sembench-qwen25-7b",
    "host": "127.0.0.1",
    "port": 18081,
    "dtype": "bfloat16",
    "server_options": []
  },
  "sampling_params": {
    "temperature": 0, "top_p": 1, "max_tokens": 8192, "n": 1,
    "stop": null, "presence_penalty": 0, "frequency_penalty": 0,
    "repetition_penalty": 1
  },
  "request_timeout_s": 1200,
  "api_key_env": null
}
```

worker入口：

```sh
PYTHONPATH=code python -m src.experiments.native_imlane_model_query \
  --config "$PRIVATE_MODEL_CONFIG" --output "$NEW_PRIVATE_RUN" \
  --author-root "$IMLANE_AUTHOR_ROOT" --method-python "$IMLANE_METHOD_PYTHON" \
  --driver "$PRIVATE_BIN/q3" --sembench-checkout "$SEMBENCH_CHECKOUT" --kind thread
```

模型服务显式`--generation-config vllm`，避免模型仓库的生成配置改变共同SamplingParams条件；
这是项目对照选择。容量／线程省略项保持原生解析，不补8／4096；`/v1/models`读回模型名和实际上下文，
普通服务未启用dev mode时`/server_info`404，其余resolved字段如实unknown。API key只从具名环境读取。
provider的一个HTTPX async client处理一个作者batch；连接池明确不设额外连接上限，无semaphore、重定向或重试。
逐请求记录实际payload／响应／usage及POST调用状态；POST数不冒充物理模型执行数，started缺terminal仍是unknown。
达到输出上限的原文保留并标记length，不替原解析器补答案；错误JSON／非文本／abort等保留后失败。

完整2000源物化计入冷scope，普通筛选仍由DuckDB完成，正常逻辑量120。
审计用同worker的调用起止区间连接UDF与HTTP批次，再逐position核对occurrence，重复消息不靠全局Counter掩盖交换。
已发布自有回执按实际文件大小读取，不套用小配置JSON的8MiB界限；整体RSS／磁盘仍由外层监督。

新driver在析构前写Linux `CLOCK_MONOTONIC`结果时钟，运行模型worker前须重建；旧798e二进制没有此字段。
结果出现后一个120秒窗口覆盖native析构、作者关闭与服务关闭，末尾8秒留给外层强制停止。
作者既有close仍有分段等待，因此必须用本模块`supervised_run(command, output, supervision_dir, monitor=...)`
绑定READY期限；supervision目录与worker输出分开。调用方持有host租约，并将`ResourceMonitor.finish().passed`
及GPU退出检查纳入最终成功，不能仅凭worker completed或API返回。构建／新接线资格见准备报告，
真实模型、物理GPU关闭、质量与性能仍待本轮后续实测。

软件HTTP CPU验证必须显式`provider_mode=software_http`并通过Python测试factory返回真实ModelService、
仅替换其spawn为无模型软件服务；普通CLI拒绝该mode，不能因写了software便意外启动GPU。
执行前范围见[原生基线校准](../../../experiments/plans/原生基线校准.md)。
