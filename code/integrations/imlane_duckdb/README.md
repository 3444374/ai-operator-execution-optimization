# DuckDB＋作者IMLane的原生Movie Q3

这是项目的原生API工作负载绑定，使用作者修改版DuckDB、DBEnd-C、coordinator与CPython executor。
它不调用records协议或SemLoom执行／调度，不声称复现作者未公开的论文Q7 UDF。
数据、消息、解析及评价复用[中立Q3合同](../../src/baselines/text/sembench_movie_native.py)。

| 文件 | 职责 |
|---|---|
| [q3.cpp](q3.cpp) | 完整四列CSV→原生普通筛选→PROCESS或THREAD prediction→单次COUNT；保存完整EXPLAIN与结果，不在本地C++回调执行算法 |
| [q3_udf.py](q3_udf.py) | Arrow `c0..c3`→一次batch-provider调用→无NULL、同序、单chunk int8；保存逐occurrence原答复 |
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

真正模型provider的服务所有权、作者推荐配置、预算与实机资格仍须单列准备；
当前CPU入口不会启动模型。执行前范围见[原生基线校准](../../../experiments/plans/原生基线校准.md)。
