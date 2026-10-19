# IMLane 普通 Map、Extract 与关系链

[`IMLaneChainBackend`](chain_backend.py)消费公共 `LotusRecordChain` 和中性
`RecordOccurrence`。Map／Extract 是同一入口的单阶段值；ME、EF、MEFM、EEF 等组合不增加
专用执行器。作者 DBEnd／Coordinator／Lane／CPython executor 拥有物理批次和调度，
唯一方法阶段循环仍为 [`LotusRecordChain.run_batch`](../../src/semantic_methods/lotus/record_chain.py)。

此切片从 `aebbbcd8` 建立，只选择性消费公共空关系接口 `f977c979`；IMLane 自有改动不修改
公共方法、引擎、关系控制器、作者 C++ 或旧 HTTP。实际验证范围及失败来源见
[CPU 报告](../../../experiments/results/diagnostics/imlane_chain_cpu_20261019/README.md)。

## 调用合同

调用方持有现有 host lease、有限进程监督、`StopBudget` 和查询外层资源责任。一次 backend
只拥有一条查询，输出目录必须已存在且位于 Git 外。调用方传入有限且可重复核对的 expected
发生项列表；`row_id`、`source_position` 全局唯一，业务 Index 可重复。数据中的业务
`row_id/call_id` 留在 `values`，不会替换调用身份。

```python
backend = IMLaneChainBackend(engine=engine_spec, resources=resources,
    author_args=owned_author_args, model_environment=assigned_environment)
stream = backend.evaluate(source_rows, chain, expected=expected_rows, columns=source_columns)
try:
    records = list(stream)
finally:
    close_report = stream.close()
frame = records_to_frame(records, columns=stream.columns, source_index=original_index,
                         empty_prototype=final_empty_relation)
```

原 source Index 的 dtype／名称和最终空关系原型由调用方保留。公共 `empty_prototype`
只在无输出发生项时复制最终零行 DataFrame 的列 dtype 和元数据，不转换非空值；它的列序必须
与实际最终列序相同。IMLane 不从空记录猜 dtype，也不运行另一套方法去补值。

单模型部署、单作者 executor、准备／模型批 1..16、最多两块 pending。query 必须声明
`EngineSpec.max_model_calls`；`输入发生项数 × 方法阶段数` 的保守上界不得超过该永久额度，
每段新模型 child 另有该段的有限上界。块内每个实际模型调用独立记账，不按来源去重。

## Extract 后继与退出

`relation_barriers=True` 使用与 Ray／Daft 相同的
[`NativeExtractRelationRows`](../../src/baselines/text/frameworks/lotus_chain_native_relation.py)。
每个原生段内 Extract 仍只能末端。只有前段 source EOF、coverage／成功核对和关闭均确认后，
公共 `complete_extract_relation` 才按原 source 顺序完成同 Index 标签广播、稀疏字段与父调用组装。
下一段消费完成关系，不回读数据或重发已有模型调用。物理 block 不构成查询 fence。

各段独立作者 scope／模型 child，不宣称跨 Extract 复用模型；装载成本也属于该布局。只有
API shutdown、已观察进程退出、namespace／SHM 和日志／采样线程核验结清才能正常关闭。
`author_physical_state` 与语义／监测失败分列；已知模型关闭证据保留，重复 close 不能把旧失败
恢复为健康。unknown、取消或失败前缀不允许启动后继，已知调用、usage、raw 回复及审计不清空。

GPU 退出由 slot owner 独立检查，CPU close 不提供 GPU 证明。待执行模型 qualifier 在每段关闭
后检查独立 GPU 状态，未确认时阻断下一段；最终 lease 也保留至资源责任结清。

## 当前资格与接入范围

新增八项与原有限链九项，共 17 个不同本地 CPU 测试通过，包括六种组合的原 SDK 消息／关系
差分、ME／EF 真实 CPU 子进程、两块乱序 ready、重复 Index、空源／过滤后空关系、已知失败、
异常作者退出、未知模型关闭和取消。这里的作者载体使用显式 fixture，模型 child 也只返回声明值。

旧冻结 `7a4524b7` 作者 CPython 已完成 ME／EF 共12次 fixture、三个scope／engine；该成功不认证
后续共享关闭修补或下述公开入口。真实模型／GPU、作者 Ray、多 executor 和性能仍待验。
历史 echo、普通 Filter 或其他框架成功单元保持各自来源。

## 公开 ordinary chain 入口

共享脚本新增 `chain-check`／`chain-run`，实现位于
[`lotus_records_chain.py`](../../src/experiments/lotus_records_chain.py)。当前只接受显式
`imlane-author-local-model-chain`，普通 Filter、Map、Extract 都使用同一有限链和作者 backend；
非末端 Extract 继续沿上述完整关系屏障。此入口接受非Q3文本列，不为通用关系附加 Movie COUNT
或 SemBench 评价；Movie Q3 和全局级联保留各自命令，IMLane 级联 selector 仍未接入。

```sh
PYTHONPATH=code python code/scripts/experiments/run_lotus_records_movie.py chain-check \
  --config /private/chain.json --source-json /private/relation.json --owner-config /private/owner.json
PYTHONPATH=code python code/scripts/experiments/run_lotus_records_movie.py chain-run \
  --config /private/chain.json --source-json /private/relation.json --owner-config /private/owner.json \
  --output-dir /private/new-query --execute-model
```

所有输入须为 Git 外、至多8MiB的普通有限JSON文件。`chain-check` 以及省略 `--execute-model`
的 `chain-run` 只做preflight，不建作者／模型、不claim预算或写查询结果；真实preflight会运行
只读方法ABI探针。启用执行后使用已有有限 supervisor 和作者进程机制，不另建调度器。

| 文件 | 精确顶层字段及合同 |
|---|---|
| config | `schema="semloom.records_chain_run.v1"`、`backend`、`chain=LotusRecordChain.describe()`、`engine=EngineSpec`、`resources=RecordsResources`、`source_sha256`、`owner_sha256` |
| source | `schema="semloom.record_relation.v1"`、`records=[RecordOccurrence.describe()]`、`columns`、`source_index={dtype,name}`、`empty_prototype=[{name,dtype}]` |
| owner | `schema="semloom.imlane_owner.v1"`、`author_root`、`method_python`、`bridge_binary`、`artifact_root`、`preflight_report`、`model_environment`、`owner_resource_limits`、`lease_sha256`、`budget`、`gpu_idle_max_mib` |

配置严格拒绝额外顶层字段。source最多4096个发生项，按唯一 `source_position` 升序；初始记录
不能带未完成 `index_updates`。单层Index为 `int64/object/string`，标签保持原str／int，名称为
str／int／null；不支持 MultiIndex 或隐式标签转换。最终空原型的dtype明确限制为
`object/string/bool/boolean/int64/Int64/uint64/UInt64/float64/Float64`，其列序需匹配最终关系。
结果以split结构及显式dtype／Index元数据保存，保留float64／uint64原值，缺失值写JSON null。

链为1..16个阶段，单deployment／executor／GPU，批行数1..16、pending至多2。
`输入数 × 阶段数` 不得超过 `engine.max_model_calls` 和声明的永久预算；引擎总上限至多4096。
`budget={path,id,limit,unit_id}` 必须指向外部owner已经建立且预留的 `CellBudgetLedger` unit，
worker只claim一次，预留须覆盖完整查询保守上界，空关系仍占至少1。失败、早停或少用均不退款，
不自动重试。`model_environment` 明确单个 `CUDA_VISIBLE_DEVICES` 及可选 `LD_LIBRARY_PATH`；
owner资源须显式给 `cpu_cores/rss_bytes/shm_bytes`，不能继承echo默认值。

[`chain_runtime.py`](chain_runtime.py)只读核对既有正常host lease的同一份bytes哈希、主机、
进程出生身份和当前worker祖先归属，不获取、stale恢复或释放该lease。caller先限制CPU亲和性，
提供成功runtime preflight、原作者／v2桥构建身份和隔离方法／模型解释器。
每个已关闭scope在下一段开始前均核作者资源状态及独立GPU空compute／idle上限；不确认即阻断。
最终 `query.json` 保留原调用／known usage／raw及关闭报告，外部lease有效性与GPU观察单列；
API关闭不替代后代／GPU证明，外层owner继续持有监督和lease责任。

入口本轮25项、owner边界45项及旧lease／IMLane兼容17项均为无child CPU合同；实际LOTUS
方法／解析与临时永久账本是真实实现，作者／ABI／GPU为显式fixture。新入口的真实作者执行及
模型资格仍需绑定最终源码，详见[入口证据](../../../experiments/results/diagnostics/imlane_chain_cpu_20261019/README.md#公开入口与固定来源)。
