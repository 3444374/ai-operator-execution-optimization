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

作者 CPython 及真实模型包分别冻结；新作者运行、GPU、作者 Ray、多 executor、公共 backend
selector 和性能资格仍待验。历史 echo、普通 Filter 或其他框架成功单元保持各自来源。
