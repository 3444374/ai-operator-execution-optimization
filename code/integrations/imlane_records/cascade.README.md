# IMLane 原 LOTUS 全局级联接点

基于通用级联 `feat/lotus-cascade-backends@6e8b1382`，以其
[`run_cascade`／`CascadeStage`](../../src/semantic_methods/lotus/cascade.README.md)为方法合同。
只新增 IMLane 阶段值适配，不修改公共算法、作者 checkout、DBEnd／Coordinator／Lane、
作者调度、队列、批次推进或原生生命周期，不嵌套 Ray／SemLoom 控制。

## 最小缺口与实施方案

现有普通 Filter wire 使用自己的调用命名空间，不能直接承载 `lotus-cascade-v1` 身份；
普通 Filter 的生成前检也明确拒绝 helper logprobs。因此独立增加以下接点，保留旧路径：

1. `cascade_workload.py` 只编码／核对通用阶段及发生项。五列输入、三列输出和显式v2不变，
   角色、原始调用身份、冻结列序、生成token／logprobs／usage均按值传输，评价标签不进入UDF。
2. `cascade_udf.py` 在作者CPython executor内使用原 `CascadePreparer`，通过现有私有
   `IMLaneEngineProcess` 执行当前固定角色。它只处理记录和结果，不决定采样、阈值或fallback。
3. `cascade_host.py` 提供 `preflight`、`evaluate(CascadeStage)` 和 `check_lifecycle`，
   由原LOTUS全局控制器调用。每个逻辑阶段使用新作者scope／解释器；阶段内复用同一engine，
   DBEnd异步提交最多两块后收集，物理block不形成新算法查询或全局校准屏障。

helper_score、threshold_sample、fallback由原方法顺序决定；每个阶段的模型API和作者owned资源
关闭确认后才允许下一阶段，未知时阻断。main可在两个阶段分别加载，加载和关闭计入成本；
不承诺跨阶段／查询同engine复用。外层caller继续拥有host lease、finite supervisor和模型／GPU排期。
已有短IPC配置继承 `VLLM_RPC_BASE_PATH`；不重做短路径修复或修改作者TMPDIR／Arrow16 ABI。

## 调用合同

caller 将同一 source 的 `preflight(method)`、`scan_spec(method)`、`expected(query_id)` 和
有限 `csv_path` 交给 `IMLaneCascadeBackend`；scan 必须覆盖 main/helper 指令的列并集。
通过 `engines={"main": EngineSpec(...), "helper": EngineSpec(...)}` 显式指定两个不同部署目录，
并提供既有 `RecordsResources`、单GPU环境和外层监督责任。backend 直接作为原 `run_cascade` 的
`evaluate` 参数，不增加另一条查询算法；公开 selector 只按下节装配现有接点。

`author_args` 复用现有 owner 的 `output`、`author_root`、`method_python`、`bridge_binary`、
`artifact_root`、`scopes`、`stop_budget` 和可选 `owner_resource_limits`；output 是仓库外已创建的私有
查询目录。caller 在分配前持有原 host lease，并负责有限 supervisor、scope 最终清点及独立 GPU
确认。backend 不自行重试、恢复 stale lease、获取 GPU 或启动嵌套执行框架。

首次非空阶段冻结 query ID；下一阶段在创建 control 目录及 owner 前核对 query、角色、模型、
指令、列序和生成策略。`close_reports` 按逻辑阶段保留模型 API／进程、作者物理结清与正常结束
证据，GPU 字段保持独立。作者 close 或监控失败持续保留；物理资源已结清不把失败阶段改为健康。

输出仍由原桥在 `ConvertFromExchange` 内独立复制。逐项已知终态先入账；关联损坏保留可验证兄弟结果
和私有原件，缺项保持unknown，失败不作为False。helper日志由通用级联方法严格解释，校准、采样、
阈值学习、fallback选择和合并全部沿原LOTUS代码对象，物理batch不重算它们。

## 验证边界

本机fixture、原LOTUS／原Q3回归与作者真实CPython检查分别登记，不互授资格。
作者实机前后核对固定checkout与构建摘要；通过唯一GPU会话安排无模型CPU窗口后才能进入服务器，
不抢占GPU／lease。真实模型、helper日志实值、阶段间GPU退出及作者Ray均单列待验。

固定运行、失败原件摘要和当前作者 CPU 进度见
[资格报告](../../../experiments/results/diagnostics/imlane_cascade_cpu_20261019/README.md)。

## 公开通用记录入口

共享脚本新增独立 `record-cascade-check`／`record-cascade-run`，实现为
[`lotus_records_cascade.py`](../../src/experiments/lotus_records_cascade.py)。只接受
`imlane-author-local-model-cascade`，不约束业务表、Q3谓词或COUNT；旧Movie
`cascade-check/run` 的schema、后端和原评价保持不变。

```sh
PYTHONPATH=code python code/scripts/experiments/run_lotus_records_movie.py record-cascade-check \
  --config /private/cascade.json --source-json /private/relation.json --owner-config /private/owner.json
PYTHONPATH=code python code/scripts/experiments/run_lotus_records_movie.py record-cascade-run \
  --config /private/cascade.json --source-json /private/relation.json --owner-config /private/owner.json \
  --output-dir /private/new-cascade --execute-model
```

config精确字段为 `schema="semloom.records_cascade_run.v1"`、`backend`、`query_id`、
`method`、`helper_model_name`、`engines`、`generations`、`cascade_args`、`resources`、
`max_calls`、`source_sha256`、`owner_sha256`。`method.instruction` 必须由caller显式提供，
不采用Q3默认；main/helper生成配置与部署按角色分别声明，目录必须不同。
main不请求logprobs，helper显式top候选1..20，标签保持原True／False；原CascadeArgs、采样、
校准、阈值学习和fallback全部由同一个 `run_cascade` 执行。拒绝未实现的参数，不代造helper日志。

source与owner复用[普通链的有限JSON／Index／最终空原型合同](chain.README.md#公开-ordinary-chain-入口)，
每文件至多8MiB。两个角色的原列序独立冻结；driver只向原控制器投影身份及角色列并集，
[`MaterializedCascadeSource`](../../src/baselines/text/cascade_records_source.py)将同值一次写为
0600的 `cascade-source.csv`，由既有原生作者阶段扫描。未引用的源列、业务同名字段、原Index及
source refs保留在原发生项；按最终决策的完整身份关联返回幸存关系，不使用评价标签。
CSV至多16MiB、4096行，position为非负int64；每个ID／列名／文本字段至多128Ki Unicode字符。
超限或更低的进程CSV读取限额明确拒绝，不截断输入或修改全局reader设置；这些是逻辑文件限额，
不声称为完整查询RSS／GPU硬上限。

原方法给出 `Q=2N+S`，学习模式 `S=int(N×sampling_percentage)`，固定阈值模式S=0。
单query `max_calls` 至多12288；helper/main每阶段实例cap各至多4096，须覆盖N／max(N,S)。
调用方既有CellBudgetLedger unit须覆盖max(1,Q)，原方法实际调用另计；失败或未用额度不退款、
不自动重试。每个非空阶段仍新作者scope／固定角色模型，main可能加载两次。

[`imlane_owned_query.py`](../../src/experiments/imlane_owned_query.py)从原普通链worker提取共同的
正常外部lease／StopBudget／永久unit／最后回执编排，两入口传各自预检和执行回调，不传假链
或复制阶段循环。既有 `AuthorRunGuard` 在原控制器的生命周期检查处验证已关闭scope的作者资源
和独立GPU状态，未确认就阻断下一角色；模型API、物理退出、监测错误和unknown仍分列。
未传 `--execute-model` 只预检，不建backend／CSV／作者／模型、不claim或写查询输出；
真实预检仍有只读ABI／固定Filter与cascade源码检查。执行沿既有有限supervisor，最终查询保存
原calls字典、LM统计、阶段关闭及物理／预算回执。业务结果保持原float64／uint64精度和空关系元数据。

新增入口资格见[固定来源及CPU回归](../../../experiments/results/diagnostics/imlane_cascade_cpu_20261019/README.md#通用级联公开入口)。
旧r3作者44次CPU fixture仅绑定其原包；当前selector的作者、实际helper／模型／GPU和性能另验。
服务器正在使用的 `8d35ddea` 普通链包保持冻结，不随本入口重打包或重新认证。
