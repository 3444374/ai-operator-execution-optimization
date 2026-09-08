# 会话公共接口、Job选择与旧适配器清理

2026-09-08，内部工程验证；基线`2ad67c06`，实现于`codex/session-api-cleanup`。
对应[三项整理设计](../../../plans/semloom_multisession_design.md)。本轮不新增PG算子或GPU调度算法，
只收敛既有执行框架，保持同步Filter/Map参考、v6协议及资源归属规则。

## 三项改动

| 问题 | 完成方式与检查 |
|---|---|
| 网关直接访问session私有状态 | 提供`fail(reason)`、`set_dispatch_enabled(bool)`、`close_consumer(clean=...)`；网关不再调用`_fail/_records`或直接设置会话状态 |
| Job轮转固定在Engine内 | `choose_flow`接收不可变ReadyJob与JobSelectionHistory，返回FlowChoice；默认函数保留原Job优先轮转，Engine继续校验候选与执行容量 |
| 测试仍依赖独立单流适配器 | 迁移两个测试模块到生产MultiSessionMapGateway，删除IncrementalMapRuntime和IncrementalMapSessionAdapter，保留共用IncrementalMapProtocol |

`close_consumer`只能在发送方不再访问交付结果后调用，gateway先join线程再调用；它归还已交付
但未显式release的结果，包括未被发送线程取走的应答。普通`close()`仍返回WAITING_FOR_RELEASE，
不会擅自释放消费者持有的数据。未知远端工作继续占用计算额度，权威终态到达后才能归还。
公开fail只失败所属流；非法Job选择隔离Engine，不会派发伪造或容量不合格的任务。

一并删除旧适配器独占的错误查询回调、HTTP全局错误字段、无作用的异常转抛及登记模式开关。
tracked调用方已全部迁移；没有发现已承诺的外部兼容接口。无法据仓库搜索保证没有仓库外程序
自行导入旧内部类；旧嵌入式调用应迁到`MultiSessionMapGateway(max_jobs=1, ...)`。
同步参考、公共backend、旧同步调度器和历史实验数据仍有消费者，本轮不删除。

## 验证

| 检查 | 结果 |
|---|---|
| 本地 | 68项相关测试通过；完整provider40项中39通过、1项平台跳过，两套结果有重叠 |
| Linux | 调度350、provider40、PG合同115、观测及运行辅助14，共519项通过 |
| PG18.3 | 回归1/1、TAP1963/1963；PG实现和扩展二进制未改，最终代码重新运行完整检查 |
| 真实模型 | 首轮单Job12次通过；修复wake后用新账本重跑单Job12次，再执行多Job18次，累计42次POST全部通过，无模型请求重试 |
| 身份 | 最终manifest覆盖643份code文件及根pyproject.toml，共644项；旧runtime的删除另作import检查 |

真实链路为PG → v6 → 同一生产gateway → session/组织器/async backend → Qwen模型 → 原PG行。
使用先前多Job验证中的合成输入和同一12/18请求driver，不调整断言：同步参照、NULL/LIMIT0/
EXPLAIN零调用、SELECT/INSERT及独立回读、错误写入原子性、取消57014、恢复及跨Job真实HTTP重叠。
受控测试补充消费者回收保持迟到请求、局部fail、替换Job选择、非法选择拒绝；默认轮转公平用例保留。
它们不替代真实模型检查，也不构成性能/模型质量结论。

模型Qwen2.5-7B-Instruct，revision `a09a35458c702b33eeacc393d103063234e8bc28`，vLLM0.25.1；
9项模型文件重哈希，单GPU/BF16/eager、模型长度4096、max-num-seqs4、batched-tokens4096、FCFS、
显存比例0.8、关闭prefix cache。temperature0，普通输出上限128、取消任务256；沿用原服务配置，
仅各次运行的隔离目录和localhost端口不同。不下载模型或混装依赖，不做性能调参。

最终单Job HTTP峰值2；多Job跨Job峰值2，14个增量任务对应14个权威终态、9个Job归零。
三次运行的model/PG/gateway进程与端口均已清理，两张GPU均回到1MiB。各次模型官方来源与9项文件
校验通过，最终源码与本地manifest一致。此次生产源码净减70行，测试场景迁移而非删除覆盖。

## Bug与修复记录

新增公共close_consumer初稿释放lease后未发送wake，独立调用方可能继续等待已经可用的容量。
在释放前后检查WakeSignal.generation的回归先失败（4未增加），补上通知后68项相关测试通过。
最终源码再跑Linux、PG和独立真实模型账本；首轮模型结果保留其原manifest及修复前的两个源文件。
现有gateway会主动推进，因此首轮12次通过不能代替这个公共接口回归。

其余失败属于验证准备/测试本身：初次测试误用了CloseReport字段名；更正为uncertain_requests。
首次打包使用了错误工作目录，空文件清单使临时副本的格式化范围扩大；弃用该副本，重建git archive，
并检查清单非空。第二次准备漏带根pyproject导致采用不同lint配置，补齐后通过；最终manifest包含该文件。
另一次preparer早于manifest上传完成而退出，没有启动模型；在新目录确认上传完成后重新准备。
上述日志均保留；未修改断言以掩盖失败，没有模型请求重试。

## 本地冗余副本

主工作区原有46个未跟踪的`… 2.*`副本。45个与受Git管理的原件逐字节相同；另一个设计文档
与`f6cd2392`中的同名原件逐字节相同。经用户授权删除，原件或该历史提交均可恢复。
审计清单在证据归档中；这些是未跟踪文件清理，不计入提交的源码删行统计。

## 复核与使用

[摘要](summary.json)、[压缩证据](evidence.tar.gz)、[逐文件hash索引](evidence-index.json)。
74份材料归档前先对明文日志/脚本/账本脱敏并运行秘密扫描，再以固定mtime压缩。
`api-final-manifest.json`对应最终源码；`api-clean-manifest.json`及`prewake-*`对应第一次成功模型运行。

```sh
PYTHONPATH=code python -m unittest discover -s code/tests/scheduling -t code
PYTHONPATH=code python -m unittest discover -s code/tests/execution_provider -t code
PYTHONPATH=code python -m unittest discover -s code/tests/postgres -t code
```

PG按平台runbook隔离运行`make installcheck`；模型driver/controller见归档，使用仓库外runtime配置
和全新账本，不复用已消费账本。当前剩余的PG多算子Job归属、动态资源策略等不属于本次三项整理。

<a id="main-integration"></a>
## 主干集成

2026-09-08，main从2ad67c06快进到61fa6967，无冲突或代码改写。合并前再次核对最终644份
源码/配置和74份归档材料，全部一致；最终12/18次真实模型记录通过，已有Linux519项与PG
回归1项/TAP1963项适用于此次源码。本次集成只补记文档，没有重新启动服务器或模型。
同查询多算子Job归属等后续功能未因本次清理而新增。
