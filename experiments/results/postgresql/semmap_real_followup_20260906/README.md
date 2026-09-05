# SemMap 有限真实模型复查（2026-09-06）

内部工程验证记录，研究对象为 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化。
实施依据为 [Map 合同 §8.4.4](../../../plans/postgresql_semmap_generation_contract.md)。

**本轮新增7次真实请求，原账本由25/32到32/32；没有推理重试。** SELECT、INSERT的输出/写回/usage，
SQL NULL零调用，真实取消、模型拒绝及两次恢复均完成了功能核对。SELECT和四个故障/恢复阶段的资源
检查通过；真实INSERT的资源测量被验收SQL自身打开的目录FD污染，原判定保留为
`inconclusive/not_evaluated`。驱动已修正，独立无模型反例验证了隔离方法；本轮没有再次请求模型重测INSERT。
不能据此声称完整真实资源资格、正式fixture3×2000、四D整体、生成质量或性能已通过。

## 1. 固定身份与实际授权

运行时代码为 `77a123de21af2f19eacad207a310109393d0894c`，与前一轮
[资源工具修复](../semmap_resource_lifecycle_20260906/README.md)相同；其验收提交为`3e5801dc`。
本轮只增加实验驱动，没有更改PG planner/executor、production provider、wire v5或生成语义。

| 对象 | 实际值 |
|---|---|
| 模型 | Qwen/Qwen2.5-7B-Instruct，revision `a09a35458c702b33eeacc393d103063234e8bc28` |
| 硬件/软件 | 单RTX4090、BF16、vLLM0.25.1、Torch2.11.0+cu130、PG18.3 |
| 服务 | localhost独立端口；context4096、seqs4、batched_tokens4096、显存比例0.8、eager、关闭prefix cache、generation-config=vllm |
| 请求 | temperature0、top_p1、n1、stream=false、stop=null、max_tokens128；HTTP timeout120s |
| 观察驱动 | 握手等待同批两端最多5s，放行后查询最多10s；资源清理与服务队列分别最多60s |
| 预算 | 同一 `semloom.semmap.4d.real.v1` 持久账本，总上限32；新增最多7次，取消/拒绝均计入 |

四个已有权重分片的SHA逐一匹配[Hugging Face固定revision的官方元数据](https://huggingface.co/api/models/Qwen/Qwen2.5-7B-Instruct/revision/a09a35458c702b33eeacc393d103063234e8bc28?blobs=true)。
配置、tokenizer、索引文件、权重、实际进程argv、PID/start-time及模型API身份的核对结果进入
[机器审计](raw/real-audit.json)。未下载模型、安装依赖、改动已有服务或使用第二张GPU。

## 2. 三次独立运行

| 运行 | 实验驱动提交 | 账本 | 结果 |
|---|---|---|---|
| real1 | `7363cf30` | 25→25 | gateway准备失败，零新增请求；错误引用历史初版预算校验器 |
| real2 | `acb88ef5` | 25→28 | SELECT通过；INSERT功能核对通过但资源测量不通过，按条件停止 |
| real3 | `9cf5ff88` | 28→32 | 仅执行尚未发出的四个故障/恢复阶段，全部有效且通过 |

real3明确标为`remaining_faults`。它没有重复SELECT或INSERT，不能与real2拼成一次完整资源通过运行。

real1引用的历史初版正则把`\Z`写成了错误转义，拒绝了完整有效的账本。只读核对确认25条记录的字段、
连续序号和64位摘要均正确；改用历史成功的`map_gateway_observer_run4.py`和`real_checks_run6.py`，
并在driver内固定完整SHA。原账本未修写，原失败脚本、日志与目录均保留。

首请求前的驱动审查还发现账本读取异常可能阻止summary落盘；修复后用`OSError`反例验证：
失败summary存在、final_attempts为null、错误类型保留、零模型请求。完整工具测试的130项与该附加
受控检查分开计数，不把它加入旧测试批次。

## 3. 真实模型观察

| 阶段 | 账本序号 | 实际核对 | 资源评价 |
|---|---:|---|---|
| SELECT | 26–27 | Unicode与非NULL空串各1次；SQL NULL不调用模型；3行ID、原始输出字节对应；prompt/output分别39/4、36/1 | valid/passed |
| INSERT SELECT | 28 | ASCII与NULL写入2行；1次模型调用；原始输出/写回字节相同，EXPLAIN calls/rows/usage对应，tokens42/7 | inconclusive/not_evaluated，原结果保留 |
| cancel | 29 | HTTP发出后取消，PG返回57014；实际模型随后以length结束，tokens32/128；取消语句未接收该完成结果 | valid/passed |
| cancel recovery | 30 | 恢复查询输出字节和usage对应，tokens38/3，finish=stop | valid/passed |
| model reject | 31 | 108000-byte输入超过4096-token上下文；服务拒绝，gateway为MODEL_REQUEST_REJECTED，PG为38000及固定消息 | valid/passed |
| reject recovery | 32 | 恢复查询输出字节和usage对应，tokens39/4，finish=stop | valid/passed |

除取消的length完成外，其余5个completion均为stop；另1次是预期请求拒绝。6个completion的实际model ID
均匹配固定服务。成功行逐字节比较的是“PG值与本次模型raw completion”，没有把输入当成质量标签。
plain EXPLAIN、LIMIT0与NULL-only也实际验证为零调用。

SELECT与四个故障/恢复阶段的provider两端socket sampled peak均为2，结束残留为0；同一存活
backend/gateway的结束FD身份匹配原基线，FD/线程增量为0，RSS在原条件内。
取消阶段分别保存了PG取消、HTTP完成和vLLM队列回空，UDS关闭没有被当成GPU停止的证据。

## 4. INSERT测量问题与修复的证据范围

real2的INSERT结束时，backend比基线多4个普通数据文件FD。PG18.3安装件中的catalog声明及独立SQL查询
确认，它们分别属于：

| filenode | 系统目录/索引 |
|---:|---|
| 2686 | pg_opclass_am_name_nsp_index |
| 2665 | pg_constraint_conrelid_contypid_conname_index |
| 2755 | pg_opfamily_oid_index |
| 2753 | pg_opfamily |

实验driver把用于验收的JOIN/ORDER BY读回查询放在被测backend、操作采样窗口中。独立无模型PG反例
只执行这条普通SQL就新增了14个系统目录FD，其中包含原4个；将读回交给另一连接时，被测backend的
FD增量为0、身份集合不变。这个对照支持“验收查询污染了资源观测”的判断，不需要推断provider泄漏。

`9cf5ff88`将读回改到独立审计连接，并放在资源采样完成之后。INSERT的SQL完成与原始输出核对事实保留；
原资源判定不改，修正后的真实INSERT资源测量尚未重跑。此时只剩4次原计划预算，因此仅执行未发出的
cancel/recovery/reject/recovery，不花新请求获取一次更好看的INSERT判定。

测量边界因此更清楚：被测SQL及其backend、gateway属于观察对象；用于核对结果的SQL由独立连接执行。
该修正只涉及实验工具，不改变生产执行或原数值阈值。

## 5. 证据、清理与未完成项

[real-audit.json](raw/real-audit.json)记录各阶段状态、数量、usage、输出SHA、PID/start-time、
baseline/operation/cleanup统计、原始文件SHA与原地哈希核验结果。三次运行的哈希均无不匹配；
原始请求/输出文本和完整日志留在服务器，没有批量导出。

账本最终32条；原25条前缀SHA仍为`d3a653fc…dead291`，与本轮开始时相同。旧结果中的25/32是
2026-09-04那次运行的历史值，本次追加记录没有改写该历史。当前余额为0，后续推理须另有明确预算。

所有本轮PG backend、gateway和独立集群已退出；模型进程及子进程按记录的PID/start-time清理，
独立端口已关闭，两张GPU结束显存均为1MiB；服务身份在停止前保持不变。未杀除本轮之外的进程。

源码与复算入口：

- [real_checks.py](real_checks.py)：固定辅助脚本身份、预算、SQL核对与阶段执行；`--remaining-faults-only`只允许账本28起步。
- [real_gateway.py](real_gateway.py)：复用32次预算/HTTP observer及SessionObserver，真实adapter只调用一次。
- [audit_real.py](audit_real.py)：服务器原地复算，仅输出允许字段；通过`--trace-auditor`复用[资源trace审计](../semmap_resource_lifecycle_20260906/audit_diagnostics.py)。
- [PUBLIC_SHA256SUMS.json](PUBLIC_SHA256SUMS.json)：本目录公开文件自身的哈希；它们与原始日志/trace的哈希分列。

本轮授权工作已执行并保留了失败。尚未完成：修正后的真实INSERT资源复查、正式fixture3×2000、
完整四D资格、模型质量/成本及性能验证。它们不能从本轮功能通过或不同run的片段推导出来。
