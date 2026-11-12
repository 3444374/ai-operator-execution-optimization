# 有限链回执比较的退出状态

问题来自 [PR #99](https://github.com/3444374/ai-operator-execution-optimization/pull/99) 的验收边界：
`compare` 已在 JSON 报告记录 `outputs_strictly_comparable=false`，但 CLI 仍退出0，
标准输出也不包含这个字段。只检查进程退出码的调用方可能把输出差异误判为通过。

## 修改与证据

基底为 `c552eddf90c9dd84a195f586a5a4114897c5fdc9`。复用原配置解析、回执读取、逐字段比较和私有报告写入，
只修正 CLI 的验收行为：**先保存完整差异，输出严格一致标记；不一致退出1，一致退出0。**
现有状态名 `captured_receipts_compared`、报告 schema 和差异明细保持；它表示比较完成，不表示性能通过。

新增检查通过真实 CLI `main`、原 JSON 读取／写入和比较函数验证三种情况：

| 临时回执 | 修改前 | 修改后 |
|---|---|---|
| 全部字段及最终关系相同 | 退出0，stdout缺少一致标记 | 退出0，stdout标记true |
| 一个输出token改变，长度不变 | JSON记录差异，但退出0 | 保存差异后退出1，stdout标记false |
| 最终关系改变 | JSON记录差异，但退出0 | 保存差异后退出1，stdout标记false |

先运行新增反例得到2个失败子场景和1个stdout字段缺失错误；修复后同组4项通过。
完整 `tests.experiments.test_record_chain_validation` 在已有隔离driver中33项通过，无跳过。
此前系统Python运行同模块时因缺LOTUS／Transformers等依赖有6项错误；失败保留，没有安装依赖或改用例绕过。
复现命令为 `PYTHONPATH=code "$DRIVER_PYTHON" -m unittest tests.experiments.test_record_chain_validation -v`。

[checks.json.gz](checks.json.gz)保存各轮完整脱敏输出、原件摘要与最终源码摘要；
仓库外原件按运行标识 `semloom-pr-followup-20261111` 保留。
本次只有临时回执、固定响应和 CPU 子进程检查，模型、GPU、服务器运行与下载均为0。

## 保留的边界

旧提交的61项服务器 CPU 检查保持其原来源，不能替代本次修改的服务器资格。
该入口每个case仍创建并关闭模型与Ray；修正退出码不会把冷查询转换为常驻暖查询。
现有Core的常驻模式绑定rolling refill，不能在两臂之一打开或通过从冷时间减启动来构造暖比较。
未来常驻对照须匹配两臂生命周期、输入、资源及完整输出，再独立校准和重复。
严格一致通过也不证明GPU饥饿、稳定吞吐或完整查询加速；报告原有性能资格限制保持。

<a id="server-step-actions-20261112"></a>
## 2026-11-12：两臂真实调用完成，严格比较失败

实测源码为 `122e6f383f9340a572a4c69c0e9e78023eeae0a3`，基于 PR #99 的准确
`55c0a7d0ef10227758b32a8587ab09cf556ace52`，仅追加完整关系确认后、自动关闭前的
观测时间；原 iterator、owner 时钟、执行、结果与关闭流程保持。
外层资源监督使用独立的 `933157666e1db81d47872fe21422ac8315ea7be3`。
运行标识为 `semloom-pr99-step-actions-20261112-r1`，两份源码及资格不能混成 main 的验证。

负载是现有数据原序前 16 条的项目自定义 Map→Map→Filter，不是 SemBench 原题。
同一 RTX 4090、Qwen2.5-7B-Instruct、vLLM 0.25.1，C4、B1、FIFO 和 record progression
保持。两臂依次执行 `one-action`（`step_actions=1`）与 `default-actions`（默认 16）；
MethodDriver 实际 advance bound 分别为 1 和 4。每臂重新创建并关闭模型和 Ray，
各运行一次，没有额外预热或重试。生成上限为 8 token、temperature 0、top_p 1、seed 0、
stop 为换行；完整记录的参数、实际版本、输入 SHA 及资源见
[脱敏观测](server-20261112.json)。没有修改参数或选取答复消除差异。

两臂各完成 48 次确认终态，总计 96 次；各自原方法回放通过，最终完整 6 行关系相同。
按 `call_id` 核对全部发生项，消息 SHA、完整 prompt token 数组、输出文本和
`finish_reason` 均为 48/48 相同；输出 token 数组只有 47/48 相同。
唯一差异为 `movie:15 / map0`：两臂文本均为 `Label: Film Review`，finish reason 均为 `stop`，
token 数组分别是 `[2476,25,16631,10294,271]` 与 `[2476,25,16631,10294,151645]`。
这不构成 token 完全等价，原因未独立确定。

**原 compare 真实退出 1，`outputs_strictly_comparable=false`。外层总状态继续保留 failed，
失败阶段是严格比较；没有把相同最终关系替代严格请求验收。** 两臂独立的 owner、模型、
Ray 关闭和外层资源检查通过，无剩余已拥有进程或 GPU compute 进程，SHM 不变，租约释放。
比较失败阶段的诊断和原件同样保留，不重跑或用成功子集覆盖。

| 实际观测（秒） | one-action | default-actions |
|---|---:|---:|
| `run_case` 入口→完整内存关系 | 57.097108444 | 44.887938895 |
| worker 入口→完整内存关系 | 76.212974822 | 63.748817948 |
| 旧 `full_owner_call_s`（含自动关闭） | 62.282338399 | 50.010018273 |
| 原 worker wall（最终报告写入前） | 82.671767093 | 70.160280597 |

第一行包含 owner 构造、evaluate、模型启动和完整关系消费，排除 `run_case` 之前的
CSV/tokenizer 加载及 caller 的 Ray 初始化。第二行由同进程的真实
`full_relation_ready_ns - process_start_ns` 计算，还包含 worker 导入、准备和 Ray 初始化，
仍不包含外层控制器进入及 worker 启动前监督。两者的终点都早于自动关闭、原方法回放、
结果持久化和 caller 的 Ray 关闭。旧 `iterator_complete_ns` 仍在自动关闭后，不能替代新终点；
全部原始 `timing` 字段及起止时间、回放和 Ray 生命周期分项保存在脱敏观测中，没有倒填旧运行。

这些是每臂 n=1 的实际时长，且输出 token 不完全一致，**不计算或宣称性能收益**，
也不能归因于 callback、GPU 饥饿或某个单独机制。步骤额度同时影响 poll、dispatch 和 delivery，
本批既不建立纯 GPU 时间，也不建立同 owner 暖查询资格。PR 保持 Draft，不合入 main。

101 份回收原件已逐项核验。完整私有原件按上述运行标识保留，归档 SHA256 为
`f1879a799e8c6afd89b7add0c797dfe7d8f13c9301967242b7eb9247ecbe3b0d`；
服务器摘要 SHA256 为 `f74345cdf2e724a09cd62ee430d4773aadc6eb6b80fc83c81cfbcf992fe5c3da`。
公开副本只保留支持结论的参数、计时、差异、资源状态和原件摘要，不发布主机、账户、私有路径、
runtime env 或完整原始包。此次整理只有离线核对，没有新增模型调用。


<a id="batch-invariant-followup-20261112"></a>
## 2026-11-12：两组成对真实比较，未见稳定步骤额度收益

原始 47/48 严格 token 一致的失败保持。新比较两臂共同显式使用
`VLLM_BATCH_INVARIANT=1`；这是新的原生数值配置，不回填旧结果，也不单独归因旧 token 差异。
原始请求、提示、8-token／temperature0／seed0／换行停止规则、同 C4/B1、FIFO、record progression
保持。仍为首16条 Movie 记录的项目 MMF，每臂48次模型调用；同一 RTX4090、Qwen2.5-7B BF16、
8核亲和性、上下文4096、batch-token4096、显存比例0.8，一次查询独立启动和关闭 Ray／模型。
没有业务预热或重试，不是原 SemBench 题目，也没有稳态吞吐资格。

首次在源码 `122e6f383` 上的一臂实际完成48次调用，原方法回放与正常清理通过，但
GPU进程初始环境／配置日志不能确认数值模式，按预定观测门禁停止；没有比较第二臂。
随后源码 `4294f14bc412cbec2c067157605c50c63d2e4d26` 增加有限时的命名 worker RPC，
直接回读 vLLM 0.25.1 的请求开关与已初始化 batch-invariant 状态。两者在全部新 case 均为 true。
省略开关时不新增 RPC；回读失败记录 unavailable，不当作 false。它不启用模式、不增加生成调用，
不依赖 `/proc` 中可能被进程标题改写的初始环境。

修复后仍使用原288次累计 ledger及原40分钟截止时间。首个96次预约不退款；仅用剩余两个
未领取的96次预约，按默认→单步、单步→默认完成两组。实际执行为原失败48次＋新四臂192次，
共240次，未重置额度。四臂均48/48完整字段／prompt与输出token／最终关系一致，跨两组也一致；
真实配置回读、原方法回放、恰好一次、物理进程／GPU／SHM与租约收尾通过。

| 组 | Core单步：到完整关系，s | 默认16步：到完整关系，s | 单步耗时变化 | 单步／默认模型 API 区间，s |
|---|---:|---:|---:|---:|
| 1（默认先） | 61.346252 | 62.364822 | 减少1.63% | 4.522000 / 4.585960 |
| 2（单步先） | 63.458745 | 62.512629 | 增加1.51% | 4.599584 / 4.495681 |

完整关系时间包含冷模型启动；模型 API 区间是实际最后 model-end 减最早 model-start，
包含排队、调度与 CPU 间隙，不是纯 GPU 时间，更不是扣除启动所得的暖查询。
效果方向不一致，每臂仅2次冷会话，不能宣称步骤额度优化有稳定收益，默认保持16。
批次不变模式的自身开销未单独校准，不与原数值配置混成统一性能比较。

### 检查与归档

4项新增观测检查加相关上下文检查共13项通过；初次旧 context IPC 测试漏传必需 limits 的错误保留，
补显式容量后通过。接纳远端同 PR 的后续 main 整合后，相关13项再通过；实际模型证据仍固定4294f14bc，
不转授整合树其他路径的真实模型资格。

controller 结束时 `copytree` 尝试复制已关闭 Ray 的 `plasma_store` socket，造成归档阶段失败。
原 FINAL.failed、错误和全部测量保留；随后只复制普通证据文件，2172项逐文件 SHA 核验，
4个非普通 socket 路径登记排除。这是归档修复，未重复模型，也未改写原 failed 为 passed。
两组成对比较与正常资源回执均在该归档错误之前完成。

[机器汇总](batch-invariant-summary.json)、[公开证据包](batch-invariant-evidence-20261112.tar.gz)和
[原件／公开摘要清单](batch-invariant-manifest.json)保留首轮失败、两组全部重复、配置、实际worker回读、
请求、资源与归档修复。完整私有原件在本地独立下载后核验SHA；公开包保留必要原始观测，
完整 Ray 服务诊断按清单私有归档恢复。解包后执行
`python local/summarize_pr99.py server/readback-followup <输出>` 可重算新两组。
