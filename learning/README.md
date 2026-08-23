# Learning Notes

## 2026-08-21 为什么公平性可以从框架外观察，但不能从完成行数猜

Daft/Ray Data 不公开内部 scheduler，不等于五臂无法比较 tail 和经验公平性。共同的
observation-only gateway 只给每个 Job 一个独立 path，原样一次转发到同一 vLLM FCFS backend；它
没有 queue、retry、admission、batch 或 route choice。这样框架仍拥有执行顺序，而 gateway 能统一
记录真实 request arrival/completion 和 endpoint actual token usage。公平窗口只取两 Job 同时存在
outstanding gateway requests 的交集，并按实际完成 token work 计算 weighted share、Jain、service
lag 与 longest no-service。`Jain(completed_rows)` 会把两个 512-row Job 恒算成 1，不能表达服务份额。

系统 headline 另用 T0--T4：T0 在 PostgreSQL 读取/child 初始化前，T1 为首批 source data，T2/T3
为首请求到达/末请求完成，T4 为完整正确结果在内存中可见。Job/group JCT 和 correct throughput
包含 source、转换、初始化、排队和模型执行；source/execution/service span 分列解释收益来源。
输出无需 sink：`writeback=none` 时 completion digest 只做边界外正确性封存。within-run victim
inflation/recovery 是隔离观测；没有 matched-solo control 时不能写成 full-solo slowdown。

## 2026-08-21 为什么 readiness 不能相信一份手写的 passed JSON

readiness 是一次证据推导，不是调用者自我声明。system-preflight 现在由可运行入口实际检查 endpoint
health、PostgreSQL/pgvector、Ray/GPU clean，并从 bounded HTTP root 的原始 run status、gate 和
per-endpoint summary 重算通过状态；后续 readiness 还会重跑同一组只读探针，要求结果与封存证据
逐字段相同。correctness smoke 则复用五臂真实 executor 跑一轮，但使用独立、显式 fresh root，避免
占用后续 rehearsal 的 canonical root。第四阶段只接受这个 root 的 `matrix_index.json`，并重哈希
manifest、Job 行数、sink digest、native upstream/adapter provenance 和 raw artifacts。

同样，formal 不能只检查 archive 里“有一个 index、一个 snapshot、一个 cell 文件”。离线 validator
必须重新核验完整五臂 cell 合同，并要求 tar 与 root 的全部文件集合及每个 SHA 完全一致。这样一份
只有 `{}` snapshot 或单个伪 cell 的形式匹配归档无法生成 rehearsal validation；formal authorization
还显式绑定 matched/native/Project 三份 config SHA，并把 native config 中冻结的 upstream commit 与
adapter SHA 逐臂对照最终 cell。它绑定的是经深校验的 validation/root/archive，而不是一组可手写的
布尔字段。后续 `862d0008` 已用该门完成一次 gateway 前的 correctness smoke/rehearsal；formal 未运行。

## 2026-08-21 为什么“同一个 endpoint”仍不足以证明五臂可比

五臂过去只检查 endpoint 进程没有 `--scheduler-cls`。这只能证明 scheduler class 没被显式替换，
不能阻止模型 revision、dtype、vLLM wheel/source、capacity、chunked prefill、prefix cache、
compile/eager 或 GPU-memory 配置漂移。新合同把模型 artifact hash、vLLM dist/source exact SHA 和
完整 runtime flags 合成一份 `service_identity`，要求 matched/native/Project 三份实际配置完全相同；
source audit 缺 expected SHA 时永远不能返回 `passed`。live gate 再逐 endpoint 核对显式 cmdline，并
以本地模型文件 hash 把声明 revision 绑定到实际 artifact。静态配置通过只写
`static_config_passed/rehearsal_ready=false`，不能被误读成已可跑 rehearsal。

本次服务器访问只有环境/归档/source 的只读 preflight，endpoint 当时未运行，未执行 correctness
smoke、rehearsal 或 formal。服务器归档同时纠正了“五臂 rehearsal 从未运行”的错误表述：

| commit | 等价入口 | 保留 root / archive | 阶段与失败原因 | 可比较结论 |
|---|---|---|---|---|
| `ea4cbb3b` | `run_saor_native_system_matched.py ... --rehearsal` | `saor_native_system_matched_matrix_20260819_r2/`；未发现独立 tar | warmup 第 1 个 Project selector-sanity cell；MFU `missing_gpu_peak_tflops` guard | 无 |
| `58154151` | 同一 rehearsal 入口 | `saor_native_system_matched_matrix_20260819_r3/`；未发现独立 tar | 同一阶段；`job 0 has no unique successful summary` | 无 |

两份 `matrix_index.json` 证明 execution mode、commit、root、cell 和原因；服务器 shell history 没有保留
逐字 argv，所以这里只写冻结 runbook 的等价入口，不把重建命令冒充原始历史。当前准确状态是：
gateway 前已有一次成功五臂 rehearsal，但它没有五臂统一 request tail/fairness，不能回答当前完整
比较；新 gateway 合同仍需全新 smoke/rehearsal。formal 仍需在新 rehearsal 归档审核后另行签发。

## 2026-08-20 为什么现在是“五臂系统表 + 独立 VTC 机制表”

五臂系统表回答完整 database-E2E 系统差异：三条 framework-owned native graph、project
frozen-static、SAOR。native graph 不能被偷偷接上项目的 bounded-ready、K/W、credit 或 selector；
static 也不能拥有 SAOR 的动态 ready/debt。旧 FIFO/DRR/VTC-style/strict-priority 数据仍有内部
归因价值，但不再跟完整系统混排。

官方 VTC 回答另一件事：在官方 S-LoRA serving stack 内，FCFS 换成 VTC 后发生什么。因此必须
同栈保留 FCFS control；它不含 PostgreSQL、Daft、Ray Data 或 Project coordinator。当前 artifact
对 CUDA/PyTorch/Ampere 的要求尚未在本项目 4090/model/runtime 上验证，所以只能登记 capability
blocker，不能在 vLLM 中重写一个近似版本后称“官方 VTC”。

三个时间概念也已拆开：`job_release_time` 是共同外部到达；`arrival_replay` 是执行器内部逐请求
回放能力；`bounded-ready` 是 SAOR 在 Job release 之后观察已经可提交的具体 work。SAOR 若在
release 前 ready/register/submit 会直接失败。eager 不等于绕过 SAOR：每个 Job 启动后，profiler
仍把组织结果拆成 concrete request envelopes，再由 bounded-ready 的 K/work/bytes 窗口执行
register→grant→submit；同一个 observed Job start epoch 会进入 scheduler request 和 trace seed，
供 SLO budget 与证据 join 使用。只是这些 request 不按 manifest 内的时间逐条睡眠回放。旧 single-head
bounded-priority 因依赖逐请求到达语义仍必须 replay。

MFU 的 peak/precision 已冻结进 config SHA、resolved
fingerprint 和 cell，但因五系统没有统一可信 FLOP numerator，本轮 MFU 诚实标 unavailable。
Project 命令继续传 peak/precision 是为了固定 denominator 和保留可复现实路径诊断，不代表已经
获得可跨五臂排名的 numerator。
五臂都使用 `writeback=none`，不连接输出 sink。native/Project 分别从执行器结果写独立 completion
evidence，按冻结 doc-id、内容 digest、行数和 exactly-once 校验；T4 是完整正确结果的内存可见时钟，
证据文件封存不计入 JCT。

本次是本地合同重构，没有连接服务器，也没有启动新的 rehearsal/formal。这里的“本次未运行”不
等于历史上从未运行：仓库已归档 2026-08-13 bounded-priority gate failure（`9ae64db3`）、
2026-08-14 server regression（`dd83136d`）、feeding gate failure（`60e47469`）和 2026-08-17
feeding-gap fail-closed stop（`f1844c0f`）。它们是可访问的失败/capability 证据，不是性能结论。

| 失败提交 | 已知执行入口 | root / archive | 失败阶段与原因 | 有可比结论？ | 当前可访问性 |
|---|---|---|---|---|---|
| `9ae64db3` | `run_shared_vllm_experiment.py --rehearsal` | 两个 development root；`saor_bounded_priority_gate_20260813_2de6f93_full.tar.gz`，SHA `be6ce0a3…` | 第二轮 0.25K debt-recovery=0，runner mechanism gate fail closed | 否；只能作 diagnostic | Git 有 compact evidence；完整 tar 在历史服务器/镜像记录 |
| `dd83136d` | 同上，formal-evidence 修复后的 regression rehearsal | `saor_bounded_priority_rehearsal_15201946_regression_20260814/` 与同名 tar | 0.25K 再次被相同机制门拒绝 | 否 | Git 报告可访问；完整 root/tar 为仓库外记录 |
| `60e47469` | `run_saor_feeding_ceiling.py --rehearsal` | `...c988622a...retry2`；Git 内 `saor_project_feeding_ceiling_c988622a_20260814_retry2.tar.gz` | feeding 92.898% < 95%，锁定 negative gate；更早两个失败 root 分别为 SSH 中断、exactly-once 字段缺失 | 只支持该次 feeding stop，不是稳定性能排名 | 最终 12-file tar 与 compact evidence 可访问；前两个失败 root 只留审计记录 |
| `f1844c0f` | `run_saor_feeding_gap_diagnostic.py` | `saor_feeding_gap_diagnostic_345bee2f_20260817`；tar SHA `f4b9793d…` | 第 11/12 cell 出现 1/512 zero-retry `ReadError`，未运行 summarizer | 否；10/12 仅描述性 | 原提交可恢复 compact evidence；完整 root/tar 为仓库外镜像记录，后续全新 root 重跑 |

## 2026-08-19 为什么“同一 workload”还要绑定 endpoint、Job 和原生执行参数

系统级比较不能只让 Daft、Ray Data 和 Project 读取同一份 1024 行文件。还必须证明三类执行器
访问同一对 vLLM endpoint；Job0/Job1 分别是冻结的 512 行且没有互换；Daft/Ray Data 实际使用的
adapter、concurrency、batch 与校准身份一致。否则即使最终都显示 1024 条完成，仍可能是在不同服务、
不同 Job 切分或不同框架配置上运行，逐 Job JCT 和系统排名没有可比性。

当前合同在 dispatch 前交叉核对 matched/native/Project 三份配置，在每个 cell 中保存 endpoint、
Job ID/SHA/行数和原生选择身份，离线汇总再从封存命令重验。这里的共同 endpoint/manifest 只是
实验控制；Daft Native、Daft Ray 与 Ray Data 仍分别拥有自己的 batching、backpressure 和 scheduler，
不会继承 Project 的 token-budget、K/W、credit、bounded-ready 或 router。本次仅完成本地合同与测试，
没有连接服务器，也没有产生新的 GPU 性能结论。

## 2026-08-17 为什么 evidence 自包含不等于只保存一个目录名

若 matrix index 保存 manifest、resource trace 和 output artifact 的绝对路径，原机器上验证通过并不代表
归档可复核：只要把完整 root 改名、复制到另一块磁盘或解压到不同目录，验证器就会继续访问旧路径。
正确合同是把不可变 manifest 封存进 matrix root，其他 cell 产物必须实际位于 root 内，并只保存相对
路径。离线解析时拒绝绝对路径和 `..` 逃逸，再核验存在性与 SHA；这样“可搬迁”仍保持 fail closed，
而不是通过跳过文件校验实现。

数据库身份也必须是矩阵级事实。每个 cell 各自拥有非空 `server_version`/`pgvector_version` 只能证明
单格有记录，不能阻止 18.4 与 18.3 混入同一排名。共享 typed `DatabaseIdentity` 现在同时服务 shard、
runner、cell 与 summary：先拒绝 sentinel，再要求所有 cell 的版本对完全相同。

最后，外层 summary 的脱敏无法清除底层已经落盘的原始异常。异常必须在 native shard summary、
multi-job Job/cell/preflight 和外层 matrix 每个持久化边界调用同一个 `redact_text()`。模拟凭据测试扫描
所有 JSON/CSV，而不是只检查最终报告；free-text redactor 还要覆盖 JSON quoted secret、Bearer 与已知
token 形态，不能只测一个 `api_key=...`。matrix index 采用 curated schema，原生 raw Job summary 再
封存 per-Job manifest 并相对化 shard artifact，避免“验证器不读取旧绝对路径”被误当成真正自包含。
本次仍只是本地证据合同修复；未连接服务器，native-system 与 SAOR formal 均未运行。

## 2026-08-16 为什么同一合同还需要 matrix instance identity

commit、config、manifest、service signature 和 scheduler owner 只能说明两次矩阵运行遵守同一份
实验合同，不能说明某个 cell 确实来自这一次物理运行。若两个 output root 使用相同合同，旧验证器
允许把 root B 的一个 cell 替换进 root A，因为逐 cell 的这些字段仍全部相等。新的
`matrix_instance_id` 在独立授权通过后为每次矩阵随机生成，并同时写入 contract snapshot、index 与
所有 cell；离线验证逐项等值检查，因此跨 root 混合会 fail closed。它解决的是运行实例归属，不替代
authorization 或 artifact SHA。

证据状态也要区分“被授权运行”与“验证器核对过授权 artifact”。仓库自身始终不能宣称授权，所以即使
summary 通过，仍写 `formal_authorized=false`；另用 `formal_authorization_verified=true` 表示本次
sealed run 的独立授权身份已经被验证。二者合成一个布尔值会把事后证据核验误写成新的授权决定。

最后，失败证据与成功 CSV 使用同一审计纪律：异常消息落盘前统一脱敏；有效 physical cell 的
`server_version`/`pgvector_version` 必须来自 timed PostgreSQL source 的真实 shard/Job 摘要，并逐
cell 写入 `all_runs.csv`。目录名、部署说明或配置默认值都不能代替实际版本证据。本次仅修合同与本地
测试，native-system GPU/formal 继续停止。

## 2026-08-16 为什么 formal 授权必须是独立 artifact

配置里的一个布尔值或命令行 `--force` 不能证明“这次运行已经被审核”：它们既不绑定代码版本，也不
绑定 manifest 和完整配置，甚至可能在默认命令中被意外打开。native-system matched runner 现在把
授权改成独立 JSON artifact，精确绑定 repository commit、原始 config SHA、解析后的 config
fingerprint 和 frozen manifest SHA。任何一个字段漂移，都会在创建输出目录、获取机器 lease、调用
Daft/Ray/Project executor 之前失败，所以“未授权”不会留下一个看似可用的空实验目录。

汇总端不能只信 runner 写出的 `passed`。它会重新计算 authorization SHA、contract snapshot SHA、
manifest 内容 SHA，并逐臂核对 service signature、scheduler owner，逐 cell 核对 commit/config/
manifest/schedule 身份。这样 runner 和验证器即使分别看到一份结构正确的 JSON，也不能把不同代码、
不同 workload 或被替换 scheduler 的结果拼进同一排名。

失败证据也不能删除。现在失败矩阵保留 `all_runs.csv`，其中每个已记录 cell 都有原始 `status` 和
`failure_reason`；但 `system_summary.csv`、Job 和 resource 性能排名全部禁止发布。这一区分
很重要：保留失败事实是可复现性，发布不完整排名则会制造选择性报告。当前改动只是本地安全 hotfix，
服务器关闭且 native-system GPU/formal 仍停止，没有产生新的性能结论。

## 2026-08-15 怎么把 7.10% 拆成 W 代价和 Project 路径代价

旧 feeding gate 只比较了两个跨时间点：direct K-only 为 13,684.90 tok/s，完整 SAOR Project
路径为 12,713.03 tok/s。它足以按预注册 95% 门停止 formal，却不能回答损失来自 work envelope、
Ray/Daft/coordinator，还是两次运行的状态波动。因此新诊断在同一轮交错三条路径：D0 只有 K，D1
在相同 direct HTTP 上增加 W，P0 再把相同 K+W 放回 PostgreSQL→Daft→Ray→bounded-ready FIFO。

读比值时先看 `D1/D0`：它只改变 W，所以低于 95% 表示 W 本身有可复现容量代价。再看
`P0/D1`：二者都有 K+W，低于 95% 才把额外差距指向 Project plumbing。D1 不看 Job ID、权重或
ready window，所以它不是公平算法，也不是 Daft/Ray 原生 baseline，只是一个隔离变量的 Project
diagnostic control。即便两项都通过，也只能说旧单点 7.10% 没有在本轮复现；旧
`locked_failed_feeding` 仍永久保留。

为什么不能只看 GPU utilization：D0、D1、P0 都可能显示 GPU 约 95% 以上，但 W admission 的空洞、
Ray actor-ready、coordinator bounded wait 或 vLLM running 深度不同，仍会产生 tokens/s 差异。因此
诊断同时保存 request/work occupancy、admission wait、Ray submit/actor-ready、vLLM
running/waiting/KV、MFU、TTFT/ITL、JCT/SLO 与能耗。运行前 PG/Ray/endpoint clean 也单独落盘，
避免再次把跨时间状态混成调度代价。服务器当前关机，现阶段只有合同、代码和本地测试，没有新 GPU
性能结论。

## 2026-08-14 direct ceiling 为什么也要显式 exactly-once evidence

`direct_no_job` 会先用 `validate_results()` 验证 manifest request 与 HTTP completion 一一对应，但
“内部已经校验”不等于 group record 能自动看见结论。group schema 后来统一读取
`expected_count/completed_count/exactly_once`；direct adapter 若不显式返回这三个字段，正确完成的
ceiling 会在 record 构造阶段以 `KeyError` fail closed。修复是在 adapter 边界把已经证明的事实
结构化写出，而不是在 runner 里给缺失字段默认值。后者会让真正未校验的 direct 路径也可能假通过。

因此 ceiling 的证据链是：immutable manifest → `validate_results()` → per-Job 三字段 → group
exactly-once gate → feeding ratio。调度器路径与 direct 路径可以不同，但 correctness 证据合同必须
同样完整。

## 2026-08-14 三个 output-token 字段为什么不能混用

同一条 chat-completions 请求现在会看到三个容易混淆的字段：

- `estimated_output_tokens`：请求进入调度器前冻结的 admission estimate。本实验使用
  `fixed_output_cap`，所以每条都必须严格等于 `completion_max_tokens=256`；它决定 K/W credit
  是否允许请求进入。
- `actual_output_tokens`：endpoint 实际生成的 token 数，来自 endpoint usage 或返回 token IDs；
  completion 时用它修正实际服务量。
- `client_estimated_output_tokens`：客户端把最终文本重新分词得到的事后诊断值。由于服务端和
  客户端的文本清理/模板/tokenizer 边界可能不同，它不等于 admission estimate，也不能反过来
  决定已经发生的调度。

真实反例中 raw prompt=1001、chat template overhead=29、endpoint output=256，因此服务 work 为
`1001+29+256=1286`；客户端重分词只有 207。审计若误用 207，会伪报 estimate 1237 并误杀正确
运行。反过来，审计若无条件信任 trace 报出的 estimate=257/512，又可能把真实低估掩盖掉。
所以当前 fail-closed 合同同时冻结 `output_bound_source=fixed_output_cap` 与 cap=256，并逐请求要求
trace estimate 恰好等于 cap；实际 work 再检查不超过这个执行前上界。这是防止证据自报放大上界
的工程决策，不是新的调度算法。

`d6259f5f` 六臂 root 的 6,144 条请求实际都满足 estimate=256，并提供有价值的诊断性能；但它的
validator 当时还没有上述逐行 equality gate，所以只能叫 diagnostic rehearsal。只有加入该 gate
后的新提交、新 root 再次通过，才能叫最终有效 rehearsal；两者都不能自动授权 formal。

## 2026-08-14 为什么“recovery grant 出现过”还不等于债务已偿还

一次 `debt_recovery` grant 只证明 selector 选中过欠服务 Job，不能证明该请求完成，更不能证明
累计 debt 已回到 cap 以下。新的 lossless ledger 因此增加 `service_completion`，离线按 endpoint+
request ID 配对 grant→completion，并把 debt 从 `>=cap` 到 `<cap` 定义为一个 empirical repayment
episode。formal rehearsal 必须同时满足 completion≥1、unmatched grant=0、episode 全部完成、
unresolved=0；P95 repayment 还要在冻结的 30s 边界内。这是请求完成粒度的经验指标，不是 decode-
token 级的理论偿还上界。

Project mechanism 的“实验有效”与“方法胜出”也分开：correctness、ready observation、公平与机制
ledger 完整时，实验有效；只有 foreground P99/lag headline 和吞吐、bulk JCT、SLO、最长无服务、
repayment 保护项同时通过，才支持当前 workload 下的 constrained-Pareto claim。门没过应报告
valid negative，不能把它重新命名成坏实验。frozen-static 没有 registered-ready ledger，所以该
公平指标为 N/A，但仍参加吞吐/JCT/P99/SLO 比较。

## 2026-08-13 为什么还要补 single-head shared FIFO

当前 frozen-static 与 bounded-ready FIFO 之间同时变化了两件事：每个 Job 的固定分区变成共享
容量，以及调度器从只看每个 Job 的一个队首变成看见有限个 concrete-ready requests。因此二者
的吞吐差不能全部写成 bounded-ready 的收益。新的三臂 bridge 固定同一 workload、K/W、vLLM
FCFS 和 FIFO 顺序：`frozen-static → single-head shared FIFO` 只观察共享容量；
`single-head shared FIFO → bounded-ready FIFO` 才观察 ready exposure 与对应执行路径。
汇总器只报告这两个观测效应，不自动判胜负或授权 formal。这仍是项目内部消融；Daft Native、
Daft Ray 和 Ray Data native 不接 bounded-ready，必须在另一张系统级表中比较。

这里的 FIFO、DRR、VTC 名称表示复用已有调度算法思想，不表示调用了 Daft/Ray/vLLM 的原生
实现。本实验真正运行的是项目 `shared_credit.py` 中的 coordinator 选择逻辑；被它释放的请求再
进入 upstream vLLM FCFS + continuous batching。只有调度所有权属于 Daft/Ray Data 自己的臂才叫
原生系统 baseline。

## 2026-08-13 bounded ready-set 为什么不是扩大 K

`experiment_walkthrough.md` 新增 ready-set observation 修订说明。新 policy
`saor_bounded_ready` 不提高 endpoint K/W，也不改变 vLLM FCFS/continuous batching；它只在
现有 K/W 内把多个已经到达的具体 request 预注册给共享 coordinator，避免每个 Job 同步等待一个
head 时把真实 backlog 隐藏掉。旧 `saor_bounded_priority` 保留为单-head 回归对照。提交 trace
现在分开记录 ready、registered、granted、submit 与 service，便于判断问题发生在数据准备、共享
credit 还是 vLLM 排队。首次服务器 rehearsal 还暴露出一个跨 trace 合同问题：actor submission
trace 只拥有 ready/registered/granted，scheduler 的 submit 时间属于 request trace，审计器必须按
`submission_id` 显式连接，不能假设两份 CSV 重复存储同一列。该问题已用生产 schema 回归测试
覆盖。修复后的两个独立 rehearsal root 均完成：0.125K 两轮全过 development gate，0.25K 两轮
bulk SLO 越界。这个结果只注册 formal candidate，尚不是正式性能或公平性结论。

## 2026-08-12 bounded-priority SAOR 与事件账本

`experiment_walkthrough.md` 的 2026-08-12 小节新增通俗说明：为什么新候选不是简单调大
foreground 权重，actual-work debt/recovery lease/reclaim barrier 分别解决什么问题，以及为什么
机制真值必须来自无损事件账本而不是 250 ms snapshot。当前只完成本地代码和测试；服务器已
关闭，两档 GPU rehearsal 尚未运行，因此该讲解不包含新性能结论。

## 2026-08-09 多 Job 共享额度的异常生命周期

共享 credit actor 的生命周期必须与一个 group run 一致，而不能只在成功路径结束。
如果任一 Job 因 HTTP、Ray 或证据门失败，runner 仍要先终止所有 child，再保存失败
trace，最后在 `finally` 精确销毁该 group 的具名 credit actor。否则下一次实验即使使用
新输出目录，也可能在同一个 Ray 集群中继承不属于自己的调度状态。清理操作现在是
幂等的：找不到 actor 时直接返回，销毁后清空本地句柄；Ray 自身清理失败只产生显式
warning，失败实验仍保留原始异常作为主错误。

代码结构导读：

- [`code_architecture_guide.md`](code_architecture_guide.md)：解释公共执行阶段与
  text/image 模态为什么是两条正交轴、每个 `src` 子包负责什么，以及路径迁移的验证门禁。

文本 baseline 的当前入门材料：

- [`text_native_baseline_guide.md`](text_native_baseline_guide.md)：区分 vLLM 服务上限、
  bounded control、Daft/Ray Data/OceanBase 原生 baseline 和项目方法，并解释
  Chat/Completions 分轨与 64→512→4096 复测流程。
- [`observability_metrics_guide.md`](observability_metrics_guide.md)：解释下一轮新增的
  TTFT/ITL、token goodput、padding、成本、公平、重复统计、代价决策和检索质量字段，
  以及哪些输入缺失时必须标 unavailable。
- [`vllm_clip_pooling_gate_guide.md`](vllm_clip_pooling_gate_guide.md)：解释图像 direct-service
  ceiling 与数据库系统 baseline 的区别，以及为什么 1-image capability 未通过时必须
  停止 5K/60K 性能测试。

学习材料只负责解释；正式 baseline 身份、状态和指标以
[`../experiments/plans/baseline_reference.md`](../experiments/plans/baseline_reference.md)
为准。

图像木桶、H2D 与 embedding parity 的当前讲解统一追加在
[`experiment_walkthrough.md`](experiment_walkthrough.md) 的 2026-08-02/03 小节；其中
明确区分正常流式性能运行和仅用于语义判定的 capture 诊断运行。

## 2026-07-30 为什么“短长都选 65K”还不能直接判动态无用

远端把三次 formal 的 E2E tokens/s 做算术平均，得到 short/long 都是
W65K，于是写成“固定 token-aware credit 已自动适配，动态 K 不需要”。
这条推理的前半部分是合理假设，但数据还没有满足判决条件。

项目正式校准使用 model-request throughput 中位数。按这个口径，short
选 W98K，long 选 W65K；而 short W65K/W98K 的 repeat CV 高达 18%/34%。
更关键的是，short 每 endpoint 最多只有约 49.3K observed work，65K 和
98K cap 都没有真正挡住请求，bounded wait 都是 0。K256、W65K、W98K
实际都一次性放行 512 行，理论上应接近，却出现 48.5% 中位数吞吐分裂。
这说明当前差异首先是实验稳定性或服务状态问题，不是 active-work limit
的因果效果。

配置还暴露了三项执行偏差：实际 transport 是 urllib 而非冻结的 async；
没有返回 token IDs，拿不到 per-request 实际 output token 分布；short
全部跑完后才跑 long，而且六个臂只是 K-only/W-only 两条一维曲线，不是
K×work factorial。

因此正确读法是：long W65K 是值得保留的静态候选，K256 过度接纳的 SLO
负结果也有价值；但动态 GO/NO-GO 必须标为 `inconclusive`。先用同一 async
runner 交错重跑 K256/W65K/W98K 等价臂，未绑定臂收敛到 5% 内后，才有资格
比较不同 workload 的静态 oracle 和交叉 regret。

## 2026-07-29 为什么要先做 K256/W98K 等价性门禁

K256 表示每个 endpoint 最多同时保留 256 个 request；W98K 在同一个 K256
上再加每 endpoint 98,304 predicted-token work 上限。当前 512 行的实际峰值
work 只有约 73,329，所以 W98K 理论上没有约束任何请求。这两个配置应该接近。

单次远端结果却是约 11,736 对 4,153 total tokens/s。trace 显示不是 W98K
真的挡住了请求：两者都达到 512 个全局 inflight、bounded wait 为 0、输入与
输出 work 一致。主要差异发生在 HTTP/vLLM request wall，而且 W98K 恰好是
第一个 full-concurrency 场景。于是这组数字更像“首次把 512 个连接/请求压上去
的冷路径”，不能解释成 active-work 策略变慢。

新的等价性门禁做三件事：第一，所有 Ray actor ready 后才启动 E2E 计时，启动
耗时另记；第二，每个配置先在完全相同的压力下 warm-up；第三，记录 HTTP
request start、response headers 和 body complete。非流式请求的 headers-wait
仍混合 connect、服务入口排队和推理，不能把它直接叫 GPU 时间。只有 K256 与
W98K 在三次 formal 中收敛到 5% 内，才能继续比较 Daft+Ray 是否用更少
active work、更快爬到吞吐上限，或在多 job 中改善 P99/SLO/fairness。

## 2026-07-29 planning batch 与 service quantum 为什么要分开

planning batch 回答“上游先把哪些完整行组织在一起”，由 token budget、
length align 等策略决定。service quantum 回答“这些行分成几个 HTTP/Ray
完成事件发送，每次完成后释放多少 active-work credit”。两者过去重合时，
一次多 prompt 请求必须等待批内最慢行完成，整批 credit 才释放，容易形成批内
HOL、波次执行和补位空洞。

现在 `service_quantum` 模式只在行与行之间切分，不会拆开一行 prompt：
例如预测 work 为 `[6, 4, 7]`、目标为 `10` 时，同一个 planning batch 会变成
`[6, 4]` 和 `[7]` 两个 completion 单元。前者完成即可独立释放 10 个 work
credit，后者无需等待整批。超过目标的单行仍保持完整、独占一个 quantum，并
标记 oversized。

因此读实验时要同时看两组指标：organization batch 的行数/work 描述数据组织，
service quantum 的行数/work 描述完成与补位粒度。只有在 planning batch、
active-work 上限和 actor slots 相同的对照中，才能把性能差异归因给 quantum；
当前代码与测试只证明语义和 trace 正确，尚未证明远端吞吐一定提升。

Ray actor pool 的另一个独立问题是“有多少客户端 worker、每个 worker 同时持有
多少请求”。现在每个 endpoint 的真实上限是 `worker 数 × 每 worker slots`，
调度器不会因为 `max_inflight` 写得更大就越过该物理上限。最初拟比较
1×16、2×8、4×4，但远端当前分布显示单请求平均约 332 work、组织批次平均约
1337 work；16 slot 只能暴露约 5.3K 或 21K work，远低于正在标定的饱和区。
因此正式对照改为 1×256、2×128、4×64：每 endpoint 总 slots 都是 256，
既保持 Checkpoint A 的饱和负载能力，又不会把“偷偷增加 offered load”误判为
更多 actor 带来的收益。每 endpoint 的 Ray CPU reservation 也固定为 0.5，
每 actor 分别使用 0.5/0.25/0.125，避免 actor 数增加时同时增加 placement
资源。least-active-work 只改变这些固定 slots 如何分到 worker。

这里的 slot-held 时间从 Ray 提交持续到结果完成，包含 Ray、HTTP 和模型服务
等待；它不是 GPU kernel 利用率。GPU 是否填满仍要看 vLLM queue/running、
GPU utilization、MFU 和 tokens/s。只有总 slots、active-work、service quantum
和 workload 都相同，actor pool 形状对照才有可解释性。

service quantum 候选也不能只按好看的 2 的幂机械选择。当前 planning batch
预测 work 的 P50≈1105、P95≈3366、最大≈5892，所以 8192 不会切开任何观测
批次，与 whole-batch control 等价。正式候选使用 512/1024/2048/4096：
512/1024 测试更积极的持续补位，2048/4096 测试只切长尾批次的较低开销方案；
每个候选仍受同一 active-work credit 和 256 actor slots 约束。

## 2026-07-28 双 4090 7B replenish 配置诊断

本轮现场数据中的 `replenish_static_k8_2gpu` 不是 request-level
replenishment：命令设置了 `ray_batch_rows=1`，但结果字段仍是
`submission_granularity=batch`。因此 1024 行被组织成 1024 个单行 batch，
`token_budget=8192` 没有机会装入多行。K=8 又只允许全局 8 个单行请求，
而 batch K=32 平均每批约 3 行，两个 K 不代表相同 offered load。

正确实验应保留合理的 row cap 与 token budget，让它们记录 packing/flush
边界，再用 `submission_granularity=request` 展开完整行请求。request K 应按
请求数与 batch baseline 的实际行数匹配，先比较 K64/K96，而不是直接复用
batch K8。当前 7B warm-up 只能用于定位配置问题，不能作为 replenish 策略优劣证据。

HOL-age 的 3 秒拥塞阈值也低于本轮约 4–5 秒的正常 batch 服务时间，因此会把
正常执行年龄当成拥塞并把窗口压向下限。需要先用静态配置校准正常服务时间，再
决定阈值或更换为不混淆 service age 与 queue delay 的信号。

vLLM 的 `estimated_flops_per_gpu_total` 是 per-GPU counter。多 endpoint 采集时，
工作量 counters 仍求和，KV 压力取最大值，但 per-GPU FLOPs 必须在 endpoint
之间取均值后再除以单卡峰值；相加会把双卡 MFU 高估约两倍。

## 2026-07-28 双 endpoint 指标与并发语义

多 endpoint 实验里的 `max_inflight` 是整个调度器的 submission 上限，不是
每个 endpoint 各自的上限。因此，单 endpoint 的 K=16 若要做保持“每卡 K=16”
的双 endpoint 诊断，首先应检查全局 K=32，而不能直接复用 K=16。一个
submission 还可能包含多行 prompt；只有整批 HTTP 响应完成后，该 submission
才释放 admission slot，这与真正的 request-level continuous replenishment
仍有区别。现在 arrival replay 可显式选择
`--submission-granularity request`：token-budget 与 flush 仍决定完整请求的
组织边界，但关批后每行作为一个完整 HTTP 请求提交，任一请求完成都会立即释放
一个 slot 并持续补位。该模式下 K 表示“请求数”，默认 batch 模式下 K 表示
“多行 submission 数”，两种模式不能只按相同 K 数值直接比较。

现在 static 路径可显式选择
`--admission-scope per_endpoint --max-inflight 16`。在两个 endpoint 上，它表示
每个 endpoint 各有 16 个 credit，同时保留 32 的 scheduler-wide 安全上限；
旧的 `global K=32` 与新的 `per-endpoint K=16` 因而可以做机制对照。自适应控制器
仍只有一个全局窗口，当前拒绝 per-endpoint 标签，避免产生名义上“每卡自适应”、
实际上仍共享窗口的伪实验。

旧数据也不能只用“双卡 K=16 / 单卡 K=16”得出扩展比约 1.1。按近似相同
per-GPU credit 比较，双卡 global K=16 对单卡 K=8 约为 1.74×，双卡 global
K=32 对单卡 K=16 约为 1.57×；这说明共享 K 确实压低同 K 对照，但双卡并非完全
没有扩展。距离 2× 的剩余差距还混有请求形状、HTTP/Ray 开销、负载平衡和模型
服务效率，必须由新的交错重复实验分解。

同日修改后的 1024 行单次 gate 中，双 endpoint
`per_endpoint K=16` 实际达到 scheduler-wide max inflight 32、约
4302 tokens/s、12.82 rows/s，修正口径 MFU 约 0.183。它相对旧
`global K=16` 的 2992 tokens/s 高约 43.8%，与旧 `global K=32` 的
4251 tokens/s 接近（约 +1.2%）。这验证了新 credit 语义能够恢复同等 offered
load，但只是单次 warm-up 级机制 gate，不是可以报告为正式提升比例的重复实验。

`least_queued` 现在把调度器已提交但尚未完成的 endpoint-local submission
计入负载，不再对静态全零拓扑反复选择第一个 endpoint。双 endpoint 采集应使用
`--model-metrics-urls` 传入两个 vLLM Prometheus 地址，否则单地址 counters
只能代表一个 endpoint。GPU 利用率、显存和功耗现在按 `endpoint_gpu_ids` 指定
的服务卡采样。旧的单 endpoint 对照如果在双卡主机上平均了“一张忙卡 + 一张
空闲卡”，约 47% 的系统均值不能解释成活动 GPU 只有 47% utilization，也不能
据此声称 utilization 与 MFU 方向相反。

动态控制器的 `fresh` 现在表示“新采样且尚未被控制决策消费”，而不只是
“采样尚未超时”。同一个 Prometheus 快照不会在调度器高速循环中重复触发
AIMD/EWMA/PID 更新；HOL-age AIMD 也按配置的采样周期更新。需要注意，现有
HOL-age 仍是最老 in-flight submission 的年龄，不等同于纯粹的提交前排队时间。

同日双 4090 单次诊断中，请求级 K=64 为约 15.20 rows/s、6784 tokens/s，
K=128 为约 13.89 rows/s、6217 tokens/s，均未超过此前 batch 级 K=32 的
约 18.71 rows/s、8317 tokens/s。这只能作为调参信号：独立 HTTP/Ray task
开销和过高并发可能抵消持续补位收益；在完成重复、交错的 K 扫描前，不能声称
请求级模式提升或降低了总体性能。

## 2026-07-26 Ray endpoint 与 actor worker 执行契约

一个 service endpoint 是独立的 HTTP 模型服务地址；一个 Ray actor worker 是向该
地址发送请求的客户端执行单元，两者不能混为一谈。配置并发上界是
`endpoint 数 × 每 endpoint 的 actor worker 数 × 每 actor 最大并发`。HTTP worker
不承载模型，因此 Ray GPU 配额为 0；GPU 由外部 vLLM endpoint 持有。正式完成请求
禁用 Ray 自动重试，避免完成结果被静默重复。CSV 现在显式记录这些配置、拓扑和
逐 worker 提交计数。Python 路径没有 Ray worker，因此 concurrency/CPU 用 0/0.0
表示“不适用”；Ray task 没有 actor worker，因此 actor concurrency 也记 0，但仍
记录实际 task CPU。fake Ray worker 同样接受 CPU、零 GPU 和禁重试配置，只用于
调试。CSV 追加前会核对已有 header，旧 schema 不匹配会拒绝写入而不是把数据写到
错误列。多 GPU 性能仍须用独立 GPU endpoint 验证，当前契约测试不构成多 GPU
性能证据。

轮转状态的生命周期必须与实验 run 一致，而不是与单次数据库 fetch chunk 一致。
因此 endpoint 内 actor worker 与 legacy endpoint 轮转都只在 run 初始化时创建；
每个 chunk 只上报自己的提交增量。job 一旦创建，后续 Ray 初始化、提交或写回异常
都会尽力写入 `failed` 终态，同时保留原异常。主 CSV 的旧 schema 也会在数据库和
GPU 工作前被拒绝；K_max runner 使用新的 `20260726` 默认文件，历史结果保持只读。

## 2026-07-26 动态 flush 与联合搜索结论

`learning/experiment_walkthrough.md` 新增 2026-07-26 章节，解释为什么
queue-adaptive 可以优于 25ms baseline，却未必优于最佳静态 50ms；同时说明
独立拼接与联合搜索在当前单 GPU 实验中为何不可分辨。

## 2026-07-20 指标选择方法论

New learning note:

```text
learning/metric_selection_methodology.md
```

解释为什么从 AI_EMBED 转向 AI_COMPLETE 后，实验观察变量需要从"阶段时延拆分"转向"请求形状 + 服务端压力 + 端到端分布"的四层变量体系。包含每个实验的最低推荐变量集和当前指标盲区。

## 2026-07-18 local vLLM Ray baseline walkthrough

New learning note:

```text
learning/local_vllm_ray_baseline_walkthrough.md
```

Read this when explaining the local `AI_COMPLETE`
`PostgreSQL -> Daft -> Ray -> vLLM` fixed row-batch baseline charts and their
boundaries.

本目录用于把项目实验、代码和术语讲成学习材料。

## 2026-07-28 最近提交审计：trace 与调度进展保证

- token budget 决定“一个组织批次最多容纳多少 token”，`ray_batch_rows`
  仍是独立的行数上限；显式配置为 1 时，每批只有一行并不是 token budget 太小。
- admission controller 拒绝请求时，只有存在 in-flight submission 才能通过
  fan-in 释放 credit。零在途仍拒绝属于控制器无法推进，应立即报错，不能对空列表
  调用 `ray.wait`。
- Ray 返回的 ready handle 先按相等语义定位，再转换成 pending 列表中的规范对象；
  scheduler 使用对象身份删除，避免“值相等的重复 handle”误删提交。
- control trace 必须写入控制器实际读取的 `hol_age_s`；request 粒度的 submission
  trace 必须沿用真实 lifecycle ID，并记录 endpoint/GPU，不能伪装成 batch ID。
- 显式 CLI 配置应优先于环境默认值。否则 shell 中残留的
  `COMPLETION_ENDPOINT_URLS` 会压过本次 `--completion-endpoint-url`，让看似单
  endpoint 的测试实际解析为双 endpoint。
- 当前 HOL 信号实际是“最老 in-flight submission 的年龄”，包含正常模型服务时间，
  不是纯 Ray 排队时间。因此 7B 单请求服务约 4–5 秒时，3 秒 congestion threshold
  会把正常服务误判为拥塞；它只能作为诊断候选，后续应改为 oldest-request slack、
  token backlog 与 arrival/service EWMA 的联合信号。

正式 CSV、严谨结果报告和论文式结论仍放在：

```text
feasibility/results/
motivation/results/
```

`learning/` 负责回答更基础的问题：

- 这个实验为什么要做？
- 数据从哪里来，经过哪些系统，再写到哪里？
- Ray / Arrow / pgvector / batch / actor / fan-in / backpressure / writeback 是什么意思？
- 每个参数在控制什么？
- 每个结果字段怎么读？
- 这个结果对课题下一步有什么用？
- 这个实验不能证明什么？

## 阅读顺序

1. `experiment_walkthrough.md`：按项目推进顺序讲解已经完成的实验。
2. `figures/README.md`：学习用实验图表清单。

## 当前重点章节

| 章节 | 内容 |
|---|---|
| 第 9 节 | GPU-backed 真实 embedding 画像 |
| 第 10 节 | CPU/GPU 对比，以及 `model_service_s` 为什么不能直接当阶段占比 |
| 第 13 节 | 真实 embedding 链路拆分：当前开题动机最应优先学习的一组结果 |
| 第 14 节 | pgai SQL 触发面冒烟验证：真实 SQL 调用 embedding 与 pgvector 写回 |
| 第 14.8 节 | GPU-backed Ray actor 链路中的 pgvector(384) 写回对比 |

## 当前重点图表

项目级图资产统一放在：

```text
figures/
```

当前学习材料、开题报告、PPT、中期汇报和毕业论文应复用同一套图：

- `figures/architecture/`：系统架构图和流程结构图；
- `figures/data/report_main/`：正文主线实验图；
- `figures/data/backup/`：解释场景选择、变量选择和实验边界的支撑图；
- `figures/scripts/`：可复现绘图脚本。

学习材料可以引用 `figures/data/backup/` 中的支撑图讲解实验来源，但不能改变图中实验事实和证据边界。

## 更新规则

每次完成新实验、代码实现或功能测试后，都要同步检查：

- `learning/experiment_walkthrough.md` 是否需要新增讲解；
- `figures/` 是否需要新增或更新项目级图；
- 本 README 的阅读入口是否需要更新。

学习材料可以讲得更通俗，但不能改变正式实验事实。

## 2026-08-10 VTC-compatible 不等于复现 VTC

VTC 的公平 scheduler 位于 S-LoRA continuous batching 内部；本项目只迁移公开 workload
形状与 actual-service/fairness 口径。正确对照必须让 static、shared FIFO 和 shared-work
共享同一 endpoint request/work 上限，并允许每个 Job 有不同到达率和行数。公平差只能在
至少两个 Job 同时仍有未完成请求的 backlog 区间计算；把整个 Job 生命周期当 backlog，
会把尚未到达或已经 drain 的 Job 错算进分母。

Direct vLLM FCFS 是外部服务锚点，不等同于项目 `shared_fifo`：前者把多个逻辑 Job 的
到达 trace 合并到同一个 bounded AsyncIO client，只保留 endpoint-local 并发边界并交给
vLLM 调度；后者仍经过项目 Ray actor 与 shared-credit coordinator。固定 256-token 输出
必须显式传 `ignore_eos`，否则自然 EOS 会缩短实际工作量，使 baseline 失去可比性。

## 2026-08-11 两 Job phase-change 实验准备

这一实验不是“多客户端越多越好”的吞吐压测，而是给状态控制器一个可辨识的四阶段
输入：短 Job A 始终到达，长 Job B 在 60--120 s 与 180--240 s 打开。先离线找出
一个供给不足但安全的 lower arm，以及一个在 A-only 时更高效、A+B 时会产生真实
waiting/KV 压力的 upper arm；在线控制器只能在这两个已验证边界内切换。

有效现象必须是两个 endpoint 都出现 `up -> down -> up -> down`，且每次 down 后
waiting/KV 风险实际下降。GPU utilization 只作交叉验证；若到达率没有 ready backlog、
上下档服务率差不足 5%，或长阶段压不出服务压力，实验按设计停止，不能靠继续加参数
或只比较整段吞吐制造正结果。

这里有一个容易误判的双重限流问题：shared credit 之外还存在每个 Job 的本地 admission。
若二者都固定为 lower K，本地层会先挡住新请求，shared waiting 指标保持 0；即使控制器
把 shared K 改到 upper，本地 K 也会让实际并发仍停在 lower。因此 adaptive 路径把本地
K/W 设为最大安全候选，只让 shared coordinator 执行上下档；离线 A-only backlog 则用
replayed arrival 到 submit 的延迟和持续占据来证明，不靠 GPU 利用率或错误命名的队列字段。

长时间 open-loop replay 还有一个与策略无关的 transport 陷阱：多个稀疏 Ray actor 会把
HTTP/1.1 连接闲置到服务端 keep-alive 过期，再在尾部复用已关闭 socket，表现为服务端
健康且记录 200、客户端却 `ReadError`。客户端连接池应比服务端更早淘汰 idle socket；
当前 vLLM/Uvicorn 环境使用显式 4 s 合同，并把它作为运行身份记录，而不是把 transport
失败算成策略负结果。项目 Ray actor 和 direct control 必须共用该值；这不是自动重试，
失败请求仍会使实验 fail-closed。

active-set 实验还要区分“外生有效性”和“策略结果”。错峰到达、真实重叠、两 Job 全部完成
属于外生有效性；foreground 是否比 bulk 先完成属于调度结果，不能拿它过滤 baseline，否则
会只保留表现符合预期的运行。借用机制也不能靠降低阈值制造：foreground 到达前，bulk 每个
endpoint 的预测 ready work 必须至少覆盖一个完整 work envelope。若供给不足，实验在静态
readiness 就停止，而不是跑完后把 7% 容量占用称为“借用”。

post-drain 也不能机械要求剩余 Job 一定占满超过 50% work。包络是二维上限，请求又不可分：
若 coordinator 没有 waiting work，空余不属于配额损失；若仍有 waiting head，则检查
`active_requests+1<=K_req` 与 `active_work+head_work<=K_work`。两者同时成立却没有释放才是
非工作守恒；任一维装不下都是合法碎片。结束顺序同样不预设为 foreground-first。

## 2026-08-11 SAOR 核心代码现在做到哪一步

当前完成的是“可独立测试的控制核心”，不是已经跑赢静态策略的正式系统：

- `core/control.py` 只定义 request/work 容量档位，不包含 K128/K160 等机器参数；
- `submission_control/saor.py` 根据 ready work、公平债务、预测 service 与代价选择有限动作；
- `submission_control/ordered_release.py` 只从每个 Job 队首释放请求，分配单调
  `release_seq`，completion 后按实际 work 修正；
- `core/execution.py` 统一维护旧 scheduler 的 pending、exactly-once completion 和生命周期证据。

这里最容易混淆的是“请求预计 work”和“一个控制周期内预计完成的 service”。长请求可以占用
很多 active work，却未必在当前周期完成同等 service，因此代码将二者分开输入。具体容量档位、
KV/waiting/GPU 信号阈值、控制周期、endpoint 数和 token/frame/pixel 换算都留在 calibration
配置或模态 adapter；动作构造还要求显式提供相对 hold 的 service/goodput/tail/energy/switch
边际预测，缺一项就拒绝构造，核心没有静默默认。phase-change 提前停止实验目前只支持低压增档动机，
没有建立可靠降档区，所以还不能把某个 KV 峰值写进算法。

## 2026-08-14 怎样读 SAOR final rehearsal

这次实验先回答“算法账本是否真的闭环”，再回答“单次性能是否值得继续”，两者不能交换顺序。

1. request admission 使用运行前已知的 `raw prompt + 29 template tokens + 256 output cap`；
2. completion 后以 endpoint total tokens 作为 actual work，修正在线 debt；
3. 离线 service lag 复用同一 endpoint actual work，但按 registered-ready completion 重放理想份额；
4. 96/96 recovery、15/15 repayment 与 1,108/1,108 projection 证明这个冻结 workload 中机制闭环，
   不构成任意到达或任意预测误差下的理论界。

VTC-style 的 lag P95 为 $62,607.5=0.955W_e$，SAOR 为 $54,376=0.830W_e$；差值
8,231.5 work 约等于 debt cap $H_B=8,192$。这说明算法确实在它直接控制的“累计服务欠账”方向
产生作用，但不能把它直接翻译成“请求快了 13.15%”：本轮 foreground P99 反而略差 0.11%，
只是吞吐、JCT、SLO 和最长无服务均保持在冻结保护范围内。

strict-priority 的 foreground P99 更低，是以吞吐、bulk JCT/SLO 和最长无完成恶化换来的经验性
latency boundary control；当前没有理论下界，不能称“理论边界”。frozen-static 不经过 shared-credit
registered-ready ledger，因此 lag/no-service 是 N/A，不是 0。

独立审核已经确认本 rehearsal 的 raw、SHA、指标和代码口径一致；授权字段和六臂全组件汇总也已
补齐。但当前完整签名 bounded-client 为 13,684.90 tok/s，SAOR 为 12,713.03 tok/s，feeding ratio
只有 92.898%，没有达到预注册 95%。所以机制 rehearsal 仍有效，当前性能 formal 却必须停止：
不能因为 GPU utilization 接近 100% 而跳过门禁，也不能调 K/W、降低阈值或重跑到通过。

这里的“有效”要再拆一层：新汇总器已经证明两侧 group CSV、manifest、运行合同、validation 与
archive SHA 属于冻结 artifact，并复算 ratio；但运行前 PostgreSQL/Ray clean 没有结构化记录，且
ceiling 只有一个 warmup-identity cell。因此论文只能写“一次性 gate 的负判决”，不能写“稳定损失
7.10%”。封存 direct 的 predicted work 还漏掉每请求 29 个模板 token；它不影响 actual-token
feeding，但 predicted/normalized-service 附属字段必须禁用。新代码已通过 typed work-cost 修正未来
证据，不修改旧 raw。
