# IMLane 原 Movie Q1 补测与监督计数成本

生产查询固定 `7e59e5a10eaceb7ac4a1bd0766b10244ca3e192d`，监督计数候选固定
`c994cf4a6f6095246838003c7362b830a9822960`。候选只改测量链，不改作者执行、调度、原查询、模型或并发额度。
先前 Mac 零模型隔离的全部负结果保留在下文；该阶段没有操作服务器、lease 或 GPU。
后续已完成[原 Q1 两模式长测](#original-q1)与[CPython 匹配历史配对](#matched-history)，原始大包留仓外。
**高历史配对缩短 14.03%，低历史回退 2.12%；尚未验证同一 owner 持续 19 轮的漂移已消失，作者 Ray 后段参与异常也未修复。**

## 问题与最小修补

现场冻结监督器 `owned_controller.py` 的 `progress` 每次 poll 执行
`SELECT count(*),max(at_ns),max(returned_ns) FROM sends`。
循环先检查资源、读进度，再 sleep 0.1 秒，至多约 10 Hz；实际周期包含检查和查询时间。
现场 SQLite 为 DELETE journal，时间字段无索引，计划为 `SCAN sends`。
`pre_send_ledger.counts()` 的历史 `GROUP BY kind` 只在阶段边界调用；此次保留。
`initialize_counts` 启动时完整核账也保留，不把它误认为同一常驻 CLI 内每轮的热路径。

修补复用既有五行 `send_totals` 和主键最大序号校验；对 `at_ns`、`returned_ns` 各建一个索引，
分别取最大值。短只读事务内读取一致快照，返回原 `(发送总数, 最大发送时钟, 最大返回时钟)`。
原始 sends、失败、未完成发送、预算、不退款、identity 和触发器检查均保留。
缺计数或索引时拒绝，不在 poll 中建表、重建计数或回退扫描。
新索引仅在既有完整初始化核账后安装；旧非空账本仍按原离线迁移规则处理。

实际监督器位于冻结私有运行包，不在仓库重复保存整份源码。
[最小部署补丁](private_harness.patch)涵盖 controller、launcher 及两个私有 helper 的局部差分，
保留现场 `activate/reserve` 对无累计时限及故障监督的改动，不以仓库旧模板整份覆盖现场私有 helper。
controller 只增加 helper import、显式 BEGIN 和读取调用。
原文件 SHA-256 为 `05cbf4ae26c9f040bf43cf9ce647fcf30b41684b8971451129fa75735a04ca01`，
修后为 `54f83704bd59f92bdad243a192e4906d1d121e7778b09b22318fa3776e56fd7e`。
只对隔离副本核对和应用；后续 owner 须在新冻结包中更新这些控制文件与既有指纹，不能在线替换活动文件。
四文件前后 SHA-256 全量映射保存在原件 `private_harness_sources`。控制字节改变后须新软件资格／zero gate 绑定，
本地账本 fixture 不替代新冻结包的作者软件资格。

终审复现了首次启动窗口：`launch` 的 ACCOUNT=0，`run_owned` 启动 child 后立即 poll；
child 在资产预检后才首次初始化。原初始化先自动提交 sends 等四表，再在事务内发布计数和索引，
因此旧 SQL 已可返回空三元组时，新 helper 仍可能因缺计数／索引拒绝。
部署补丁仅在 launcher 的 `run_owned` 前完成既有 `initialize()` 并等待 identity 发布，
两臂同样准备；workload 后续初始化继续原完整审计。缺 schema 的 poll 仍报错，没有弱化故障检查。
单独零请求回归覆盖“提交前旧读可见、新读拒绝；准备完成后首次新读为零三元组”；
性能 fixture 本就先准备再 poll，无需为此重跑22格。准备边界未被移出报告的 fixture 总成本。

## 预先固定的零模型方案

来源为已关闭软件验证导出的 `scope/sends.sqlite`：8,000 行，software/gateway 和 software/fixture 各 4,000，
全部已返回。原归档 SHA-256 `5cc81ae679607aadd43ca6db61529347ba9538e77307aa80a9d46fb4b42ee804`，
成员 SHA-256 `6dbb0a2d168b478d442daefd167342f0f85ed7b234d8911d6f3a8815b9103a30`，2,494,464 bytes。
原件及数据库副本留仓外；已关闭 caller gate 不重开。完整副本用于索引前后语义核验，
并将保留行回放到全新临时开放 fixture，生成 0／6,000／114,000／228,000 行历史。
只在新回放 fixture 中平移历史时钟，使新请求晚于历史；不改原账本时钟或重置其预算。

Mac 本地每规模按 before→after→after→before 执行，两个方向各一对；不清 OS 页缓存，不宣称真冷状态。
每格四个 spawn worker，各 64 个逻辑请求；每逻辑请求嵌套三次实际 reserve，
一个 localhost HTTP kernel 等待 0.12 秒，再逆序 finish。它隔离账本干扰，不冒充作者进程拓扑或原查询。
每格固定 244 个本地 POST、768 条新预留、756 次 finish；注入传输异常、HTTP503 和 12 条未返回 gateway，
用于确认失败及 unknown 不被退款或遗漏。监督循环执行冻结函数，OS 进程列表为空替身，
SQL 连接保持只读、timeout 0.5 秒、DELETE journal；poll 完成后 sleep 0.1 秒。

记录每个 progress 调用的墙钟／线程 CPU、实际 poll 间隔、reserve/finish 的 BEGIN／UPDATE／事务／commit 区间、
初始化和索引构建、完整 fixture（复制、启动、请求、离线核账和退出）成本。
Python sqlite3 不提供默认 busy handler 的精确等待分解；BEGIN／UPDATE／commit 包含 SQL、I/O 和锁等待，不能改名为纯锁等待。
受控小账本另验证持有写锁时 reserve 阻塞、持有读锁时 finish 的提交阻塞，并记录释放时钟；不拿该注入等待解释现场幅度。
失败、超时和较慢格保留。局部 fixture 上限为每格 60 秒，只保护本地诊断，不更改实际健康任务策略。

执行入口为 [probe_send_progress.py](../../../../code/scripts/benchmarks/probe_send_progress.py)，默认只输出上述计划，
`--execute` 需要本地账本副本、固定 before 两个 helper、监督器前后副本和全新输出目录。
不访问模型或外部服务。停止条件为计数、实际峰值 4、完整核账、进程退出或输入副本指纹不符。

## 实测结果与归因

环境为 Mac arm64／10 个逻辑 CPU、Python 3.12.6、SQLite 3.45.3，DELETE／synchronous=FULL／4KiB 页。
16 格初始 ABBA 全部有效，实际每格 768 条预留、756 次 finish、244 个本地 POST、68 条失败完成和 12 条未返回，
HTTP 峰值均 4；全部 poll 三字段单调且不超过最终全史审计值。全部失败和较慢重复保留。

下表 before 两值依执行位置 0／3，after 两值依 1／2；不将调用样本冒充独立查询重复。

| 初始历史行数 | progress均值 before→after ms | progress线程CPU均值 before→after ms | fixture复制至清理 before→after s |
|---:|---|---|---|
| 0 | 0.857／1.037 → 1.076／1.069 | 0.751／0.777 → 0.776／0.846 | 8.503／8.430 → 8.561／8.438 |
| 6,000 | 3.058／3.625 → 1.390／1.504 | 2.370／2.453 → 0.915／0.898 | 8.664／8.763 → 9.049／9.003 |
| 114,000 | 14.747／18.824 → 1.641／1.213 | 14.144／16.708 → 0.738／0.804 | 8.597／9.040 → 8.978／8.862 |
| 228,000 | 25.350／33.004 → 1.645／1.651 | 24.664／25.590 → 0.852／0.639 | 9.138／9.867 → 9.500／9.568 |

228k 的正序完整 fixture 变慢 3.956%，反序改善 3.039%；有方向差异，没有稳定净收益。
实际 poll 平均周期 before 129.3／136.8ms，after 105.1／105.9ms，符合“工作后再 sleep 0.1 秒”，
不是任意提高监督频率造压力。单次 progress P99 为 before 47.97／258.57ms、after 15.98／50.36ms，
不能把 mean 的下降写成尾延迟全部消失。

新增索引也付出写成本。228k 行的原始调用区间如下，均含 SQL、I/O 和可能的自然等待：

| 区间／指标 | before两次 ms | after两次 ms |
|---|---:|---:|
| reserve连接范围均值 | 0.962／1.176 | 1.825／1.693 |
| BEGIN IMMEDIATE调用P99 | 3.803／4.454 | 22.431／21.202 |
| reserve提交均值 | 0.225／0.387 | 0.361／0.349 |
| finish连接范围均值 | 0.951／2.164 | 2.096／2.337 |
| finish UPDATE调用P99 | 10.181／36.457 | 22.951／24.264 |
| finish提交均值 | 0.305／0.530 | 0.422／0.474 |

完整启动核账未删除：228k 时每格五次初始化的历史 GROUP BY 累计约 397–431ms；after 安装／检查索引累计约 84–86ms。
这些调用可能重叠，不能相加充当墙钟，也不能从完整 fixture 中扣除。
fixture复制至清理包含新副本、初始化、四 worker 启动、请求、记录队列传回、全账本核验与正常退出；
公共造史准备另列在原件中，最终 gzip 样本导出未单独计时，不将此列称为整个诊断脚本的端到端时长。

在父会话要求保留负结果后，另做固定 228k、相同四 worker／三元组监督政策的对称六格：
before→index_scan→after→after→index_scan→before。
`index_scan` 只有新索引／新 helper 初始化，仍运行原进度 SQL；它用于区分索引写入和读取变更，不是新执行机制。

| 执行位置／臂 | progress墙钟／CPU均值 ms | 请求工作区间 s | fixture复制至清理 s |
|---|---:|---:|---:|
| 0 before | 24.918／23.176 | 8.187 | 9.456 |
| 1 index_scan | 26.730／26.438 | 8.318 | 9.293 |
| 2 after | 1.277／0.766 | 8.444 | 9.409 |
| 3 after | 1.303／0.673 | 8.331 | 9.294 |
| 4 index_scan | 26.023／25.691 | 8.239 | 9.264 |
| 5 before | 25.312／24.131 | 8.224 | 9.201 |

同扫描条件下增加索引，请求工作区间变慢 1.602%／0.175%；同索引条件下切换快速读取，变慢 1.516%／1.116%。
finish 连接范围均值 before 0.832／0.998ms，index_scan 1.471／1.385ms；快速读取 after 为 1.870／1.489ms，
after 的 reserve BEGIN IMMEDIATE P99 为 22.586／20.503ms，index_scan 为 3.797／10.083ms。
索引维护、供给相位与争用均可能影响结果，不能把这些混合区间全部命名为锁等待，
也不能断言索引写成本定量解释了全部差值。六格各自完整值保留，不和第一组混成更多同配置重复。

## 语义、锁及证据边界

已关闭 8k 整本副本加索引前后，sends 全行摘要均为
`aa506af93bc6bfeeb21e25c92eabcf8889f10bd23a07492a862681ed3d4dd446`，caller_gate 仍为 closed=1，三元组完全相等。
旧 SQL 计划仍为 SCAN；两个独立 MAX 查询为 COVERING INDEX SEARCH，最大序号仍走主键。
受控持写锁时 reserve 的 BEGIN 与持锁区间重叠 103.48／103.60ms；持读锁时 finish 的 COMMIT 重叠 100.60／104.76ms。
原错误/失败检查保持；这些注入等待只证明阻塞路径，常规 busy handler 的纯锁等待仍未采集，不能用注入值归因现场。

定向预算、时序和新进度回归完成 35 项，0 跳过。命令（工作目录 `code/`）：
`python3 -B -m unittest tests.experiments.test_send_counts tests.experiments.test_send_ledger_timing tests.experiments.test_send_progress -v`。
独立子代理已独立通过其中预算14项＋进度10项，并完整等值复算22格原件／summary、源指纹、整本与锁证据；
首次准备竞态、私有控制差分、调用命令及导航均通过独立终审；认可范围仅为候选分支提交，不含活动 baseline 整合。
fixture 用固定7e59仓库 helper，在两臂同样启用内存分段记录，未运行真实HTTP hook安装；
现场冻结私有 helper 的版本／指纹另存于原件，不能把它们当作同一二进制。
原实际监督函数则来自上面精确指纹，除替换三字段读取外，其执行顺序保持。

所有逐次区间、版本、源指纹、故障及源归档映射集中保存在 [raw/paired.json.gz](raw/paired.json.gz)，
当前派生值为 [summary.json](summary.json)，运行 `python recompute.py summary.json` 可由[复算入口](recompute.py)重新生成。
初始 ABBA 执行 probe 的 SHA 为 `bfbf4700a3ed2d77457f9dacaae4208d042e4140f8b2f9790d7dc4248bb88028`；
后来仅为复现入口补充源指纹、保留NULL的回放、起始失败增量断言和整本／锁／三臂诊断，
新增离线核对不在最初16格的计时中。执行版源与完整数据库留仓外，以原件SHA由运行owner取得；Git只保留必要紧凑区间。
首个运行在监听 localhost 前被沙箱拒绝，无POST；获准后的新运行完成，原失败和摘要未删除。

三次实际调用的完整脱敏 argv 分别保存在原件的 `metadata.command_redacted`、`checks_metadata.command_redacted`
和 `attribution.metadata.command_redacted`；路径统一为占位符，工作目录均为隔离职责 checkout。
执行版 probe SHA 分别为：初始 ABBA `bfbf4700a3ed2d77457f9dacaae4208d042e4140f8b2f9790d7dc4248bb88028`，
整本／锁 `894f5b6fdbb1cf4c4c8f82a578a3a81da3db78a4e345159371e2288943a38246`，
三臂 `88684a5e1777ea2be3fb95df0fc99357064d39c86edf6d6e4963da097721dcf3`。
以下为共用参数的等价展开；`--checks-only`、`--attribution` 两次分别使用上述对应执行版，输出目录各自全新：

```sh
fixture_args=(
  --execute
  --ledger-copy /path/to/closed-zero-sends.sqlite
  --baseline-ledger /path/to/before_pre_send_ledger.py
  --baseline-counts /path/to/before_send_counts.py
  --controller-before /path/to/frozen/validation/owned_controller.py
  --controller-after /path/to/controller-copy/validation/owned_controller.py
)
python3 -B code/scripts/benchmarks/probe_send_progress.py "${fixture_args[@]}" --output /path/to/new/paired-progress-2
python3 -B code/scripts/benchmarks/probe_send_progress.py "${fixture_args[@]}" --checks-only --output /path/to/new/progress-checks-1
python3 -B code/scripts/benchmarks/probe_send_progress.py "${fixture_args[@]}" --attribution --output /path/to/new/progress-attribution-1
```

上述 Mac 阶段模型调用为零。该替身即使降低扫描 CPU，也不能直接解释现场 CPython 的持续漂移，
更不能排名早期 CPython 与共用累计账本的晚期作者 Ray。下列 Linux 阶段使用独立运行身份和真实模型；
不回填为 Mac 的正结果，也不修改已冻结的活动 baseline。

<a id="original-q1"></a>

## 原 Movie Q1 两模式 19 轮：76,000 次模型调用

最初核查的 main 固定为 `f1820755564532fff39bafc1fd0f708e0f8c7739`，
其[四臂长测](https://github.com/3444374/ai-operator-execution-optimization/blob/f1820755564532fff39bafc1fd0f708e0f8c7739/experiments/results/diagnostics/main_queries_20261015/README.md#long-run)
只有 LOTUS 原生、Daft、Ray Data、SemLoom，配置中没有 IMLane CPython／作者 Ray；没有该轮 IMLane 执行失败记录。
这里说明当时固定版本的遗漏，不代表随后推进的远端 main 状态。
旧 Q2 小轮不替代本次原 Q1 补测，也不把不同 C4／C8、服务和生命周期拼成严格六臂排名。

本次使用 baseline `7e59e5a1` 的原查询。IMLane 桥／session 最近生产变更为 `9766e784`，
默认桥关闭修正 `1ab0ae05`，显式 LOTUS owner 借用 `4271dc44`，默认空总期限整合 `2afcaf47`；
可选 async 最近 `3bd53587`，公共包装清理 `d1235940`，均已包含在固定 baseline。
作者源固定 [IMLane0@072f8257](https://github.com/IM-DM4AI/IMLane0/tree/072f8257db80569e42c9f5727d839587df529652)，
接法见[原生桥与生命周期](../../../../code/integrations/sema/imlane_calls.md#bridge-lifecycle)。
原 LOTUS 仍拥有 formatter、sem_filter、SDK 解析及 LIMIT5；完整 uncached batch 经 DBEnd Setup／Register、
单行 Arrow 和作者 Lane ExecuteFunction／回调交付。异步宿主提交不表示 UDF 是 async。
本次是默认同步 UDF：每次 asyncio.run 后恢复作者 loop，逐调用建关 HTTP session；`reuse_bridge=False`。
可选 async／显式复用桥的历史资格保持独立，不由本次模型长测补认。

先以两模式各完整 2,000 行的封存响应回放完成软件资格：共 4,000 fixture、0 model，
完整 SDK／Filter／结果身份通过；CPython／Ray 分别 35.2173／37.1270 s。
随后每模式顺序 19 条完整 Q1，每条 2,000 行，共 38 query／76,000 model／228,000 三跳预留，fixture 为 0。
每模式一个 CLI／Coordinator／LM owner，19 query 之间保留作者 worker，每 query 新 DBEnd bridge；
两模式共用一个模型服务。首条是资格查询，后续 18 条单列，无额外模型预热、重试或补轮。
完整查询的 release→consumer EOF 保留逐 query 原生桥创建／处理；owner、服务准备与退出另行核验。

| 模式／阶段 | 完整查询数 | query均值 s | 最小／最大 s | 逐query请求P99均值 ms | HTTP平均在途均值 |
|---|---:|---:|---:|---:|---:|
| CPython／首次资格 | 1 | 72.452047 | 72.452047／72.452047 | 151.341946 | 3.457356 |
| CPython／后续 | 18 | 78.968336 | 72.692574／85.104898 | 171.484865 | 3.311792 |
| 作者 Ray／首次资格 | 1 | 85.415375 | 85.415375／85.415375 | 188.965967 | 3.120704 |
| 作者 Ray／后续 | 18 | 99.039300 | 85.644759／111.200781 | 192.706185 | 2.667578 |

这些 P99 列是每条 query 的 nearest-rank 请求 P99 再等权平均，未把 18 条查询合并成一个尾分位样本。

旧长测共用服务从model-start至首次物理engine身份核对为48.387224 s；两mode的常驻设施计时如下，
来源成员和SHA保存在JSON。public owner启动以startup.json的started→ready为准；作者启动单独计时，
不能把两者相加称完整应用启动，LM的实际lazy准备仍在query中。作者关闭为author-cleanup的close_started→close_completed。

| 原长测模式 | public owner启动 s | 作者启动 s | 作者关闭 s | 末EOF→public owner关闭标记 s |
|---|---:|---:|---:|---:|
| CPython | 8.603710 | 1.520731 | 0.621929 | 1.325037 |
| author_ray | 8.534396 | 7.150489 | 2.539580 | 1.101097 |

末EOF到关闭标记含最后记账／报告间隔，不是独立close耗时。完整服务退出耗时为unavailable：
原service-cleanup仅记录callback时间和物理清点结果，没有整个cleanup的起止时钟，不能拿callback冒充全部退出。
自然退出／无残余已核验，与耗时未完整拆分是两件事；新八格同样保留该缺项，逐格启动／作者关闭及epoch边界见JSON。

CPython 第 19 条为 84.7889 s，相对首条 72.4520 s 增长约 17.03%；首轮／后续完整 38 个原值均在 CSV。
作者 Ray 第 10 条 Lane2 完成 310 次后停止参与，第 11–19 条该 lane 的 Execute 计数为 0，其对应 PID 在采样时仍活着。
最后一次 End 与 Coordinator 处理 Restore 在同一毫秒，随后仍有 Register，但这不证明执行 driver 从等待返回。
源码尚未发现主动从 4 调为 3 的策略；现有闭合日志更支持完成后恢复／再次取队列之间停滞。
`RunWaitLaneTask` 与 `RunRestoreLaneTask` 对 ready／notify 使用不同加锁方式是具体候选，
缺线程栈、ready 标志和 wait 进入时钟，不能将 lost-wakeup 说成已确认根因。

<a id="matched-history"></a>

## Linux 资格与 CPython 匹配历史配对：16,000 次新模型调用

两版分别完成 Linux 原全 Q1 软件资格，每版 2,000 fixture／0 model、12 项历史账本检查及实际 22 项控制绑定通过。
软件查询 before／after 为 35.181492／35.012724 s，不作为模型加速证据。
真实模型只测 CPython，按下表固定顺序执行；两种历史量各 before→after→after→before，每版每档 n=2。
每格都新建同样的原 CLI／Coordinator／LM、四个作者 worker 和模型服务，仅匹配历史量和测量控制差分。
高历史导入旧闭合 CPython 的前 114,000 行，保留原始时间、每跳 38,000 行，使用新 inode；
不复制旧 gate／身份／网关注册，不重发旧请求，不退款。新增按 `sequence > floor` 核对：每格恰 2,000 model／6,000 三跳。
高历史绝对预算为 40,000 逻辑／120,000 三跳，减去历史后新增仍限 2,000／6,000；低历史直接限 2,000／6,000。
总计 16,000 新 model／48,000 三跳，fixture0，无额外预热、重试或补轮。

| 执行格／CPython版本 | 历史行 | 完整query s | HTTP窗 s | 请求P99 ms | HTTP峰值／平均在途 |
|---|---:|---:|---:|---:|---:|
| m0-low-before | 0 | 72.865626228 | 71.315009708 | 153.554236 | 4／3.437594728 |
| m1-high-before | 114,000 | 85.319876037 | 83.924591152 | 199.554384 | 4／3.182777326 |
| m2-high-after | 114,000 | 74.012661299 | 72.427092189 | 154.621253 | 4／3.420830928 |
| m3-low-after | 0 | 74.193576270 | 72.644837574 | 156.784845 | 4／3.404524698 |
| m4-low-after | 0 | 74.557150861 | 73.152997680 | 154.654261 | 4／3.383019124 |
| m5-high-after | 114,000 | 72.203293526 | 70.781586028 | 154.403776 | 4／3.459746664 |
| m6-high-before | 114,000 | 84.758400917 | 83.371054589 | 194.044320 | 4／3.190082128 |
| m7-low-before | 0 | 72.797914361 | 71.288181711 | 152.883947 | 4／3.449305074 |

完整 query 为 release→consumer EOF；HTTP 窗为首 dispatch→末完整 body；请求延迟为该 gateway dispatch→完整上游 body，
包含账本、队列和传输，不是纯模型耗时。P99 每 query 用 2,000 请求的 nearest-rank，第 1,980 个排序值；
平均在途为所有请求区间长度之和／HTTP 窗，峰值由区间事件扫描取得，不能用声明 C8 代替实际并发。

| 历史行 | 完整query均值 before→after s | 相对变化 | 请求P99均值 before→after ms | 平均在途 before→after |
|---:|---:|---:|---:|---:|
| 0 | 72.831770→74.375364 | +2.119% | 153.219091→155.719553 | 3.443450→3.393772 |
| 114,000 | 85.039138→73.107977 | -14.030% | 196.799352→154.512515 | 3.186430→3.440289 |

低历史正序／反序 after−before 为 +1.327950／+1.759237 s；高历史为 −11.307215／−12.555107 s，方向各自一致。
这支持旧全历史监督扫描的成本解释，并验证候选在此高历史配置的收益；低历史索引／读取成本的回退仍保留。
逐格 fresh owner/service 只证明匹配历史成本，不等同持续 19 轮漂移消失。每组 n=2 也不能估计可靠 query P99。

### 实际共同配置、资源与质量

原 76,000 与新 16,000 均为单张 RTX4090（24,564 MiB）、Qwen2.5-7B-Instruct，
LOTUS1.2.4／LiteLLM1.95.0／OpenAI2.50.0／pandas2.3.3／Ray2.56.1／vLLM0.25.1；
SemBench `c814e380` 的原 Movie Q1、SF2000、LIMIT5。模型、数据、sem_filter 本体及实际 bridge 二进制 SHA 见 JSON。
实际生成合同为`max_completion_tokens=8, temperature=0, top_p=1, stop=["\n"], logprobs=false`；
已对全部46条query／92,000份protocol核对，protocol成员和SHA逐query列入CSV。
作者 n4、META6、host task threads8／C8，`max_pending_calls=128` 是待交付窗口；新八格实测 HTTP 峰值均为 4；原长测逐 query 峰值见 CSV。
driver／author 使用 CPU8–15，服务 CPU0–7；BF16／eager／TP1／PP1／maxlen4096／maxseq4／batchedtokens2048／
显存利用参数0.8／seed0／prefix-cache off／chunked-prefill on。该参数不是显存硬上限。
request 超时30 s；健康 query／owner 无累计墙钟截止；startup180 s，work／backend无进展及cleanup各120 s，故障监督保持。

资源使用 prepared API submit→EOF 窗，CPU按已匹配 PID／birth 的累计差分，GPU／gauge按相邻窗内样本梯形插值，边界不外推。
下表 CPU和GPU、MFU按两个 query 等权平均，显存为组内采样峰的最大值；完整逐 query 资源与覆盖时长见 CSV。

| 历史／版本 | client／service／controller CPU核 | GPU busy均值 | 显存采样峰 MiB | 完整query MFU估计 |
|---|---:|---:|---:|---:|
| 0／before | 0.795／0.920／0.402 | 98.73% | 19528 | 28.72% |
| 0／after | 0.795／0.909／0.399 | 98.19% | 19522 | 28.13% |
| 114,000／before | 0.703／0.930／0.531 | 98.42% | 19566 | 24.60% |
| 114,000／after | 0.809／0.947／0.401 | 99.24% | 19484 | 28.62% |

八格实际最大内部采样间隔约1.001461 s，秒级 service waiting 样本为0不能排除亚秒排队。
高历史 source检查＋导入由3.963增为4.274 s，增加0.311 s；每格服务启动至 query release 约60.51–63.38 s。
启动、初始化、SDK事后审计及最终退出不混入 query 窗，也未被声称消失。
MFU为 useful-forward估计，所有11项资格通过；采用相同 P/O、公开系数和
[NVIDIA Ada白皮书附录A第30页](https://images.nvidia.com/aem-dam/Solutions/geforce/ada/nvidia-ada-gpu-architecture.pdf)
的稠密BF16／FP32累加165.2 TFLOP/s。公式及充分统计量在JSON／CSV，可离线复算；不是硬件实测FLOPs，也不是独立于墙钟的因果证据。

每条模型query的P总257,591／O总8,000相同；SDK raw body／row／stage／call／response关联与完整Filter核验通过。
原38条和新8条的LIMIT5官方metric均1.0，完整2000行分类仍有FP11／FN436–438（新八格FN436–437），
`quality_equivalence_proven=false`。LIMIT后成绩不能证明全量分类或逐行输出等价，不能据此声称质量达标的调度收益。
LiteLLM公共价格表GET超时后用本地副本的警告保留；它不是模型路由fallback，额外模型协议请求仍为0。

<a id="remaining-issues"></a>

## 已修范围、未修问题与未触达路径

| 分类 | 证据与当前状态 |
|---|---|
| 已验证测量候选 | `c994cf4a`以五行计数＋双索引替换在线全史扫描，保持预算、unknown、原三元组和故障政策；Mac负结果、Linux资格、高历史收益和低历史回退均保留。本次未合main或修改冻结活动baseline。 |
| 未修／待验证 | CPython同一owner持续19轮的漂移、作者Ray Lane2恢复／再取队列异常均未被新配对解决。最小后续是独立预算的patched CPython同owner19×2000（38,000新model）；Ray优先零模型原生路径见证。此次不自动追加。 |
| 源码确定错误分支，健康运行未触发 | 作者`dbend_client.cpp`在uv_write提交后delete RequestContext，写完成错误回调仍可引用裸指针；非空LoadFuncTask status不收集、空结果可保留all_ok。静态条件可证，但未做真实故障／sanitizer，不能据此解释本次Lane停滞。 |
| 历史未触达legacy API | 固定f182的`ray_data_http.py`重复doc_id覆盖、多余结果丢弃，FakeRay CPU复现；baseline7e59已有该guard。两固定版本旧入口仍可把错误endpoint_index覆盖为输入值。该入口由旧orchestration CLI使用，不是本次Movie NativeCallExecutor路径，未在c994修复。 |
| 合理原生差异／适配成本 | 原生图与SemLoom的owner、worker、bridge寿命各自保留；SemLoom准备2N−1次、native N次的CPU probe不等于多发模型。IMLane预检／写文件及多层响应计量有重复编码，Lane归还前Arrow深复制有必要所有权作用，未量化时不扣除。 |
| 验证缺口 | 可选async＋reuse、共享Coordinator并行查询／独立取消、PG和任意大输入／redirect组合未由本轮覆盖。所选同步默认桥已完成原2000行两模式实测；当前sem_filter内容已实际绑定，不代表所有文字入口都已补源码守卫。 |

上述代码判断均对应固定版本；源码与CPU依据不外推后续远端分支。生产入口见
[NativeCallExecutor](../../../../code/src/baselines/text/frameworks/native_calls.py)、
[LOTUS接点](../../../../code/src/experiments/postgresql/supplier_adapter_query.py)、
[旧Ray HTTP入口](../../../../code/src/baselines/text/frameworks/ray_data_http.py)；
作者错误路径定位于固定072f源的`src/dbend/dbend_client.cpp:313–340`／`include/dbend/dbend_client.hpp:86–90`，
Lane等待／恢复定位于同一作者执行源码。作者源码未在结果目录重复保存。

<a id="model-recompute"></a>

## 精简证据与复算

公开Git只保存[46条query原表](model_queries.csv)、[实际配置／归档与指纹](model_runs.json)和
[标准库复算入口](recompute_models.py)。原38条＋新8条全部保留，没有选择较快子集；不重复打包原始日志、模型或数据。
默认可由CSV重算两模式首次／后续及八格配对、计时与MFU；可选本地闭合原件还能核对92,000请求的
SHA、单请求时间／P99／并发／token、响应身份，以及只读SQLite新账和高历史前缀。
它不重新运行SDK或模型，也不代替已完成的完整SDK／物理关闭独审。
可选原件参数中，original-scope指向旧模型包的scope；paired-scopes下须按八个case_id各放对应scope内容。

```sh
python3 -B recompute_models.py
# 取得并解压对应仓外归档后，传真实的canonical目录；本命令只有CPU/只读文件访问：
python3 -B recompute_models.py --original-scope /path/to/original-model-scope --paired-scopes /path/to/paired-case-scopes
```

原软件包run_id为`imlane-q1-7e59-20261016-b4a793-zero`，模型包为`imlane-q1-7e59-20261016-b4a793-model`；
新配对前缀`imlane-progress-c994-20261016-9f37a2`，八格m0–m7及两格z-before／z-after在JSON逐一映射归档SHA、字节数和成员数。
按run_id＋archive SHA向运行owner取回仓外原件；CSV每query记录observations／persistent-query的相对成员及SHA。
原模型包188,235,989 bytes／1,193普通文件；新10包共73,197,831 bytes／1,236普通文件，逐SHA本机备份核对通过。
新包20个链接只记录目标、不跟随，24个非普通IPC对象记录跳过；冻结静态source／validation／helpers由原inputs归档保留，
不把普通文件归档说成磁盘所有对象均复制。原旧账本SHA保持不变。

两批模型均自然结束，actions／forced／survivors／errors为空，结束时GPU／SHM空、lease正常释放。
新配对的结果与关闭／预算已分别独审，终点300绑定成员和11,380源码成员不变；分析及独审指纹见JSON。
本次公开入口额外对46条query、92,000响应与闭合账本复算通过。审计、归档及本次发布均未重启模型。
