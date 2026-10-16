# IMLane 长测监督计数：Mac 零模型隔离

本项只修测试链的在线进度查询，不改 IMLane 作者执行、调度、查询、模型或并发额度。
分支基点为 `7e59e5a10eaceb7ac4a1bd0766b10244ca3e192d`；活动 checkout、服务器、lease 和 GPU 均未操作。
**去扫描候选通过语义核验，但 Mac 完整 fixture 未取得稳定收益；不能认定现场 17% 减速已解决。**

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

本地 SQLite／HTTP 替身即使降低扫描 CPU，也不能直接解释现场 CPython 72.45→84.79 秒的 17% 增长，
更不能排名早期 CPython 与共用累计账本的晚期作者 Ray。
同配置真实模型少量前后配对由唯一运行 owner 另行安排，本项模型调用为零。
当前交付是去扫描候选与负结果，先保留活动包。Linux释放后的小配对及同配置真实模型验证仍待执行；
不为追求正结果削弱监督语义、改轮询政策或再扩机制。
