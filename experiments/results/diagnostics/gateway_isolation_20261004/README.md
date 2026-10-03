# gateway多查询干扰的本地组件诊断

按[gateway诊断清单](../../../plans/查询调优.md#gateway-isolation-probe)完成40个新进程case、
1,088次模拟调用，主清单全部通过；HTTP、模型与PG查询均为0，生产执行代码与默认配置未改。
本报告与紧凑证据随`codex/gateway-isolation-diagnostics`实验分支保存；运行源码身份仍以原始摘要表示。

**观察信号：** 状态拥有者或同步observer受阻时，其他查询的接纳和未派发工作取消也会延后；
payload线程受阻时，接纳和取消仍快，但已接纳工作等待派发。慢发送在本组条件下没有传播同等等待。
这是受控组件行为，不是服务器、真实Ray、SQL、模型质量或GPU性能结论。
下方[服务器补充](#server-platform)另列真实Linux/Ray/Daft/Arrow与本机HTTP检查，不与上述模拟时间合并。

## 执行、设置与来源

现有macOS/arm64、Python3.12.6；不安装依赖或连接服务器。实际运行`MultiSessionMapGateway`、UDS帧、
`QueryRegistry`、连接消息槽、共享`SessionEngine`、`BoundedAsyncBackend`和`RayMapTransport`。
非本次UDS的连接直接拒绝。`SyntheticTable`、`SyntheticRay`和独立事件循环worker替代厂商表、API和服务，
kernel peer身份使用fixture；真实Linux凭据未在此完成核对。

三个Job各一条流：A/B产生结果，C只接纳一条且不开启派发。干扰开始后关闭C控制连接，
测量尚未派发任务的取消/排空，不是远端计算取消。全局活动容量4；每Job检查4行，预热/测量16行。
存储按3个Job预留、计算使用既有shared配置。最大8个连接、批次至多4行、输入窗口2MiB、表引用额度4MiB。
模拟worker每行10–12毫秒，准备0.5毫秒/批、put0.2毫秒，均为人为设置，不是服务校准。

四个干扰位置各有零延迟控制和250毫秒等待。每配置一次检查、一次预热、三次交错测量，
总调用为`8×(2×4+4×2×16)=1,088`，另有40条接纳后取消的C任务。
整体18.036秒，包含40个Python子进程与证据写入；单case20秒、整体5分钟，首次错误停止。

来源为`main@f4646daaee00ee09fb5c44f5cd629f4686aa9c8d`和本次诊断入口；运行源码SHA-256为
`ee7c926c8afa8d14ae4a8695577df3c2f6a254b74ff9b8309f83615c76619d85`，归档成员为`source-suite.py`。
其他来源、UTC起止时间及平台身份见[完整清单](raw/summary.json)。
`comparison_role=project_scheduled_method`标识自有Core控制，`formal_baseline_eligible=false`；
不是Ray Data、Daft或数据库原生baseline。

## 计时定义与主要结果

以下单位均为毫秒，报告三次中位数。B开始为首条offer发出前的客户端观测，接纳为该条accepted回复读完；
首结果为第一条完整wire结果校验完，全部消费为全部独立结果校验完。
C取消从控制连接关闭前标记到`job_drained`观测，C模型调用为0。
`case_seconds`从模拟worker已就绪、gateway构造之前到本地退出结束，不含配置落盘和worker创建；
整体18.036秒另外包含这些准备及子进程启动。主数字不称SQL JCT。

| 干扰位置 | B首条接纳：控制→等待 | B首结果：控制→等待 | B全部wire消费：控制→等待 | C排空：控制→等待 |
|---|---:|---:|---:|---:|
| 状态拥有者任务准备 | 0.671→261.374 | 24.355→296.878 | 116.645→398.579 | 0.814→261.754 |
| payload线程准备 | 0.608→0.618 | 29.477→295.922 | 122.381→396.662 | 0.606→0.461 |
| 结果连接慢消费 | 0.763→0.587 | 30.443→28.998 | 108.540→108.554 | 0.703→0.651 |
| 异步传输同步observer | 0.739→263.433 | 28.251→292.372 | 123.323→388.956 | 0.672→263.408 |

全部主要重复如下，其他首行、准备、资源和检查/预热原值仍归同一[清单](raw/summary.json)。

| 干扰位置 | B消费控制三次 | B消费等待三次 | C排空控制三次 | C排空等待三次 |
|---|---|---|---|---|
| 状态拥有者准备 | 116.645 / 115.899 / 133.807 | 398.579 / 394.390 / 401.254 | 0.942 / 0.814 / 0.743 | 262.662 / 261.754 / 253.057 |
| payload准备 | 123.581 / 121.603 / 122.381 | 396.662 / 400.357 / 395.899 | 0.785 / 0.435 / 0.606 | 0.596 / 0.461 / 0.440 |
| 慢消费 | 108.540 / 109.382 / 106.361 | 107.162 / 110.547 / 108.554 | 0.703 / 0.718 / 0.541 | 0.651 / 0.858 / 0.581 |
| 同步observer | 124.739 / 121.024 / 123.323 | 386.046 / 388.956 / 389.205 | 0.781 / 0.672 / 0.624 | 261.443 / 263.744 / 263.408 |

慢消费没有在Core线程sleep。A每条固定结果有60,000个换行，两臂内容相同，socket缓冲设4KiB；
等待臂延后A读取，实际发送最大阻塞为251.852 / 252.812 / 252.382毫秒，租约在发送结束前仍被原实现保留。
B没有出现同等250毫秒延迟，但未进行统计等价检验，也未覆盖更多Job、存储不足、CPU繁忙或其他后端。
固定文本只是协议fixture，不证明模型生成质量。

## 延迟实际发生在哪里

状态准备注入`execution.prepare_task`，线程为`fixture-gateway-owner`；它拥有Core状态，B接纳和C排空一同等待。
observer注入第一次`ray_first_submit`，线程为`semloom-async-io`。
[gateway观察入口](../../../../code/src/execution_provider/multiplexed_gateway.py)在调用observer时持有同一互斥锁，
状态线程也通过该入口记录事件，因此I/O线程中的阻塞仍可拖住状态操作。
不能据注入结果声称生产默认存在250毫秒或旧实验约5秒的固定开销。

payload干扰位于`semloom-payload_0`。B接纳、C排空快，但B首次结果慢。
三次等待测量在干扰期间均记录到Core活动占用4，而释放干扰前worker收到调用为0。
B逐行Core接纳观察→提交观察的中位数，再取三次中位数，为64.143→334.478毫秒；
提交→worker入口观察为2.780→3.604毫秒。逐行值与负差计数见[等待分析](raw/waits.json)。
延迟主要在B获得Core派发之前，不能只归于B在payload线程池里排队。

**工程推断：** 逻辑活动名额包括后端准备/排队，现有上限保护整条在途路径。
增加payload线程未必能让B获得派发资格。若调整名额取得时机，应另行控制prepare/ready存储，
保留逐行身份和未知远端工作的责任；本轮未做这些修改。
等待区间包含组织、容量及观察，不解释为独占的队列CPU时间。384个B测量行的两类差均未出现负值，
不能推导其他环境也没有跨线程观察倒置；区间与中位数不相加为查询完成时间。

24个测量case共663个准备批次，其中610个为单行，总行数768。
容量4、错开完成及跨Job轮转共同参与，缺单Job/同供给时序对照，不能把小批次全部归于轮转，
也不能据替身表/调用开销启用成组RPC或重新启用就绪等待。

## 资源、失败与核对

40个成功case的观察占用均符合各自额度。总体最大Core持有32条、输入5,516字节、输出预留32MiB、
逻辑活动4；worker最大活动4、替身对象记账最大1,192字节。输出预留不是实际物理内存，
替身表计费为`8+Σ(24+payload字节)`，不是Arrow/Ray内存测量。
`ru_maxrss`只保存OS原值与新进程来源，单位未规范化，不用于RSS比较或全链路内存结论。

1,088个发送/完成/Core终态键一致，wire结果逐行校验并按序号核对重建；C接纳但不调用worker。
所有成功case的Core账本、transport引用及未知集合归零，worker/actor退出、新增线程归还、FD数量前后相同。
未执行PG重排、真实Ray取消、远端GPU停止或重启恢复。

相关30个测试方法中29通过，1个Linux peer检查在macOS跳过；新诊断覆盖8个正常配置、已有目录保护，
以及主动消费者失败后的证据保留/退出。首次接线错误为对不可变装配对象赋值，模拟调用0，
失败进程内尚有I/O守护线程；`initial-owner/`、`source-initial.py`和初次日志保留，退出由进程结束完成。
修正为替换装配对象后完成主清单，没有把该失败写成已正常排空。

## 保存与复算

[压缩证据](raw/evidence.tar.gz)含170个成员，保存全部case配置/事件/结果、失败和来源快照。
[清单](raw/storage-manifest.jsonl)将`summary.json`定位到[报告使用的唯一副本](raw/summary.json)，
共171份文件的字节/SHA-256恢复核对通过。归档690,794字节，SHA-256为
`6e7fe3266d81ab9de948c9466dbe414960e52ddd3f6bee4a7de61bd444345556`。
[恢复核验](raw/verification.json)和[主指标复算](raw/replay.json)对应同一运行，其他原件仍在Git外，未删除唯一数据。
JSON副本只做紧凑编码，解码后内容一致；summary原始格式仍在Git外，两种字节摘要分别登记。
通过运行标识`GPU-gateway-isolation-20261004/suite-1`及清单摘要向维护者定位原件，不登记本机绝对路径。

在仓库根目录使用新恢复目录和新输出目录：

```bash
mkdir /tmp/new-gateway-evidence
tar -xzf experiments/results/diagnostics/gateway_isolation_20261004/raw/evidence.tar.gz -C /tmp/new-gateway-evidence
cp experiments/results/diagnostics/gateway_isolation_20261004/raw/summary.json /tmp/new-gateway-evidence/suite-1/summary.json
PYTHONPATH=code python3 code/scripts/profiling/gateway_isolation_probe.py --replay /tmp/new-gateway-evidence/suite-1 --output /tmp/new-gateway-replay
PYTHONPATH=code python3 experiments/results/diagnostics/gateway_isolation_20261004/raw/analyze_waits.py --evidence /tmp/new-gateway-evidence/suite-1 --output /tmp/new-gateway-waits.json
```

主指标由[诊断入口的`analyze/aggregate`](../../../../code/src/experiments/gateway_isolation_probe.py)计算；
阶段等待、容量快照和批次分布由[独立脚本](raw/analyze_waits.py)计算。
每个时长为对应结束观测减开始观测，三次中位数由`statistics.median`计算；负差另计，不静默剔除。

## 下一步取舍

先核对同步辅助观察、任务准备和逻辑活动名额的含义，保留发送前必须完成的身份/计数检查。
同gateway/Core的直接HTTP与Ray HTTP匹配诊断，以及Linux凭据、未派发工作取消和资源检查已由下方服务器补充完成。
模型计划、输入、额度及停止条件另行明确后再增加真实请求。
当前不改默认、不移除gateway、不加等待窗口，不据本次注入结果声称SQL或GPU加速。

<a id="server-platform"></a>
## 服务器补充：Linux凭据与真实Ray的无模型检查

用户允许使用服务器后，按[平台清单](../../../plans/查询调优.md#服务器平台与真实ray的无模型验证)完成16项检查，全部通过且没有跳过。
真实Linux `SO_PEERCRED`凭据及跨进程UDS资源检查运行；随后直接HTTP与Ray HTTP两路径完成272次本机确定性HTTP请求。
模型、GPU执行与PG查询均为0，四个250毫秒干扰情形未在此重测，不能把平台接入当作干扰结论的扩大验证。

实际环境为Linux、Python3.12.3、Ray2.56.1（commit `06729817574615e25afe8f20902b37660c2e6962`）、
Daft0.7.21、Arrow24.0.0、HTTPX0.28.1。机器128个CPU、容器32个CPU配额，Ray只登记4CPU/0GPU，
128MiB对象存储、一个HTTP actor；Daft/BLAS各4线程，没有物理CPU隔离。两张4090保持空闲。
数据盘有约314GiB可用，使用独立源码与产物目录；现存服务器仓库、数据库、模型、软件及历史结果没有修改。
`core,text`只读环境报告通过；源码来自`f4646daa`加明确摘要的诊断入口，854个文件传输后逐项验证。

两个生产Job各4行检查或16行预热/测量，第三Job只接纳一行后关闭控制连接。
全局活动4、每Job留存16行，总输入12MiB、结果预留48MiB、8个连接，Ray窗口2MiB、表引用4MiB、Daft批次4行。
各路径一次检查、一次预热、三次交错测量，`2×(2×4+4×2×16)=272`；服务每行确定性等待10–12毫秒。
本机HTTP服务器、HTTP协议及其等待也在消费时间中，不把该数值当作纯模型或纯RPC成本。

| 三次中位数，毫秒 | 直接HTTP | Ray HTTP |
|---|---:|---:|
| gateway创建至监听就绪 | 1.239 | 271.871 |
| 首条生产offer至全部wire结果消费 | 517.345 | 754.079 |
| 未派发Job取消至排空 | 4.549 | 4.159 |
| gateway关闭 | 2.520 | 4.019 |
| 创建至gateway关闭，含登记/消费 | 590.879 | 1098.969 |

完整重复见[复算结果](raw/server-analysis.json)的`gateway_metrics`，共享Ray启动9.794秒另列。
每case新建gateway及actor，集群在本次测量期间共享；这与先前真实模型共享worker的生命周期不同。
全部行键、accepted-prefix、固定输出、Core占用、实际Arrow表字节、Ray共享时钟及最终引用归还复核通过。
未派发取消不证明已运行HTTP或GPU任务可停止，PG snapshot、权限、结果重排与错误路径仍需数据库接入检查。

首次后台启动的stdin写法导致监督脚本未运行，0调用；改为保存脚本后启动。
首次平台case因总输入2MiB不足以分配三个shared Job而拒绝，0调用，修正为12MiB。
两条检查完成16次后，记录器第二次写入只允许新建的`progress.json`而停止；改为每case独立文件后只执行剩余256次。
这些失败、源码与清理记录均保留，未重放已完成的16次。

本段与[共享预付计数服务器检查](../mapped_request_budget_20261003/README.md#server-observer)共享唯一
[1,339,336字节归档](raw/server-evidence.tar.gz)：173成员，其中172项由内置清单逐项核对，归档摘要保护清单本身。
SHA-256为`3efc2472742ed6eacda82ee3fa447e3b7f5e643f83700b71372e22f977b0b417`；
公开源与私有原件的对应见[保存记录](raw/server-storage.json)，失败、全部重复、逐行事件及SDK来源均可恢复。
私有原件178成员在独立本机备份中实际恢复并按字节核对；服务器原件仍保留，二者通过运行标识
`GPU-gateway-server-20261004`及私有归档SHA向维护者定位。私有SQLite原件仅在Git外，公开保留只读表投影。

```sh
python3 experiments/results/diagnostics/gateway_isolation_20261004/raw/analyze_server.py \
  experiments/results/diagnostics/gateway_isolation_20261004/raw/server-evidence.tar.gz /tmp/new-server-analysis.json
```

该命令只读取归档，核对行身份/次数/容量及全部测量的共享时钟，然后复算阶段；输出须尚不存在。
最终未发现本轮Ray/worker/HTTP/gateway运行进程，两张GPU均0%利用、各1MiB驱动常驻占用；
进程退出与引用检查不代替未采集的物理内存峰值。本报告不更新原生Daft/Ray Data或GPU排名。
