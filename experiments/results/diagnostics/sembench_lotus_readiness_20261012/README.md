# SemBench Movie 原查询接续准备

本切片把固定 SemBench Movie Q1/Q2/Q3 接入已有六条 LOTUS 执行路径。它复用原查询、原
`sem_filter`、原 LIMIT／COUNT 和官方 MovieEvaluator，不用 Movie 派生 Map 代替查询。
执行器、SDK 响应转换、HTTP 客户端寿命及 IMLane 桥仍由原模块拥有，本切片没有修补这些核心。
数据库执行、模型质量、共同资源配置和性能比较仍需各自的验证。

**最新软件资格：16 pass、2 unsupported、0 notrun，累计128/144次替身POST。**
LOTUS原生、Daft、Ray Data和SemLoom各Q1/Q2/Q3正常；两种IMLane各Q2/Q3正常。
IMLane原始2000行Q1在当前完整调用适配器发送前拒绝，预算分配及模型POST均0；只确认本适配器
有效128行，没有把此值归因于作者底层kernel。全部实际模型／GPU／PG运行仍为0。

## 来源与实现

基线为 native `fd8c00bca70804075cc7f527d24e27eaea2c60c5`。只接续稳定性提交
`2a7558097855befbda77f3302815433804247e34`、`a21fc084cf648df7118595013f51f41c129a6361`
的消费提示和确定失败处理；本独立分支对应 `905f8c3a`、`dd14d592`。
不含随后多副本路由或 task-8 正在修补的公平性／生命周期改动。

[SemBench](https://github.com/SemBench/SemBench/tree/c814e3807e72d4cf876b852b17e77f3cc94575c2)
固定 `c814e3807e72d4cf876b852b17e77f3cc94575c2`，查询／评价目录的 tracked 文件无改动。
上游 requirements 的 LOTUS 是 1.1.3；本切片明确使用 LOTUS 1.2.4，不称完整上游环境复现。
原生查询由[已有入口](../../../../code/src/baselines/text/sembench_movie.py)直接调用 `_execute_q1`、
`_execute_q2`、`_execute_q3`；只绕开 benchmark 构造器的服务准备、预热及近似模型初始化。

| 对象 | 原行为与本切片落点 |
|---|---|
| Q1 | 完整 Reviews 关系 → 原提示的 `sem_filter` → `head(5)` → `reviewId` |
| Q2 | 原查询先取 `id='taken_3'` → 原提示的 `sem_filter` → `head(5)` → `reviewId` |
| Q3 | 原查询先取 `id='taken_3'` → 另一条原提示的 `sem_filter` → `shape[0]` 的 COUNT |
| 执行接点 | [supplier runner](../../../../code/src/experiments/postgresql/supplier_adapter_query.py)中同一个 LM 实例的完整 uncached batch；已有 native／Daft／Ray Data／SemLoom／IMLane 执行器不复制 |
| 输入与评价 | [MovieDataset／MovieQuery](../../../../code/src/experiments/postgresql/sembench_queries.py)保留 `reviewId` 和独立 `movie-row-N`；模型只接收原评论字段，标签用于预检和 EOF 后质量评价 |
| 查询复用 | [原有常驻 owner](../../../../code/src/experiments/postgresql/persistent_native_adapter_query.py)保存 LM／运行时，查询的决策、结果、提示和预算分别创建；发生错误后禁止继续复用 |
| 预检与命令 | [明确的 Movie CLI](../../../../code/src/experiments/postgresql/sembench_query_cli.py)和[示例配置](../../../../deploy/autodl/sembench_lotus_queries.example.json)；缺版本、摘要、路径或容量就失败 |

LOTUS 原方法始终拥有提示、格式化、最终解析和关系行为。中间 SDK 响应转换与异常类型的
不对称仍按独立审计处理，不能将此处正常结果写成六路径异常语义等价。

## 唯一原始准备集

选择已有 `movie-sf2000/prepared2`，不重新抽样或重新生成 CSV，不混入此前 512 行 Map、调参或
独立评价清单。完整机器路径放在任务私有交付 `handoff-data-bindings.json`；Git 中只登记身份与环境变量。

| 文件／内容 | SHA256 |
|---|---|
| 原 `Reviews.csv`（654765 字节） | `ad5def7df4ece425c741c82d73687b8cb4a61952e1580c0427879cbef3033aaf` |
| 清单内容摘要 `manifest.sha256` | `e753baa59f487203ad3148653d15726311c992dc742ae97d6d7825ab5acca4e2` |
| `manifest.json` 文件字节 | `3e8066677e0c9d85005f612b188227ed60354eccd37523e624d9af6359146f97` |
| `raw.jsonl` | `88eceb0f234a54fd38e874b49e627406434716c6b4096c0925befa0b1213eade` |
| `references.jsonl` | `4f61423c016ca467a4f538dcdc9d935080678b20adc05875b37f8a86f2a30059` |

实际 CSV／准备文件核对：2000 个独立行身份、1865 个唯一 `reviewId`、135 次额外重复出现、
105 部电影、1485 行正向标签；`taken_3` 有 120 行，其中正向 14 行。旧文字中的 116 部电影
不适用于这份已固定 CSV；不据此重新生成输入。原生成器及 Kaggle version4 的摘要保留在同一清单。

`--data-preflight` 核对 CSV 与准备字段／标签逐行一致、源码 HEAD／tracked 改动、版本、文件摘要、
行序和身份。`--preflight` 再检查实际执行配置、预算和作者 Coordinator 身份；两者均不发模型请求。
旧归档属主只通过 `git -c safe.directory=<所选目录>` 在单次只读命令中处理，不改全局 Git 配置。

## 3×6 的覆盖与资格

源码已接通全部 18 个组合；实际库软件结果见本目录 `verification.json` 和 `coverage.csv`。
“原数据行数可接收”不等于模型或共同资源比较已通过：

| 原始数据输入 | LOTUS 原生 | Daft | Ray Data | SemLoom | IMLane CPython | IMLane Ray |
|---|---|---|---|---|---|---|
| Q1：2000 个模型输入 | 行数范围内，未运行模型 | 同左 | 同左 | 同左 | **发送前拒绝：128 行上限** | **发送前拒绝：128 行上限** |
| Q2：120 个模型输入 | 行数范围内，未运行模型 | 同左 | 同左 | 同左 | 120≤128；字节／运行时另检 | 同左 |
| Q3：120 个模型输入 | 行数范围内，未运行模型 | 同左 | 同左 | 同左 | 120≤128；字节／运行时另检 | 同左 |

软件小fixture资格与上述原数据资格分开：前四臂的Q1/Q2/Q3为pass；IMLane的Q2/Q3为pass，
原始Q1的两项明确拒绝为unsupported；这个18单元软件清单没有notrun项。

完整六臂示例的 `--preflight` 应因 Q1 的两个 IMLane 单元失败，不能悄悄切块。
原方法整批供给保持；本切片没有扩大 IMLane 的 128 行／32MiB 输入支持，也不声称 C++ 输出
驻留受到聚合 32MiB 保证。原生完整响应 8MiB、SemLoom 1MiB、两类最终批次留存 16MiB，以及
响应解释、期限起点、连接复用和提前关闭诊断的差异，仍是公平比较前置项。

## 软件验证与失败保留

固定 driver 版本：LOTUS 1.2.4、pandas 2.3.3、DuckDB 1.5.4、SciPy 1.17.1、LiteLLM 1.95.0、
Ray 2.56.1、Daft 0.7.21；IMLane 作者版本／构建／模式及 PID start ticks 由现有 Adapter 核验。
软件 fixture 只含 8 行，Q2/Q3 取其中 6 行；所有原方法／官方 evaluator 仍从固定 checkout 加载。
fixture 明确带重复公开 `reviewId`、重复文本和独立行身份；故意含一个假阳性和一个假阴性，
因此 Q3 可以 COUNT 完全正确而行分类仍有两处错误。既有计时包含查询准备及完整消费，官方评价
在 consumer EOF 后单列；这些小规模替身耗时不用于性能排名。

服务器使用用户分配的 CPU 8–15，零模型／GPU；源码上传先被自动审批拒绝，随后取得明确许可。
自动整机 GPU profile 因 8<10 CPU 及未设置 DB／GPU 变量失败，原报告保留；另按明确的软件诊断
范围保存 CPU8、无 PG／GPU 的 preflight，没有据此声称完整模型环境已准备。

首轮 14 次 POST 后，诊断进度文件重复创建触发 `FileExistsError`；原生 Q1／Q2 的正常结果与评价
已保存。第二轮在发送前因旧 Ray 临时目录存在而停止，新增 POST0。修正快照文件名和临时路径后，
原时间窗结束；用户明确延长最多 5 分钟，沿用累计 14 次、总上限 144 次，只续跑未完成项。
原失败、源码摘要、开始时间、计数器和各轮退出状态都保留在私有原件，不覆盖失败轮。

延长后的真实库轮完成其余原生Q3、Daft的三条查询、Ray Data的三条查询，累计60次POST；
全部九个查询摘要和官方评价正常。随后测试把SDK的token字段写死为`max_tokens`而触发
`KeyError`；实际编码使用`max_completion_tokens`。已修正验证键名，保留原请求；原查询实现未改。
剩余路径的续跑请求在延长时间窗结束后被编排器拒绝，新增POST0，不再次启动。

首次交付实际库覆盖为9/18：LOTUS原生、Daft、Ray Data各Q1/Q2/Q3；SemLoom、两种IMLane各三条
软件查询未验证，原生授权错误及作者超时／取消的软件追加也未运行。还有84次POST额度，但两次
时间授权均结束，不能仅凭剩余计数继续启动。原生Q1/Q2共用一个LM/owner，Q3为续跑后的新owner；
Daft及Ray Data各自三条查询共用owner。不能把这些记录称为原生三条查询全部同owner的通过证据。

以上9/18是首次交付的保留状态。父随后授权同CPU再最多5分钟，累计上限仍144、已用60不重置。
剩余窗口85.6秒正常结束，追加SemLoom三条及两种IMLane各Q2/Q3，不重跑旧九项；同时用原2000行
数据验证IMLane Q1拒绝。正常查询共104POST，作者两模式完整HTTP／超时／取消16POST，原生LOTUS
授权错误8POST，总计128。授权错误保留AuthenticationError，owner被标记不可复用，后续查询POST0。
作者两模式取消后的远端HTTP状态仍unavailable，结束时自有进程及共享内存归零，不声称未知远端完成已被消除。
还余16POST，但不为用完额度扩大检查；完整模型／异常等价及资源比较仍待各自资格。

控制配置没有强制统一：旧九项保留C8来源；本轮SemLoom用现有默认C4，两种IMLane按已有作者
6个任务线程声明C6，不修改作者执行器／线程／bridge/kernel。CLI允许显式`arm_native_options`，
原生较低配置不会因为另一路径需求被升级；这些不同来源正常结果不用于性能排序。
归档下载的Ray `session_latest`绝对符号链接被安全解包器拒绝；原压缩包保留，随后只提取常规文件并登记跳过链接。

模型请求0、PostgreSQL 查询0；软件失败与后续有效覆盖分别列于 `verification.json`。
正常消费、官方评价、错误／取消和退出各自按实际覆盖登记，不借用其他任务的新核心能力。

## 发布整合与本机回归

本切片的三个交付提交（`1351a45f`、`34afde9b`、`e5d35205`）与两项必要稳定性依赖，
已按最终净变更整合到原生发布分支的 `286e4ab5` 之后。源码、配置及原保存证据与最终交付
逐字一致；并行项目日志两侧记录均保留。公开材料使用修正后的 Ray 路径占位符，原提交身份
登记在[本机整合检查](integration-check.json)，未将修订前的私有路径加入新发布提交历史。

受影响的原方法查询、常驻入口、错误观察、清理、消费提示和确定失败处理共 **92 pass／15 skip**。
15项实际厂商库或运行时检查未启用；CLI帮助退出0，缺必要环境变量的预检明确退出1。
首轮17项被本机沙箱的端口或进程读取权限阻止，原日志保留；相同源码在本机权限下完成上述回归。
本轮没有服务器、模型或GPU运行，既有 **16 pass／2 unsupported** 与累计128次软件POST保持原身份。
该历史发布当时尚未纳入task-8的共享执行修订；后续六块及修后组合检查见[发布组合记录](../../postgresql/native_executor_baselines_20261012/README.md#native-release-composition)，原Q1两项前置拒绝与旧来源保留，不增加性能结论。

## 可运行命令

先按私有交付文件设置 `SEMBENCH_CHECKOUT`、`SEMBENCH_MOVIE_MANIFEST`、`SEMBENCH_MOVIE_CSV`。
执行配置的模型文件、tokenizer、输出、短 Ray 父目录和两种已就绪 IMLane 配置由调用方提供。

```sh
PYTHONPATH=code "$DRIVER_PYTHON" code/scripts/experiments/run_sembench_lotus_queries.py \
  --config deploy/autodl/sembench_lotus_queries.example.json --data-preflight

# 全六臂 Q1/Q2/Q3 在当前 IMLane 容量下应明确失败，模型请求0。
PYTHONPATH=code "$DRIVER_PYTHON" code/scripts/experiments/run_sembench_lotus_queries.py \
  --config deploy/autodl/sembench_lotus_queries.example.json --preflight

# 只有独立授权、完整 preflight 通过的具体配置才执行。
PYTHONPATH=code "$DRIVER_PYTHON" code/scripts/experiments/run_sembench_lotus_queries.py \
  --config /path/to/private/qualified-movie-config.json --execute

PYTHONPATH=code python -m unittest discover -s code/tests/experiments -t code \
  -p 'test_sembench_queries.py' -v
```

要使用当前可接收的子集，显式生成仓库外配置：前四臂的 Q1/Q2/Q3、一次预热加三次测量，
`max_attempts=35840`；两个 IMLane 仅 Q2/Q3 时为 `1920`。这只是具体清单的预算与行数检查，
不自动授权模型运行或消除共同资源／错误语义差异。全部六臂计划是 53760 次模型请求，当前不能执行。

本机最终针对性回归见私有`local-regression05.log`；实际库用例由
[显式启用的测试](../../../../code/tests/experiments/test_sembench_lotus_libraries.py)提供，缺少
`SEMLOOM_MOVIE_ACTUAL_ARMS`时明确skip。第一轮本机的macOS进程采样权限错误与本切片的owner
测试兼容性错误分别保留；修正后使用获批的只读进程访问完成同组检查。

本准备能支持原方法在不同已声明执行器上的完整查询／质量记录，并继续原生 API 与 SemLoom
执行接入的工程比较。它不支持六臂 2000 行 Q1 的资格、模型质量等价、调度性能优势、并发多 Job
公平性、跨节点或 PostgreSQL SQL 端到端结论。
