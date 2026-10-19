# IMLane 普通 Filter 的 CPU 接线资格

本页保留 `0190e8cd` 提交时的本机 CPU 快照与当时通道阻塞；后续指定通道恢复后的
[作者 C++／CPython 无模型实测](../imlane_filter_author_cpu_20261019/README.md)单列，不改变本页原件身份。真实模型和作者 Ray 仍待验。

职责分支 `feat/imlane-local-filter` 从 `integration/data-execution@c0be45dd1278163a9e1e612694ff9752cfb433e0`
独立建立。实现与验收映射见[模块 README](../../../../code/integrations/imlane_records/README.md#普通-filter-v2cpu-资格作者实机待验)，
依据为[Filter 计划](../../../plans/数据组织.md#lotus-filter-data-execution)及
[原生执行／模型所有权](../../../plans/系统架构.md#model-engine-execution)。
本报告属于 AI 语义算子数据执行的适配检查，不是模型、性能或 main 接纳结论。

## 实现范围

复用既有 `FilterMethod`、`LotusFilterMethod`、`EngineSpec`、`GenerationSpec`、调用账本、
原 parser／COUNT 与原 SemBench evaluator。新增 IMLane 工作负载、字符串身份到 int64 的可逆映射、
有限宿主提交／收集、固定解释器配置及私有模型进程启动装配。共享 engine 与进程组件未修改，
未改公共 CLI／公共记录合同、作者 Coordinator／Lane／执行器、活动 baseline 或旧 HTTP 配置。

显式 v2 在原三列输出内保留逐行 engine 终态，默认 v1 echo 规则不变。合法失败允许部分未知，
包括共享 engine 的 JSON 字符串 `null`；已知 usage、token IDs 与提交证据不补零、不当作 False。
engine UID 绑定实际 ready 实例；成功 token IDs 与 usage 必须一致。坏条目使 worker 失败，
其他已核验条目的终态不被抹掉。host 首次发现失败即停止新供给，对已接纳块有限排空。

本版每查询一个独立 owner／engine，在该查询多块间复用。两个查询使用独立模型、控制目录及
取消状态，不声明跨查询同 engine 复用。该范围符合计划中的查询私有资源与独立实例责任，
后续跨查询模型复用须另验。输出仍在作者释放 Lane 前复制；v2 C++ 新检查尚未实机运行。

## 已运行的 CPU 检查

Python 3.12.6、Arrow 24.0.0、LOTUS 1.2.4、LiteLLM 1.95.0、OpenAI 2.50.0，
仅复用既有本机隔离 CPU 环境，没有安装、升级共享环境或下载模型。自动机器匹配因 Darwin／无 GPU
失败的原记录保留；随后使用明确标为 CPU fixture 的私有 Darwin profile，
`core,text,semantic-benchmarks` 只读 preflight 通过，不借此认证 Linux／GPU。

| 检查 | 结果与边界 |
|---|---|
| 方法、IMLane／旧 echo owner、原查询及私有进程回归 | 最终 65 项通过，零 skip；包含新 Filter 模块 22 项 |
| 共享 engine 合同回归 | 独立新解释器 39 项通过，零 skip；全部 vLLM 分配和结果均为替身 |
| 原方法至结果消费 | 真实 LOTUS formatter → 真实 CPU 子进程 → 原 parser／COUNT → 原 SemBench evaluator；配置的固定响应，无模型推理 |
| 身份与部分结果 | 重复来源不同 call ID、标签排除、跨查询／缺失／重复／乱序拒绝；块内已知失败、坏 prepare／engine 后缀、已接纳块排空、unknown 不填零 |
| 资源 | CPU 子进程及观测后代退出、reaper 停止、构造失败责任、close 失败可重试；GPU 退出未知时 lease 保留的替身检查 |
| 独审 | 协议／Spec 与资源责任两份独立审阅，发现项修复后均接受；关键摘要在 [qualification.json](qualification.json) |

65＋39＝104 项相关检查，不把后续定向复测相加成新样本。最终模块 22 项与最后身份增量的
整组复测原件分别保留。模型环境要求无 LOTUS 的 guard 保持：一次把方法与 engine 测试混在
同一进程的回归触发该 guard，失败原件保留，随后按实际环境隔离分组通过。
更早的 fixture 字段错误、沙箱限制和测试修订也保留，没有删除正确性检查。

完整查询用例使用公开合成四行 CSV，其中三行符合 `taken_3`；固定响应产生 COUNT＝1，
分类核对一条正例、两条负例。配置 token IDs／usage 只是软件 fixture，不是模型实测 token。
该用例验证原方法和评价接线，不代表原始 Movie 数据质量。CPU 子进程使用同一 CPU venv 的
另一个进程，未导入 LOTUS／vLLM；不能据此认证待测的独立 vLLM 模型解释器。

## 未完成的实际检查与阻塞

| 动作与目标 | 状态 | 原因／已完成部分 |
|---|---|---|
| 复用获准服务器的指定既有 SSH ControlMaster | blocked，未拒绝，结果明确未连接 | 指定 socket 不存在，检查返回 255；未另建连接或使用其他服务器 |
| 作者 Arrow16.1／Python3.12 C++ 构建与 v2 检查 | pending | C++ 源码及检查已准备，未编译；原 echo 二进制不重新绑定新源码 |
| 新 helper＋原 LOTUS＋CPU 响应子进程的作者 CPython 往返 | pending | 有限 `verify_filter` 入口已准备；原作者 10scope echo 资格保持原身份 |
| 独立 Arrow16 方法环境副本及真实模型双查询 | pending，由集成 owner 排期 | 没有创建方法副本、预留服务器预算、启动 GPU／模型或改变共享环境 |
| 作者 Ray 的 GPU／环境与模型 owner | pending | 未实现 `num_gpus=1` 等装配，不借用 Ray Data 或其他后端资格 |

本轮真实模型请求和 GPU 请求均为 **0**。既有 socket 缺失是环境阻塞，不是自动审批拒绝；
没有未知的远端执行或遗留本轮远端 lease。

## 集成与复测合同

owner 可选择性接入本职责提交；最终 baseline 仍须保留 Daft、Ray Data、IMLane，未完成臂列缺项。
接公共 selector 时只需为既有 `run_movie_query` 提供返回 `FilterResults` 的 lazy factory，
其 author scope finalizer 返回现有 `owner.resource_state`。本职责没有修改公共接口，
也未新增统一线程池、调度器或 HTTP 网关。

恢复已获准的既有通道后，先核作者构建身份、磁盘／环境、当前 lease 和所用源码闭包，
再在独立方法副本中保持 Arrow16.1 与固定 LOTUS 来源；新输出目录编译新桥。
依次运行显式 `no-model` 入口、由 owner 安排的非空双查询 `local-model` 单元，
保留每查询永久预算、全部终态及未知项。关闭须先过同一作者 Lane，随后独立核
PID 出生身份／后代、namespace／SHM 和 GPU compute／显存；未知时保留 lease。
不得把 CPU 或 echo 结果写成 data-to-local-model 已贯通。

原始失败、测试日志、私有 CPU profile、源码摘要和连接阻塞记录统一保存在仓库外的
`imlane-filter-cpu-20261019` 证据归档；归档名、成员数及 SHA-256 见 qualification.json，
按运行标识向本任务执行环境获取。Git 只保留此报告与紧凑的机器可读资格记录。
