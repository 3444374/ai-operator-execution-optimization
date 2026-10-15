# main Core 与完整响应基础资格

本次以 main `06854ecd462d17ecdf43f3da793fb009ba12dc0d` 为底稿，选择 integration
`671a22dd4285319209396ece3572bb89ff5ab74e` 的必要实现；没有合并其工程祖先或完整结果目录。
当前生产文件、来源与字节摘要见 [checks.json](checks.json)，接口见
[native task 说明](../../../../code/src/execution_provider/adapters/README.md)。

## 范围与计划

Core 的空释放须保持等待代次；满额完成事件须继续进行有界排空，空轮询后可等待；暂停派发不得误报可提交。
同一 session 只复用最近未接纳后缀的有限元数据检查，变化的类型、值、配置、当前容量及取消仍须检查。
完整响应保留状态、重复 headers 和原 body，逐请求 cookie 隔离不改变连接池复用。
Ray worker 模式必须匹配；只有可识别的 HTTP 前失败可释放责任，未知远端工作与取消继续计费。

该范围依赖既有 `IncrementalExecution`、Job、结果租约和 payload 接口。七个生产文件足以组成闭包；
Ray 额外 worker-ID 观测与 payload 分段计时未带入，原 payload materializer 保持。
其余前端、replica、PZ、Animals 和新 HSE 策略不属于本批。通过当前回归、同条件组件对照和独立规范／需求审查后，经 PR 接纳并复验 main。

## 行为与环境检查

| 检查 | 实际结果与适用范围 |
|---|---|
| 原 main Core 加新回归 | 72 项，9 个失败、8 个错误记录；空释放代次、排空提示及元数据复用均有复现。错误包含原实现尚无复用检查状态。 |
| Mac 定向回归 | 116 项全过；Core、native 生命周期、完整响应和实际 HTTPX。 |
| Linux 调度 | 432 项全过，保留旧调度、方法、准备与多 session 路径。 |
| Linux 执行层 | 192 项中 183 项通过；9 项 PG 图像集成因未设置 DSN／图像资产跳过。 |
| 实际库 | Python 3.12.3、HTTPX 0.28.1、Ray 2.56.1、Arrow 24.0.0、Daft 0.7.21；自动选中机器 profile，core preflight 通过。 |
| 实际 CPU Ray | 显式启用 native 与 failure integration，使用自建 `num_gpus=0` 集群、localhost HTTP 和本轮独占临时目录，不加入其他集群。 |

Mac 完整调度套件因缺 Arrow 有一个导入错误；完整执行层套件还受沙箱 socket 权限限制。
这些失败保留，不称本地完整通过；Linux 完整相关套件用于补足环境检查。
Mac 默认 profile 无匹配后，使用仓库外的真实 Darwin CPU fixture profile 检查，缺少生产依赖如实记为 missing，未混装。

原始日志的运行身份为 `main-convergence-20261015/core-transport`，Linux 日志归档 SHA-256
`309c09d09d898ea5ed306701d88fc74c3e7cd427806d299307362fe4f40b8997`；
本地复现和完整套件失败亦保留在维护者的同批 audit 目录，可按运行身份与摘要取得。

## 重复 offer 的 CPU 成本诊断

[全部 28 个计时单元](metadata-cost.json)比较原 main 的 `SessionEngine`／元数据 validator 与候选。
两者由精确源码加载到同一 Python 进程，使用相同不可变 task、Core 容量、producer 和受控 backend；
首项已接纳，结果存储使第二项持续返回 BACKPRESSURE。没有 dispatch、HTTP、PG、模型或 GPU 工作。
每种 1／16-stage 描述交替运行两种实现，7 次重复，每单元先预热 200 次，再计时 5000 次 offer，
使用进程 CPU 时间，计时期间两组均关闭 GC，全部记录核对零新增接纳和结束后零资源残留。

| 每 task stage 数 | 原 main 中位数 ns/offer | 候选中位数 ns/offer | 变化 |
|---|---:|---:|---:|
| 1 | 11642.0 | 7239.4 | -37.817% |
| 16 | 29511.0 | 10859.8 | -63.201% |

计算入口保留为仓库外 artifact `main-convergence-20261015/core-transport/metadata-offer-cost-runner.tar.gz`，
归档 SHA-256 `014bb647bcadadd02860cab900a555ff31951f99a8a1e899d78a7232f92e816c`；
其中 `metadata_cost.py` 的摘要与 `metadata-cost.json.script_sha256` 一致。维护者可按该运行身份和摘要提供原件；
将固定候选 checkout 恢复为 `main-prep` 后，在其上级目录执行该脚本，可重建同一基线比较与全部计时单元。
此计算入口不进入生产目录。

这是反复提交相同未接纳元数据时的组件诊断，不代表每次生产 offer 都可复用，也不代表查询或 GPU 加速。
原 PZ 文档与统计修复保持；完整 LOTUS 查询、当前组合的 PG 图像和匹配真实模型验证仍需后续各批自己的证据。
