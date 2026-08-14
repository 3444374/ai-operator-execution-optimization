# 现在的代码为什么这样分层

## 1. 先看两条互相独立的轴

代码不是简单分成“文本文件”和“图像文件”。项目同时有两种分类方式：

1. 执行阶段：数据读取 → 工作量估计与组织 → 调度 → 模型服务 → 观测 → 写回；
2. 数据模态：文本 prompt 与图像 bytes/tensor 的语义不同。

公共调度只实现一次。文本把 prompt/output token 换算为 work units，图像把
pixel/frame/preprocess cost 换算为 work units；之后 packing、credit、flush 和 routing
不应知道输入究竟是文本还是图片。

## 2. 目录对应什么职责

| 目录 | 当前问题 |
|---|---|
| `src/data/` | 数据从 PostgreSQL/Daft 读入、物化为 Arrow/Daft batch、最终写回哪里 |
| `src/planning/` | 在不调用执行引擎的前提下，如何估计成本和决定 batch membership |
| `src/scheduling/` | 何时准入、发往哪个 endpoint、何时 flush、完成后如何释放 credit |
| `src/serving/` | 如何调用 Chat/Completions/Embedding endpoint，如何探测 vLLM |
| `src/modalities/text/` | prompt 与 output token 的文本专属语义 |
| `src/modalities/image/` | encoded bytes、tensor、CLIP embedding 与图像审计语义 |
| `src/observability/` | timing、GPU/CPU/能耗、trace schema 和 profiler 接线 |
| `src/baselines/` | ceiling、direct control、框架/产品原生 baseline；不得引入项目 scheduler |
| `src/experiments/` | 场景交错、冻结校准和多 job 实验编排 |
| `src/infrastructure/` | PYTHONPATH/线程环境与单 runner 租约 |

Arrow/Daft materializer 放在 `data/`，不是 `planning/`。原因是 materializer 会实际
调用 Arrow/Daft API；planning 必须保持纯函数式决策，否则无法证明同一 packing 策略
能跨 Arrow、Daft、文本和图像复用。

## 3. 文本、图像和 baseline 如何隔离

文本与图像的项目实现分别在 `modalities/text`、`modalities/image`。框架原生对照不和
项目实现混放：文本 vLLM Bench/bounded/Daft/Ray Data/OceanBase 在
`baselines/text`，图像 Daft built-in/Ray Data native graph 在 `baselines/image`，
共同的 manifest、结果和 provenance 门禁在 `baselines/common`。

这样做的直接作用是：看到一个文件路径就能判断它是否允许使用项目 credit/router。
`baselines/*/frameworks` 若 import `src.scheduling`，架构测试会直接失败。

## 4. HSE static core 怎样跨层协作

新的异构执行路径仍遵守同一依赖方向：`planning/blocks.py` 只定义 block 身份和物理表示；
`scheduling/runtime/stage_broker.py` 只做状态/lease/bytes/work 账本；图像 adapter 在
`modalities/image/staged.py` 计算 image-specific signature 和 NCHW 大小；
`modalities/image/staged_execution.py` 才调用 Ray。

CPU actor 一次返回两个独立 Ray ObjectRef：小 descriptor 与大 prepared tensor。driver 只取
descriptor 并把 block 原子转成 ready；随后把 tensor ref 作为 GPU actor 的顶层参数提交。这样
Ray 仍负责对象依赖和资源放置，项目 broker 只负责“什么已经 ready、能否继续放行”。
ready bytes 在 CPU lease 发出前预留，因此多个 prepare 同时完成也不会越界。

CLI 默认仍是 `direct_dependency`；`--project-execution-mode hse_static` 只是一个显式项目方法
候选。当前单元/fake-Ray 测试证明状态和内存账本，不证明 GPU 性能更高。

## 5. 之前的路径迁移只改变了什么

这次迁移只改变文件归属和 import 路径，没有改变算法、默认参数、CLI 参数或 CSV
schema。旧的 6 个 `profile_*` 和 11 个 scheduling 兼容壳已删除，避免同一个实现有
两个入口。`tests/architecture/test_architecture_boundaries.py` 用 AST 检查跨层 import，
并阻止旧路径重新出现。

## 6. 大文件现在如何拆分

`observability/metrics/` 已按 timing、CSV、statistics、resources、vLLM 指标拆分；
`serving/backends/` 已按公共合同、embedding、completion 拆分；
`experiments/shared_vllm/` 已按 config、runtime、evidence、metrics、runner 拆分。三个包的
`__init__.py` 继续导出原来的公共 API，因此调用方不需要知道内部文件位置。
SAOR 的在线 selector 与离线数学验证也分开：`scheduling/submission_control/saor.py` 只根据
当前 debt、活动集和在途 work 决定下一条请求；
`experiments/shared_vllm/saor_projection_evidence.py` 不调用 selector，而是从落盘的原始
event 字段重新计算同一决策。这样即使在线公式写错，验证器仍能用独立手段把 rehearsal 判失败，
不会出现“实现和测试共同相信同一个错误结果”。

文本 work 也分 raw observation 与 effective admission 两层。request evidence 永久保存 tokenizer
对原始 prompt 的计数；chat template 的服务侧固定开销由
`analysis/audit_chat_prompt_overhead.py` 从 request/submission join 独立校准。调度器只消费
`raw prompt + calibrated protocol/template overhead + output estimate`，不能把模型特定的 29
写进 selector，也不能把加过 overhead 的值伪装成原始 prompt evidence。这使估计上界既能覆盖
真实服务 work，又保留跨模型重新校准所需的原始数据。

`scripts/` 已按 data/services/baselines/profiling/experiments/analysis 分组，`tests/`
按生产域镜像。数百条当前复现命令已同步迁移；已执行实验的 raw manifest 不改写，因为
其中的旧路径属于证据的一部分。`data/materializers/text.py` 等剩余大文件后续仍按一次
一个职责处理。

## 7. 如何判断迁移是否正确

- 静态门禁：源码可编译，旧 import 搜索结果为空，AST boundary test 通过；目录化测试
  使用 `python -m unittest discover -s code/tests -t code -p 'test_*.py'`；
- 本地行为：不需要 GPU 的单元测试保持通过；缺 Daft/psycopg 或 macOS Ray 权限要和
  真正代码失败分开报告；
- 远端行为：服务器恢复后，用文本 64 行 baseline gate、图像 256 行 resource/correctness
  gate 核对 CLI、summary schema、exactly-once 和 digest；
- 只有远端 gate 与本地全量依赖测试都通过，才合并到 main，正式性能实验随后单独运行。
