# Learning Notes

代码结构导读：

- [`code_architecture_guide.md`](code_architecture_guide.md)：解释公共执行阶段与
  text/image 模态为什么是两条正交轴、每个 `src` 子包负责什么，以及路径迁移的验证门禁。

文本 baseline 的当前入门材料：

- [`text_native_baseline_guide.md`](text_native_baseline_guide.md)：区分 vLLM 服务上限、
  bounded control、Daft/Ray Data/OceanBase 原生 baseline 和项目方法，并解释
  Chat/Completions 分轨与 64→512→4096 复测流程。

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
