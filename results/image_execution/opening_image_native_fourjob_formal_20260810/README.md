# 图像原生 single→four-job 正式观察（2026-08-10）

> 结论先行：服务器重启后按 cold-start 合同恢复 PostgreSQL/Ray，Daft built-in 与
> Ray Data native 图像矩阵 40/40 runs 全部通过。四 Job 下，Daft built-in 的 short
> JCT 相对自身 single 仅 +1.76%，但三个 long 分别 +214.92%/+218.77%/+112.73%；
> Ray Data 的 short +9.56%，三个 long +31.96%/+6.05%/+64.10%。这证明同一图像
> workload 的多 Job 干扰具有强烈的 Job/执行图依赖性，但不证明 Project 或 state-aware
> 方法胜出，也不允许用两条原生路径的绝对 JCT 作框架排名。

## 1. 实验目的

用与后续 Project static/proposed 完全相同的 immutable 图像 Job manifest，先建立由被测
系统拥有执行与调度的原生 single→four-job 观察。问题是：一个 2K short 在 `t=0` 到达、
三个 3K long 在 `t=0.5s` 到达时，Daft built-in `embed_image` 与 Ray Data native staged
graph 的各 Job JCT、组完成率和资源状态怎样变化。

本实验只回答原生轨内部的干扰现象。Project staged descriptor/observe-only snapshot、
frozen static/proposed、DuckDB 与 VTC-compatible 均不在本矩阵中。

## 2. 实验设置

| 项 | 冻结合同 |
|---|---|
| 平台 | AutoDL，2×RTX 4090；Ray 2.56.1、Daft 0.7.21、PyTorch 2.12.1+cu130 |
| 数据 | PostgreSQL `coco_train2017_60k`；short 2,000 行，long1/2/3 各 3,000 行；四段 doc-id 互斥 |
| 输入规模 | short 332,487,313 encoded bytes；long1/2/3 为 495,813,366 / 491,442,197 / 486,296,498 bytes |
| 到达 | short 0 s；三个 long 0.5 s；runner 校验真实 overlap |
| 模型 | 本地 CLIP ViT-B/32，float16，L2-normalized embedding；不写回 sink |
| 原生臂 | Daft built-in `embed_image`；Ray Data native CPU preprocess + GPU actor graph |
| 重复 | 每系统 4 个 single + 1 个 four-job；每臂 1 warm-up + 3 formal，共 40 runs |
| 版本 | repository commit `c4fa45508b2de0fbcb4ee372eb899f47a96bc745` |

运行入口：

```bash
PYTHONPATH=code python code/scripts/experiments/run_image_native_multijob.py run \
  --config deploy/autodl/opening_image_native_fourjob.example.json
```

完整服务器证据位于
`/root/autodl-tmp/experiment-artifacts/opening_image_native_fourjob_formal_20260810/`；机器
preflight 位于 `/root/autodl-tmp/experiment-artifacts/preflight_20260810/remote.json`。

## 3. 严谨性自检

- matrix `passed`，40/40 runs passed；10 warm-up + 30 formal group。
- 48 条 formal Job 记录全部 exactly-once；累计读取 132,000 行。
- 两系统各 15 个 formal group，其中 four-job 各 3 次；manifest SHA256 均为
  `fd8cff32…a64765`。
- short 与三个 late Job 的 overlap 在 6 个 four-job formal group 中全部为正。
- formal start lateness 均约 0.08 ms，排除 barrier 起跑偏差。
- Daft four-job group JCT CV 为 2.01%；Ray Data 为 8.87%。但部分逐 Job CV 较高：
  Daft long2 JCT CV 37.76%，Ray Data single long1/2 为 17.96%/19.96%，故逐 Job 差异必须
  同时报告重复稳定性，不能只看均值。
- 原生 adapter 没有统一 batch preprocess/H2D/forward 分位，相关字段为空；本报告不补造
  阶段计时，也不把 `source_next_total_s=0` 解释为零 source 开销。

## 4. 实验设计

每条原生执行图先对 short/long1/long2/long3 分别运行 single-full，再运行同四份输入的
并发组。slowdown 定义为 `JCT_four / JCT_single - 1`，只在同一系统、同一 Job、相同完整
结果语义内计算。两系统的调度 owner、物化边界与 first-output 语义不同，因此不计算
Daft/Ray Data 加速比。

## 5. 实验数据

### 5.1 逐 Job single→four-job（formal mean，n=3）

| 系统 | Job | single JCT | four-job JCT | slowdown | four-job CV |
|---|---|---:|---:|---:|---:|
| Daft built-in | short | 19.00 s | 19.34 s | +1.76% | 3.47% |
|  | long1 | 23.13 s | 72.83 s | +214.92% | 15.83% |
|  | long2 | 22.79 s | 72.66 s | +218.77% | 37.76% |
|  | long3 | 23.35 s | 49.68 s | +112.73% | 23.48% |
| Ray Data staged | short | 16.60 s | 18.19 s | +9.56% | 14.81% |
|  | long1 | 22.53 s | 29.72 s | +31.96% | 8.73% |
|  | long2 | 21.38 s | 22.67 s | +6.05% | 16.54% |
|  | long3 | 21.22 s | 34.83 s | +64.10% | 9.60% |

### 5.2 组级状态（four-job formal mean，n=3）

| 系统 | group JCT | images/s | JCT CV | GPU util mean | estimated E2E MFU | GPU energy | CPU busy cores |
|---|---:|---:|---:|---:|---:|---:|---:|
| Daft built-in | 90.03 s | 122.22 | 2.01% | 2.65% | 0.00646 | 8.75 kJ | 10.48 |
| Ray Data staged | 36.80 s | 300.44 | 8.87% | 1.87% | 0.01588 | 4.09 kJ | 12.72 |

GPU util 与 MFU 都很低，和此前“图像链路受 CPU prepare/数据供给约束”的画像一致；它们
不是 feeding-saturated GPU 容量结果。组级绝对 JCT/images/s/energy 只描述各路径当前执行
图，不作跨框架性能排名。完整紧凑数据见 [`data/group_summary.csv`](data/group_summary.csv)、
[`data/job_summary.csv`](data/job_summary.csv) 和
[`data/slowdown_summary.csv`](data/slowdown_summary.csv)。

质量门只覆盖 source identity、encoded-byte digest、embedding 输出合同与 exactly-once；
本矩阵无 PostgreSQL/pgvector sink 和检索闭环，因此 recall@k/nDCG@10、写回时间和
`$/M images` 为 N/A。

## 6. 结果解释与课题含义

**事实**：两条原生路径都出现非均匀的 Job slowdown。Daft short 基本保持 single JCT，
三个 late long 明显延后；Ray Data long2 接近 single，而 long3 退化 64.10%。相同 Job 数、
相同 encoded-byte 量级和相同到达 offset 并不能推出相同服务份额。

**推断**：调度状态必须至少区分每 Job 的 ready/active/remaining work、first output、完成率
和执行阶段；只看总 GPU util、总 images/s 或 Job 数会隐藏 long 间分配差异。这直接支持
staged descriptor + observe-only per-job snapshot 的必要性，并给后续 Project
static/proposed 提供不可变 workload 与原生现象参照。

**不能声称**：不能说 Daft 或 Ray Data 的内部调度算法导致某个具体完成顺序；不能说
Ray Data 比 Daft 快；不能说 Project state-aware、shared credit 或 VTC 已解决图像干扰；
不能从一个 offset、一个 Job shape 外推 weighted priority、长先短后或其它图像数据集。

## 7. 下一步

1. 保持本 manifest/SHA、资源与 1+3 合同不变，运行 Project single-full、frozen static 与
   observe-only proposed-role；snapshot 仍不驱动决策，先验收构建开销、freshness 与
   legacy-equivalent credit。
2. 只有 observe-only 门通过，才启用 stage-aware controller，并与同上限 frozen static
   做独立 A/B；不以本原生表替代 Project 因果对照。
3. 图像 Project 完成后用 `summarize_image_multijob.py` 生成 combined 证据；DuckDB 与
   VTC-compatible 继续串行，避免共享 GPU/服务污染。
