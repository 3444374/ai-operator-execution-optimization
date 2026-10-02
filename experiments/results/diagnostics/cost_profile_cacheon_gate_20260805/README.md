# 双 4090 cost-profile cache-on 提交后门禁（2026-08-05）

本门禁验证 cache-on 主合同能否在已提交的 `main` 上通过真实
PostgreSQL → Daft → shared Ray → 双 vLLM 链路。它只验证运行与观测语义，不评价
active-work 候选性能。

## 1. 实验设置

- Git：`2b7da6cd443ed581865824ac4ca7f73e4755e91b`；
- 服务器：32 CPU、240 GiB RAM、2×RTX 4090；
- 服务：Qwen2.5-7B，vLLM 0.25.1，两个 endpoint 均带
  `--enable-prefix-caching`，共享 Ray 为 32 CPU / 2 GPU；
- 数据：PostgreSQL `sharegpt_multiturn`，512 行，输出上限 256；
- 配置来源：从已提交的 `dual_gpu_cost_profile_pilot.example.json` 只截取
  `work32768`，不改变 common args 或 service metadata。

## 2. 实验设计

单候选 `max-active-work-per-endpoint=32768`，1 warmup + 1 formal。该最小矩阵用于检查
端到端可运行、cache 三重声明一致、逐请求 exactly-once、双 endpoint 激活和共享 Ray，
不用于估计 CV、CI、最优 active-work 或 CE0–CE6 精度。

## 3. 严谨性自检

- manifest：2/2 completed、0 incident；
- 两个 run 均 512 request events、512 unique `doc_id`、512 completed；
- 两个 run 均同时使用 `endpoint-0` 和 `endpoint-1`；
- CSV 的 `service_prefix_caching=enabled`，live 进程也启用 prefix cache；
- stdout/stderr 中 `Started a local Ray instance` 计数为 0；
- `vllm_metrics_status=ok`，cache hits/queries 合法且非零。

首次尝试在首个 run 前因 PostgreSQL 5432 未启动而失败；失败目录保留在服务器
`cost_profile_cacheon_gate_20260805_0936`，未 resume。启动 PostgreSQL 后使用全新目录
完成本门禁。

## 4. 实验数据

| phase | rows | cache queries | cache hits | hit rate | request trace | endpoints |
|---|---:|---:|---:|---:|---:|---|
| warmup | 512 | 282,504 | 97,488 | 34.51% | 512/512 | 0,1 |
| formal | 512 | 282,695 | 95,248 | 33.69% | 512/512 | 0,1 |

紧凑数据见 [`summary.csv`](summary.csv)。服务器原始文件哈希：

- `runs.csv`: `16bce77f35a20c6fb7b5ce58d4c926c8a8ba2919091c4a07a20876f6fdc3d5a5`
- `manifest.json`: `89ace0a10e6e85e5d828595aaebf9634d219ddcecfa1770aba7f6ec698eaec26`

## 5. 结果解释

### 实验事实

cache-on 不只是进程开关：两个 run 都产生了非零 cache query/hit delta，hit rate 约
33.7%–34.5%。新 CSV 字段、live 服务检查和 manifest 声明一致；共享 Ray 与双 endpoint
链路均可运行。

### 合理推断

正式 320-run 可以沿用 cache-on 合同进入下一阶段门禁，但仍需单 runner、共享 Ray、
服务空闲和完整 80-cell 审计。此处不能推断 cache 带来多少加速，因为没有 matched
cache-off 对照。

### 不能声称

- 不能声称 32,768 是最优 active-work；
- 不能用一轮 formal 报性能差异、稳定性或 estimator 精度；
- 不能把本轮 hit rate 当作同一行 pre-execution cost-model 特征。

## 6. 对课题的含义

正式 vLLM 性能主轨现在与真实部署一致采用 cache-on，并且缓存状态在 CSV、manifest 和
live 进程之间可审计。cache-off 仍可作为机制消融，但不能与主轨样本静默混合。

## 7. 下一步

由远端 agent 按 formal 计划运行单一 320-run；启动前再次检查 PostgreSQL、两个 cache-on
endpoint、共享 Ray 和 host-scope lease。完成后先审计独立性与 cache counters，再进行
formal-only context-LOO；任何门禁失败都不生成 CE 排名。
