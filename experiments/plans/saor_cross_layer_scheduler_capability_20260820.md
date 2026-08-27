# SAOR 与 DRR/VTC-on-vLLM 跨层比较能力合同

日期：2026-08-20
状态：`blocked / no-formal-authorization`（已完成设计、tag 审计、服务器 installed-source
只读取样、骨架、纯逻辑测试与 schema；尚未用新 CLI 生成 exact-SHA evidence，未运行
capability/GPU）。2026-08-27 仍不是当前执行项；先完成 PostgreSQL 中立语义算子与 provider 生命周期资格验证。

## 1. 研究问题与证据边界

本能力组回答：在同一数据库 AI workload 和物理资源下，把公平调度放在 Daft/Ray 上游提交层
（SAOR），与放在 vLLM 模型服务层（DRR/VTC reproduction），完整系统的经验表现有何差异。

它是五臂 database-E2E 主矩阵之外的独立四臂组，不删除或改写五臂、官方 S-LoRA VTC capability
和历史 Project 内部消融。允许的结论只有“上游 SAOR 与引擎内 DRR/VTC reproduction 的完整系统
经验差异”；禁止写成同层 selector 的胜负。

| headline arm | 数据执行层 | 模型服务层 |
|---|---|---|
| Daft Native/Ray + vLLM native FCFS | Daft `prompt()` Ray 原生执行 | vLLM native FCFS |
| Daft Native/Ray + DRR-on-vLLM reproduction | 与上一臂完全相同 | 项目复现 DRR，`--scheduler-cls` |
| Daft Native/Ray + VTC-on-vLLM reproduction | 与上一臂完全相同 | 项目复现 VTC，`--scheduler-cls` |
| SAOR + vLLM native FCFS | Daft + typed Ray actor + SAOR | vLLM native FCFS |

前三臂禁止 bounded-ready、Project K/W、shared credit、debt/recovery、上游状态感知和 Project
request reordering。SAOR 保留完整上游机制，但其服务层仍是 native FCFS。

## 2. vLLM 0.25.1 源码接口审计

审计分成两层，禁止把 tag-level 阅读冒充服务器安装证据：

1. **官方 tag-level 已审计**：`v0.25.1/vllm/config/scheduler.py` 的内置 policy 只有 `fcfs` 与
   `priority`，同时提供 `scheduler_cls`；该配置明确把自定义 scheduler 标为非公共接口。自定义类若
   不是该版本 `AsyncScheduler` 的子类，会使 async scheduling 退化。因此 skeleton 必须继承
   `vllm.v1.core.sched.async_scheduler.AsyncScheduler`。
2. **实际安装源码已只读取样、正式 evidence 待补**：2026-08-21 在冻结服务器 vLLM venv
   只读确认版本为 0.25.1，并读取五个关键源文件与 dist-info 的 SHA-256；这些期望值已写入五臂
   `service_identity`。新 `audit_vllm_0251_source.py` 只有在 marker、版本、distribution 和逐文件
   SHA 全部精确相等时才返回 `passed`，缺 expected SHA 时保持 blocked。canonical `main` 尚未包含
   该 CLI，且服务未启动，因此仍须合入后生成并归档 CLI evidence，不能把本次终端取样冒充完整门禁。

`v0.25.1/vllm/v1/request.py` 暴露 `request_id`、`client_index` 与 trace headers；其中
`client_index` 服务于 frontend scaling 的输出路由，不作为数据库 Job 身份。scheduler loop 同时拥有
waiting queue、chunked prefill、KV allocation、preemption 与 continuous batching 状态。未核对实际
安装 SHA 前，不猜测私有 hook，也不复制整个 `schedule()`。

源码入口：

- <https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/config/scheduler.py>
- <https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/v1/core/sched/scheduler.py>
- <https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/v1/core/sched/request_queue.py>
- <https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/v1/request.py>

当前 scheduler module SHA-256 为
`4580b9e1092536dc7640aabad9b0167d916188fd087ef4b2953f355580925cc1`。配置加载时重新计算并
拒绝漂移。

## 3. scheduler skeleton 与 FCFS parity 门

`vllm_scheduler_plugin.py` 暴露三个固定 class path：

- `CustomFCFSScheduler`：只继承 frozen `AsyncScheduler` 并调用原实现，用于 capability parity；
- `DRRScheduler`：名称固定为 **DRR-on-vLLM reproduction**；
- `VTCScheduler`：名称固定为 **VTC-on-vLLM reproduction**。

DRR/VTC skeleton 当前主动抛出 blocked，而不是用 FCFS 冒充算法实现。只有以下证据全部 passed 后，
才允许接私有 vLLM adapter：native/custom FCFS 请求顺序、chunked prefill、prefix cache、KV
allocation、preemption、async scheduling、吞吐非劣与 module SHA。门槛不因结果不利而修改。

## 4. Job identity 合同

目标链为：

```text
PostgreSQL job_id
  -> Daft/Ray request
  -> X-Request-Id
  -> vLLM Request.request_id
  -> strict client_id decoder
```

typed identity 格式为 `saor-xlayer.v1/jobN/<32 lowercase hex unique token>`。缺失、非法、重复均
fail closed；没有 default client，因此不能把全部请求静默折叠到一个队列。

当前 Daft 安装源码缺失，无法证明 `OpenAIProvider`/`prompt()` 支持逐行 header，身份能力状态保持
`blocked_unverified_daft_transport`。若冻结 Daft 确认不能逐请求透传，最小 adapter 是：每个 Job
一个无状态 pass-through listener，只注入唯一 typed `X-Request-Id`，不得缓存、排队、限流、重排或
重路由；并且四臂统一使用。实施该 adapter 前等待独立确认，不重写 Daft scheduler。

## 5. 纯算法语义

### DRR

每个 client 一个 FIFO 与 deficit；backlogged client 每轮增加固定 quantum，队首
`prompt_tokens + frozen_output_cap` 不超过 deficit 时派发并扣除同一 estimated work。deficit 在
持续 backlogged 的轮次间保留，idle 后按标准 DRR 清零。完成后的真实输出不会追溯修改历史决定；
自然 EOS 只登记为未来 estimated-output variant。该实现没有 SAOR debt/recovery 或共享 credit。

### VTC

每个 client 一个 virtual counter。新 client 或重新活跃 client 抬升到当前 active set 的最小
normalized counter；选择 counter 最小的 backlogged client并作确定性 tie-break。prompt service
在 dispatch 时按实际 token 计入，output service 随生成可增量计入或在 completion 补齐；不预测
未来真实输出。只要有 backlog，`pop_next()` 必须返回请求。

纯单测覆盖 active/inactive lift、overload、weighted proportional service、equal-weight
service-difference synthetic bound、work conservation、实际输出计费、确定性 tie，以及缺失/非法/
重复身份反例。这里验证的是算法 oracle，不代表已保持 vLLM continuous-batching 语义。

官方 `Ying1123/VTC-artifact@192c2e...` 继续作为 counter/lift/synthetic suite 的语义参考；它不
进入四臂 headline performance ranking，也不被本 reproduction 替代。

## 6. 完整比较与证据 schema

四臂共同冻结 PostgreSQL source/sink、两份 Job manifest、`job0@0s/job1@5s`、模型、tokenizer、
chat template、vLLM 0.25.1 installed source/build、output cap、temperature、`ignore_eos`、GPU、
endpoint/KV、计时边界与 exactly-once。前三臂另要求完全相同的 Daft/Ray 数据路径，只改变 service
scheduler。

必报指标：database-E2E throughput、group/per-Job JCT、P50/P95/P99/SLO、service lag、maximum
no-service interval、starvation/work conservation、GPU/vLLM/energy、per-client accumulated
service、correctness/exactly-once。

配置入口是 `deploy/autodl/saor_cross_layer_scheduler_capability.example.json`；typed loader 会检查
四臂身份、禁止控制项、class path、module SHA、共同指标和 claim boundary。未来 evidence builder
要求四臂共同 contract SHA、scheduler owner、service scheduler、identity/parity proof 和完整指标；
当前 capability blocked 且 formal 未授权，因此拒绝发布 performance report。

## 7. 当前门禁与下一步

当前三项 blocker：

1. 在冻结 vLLM 0.25.1 服务器环境运行 installed-source SHA audit；
2. 审计 Daft/Ray 到 `Request.request_id` 的逐请求 identity-only 链；
3. 先运行 custom FCFS capability，完成八项 parity，再决定是否实现私有-loop DRR/VTC adapter。

在此之前，不连接服务器、不启动 vLLM/GPU、不跑 rehearsal/formal、不改 effect-size 门，也不合并
`main`。formal 仍需独立授权 artifact。
