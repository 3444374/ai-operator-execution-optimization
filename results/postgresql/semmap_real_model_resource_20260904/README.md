# PostgreSQL 生成型 SemMap：真实模型通过与资源资格失败

日期：2026-09-04

历史采集源码已从工作树退役；[退役源码索引](../retired_sources.json)保存已推送 `d93e3f9b` 的源码
URL、Git blob 和 SHA-256，可恢复原始字节。`raw/SHA256SUMS` 保留采集时的原始清单：其中四个
源码条目从固定 Git 版本校验，其余采样和失败证据仍在本目录。下文保留当次结论，不作为当前运行入口。

源码：`main@b19486a1a475ff8dc867c4533c7ab95c0d8dc15b`

结论：真实模型纵向链路通过；预先规定的资源条件未通过，四 D 尚未全部完成。

## 验证对象

本轮使用独立 PostgreSQL 18.3 prefix 重新以 `-O2 -Werror` 构建 `semloom_pg`，扩展安装件与源码构建件
SHA-256 一致。固定服务为 Qwen/Qwen2.5-7B-Instruct revision
`a09a35458c702b33eeacc393d103063234e8bc28`、vLLM 0.25.1、Torch 2.11.0+cu130、单张 RTX 4090、
BF16、4096-token context、`max_num_seqs=4`、`max_num_batched_tokens=4096`、eager、关闭 prefix cache，
并使用 vLLM generation defaults。模型和 tokenizer 只从已有本地文件读取，没有下载、换模型、改精度或重试。

真实请求使用持久化的 32 次上限账本。每次 HTTP POST 在发送前预留序号，失败、取消和结果未知均不退款。
运行结束时为 25/32；SQL NULL 没有创建请求。

## 真实链路结果

PG18.3 → wire v5 → UDS gateway → vLLM → PG 的生成型 Map 通过：

- 首个 warmup 在 PG 中得到模型原始输出 `warmup`，model identity、finish reason 和 usage 均通过验证；
- SELECT 覆盖 ASCII、多行、引号/反斜杠、Unicode、字面 `TRUE/FALSE/UNKNOWN/NULL`、花括号、非 NULL
  空字符串和 SQL NULL。11 行保持关联，其中 10 个非 NULL task、NULL 输出为 SQL NULL；
- INSERT ... SELECT 写入 11 行，`EXPLAIN ANALYZE` 报告 model calls/accepted/emitted 为 10/10/10，
  prompt/output usage 为 390/40，与逐响应记录一致；
- 500 ms statement timeout 得到 SQLSTATE `57014`。对应模型请求已经发出并在外部以 128 tokens、
  `finish_reason=length` 完成，但取消语句没有接收该结果；下一次查询成功；
- 108,000-byte 输入超过 4096-token 服务上下文，vLLM 以 4xx 拒绝。gateway 返回
  `MODEL_REQUEST_REJECTED`，PG 映射为 SQLSTATE `38000` 和固定脱敏消息；下一次查询成功；
- 25 个请求包含 24 个模型 completion 和 1 个预期 request rejection。24 个 completion 的模型身份均匹配；
  其中 23 个 `stop`，另 1 个是已被 PG 取消的 `length` completion；
- gateway 在真实运行前后 FD 均为 4、线程均为 1，结束 RSS 比预热基线增加 1,544,192 bytes；
  vLLM 的进程身份和启动参数前后逐字一致。测试后按 PID/start-time 发送 SIGTERM，主/子进程、端口和
  GPU 显存均已释放。

前几次未消耗或部分消耗的失败也保留：无表 warmup 和带 `ORDER BY` 的未支持查询形状均由 PG 在 HTTP 前
拒绝；实验账本正则错误在 HTTP 前失败；首个真实 warmup 后，采集器因 Transformers 5.14 未指定
`return_dict=false` 而误计 token。离线复核后，驱动与 vLLM 环境使用相同 chat-template SHA-256，均计算
38 prompt tokens，与服务报告一致。成功运行复用该 warmup 证据，没有重复请求。

## 资源运行结果

fixture-only 主压力没有调用模型，功能传输完成：

- 同一个预热 gateway 处理 4 个 session：1 个 warmup task，加 3 轮各 2,000 个非 NULL task；
- 每个输入为 100,000 bytes，每个输出为 65,536 bytes；
- C 客户端使用 `PQsetSingleRowMode()`，逐行核对完整输出后立即 `PQclear()`；三轮均得到 2,000 行；
- gateway 记录 6,001 个 task、4 个 session start 和 4 个 session end，全部 task digest 相同；
- 模型账本保持 25/32。

但是，运行在 60 秒恢复窗口结束后触发了固定资源断言。预先规定的条件是：gateway peak/end RSS 增量
不超过 32/16 MiB，PostgreSQL backend peak/end RSS 增量不超过 16/8 MiB，backend 与 gateway 的 UDS FD
合计 peak/end 增量不超过 2/0。

本轮同步已补齐失败采样：`raw/semmap_res2/stress/measurements-aggregate.json` 收录全部 93 个
attempt 的 violations/baseline/peaks/ending/tasks/sessions 与 samples 计数；attempt 1/47/93 三个
代表文件以散件保留（每文件含完整 1,534 条 samples 时间序列）。`raw/semmap_res2/` 下完整
summary/log 已落盘。采样显示 93 次均记录
`metric=uds_peak_delta, extra_metric=fd, observed=3, limit=2`，即 `gateway+backend.uds_peak_delta`
超过阈值，因此当前仍不能判定为通过。全部 93 个原始 attempt 文件（tar SHA-256 `cffd58c9…`）
与完整精选证据包（bundle SHA-256 `c1cd4218…`）保存在仓库外服务器 artifact
`semmap_4d_b19486a1_20260904`。

按照停止条件，本轮没有重跑资源压力，也没有继续执行后置的 fixture 取消、provider 断连和 gateway
退出/恢复子项。失败后 client、gateway 和 PG18.3 集群均已停止，端口和 UDS listener 已释放。

### 指标实现口径的事后审计（2026-09-04 补记，不改变本次 verdict）

本次运行按实际执行的 metric schema v1 判定失败。后续审计确认 v1 的 `uds_peak_delta`
实际采集的是 backend 与 gateway 的**进程总 FD** 峰值增量，与 `uds_peak_delta` 名称和冻结合同
所称的 provider UDS FD 不是同一对象；93 个 attempt 均为对同一不可逆峰值的重复判定。
诊断运行（metric schema v2，`experiments/plans/postgresql_semmap_generation_contract.md` §8.4.2）
进一步把 v1 观察到的 +3 分解为：backend 新增 provider UDS client socket ×1（connected 端无绑定路径，
不在 `/proc/net/unix`）、backend `anon_inode:[eventpoll]` ×1（PostgreSQL WaitEventSet 正常瞬态）、
gateway accepted provider 会话 ×1，三类均于查询结束后释放。原始失败结果和全部采样继续保留；
本次运行证明 3×2,000 fixture task 功能完成及结束态 FD 回到基线，但不能据此判定 intended
UDS resource gate 通过。后续以预登记的 metric schema v2/v2.1 进行独立重跑，不追溯修改本次 verdict。

## 结论边界与下一步

本轮可以声称：受限三参数生成型 Map 已在 PostgreSQL 18.3 中通过真实 Qwen2.5-7B 固定服务完成 SELECT、
INSERT、NULL/空串、取消、模型拒绝和恢复的纵向执行。不能声称模型生成质量、性能、资源资格、多算子、
有界多会话或四 D 整体已经通过。

下一次资源运行前须先修复实验 runner：无论通过或失败，都要在检查阈值前落盘原始采样、逐项 peak/end
差值和失败项；保持同一 workload、阈值和 PG18.3/fixture 身份。没有新的运行授权时不自动重跑。
资源条件与取消/断连/exit 子项通过后，才可结束四 D 并进入可组合执行。

公开摘要见 [`raw/qualification.json`](raw/qualification.json)。完整选定证据保存在仓库外 artifact
`semmap_4d_b19486a1_20260904`；下载包 SHA-256 为
`c1cd42182f16b6f607909128b8eb39cbd660e20014b31433c37c1d4c42f78a30`。公开文件不包含服务器地址、
账号、凭据、绝对运行路径或模型 payload。
