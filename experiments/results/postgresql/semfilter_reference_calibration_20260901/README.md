# exact SemFilter 首轮真实 reference calibration：输出格式失败

本报告是内部实验记录。对应两项研究内容的共同支撑——语义算子代价估计；不比较调度策略。
来源为 PostgreSQL 18.3 / Qwen2.5-1.5B-Instruct 的本次真实观测，不是 golden fixture。

结论：预热完成，但第一个 training 查询收到非法模型输出后终止。没有完整 training observation，
held-out 未运行，没有拟合、没有校准 artifact，也没有执行 planner artifact 加载验收。
`dcde2be5` 的机制资格仍成立；本次不能把真实成本校准标成完成。

## 1. 目的与设置

检验当前固定 reference 模型、语义和 workload 能否产生可独立验证的成本校准数据。
唯一实施来源为[架构计划工作包五](../../../plans/postgresql_ai_semantic_operator_architecture_20260827.md)，
首次请求前的采集合同提交为 `2feab7d4`、`7042132e`；后者只修正等长 cell 导致固定项与 calls 必然
共线的设计问题，没有根据模型观测改变条件。运行源码为 `7042132e`，生产实现与 `dcde2be5` 相同。

- PostgreSQL 18.3，独立 socket-only 集群；本次重新 `-Werror` 构建，系统默认 18.4 未使用或改动。
- 现有 Qwen2.5-1.5B-Instruct、vLLM 0.25.1、BF16、TP=1、单 RTX 4090，单 localhost endpoint。
  FCFS、eager、prefix cache 关闭，context/token budget 4096、max sequences 1、GPU memory utilization 0.25。
- instruction：`The input asks for writing, explaining, or debugging computer code.`
  原有 prompt program/parser 不变；temperature=0、top_p=1、max_tokens=8、n=1、stream=false、stop 为换行。
  不加 constrained decoding，不 trim/重写输出，不 retry。
- ShareGPT Vicuna unfiltered 的完整首个人类 turn；非空、无 NUL、UTF-8 ≤4096 bytes，conversation 和
  payload 去重后按预注册 SHA 排序。64 条 warm-up、768 条 training、384 条 held-out，三组互斥。
- training cell rows 为 32/48/64/80/96/112/144/192，held-out 为 64/80/112/128；每 cell 计划三重复，
  seed=20260901 打乱顺序。原始文本不进入 Git，公开 manifest 保存输入/会话摘要和分组。
- 预先要求五项最大 held-out 相对误差 ≤0.20；公式为 `abs(predicted-actual)/max(abs(actual),1)`。
  本次未进入误差评价，不能把未测误差记作 0。

模型/config/tokenizer/chat-template 文件 SHA 位于 [model_files.json](raw/model_files.json)；源数据 SHA、
分组与运行配置见 [workload manifest](raw/workload_manifest.json)、[execution identity](raw/execution_identity.json)。
本次以实际文件哈希固定已有模型资产，没有独立确认其 Hugging Face revision commit，该字段不可声称已核验。
workload/service signature 由外部采集工具提供；工具核对实际文件、PID/start-time、cmdline、版本与 GPU/driver，
不是 PostgreSQL 自动探测现场环境。

## 2. 合规自检与计时

core/text/text-qwen15b/workload-text preflight 为 `ok`。模型和数据均使用现有资产，没有下载、安装或改动服务实现。
SQL 由 PostgreSQL planner/executor 执行；observer 只包围现有 fixed adapter 的 `complete()`，原请求与结果原样
透传，未复制 wire/session loop 或改变 provider seam。

逐请求时长用 monotonic clock 测量，包括 fixed adapter 的 HTTP 编码、DNS/连接、发送、服务等待、接收与解析，
不含 PostgreSQL child/prompt/parser、UDS 或 observer 落盘。cell 的 `service_milliseconds` 是成功调用时长之和，
**不是纯 GPU compute，也不是 SQL 总时长**；完整 warm-up 的 SQL EXPLAIN 原始输出另存。
没有 baseline、消融或优化臂，因此没有性能排名、吞吐/能耗/公平性结论。

## 3. 全部已运行数据

| 阶段 | 计划输入 | 已收到模型响应 | raw output | prompt/output tokens | 请求时长合计 |
|---|---:|---:|---|---:|---:|
| warmup-c0-r0 | 64 | 64 | TRUE 1、UNKNOWN 63 | 9671 / 128 | 3168.918231 ms |
| training-c2-r0，首个 measured cell | 64 | 23 | UNKNOWN 22、INVALID 1 | 3212 / 46 | 1171.859143 ms |

warm-up 查询完整完成，PostgreSQL 的 semantic input、model calls、usage、最终 1 行输出与 observer 一致。
此处的一致性指 **actual** 计数：该查询 planner 预计 input/calls 为 8/8，实际为 64/64；预计 prompt/output
tokens 为 1144/64，实际为 9671/128，预计输出 3 行、实际 1 行。这些未校准估计不能被隐藏。
`split` 和 `cell` 在采集表中有关联，普通谓词基数估计也需单独核对；本轮未将差异归因为 semantic
planner 错误，更没有用真实 input rows 条件下的离线预测冒充 PostgreSQL 总体预测精度。
training 行是**失败查询的已观测前缀**；其输入/输出行数估计不能充当完整 observation，不能用于拟合。
它没有完整 EXPLAIN ANALYZE，不能根据前 22 个有效模型结果声称 SQL 查询成功或输出行数为 0。

第 23 个响应为 3-byte、2 output-token 的非法内容，finish reason 为 `stop`；原模型内容不公开，只保留
长度、SHA 和类型。PostgreSQL 报 `22000`，精确消息为：

```text
SemFilter model completion must be TRUE, FALSE, or UNKNOWN
```

全部 87 条响应见 [逐请求 raw](raw/runs/)，逐次汇总和停止状态见
[held_out_report.json](raw/held_out_report.json)，错误见 [collect-failure.json](raw/collect-failure.json)。
本次 observed prefix 的 output usage 都是每调用 2 tokens；这提示后续需要检查 calls/output 共线性，
但没有完整 training 数据，本次未做 rank 检验或系数拟合，也不据此推断未运行数据的 rank。

## 4. 恢复、测试与证据归档

失败后重新逐字节核对全部 1216 行数据库输入，源文件、模型文件和服务身份均未变化；fresh connection
`SELECT 1` 成功，gateway socket 已移除。[事后核验](raw/post_failure_audit.json)不等于同一 backend 的
savepoint 测试；后者仍由既有 TAP 支持。

本次重新运行 PostgreSQL Python 合同 45/45、gateway migration 5/5、calibration contract 5/5，以及
采集脚本合成数据检查 6/6；[日志](raw/logs/)和[资格摘要](raw/qualification.json)已保存。
没有重新运行完整 PGXS regression/TAP，历史 1/1、437/437 仍绑定 `dcde2be5`，不冒充本次结果。

仓库外持久化证据包为 `postgresql_semfilter_real_calibration_777f0382_20260901_r1`，保存完整采集脚本、
私有输入、原始服务/数据库日志、源/模型身份、失败记录、二进制和停止后的 PGDATA；公开子集在本目录
`raw/`，其 [SHA256SUMS](raw/SHA256SUMS) 与私有主清单分别验证。本次 endpoint、gateway 和 PostgreSQL
均已停止；清理只针对本次资源，停止后的 PGDATA 保留用于审计。
构建器输出的一处行尾空白和 initdb 输出的末尾空行按原字节保留；本目录 `.gitattributes` 只对这两个
日志关闭相应空白提示，不修改证据 SHA，也不放宽源码或其他文件检查。

raw 中的 Python 文件是本次一次性 observer/audit 的精确快照，不是新增生产 CLI 或另一份实施计划。
复现需先按 runtime runbook 准备仓库外目录，再按相同 source/model SHA 和 preregistration 建立独立集群，
依次运行 `collect.py prepare`、`collect.py collect`；参数和调用由快照的 `--help` 及配置 manifest 定义。
本轮错误不能通过删除样本后继续同一 run 来“修复”。

## 5. 事实、推断与下一步

- **事实**：严格 parser 拒绝了真实模型的非法输出；请求没有被自动改写或重试。held-out、拟合、artifact
  与 planner 加载均未执行，误差和服务系数为 unavailable。
- **推断**：当前固定模型在这一输入分布下尚不满足可靠的输出格式要求，小规模 capability 成功不足以
  支持成本校准。这里没有人工 TRUE/FALSE 标签，不能给出语义准确率。
- **不能声称**：真实 reference cost 已校准、第二 physical path 可比较、系统性能提升，或所有模型都无法满足合同。
- **下一步**：先为下一轮选定能够遵守同一严格输出合同的 reference model/profile，重新预注册并保持
  validation split 独立（本次 held-out 没有被运行或用于调参）；如修改 prompt/parser/generation，必须作为
  新 semantic identity，而非悄悄修补本次。
  输出合同通过后再检查 training 的可辨识性、普通谓词输入行估计和 held-out 误差，仍不放宽本轮 20%
  要求或引入第二路径。
