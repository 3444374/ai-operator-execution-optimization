# SemMap 推送前真实模型复验（2026-09-06）

本文件是内部验证记录，研究对象为 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化。
依据为 [Map 合同 §8.4.6](../../../plans/postgresql_semmap_generation_contract.md)。用户明确授权真实
测试通过后提交推送。本目录 Python 文件是本次运行的证据快照，不是后续实验的公共入口；实际
PG、provider、预算、观测与资源判定均复用 `code/` 的实现，不导入旧结果脚本。

**真实请求8次，六个测量阶段全部有效且通过。修正测量方式后的 INSERT 资源复查已完成。**
本次没有改生产代码；被测运行时代码为 `b7eeea536c1f9c57faadb01ccdc8b4ae6658e50d`，本地基线
`1a8b7e8d` 与其只有文档差异。结果来自一次完整运行，没有把旧失败与新成功拼接成通过结论。

## 1. 配置与预先规定的条件

| 对象 | 本次配置 |
|---|---|
| 路径 | PostgreSQL18.3 → wire v5 → 当前共享观测 gateway → 真实 vLLM → PostgreSQL |
| 模型 | Qwen/Qwen2.5-7B-Instruct，revision `a09a35458c702b33eeacc393d103063234e8bc28` |
| serving | vLLM0.25.1、Torch2.11.0、Transformers5.14.1；单RTX4090、BF16、tensor parallel 1 |
| 容量 | context4096、max-num-seqs4、batched-tokens4096、memory-utilization0.8、FCFS |
| 生成 | temperature0、top_p1、n1、stream=false、stop=null、max_tokens128、timeout120s |
| 服务选项 | eager、关闭prefix cache、generation-config=vllm |
| 预算 | 新ID `semloom.semmap.prepush.20260906.v1`、总上限8；原32/32账本字节未变 |
| 采集 | schema v2.1 / phase-lifecycle-3；20ms采样，原资源阈值不变 |

每个task在模型派发前使用已有测试参数固定等待100ms，以采到短请求的两端socket；不改消息、
生成参数或响应。该等待属于测量辅助，本次没有性能比较。没有质量标签、对照算法或消融，
输出正确性指PG输出与实际模型completion字节一致，不等于生成质量已经合格。

全部9个权重/tokenizer/配置文件与既有固定manifest哈希一致，四个权重分片同时匹配已保存的官方
revision元数据；core/text预检通过，没有安装或下载。模型端点、服务进程PID/start-time、cmdline
和配置哈希相互绑定，测试前后核验一致。机器路径、用户、GPU及端点来自私有settings。

## 2. 实际请求与测量结果

| 阶段 | 请求序号 | 结果 |
|---|---|---|
| PG预热 | 1 | 成功，单列在测量外 |
| SELECT：Unicode、空串、SQL NULL | 2–3 | 输出字节、顺序、NULL、模型身份、finish reason和usage通过 |
| INSERT：ASCII与SQL NULL | 4 | 写回、独立读回、计划计数与资源均通过 |
| 取消 | 5 | SQLSTATE57014；外部生成最终为length/128 tokens，PG丢弃结果，等待其终态及服务回空 |
| 取消后恢复 | 6 | 成功、资源通过 |
| 超长输入拒绝 | 7 | 18037 prompt tokens超过服务context，SQLSTATE38000，资源通过 |
| 拒绝后恢复 | 8 | 成功、资源通过 |

plain EXPLAIN、LIMIT0和NULL-only实际零请求。新账本最终8/8；取消、拒绝均计数，没有推理重试。
六阶段均为 `measurement_status=valid`、`policy_status=passed`；诊断不赋予正式压力资格。

六阶段的gateway+backend provider socket同时峰值增量均为2，结束均为0；两个角色的结束FD与
线程增量全部为0。INSERT的两个角色RSS峰值/结束增量也均为0。最大RSS增量出现在预期拒绝：
backend峰值1372160 bytes、结束741376 bytes；gateway峰值1024000 bytes、结束937984 bytes，
均满足原阈值。逐项数值与completion哈希见 [真实审计](raw/real-audit.json)。

INSERT只执行一次：模型调用/接受/输出计数各1，prompt/output tokens分别42/7；SQL NULL不调模型。
读回发生在cleanup采样完成之后的独立连接，原始plan、rows与连接身份先保存再核验，避免验收JOIN
污染被测backend。此前INSERT的inconclusive原记录保留，此处是新的通过证据。

## 3. 准备失败、复核与清理

第一次启动在零请求时失败：未将serving venv的bin目录加入PATH，导致已安装的`ninja`不可见。
失败服务已退出，端口关闭、两GPU均1MiB。第二次在新目录修正PATH、使用外部指定的短临时路径，
沿用同一空账本；没有重置或扩充预算。首次脚本、日志及controller summary原字节留在服务器，
其哈希登记在公开审计中。

派发前两路复核补齐端点绑定、完整进程组清理、失败计划先落盘及显式机器设置。4项无模型
控制器反例在本地允许ps的环境和Linux各通过，覆盖错误端点拒绝、父子退出、忽略TERM，以及
组长先退出但子进程继续运行；受限本地sandbox最初禁止ps，该轮不能作为通过证据。
独立PG预检确认INSERT计划识别与LIMIT0，模型请求0。

成功运行结束时，已观测backend/gateway均不存在，PG pidfile消失，模型端口关闭；两张GPU均恢复
到1MiB。原始run的1162项哈希（含嵌套重复项）全部匹配。公开文件仅包含允许字段摘要与脚本，
未导出原始模型文本、私有FD路径、真实runtime env或凭据。

## 4. 复查与证据范围

[审计器](audit.py)、[SQL检查驱动](real_check.py)、[服务控制器](launch.py)和
[无模型控制测试](test_controller.py)保存本次实际版本。驱动SHA与服务器运行版本逐一匹配。
私有settings、模型身份清单与完整raw留在服务器；公开文件自身哈希见
[PUBLIC_SHA256SUMS.json](PUBLIC_SHA256SUMS.json)。

这证明当前同步SemMap真实链路及所列小规模资源场景通过，关闭独立审计连接后的真实INSERT复查。
修复后的正式fixture3×2000尚未复验，质量/成本、性能和可组合执行仍按各自计划推进。
