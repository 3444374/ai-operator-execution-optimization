# PostgreSQL Map 总字节留存预算

日期：2026-09-10。角色：内部实现与功能/资源诊断记录。研究对象为 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化。
对应[工作包 C 规格](../../../plans/data_organization_batching.md#工作包-cd-的实施规格2026-09-10继续)。

实现默认关闭的 `enable_total_window_budget`：行数 L 是最多留存行数，字节 B 控制实际能留存多少行。
旧 B/L 模式保留。新模式按行 MemoryContext 的实际分配及声明的固定元数据预留计费；输入、消息、slot 和完整结果缓冲一起接纳。
完成返回只填入预分配结果，直到 PG 消费并释放行才归还空间。一个待接纳行可占用独立暂存 S，不能占用已接纳行的结果空间。

## 计费范围与失败规则

- slot 原始 Datum 大小在 TOAST 解压前核对；转换、复制和分配前检查保守上限，分配后再核对实际上下文字节。
- 暂存默认 16 MiB，独立转换上限 1 MiB；协议接收暂存另报峰值。B 不表示整个查询 RSS，也不包含 ordinary child plan 的 work_mem。
- 固定行数组及 provider 关联元数据仍随 L 增长。若单行无法放入 B 减元数据后的空间，或暂存不足，明确失败，不能声称任意 L 都可用。
- `EXPLAIN ANALYZE` 提供配置、峰值及等待次数；测试日志使用 backend PID 与节点内递增 ID 区分观测。
  多节点限额/峰值可相加，峰值之和不是同时刻 RSS。错误上下文回调只清空记账，避免访问已经被 PG 删除的子上下文。
- 为每行预留完整输出可能增加短输出场景的实际内存；本包证明有界留存与推进，不声称整体内存下降。

## 受控验证

PG18.3 严格编译和回归 1/1 通过；完整 18 个 TAP 文件 2134 项通过，随后对新增四个断言及最终释放处理执行专项 37 项通过。
两者有重叠，不合并成一次完整 2138 项运行。本地/Linux PG 合同各 116 项通过；新增内存审计、工作量和监督器相关 7 项通过。
真实 PG 加本地 HTTP fixture 完成 120 行，HTTP 峰值 2，最终活跃为 0；这是受控响应，不是真实模型。

异质 10 行反例中，旧 B=1 MiB/L=2 成功，L=8 因 B/L 拒绝；total L=8 实际峰值留存 6 行，
计费峰值 1,028,688 字节，暂存峰值 178,832 字节，等待 5 次后完成，结果按输入关联。
专项覆盖乱序、满窗口与待接纳行、NULL、LIMIT0/1、volatile 只求值一次、巨大 TOAST 预检查、
单行/元数据不兼容、65,536 字节合法输出及 65,537 字节拒绝、取消与新查询恢复。
原三次失败分别为测试日志目录缺失、fixture 等待条件错误、旧实现没有新 GUC；日志全部保留。

来源：[受控汇总](raw/c-public/public-controlled.json.gz)、[完整 TAP](raw/c-public/tap-full1.log.gz)、
[最终专项](raw/c-public/release-final.log.gz)、[PG 回归](raw/c-public/regression.log.gz)。

## 独立真实模型诊断

用户单独授权最多 288 次模型 POST、单 RTX 4090、从模型准备开始 15 分钟，首次有效性失败停止且不重试。
新账本 `semloom.total-window.C1.20260910` 实际预留/出站均 288，服务日志独立核对 288 个 HTTP 200；
从模型 SHA 核对至服务停止共 202.55 秒，没有失败或重试。

Qwen2.5-7B-Instruct revision `a09a35458c702b33eeacc393d103063234e8bc28`，vLLM 0.25.1；
BF16、TP1、context4096、max_num_seqs128、max_num_batched_tokens8192、GPU 利用率配置 0.8、FCFS，
prefix cache/chunked prefill 开启，每单元清空前缀缓存并检查服务日志。模型文件 SHA 和实际 argv 核对通过。
Movie v4 原始前 128 个行出现，保留重复 reviewId，不按标签选样；输入 47–131 tokens，输出上限 128，不截断。
固定 C=8、核心输入/结果 128/64 MiB、PG B=1 MiB、S=4 MiB；equal L=2，total L=32。

| 单元 | 行/POST | JCT 秒 | total 计费峰值字节 | 实际留存峰值 |
|---|---:|---:|---:|---:|
| equal 预热 | 16 | 0.794306 | 未启用新计费观测 | — |
| total 预热 | 16 | 0.402237 | 1,026,264 | 13 |
| equal 验证 | 128 | 5.407468 | 未启用新计费观测 | — |
| total 验证 | 128 | 2.258928 | 1,028,312 | 13 |

两种模式全部输出合法且关联正确。128 行分类均为 TP75、FP2、TN33、FN18，准确率 84.375%；
16 行预热均为 TP8、FP1、TN4、FN3。合法但答错仍计入；这不是情感质量已充分验证的结论。
total 验证暂存/转换/接收峰值为 69,688/1,024/8,192 字节，等待 115 次，最终留存行、暂存行和计费字节均为 0。
L 与内存策略同时变化、每点只有一次且查询很短，因此只登记功能与资源诊断，不能将时间差归因于组织算法或宣称强静态参照成立。

来源：[完整运行与账本](raw/c-public/real-run__campaign-result.json.gz)、
[源码与 PG 库指纹](raw/c-public/real-proposal__source-snapshot.json.gz)、
[模型核对](raw/c-public/real-run__model-verification.json.gz)。运行的 722 个非 Markdown 代码文件与准备时本地 C 实现一致，
随后 D 的本地改动未上传至这份模型运行源码。

## 清理与后续

模型正常退出，无强制杀进程；GPU 无计算进程，模型端口可重新绑定，见[清理检查](raw/c-public/cleanup.json)。
本包结束时自有 PG 及临时目录访问 ACL 暂留给已授权的 D 受控验证；D完成后已停止PG并恢复ACL，
见[最终清理](../map_organization_20260910/README.md#独立真实诊断与清理)。
私有原始消息、预测、数据库和失败材料保留在仓库外；公开材料经过脱敏，文件 SHA 见[清单](raw/c-public/manifest.json)。
下一步为相同字节、活跃请求/work 和候选窗口下的组织控制；C 本身没有改变组织算法。
