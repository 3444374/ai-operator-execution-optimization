# PG 单 Map 首次真实数据尝试

状态：部分可用，真实调用已停止；SQuAD 小样本的关联与答案检查可用，ShareGPT 未通过。
本轮累计 48/68 次物理请求，剩余 20 次未使用。没有容量曲线、强静态配置或组织策略性能结论。
对应[当前数据执行计划](../../../plans/data_organization_batching.md#当前-pg-单-map-数据执行切片)，
研究内容为 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化。

## 目的、设置与合规自检

先验证固定数据 → PG 单 Map → 真实模型 → 完整结果消费 → 评价/证据，
再决定容量、组织与多 Job 实验的准备工作。本轮没有实现或改动调度算法。

- 源码基线 `main@0ba12bfa`，隔离分支 `codex/data-execution-pilot`；上传独立源码副本，
  681 个文件逐项核对。PG 实现与既有已验证源一致，仅 README 不同；复用已核对 SHA 的 PG18.3 扩展。
- 只使用 GPU 0 上的一个 Qwen2.5-7B-Instruct 副本，revision
  `a09a35458c702b33eeacc393d103063234e8bc28`，9 个模型文件重新核对。
  BF16、TP1、FCFS、eager、关闭前缀缓存，max_model_len=4096、max_num_seqs=4。
  这是复用的功能检查配置，未调优；GPU 1 未参与，不能称双卡/分布式性能验证。
- 同步 profile=`openai-compatible-fixed`，增量 profile=`incremental-map`/v6。
  一个 Job、PG 输入窗口 2、PG 行缓冲预算 8 MiB；网关 held/active requests 各 2，
  input/result 预算各 2 MiB。实际 EXPLAIN 输入窗口为 2，默认 work=1，未注入旧 credit。
- 只创建独立 PG cluster/临时输入表，模型、网关和 PG 均由本轮 controller 管理。
  未改原数据库、服务器主仓库或安装依赖；原服务端工作目录版本较旧，未拿它冒充本轮源码。
- 第一次系统 Python 预检因缺依赖失败；换用原有独立 driver Python 后通过，不在 serving 环境混装。
  [预检失败](raw/preflight.json.gz)、[driver 预检](raw/preflight-driver.json.gz)均保留。

完整服务参数、模型和源码身份见 [run identity](raw/run-identity.json.gz)、
[PG 实现核对](raw/prepare--pg-source-binding.json.gz)、[模型校验](raw/prepare--model-verification.json.gz)。
实际调用由同一份[持久账本](raw/prepare--ledger.jsonl.gz)记录；失败或不确定请求不退还额度。

## 设计与实际结果

SQuAD 原始 dev-v1.1.json 为 10,570 行，完整 SHA 与既有 importer 一致。
新 workload `squad_v11_pg_map_v1` 明确使用 system 指令与 user 输入两条消息，
context 分组划分 tuning/evaluation，每侧准备 64 行。真实调用只使用 tuning 前 2/16 行。
这是从官方 dev 派生的小样本，不是官方隐藏测试集；原历史单消息 workload 未修改。

| 实际执行 | 请求数 | SQL 完整返回 | 答案/合同观察 | 查询时间 |
|---|---:|---:|---|---:|
| SQuAD 同步预检 | 2 | 2 行 | EM/F1 各 100% | 0.219736787 s |
| SQuAD 同步参考 | 16 | 16 行 | EM/F1 各 100% | 1.709610934 s |
| SQuAD 增量 | 16 | 16 行 | 离线按序号/payload/输出重审通过，EM/F1 各 100% | unavailable：原检查失败前未保存 SQL 计时 |
| ShareGPT 同步预检 | 2 | 2 行 | 非空、stop；后续语义审阅发现并非正确摘要 | 1.496024247 s |
| ShareGPT 同步参考尝试 | 12 | 查询报错，无完整 16 行结果 | 第 12 行 finish_reason=length | unavailable：失败前未保存完整查询计时 |
| ShareGPT 增量 | 0 | 未执行 | 前序失败后停止 | unavailable：未运行 |

EM 为答案完全匹配率，F1 为归一化词重叠分数，使用全部标准答案的最大匹配；这些百分数
只描述这 16 个短输入问题。每条结果由原 ID 关联，错误/缺失不能从分母删除。
上表每个时间只有一次观察，包含释放 SELECT 到 fetchall 完成；导入、ANALYZE、无执行 EXPLAIN、
模型和 PG 启动另作准备，不计入此时间。所有臂保留 SQL/模型/完整参数、实际消息和结果核对。
不根据同步与增量的差值报告加速比；增量的完整 SQL 计时不可恢复，不能拿 HTTP 区间代替。

[同步逐项报告](raw/run--squad-sync-report.json.gz)、[增量离线重审](raw/squad-offline-reaudit.json.gz)、
[ShareGPT 停止现场](raw/prepare2--driver.log.gz)、[原运行状态](raw/run2--summary.json.gz)。

## 失败、修复与尚未解决项

1. **检查脚本混淆完成顺序与结果顺序。** 首轮在 34 次请求后因增量关联断言停止。
   原完成序号为 `0,1,2,3,5,4,6,7,9,8,10,12,11,13,14,15`。
   离线重新核对序号集合、PG semantic payload digest、完整出站消息/生成参数及返回文本，
   全部相符。重复序号、错误 payload、错误输出三个篡改检查均被拒绝。
   原失败保留；修正检查脚本后没有重跑 SQuAD，也没有凭恢复成功补造 JCT。
2. **续跑启动器原本只接受空账本。** 首次准备 ShareGPT 续跑被 guard 拒绝，新增 POST=0。
   保留[拒绝日志](raw/controller2.log.gz)，随后明确以原账本 34 起始、68 总上限继续，不新建额度。
3. **ShareGPT 任务和输出合同未通过。** 第 6 行输出遵从了待摘要文本内的指令，第 12 行实际回答
   输入中的开发问题而非生成请求摘要，并在 128 tokens 结束为 length。PG 正确拒绝截断结果。
   多个其他输出也偏向回答问题或概括内容，而非概括请求意图，详见[逐行审阅](sharegpt_review.md)。
   不提高输出上限、不换提示后继续汇总；48 次后停止，剩余 20 次未用。
4. **ShareGPT 源文本身份不完整。** 临时准备脚本复用了 evidence 的脱敏写入函数，
   误改了第 7 行的输入。完整原文件 SHA 不足以证明派生输入没变；
   [源文本往返审计](raw/sharegpt-source-audit.json.gz)发现 15/16 原样相符。
   本轮不能称合格原样 ShareGPT；上述第 6/12 行问题不受这处修改影响，仍保留为逐行失败观察。
   该临时脚本只保留为失败来源，不能作为可复用 runner。
5. **tokenizer 计数检查有误。** 原脚本对返回字典调用 len，得到字段数 2；
   [原记录](raw/prepare--token-check.json.gz)不能作为 token 证据。
   显式取得 token ID list 后完成[离线修正](raw/token-check-corrected.json.gz)，
   与已完成请求的实际 usage 范围一致，所选输入均未超过 4096 上下文，没有增加模型调用。
6. **既有评价器的缺失行边例。** 参考答案归一化为空时，缺失/NULL 预测曾错误得到满分。
   [失败复现](raw/evaluator-before-fix.log.gz)后改为缺失行直接计零、仍保留分母。
   真实 SQuAD 的输出均非空，本次修复不改变上述分数；属于离线评价修复，不是模型行为变化。

## 资源、完整数据画像与证据强度

SQuAD 增量真实 HTTP 峰值为 2，核心 held tasks 峰值 2，input bytes 峰值 2,054，
result bytes 预留峰值 2,097,152；结束 held/input/result/active requests/active work 全部为 0。
result bytes 是预留预算，不是实际输出体积或进程 RSS。
网关[事件时间线](raw/run--squad-incremental-events.jsonl.gz)增加单机单调纳秒时钟；
[进程 RSS 采样](raw/run--squad-incremental-rss.json.gz)每约 20 ms 观察 PG、网关和客户端。
没有 PG 实际留存 payload 的完整时间序列、服务端排队时间线或逐行消费延迟；这些项明确不可用。
本轮客户端在小样本上 fetchall，不能据此声称大规模流式内存已验证。

[完整 SQuAD 离线输入画像](raw/squad-population-profile.json.gz)为 10,570 问题、2,067 个不同 context，
其中 2,056 个 context 对应多个问题。完整 Map 消息的 input tokens 最小/中位/P90/P99/最大值为
76/205/310/496/852；没有记录加 64 输出预算后超过 4096。这里只计 tokenizer 输入量，不是 GPU 时间。
当前 tuning 的 64 行为 92–257 tokens，覆盖窄于整体；共享 context 是可测的复用机会，尚非缓存收益证据。

事实：受限单 Map 已能在真实问答数据上产生可核对结果，乱序完成由 PG 正确关联返回。
推断：下一步应扩大有代表性的调优输入并建立静态容量画像，而不是先迁入更多调度算法。
不能声称：强静态配置已建立、长度组织有效、ShareGPT 质量合格、双 GPU 性能、完整组合流水或框架优势。

## 验证、原始资料与清理

本地与 Linux 最终各 125 项相关测试通过，覆盖新增清单、原 importer/评价器、请求观测器和旧 SQuAD
baseline 合同；两处是同一套测试，不相加。Linux 扩大套件时漏传一份既有 example 配置，
[原失败](raw/final-tests.log.gz)保留；补入同版本配置后[125 项通过](raw/final-tests-restored-config.log.gz)。
本地见[最终测试](raw/final-local-tests.log.gz)。没有重跑完整 PG TAP；PG 生产代码未变，另做独立
无模型 SQL/EXPLAIN 检查和上述真实查询。最终修改文件与服务器逐项哈希核对，见
[最终代码核对](raw/final-code-verification.json)。

原始完整输入、预测和事件保存在仓库外，并已备份回本机；公开 raw 中消息与输出只保留
已落盘值的摘要，POST 原字节哈希以账本为准。源文本/预测不能因为公开证据脱敏而暗改实验输入。
私有原始资料共 83 文件核验，归档 SHA 记录于 run identity；恢复时先核对归档与文件哈希。
本目录 raw 是脱敏派生证据，文件完整性见 [SHA256SUMS](raw/SHA256SUMS.json)。
零模型复核可用 `python code/scripts/baselines/squad_pg_map_pilot.py prepare/evaluate`；
两次 controller 与准备脚本仅作为本次历史运行原文保存，存在上述已知缺陷，不作为直接重跑命令。

最终两次模型进程均退出，监听端口关闭，无本轮 PG/gateway/model worker，两个 GPU 各 1 MiB、利用率 0；
[清理核对](raw/postflight.json.gz)保留证据。未关闭整台服务器，原数据和原有环境保留。

## 下一步

继续 SQuAD：先让失败前也保存计时/结果，按完整长度分布选 context 不交叉的调优样本，
再以匹配 direct 和 PG 静态执行建立容量画像。单卡用于参照，双卡需先验证两个 TP1 副本的实际
endpoint 接入与路由。之后才比较固定行数、工作预算和有限窗口长度组织，再按需要进入多 Job。
ShareGPT 先解决输入身份与摘要质量问题，不阻塞 SQuAD；旧 shared credit 与公平策略保留并按需复用。
