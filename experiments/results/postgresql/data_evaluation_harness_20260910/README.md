# 数据准备与执行记录修复及服务器验收

日期：2026-09-10。文档角色：内部工程验证记录。对应研究对象为 PostgreSQL 内置 AI 语义算子的
外部分布式物理执行与调度优化；本轮只修复研究内容一/二后续测量所需工具，不开展方法实验。
当前计划见[数据执行切片](../../../plans/data_organization_batching.md#首次尝试后的决定)。

当前状态：本地/Linux 各 168 项相关测试通过，服务器新 recorder 的 7 项 PG 检查通过；
SQuAD 同步预检 2 行、同步 4 行和增量 4 行共 10/10 次真实请求通过。实际出站消息、token usage、
结果关联、先执行记录后评价及资源释放已核对，见下方“服务器完整验收”；实现提交 `5085c6ff` 已合入 main。
此前本地阶段与零模型请求恢复检查按时间保留，不能把它们的 0 次请求误读为最新累计值。

后续审查限定：本报告的真实完成检查从 completion sequence 与唯一 payload 构造预期映射，
因此没有独立证明 PG producer 序号到 SQL 行的完整关联。生产关联机制不因此被判定损坏，
但不能把本次 10 次验收作为独立 producer 证据；原始控制器和结果保持原样。
后续修复与单卡画像由[当前工作](../../../plans/data_organization_batching.md#当前工作可复跑的单卡静态容量参照2026-09-10)维护。

## 目的与设置

修复[首次真实数据尝试](../data_execution_pilot_20260909/README.md)暴露的准备与观测问题：
原文误被证据脱敏改写、tokenizer 字典被当作 token 序列、验证失败导致执行时间未保存，以及
检查器错误要求完成事件有序。原始模型运行、失败、48/68 请求使用量与缺失 JCT 均保留，不覆盖旧证据。

实现基于 `687eb8def8a045320c8241b18257582128c97d5a`，在独立分支 `codex/data-evaluation-harness`。
验证环境为 macOS/Python 3.12.6，使用 unittest fixture、HTTPX MockTransport 和本机 HTTP 测试服务；
没有启动模型、PG 或真实 gateway。本机没有 psycopg/Transformers，未安装或下载依赖。
服务器在认证前关闭连接，随后用户确认已关机并要求先完成本地工作；此后没有重连或远端运行。

## 合规自检与设计

| 检查对象 | 修复与可观察条件 |
|---|---|
| workload 与公开证据 | 私有原文保存在 Git 外，目录 0700/文件 0600，拒绝覆盖；公开摘要另用 schema，只含计数/哈希 |
| ShareGPT 选样 | 源文件 SHA 必填，首个 human 原样保留，UTF-8 字节筛选，不裁剪、不规范化、不替换为空的首轮 |
| 文本与 token | ID 关联后比较 UTF-8 完整值；完整消息模板显式请求 token IDs，字典只取 input_ids，非法容器拒绝 |
| 执行记录 | SQL 前持久化起点，消费时追加原始结果；异常仍尝试保存执行终点/状态，再做独立评价，保留异常传播 |
| 乱序关联 | 使用实际 producer 序号、ID、semantic payload digest、原输出、模型及 stop，重复/缺失/错配拒绝 |
| 出站核对 | 可选完整请求值哈希及次数清单；原文/参数/次数不符在 HTTP 前拒绝；仍保留 durable ledger 预留次数 |
| 公开 JSON | 先对字段值脱敏再序列化，保持 JSON 可解析；完整私有事件另存，哈希先于脱敏计算 |

没有修改 PG、wire、Engine、调度算法或 Map 的 stop 要求。没有自动续用剩余 20 次额度，也没有
通过放大输出预算、换提示、删除失败行或允许 length 来改变旧实验。公开接口依据及采用理由见当前计划。

## 全组件数据与验证结果

最终相关回归 **168/168 通过**，来源为 [原始测试输出](raw/related-tests.log.gz)，完整命令、基础提交与
环境见 [validation.json](raw/validation.json)，实现/测试文件身份见 [source_sha256.json](raw/source_sha256.json)。
这是一次本地测试套件执行，包含旧 SQuAD importer/评分/runner、服务检查、同步/异步观测以及新工具回归；
不是 168 次模型请求，也不是吞吐重复实验。

新增用例覆盖原样 JSON/CSV 与 Unicode/空白、token 返回结构和上下文等号、部分结果后错误、
首行前错误、取消/结果上限、空结果、evaluator 失败后执行状态不变、PG cursor/generator 关闭、
逆序完成及重复输入内容、同步/异步 HTTP 前拒绝。拒绝场景观察到 1 次账本预留、0 次真实模型 POST；
本机 HTTP fixture 不计作模型服务资格。

开发时先复现了公开 JSON 的引号损坏，失败输出保留在
[public-json-before-fix.log](raw/public-json-before-fix.log.gz)。修复后同一断言与最终相关套件通过。
公开日志落盘前调用现有 `redact_text`，它们只含测试 fixture；私有真实数据未复制进本目录。

| 观测范围 | 本轮结果 |
|---|---|
| 真实模型调用 | 0 次；旧实验仍为 48/68 次 |
| 准备文件原文往返 | 合成敏感形态/Unicode fixture 通过；原始 ShareGPT 服务器资产重新准备尚未执行 |
| PG 原文读回、实际生产序号 | pending：只验证可复用 API 与 fake cursor，完整控制器尚未接入真实 PG |
| 服务 tokenizer/template 一致性 | pending：只验证返回结构/公式，尚未使用服务器实际 tokenizer |
| GPU、资源回收、JCT、质量、吞吐 | 本轮未测，不能填零或补回旧缺失值；测试耗时不作性能指标 |
| baseline/消融 | 不适用：缺陷回归，不是方法对比；正式比较仍按当前计划执行 |

## 事实、限制与对课题含义

源码和本地测试支持上述工具行为；不证明完整 PG→模型→PG 原文链已经重新验证，也不证明
ShareGPT 摘要质量恢复或双 GPU 路由完成。ShareGPT 暂停的是当次请求意图摘要配置，数据源仍保留。
当次第 12 行 450 输入 tokens 加 128 输出预算低于 4096，这是旧修正证据，不能解释为输入超上下文。

`record_pg_query` 是测量 API，不拥有连接、有限 statement timeout、事务、服务或模型额度。
调用者必须提供结果上限，记录含磁盘写入开销；结果文件上限不等于客户端/PG RSS 上限。
磁盘故障或强制终止可能阻止完整终点保存。无 ORDER BY 的 SQL 不承诺输入顺序，关联 API 的序号
必须来自实际 producer。请求哈希清单也必须独立由核对过的消息构建，不能从错误出站值反向生成。
公开事件的通用脱敏不能替代发布前隐私检查。

## 下一步

1. 本次服务器工具验收及 main 集成已完成。后续模型检查另定请求数量和停止条件，
   不自动花费旧剩余额度。
2. 继续 SQuAD 代表性调优输入与单卡静态容量，再验证共享全局容量下两个 TP1 副本的实际路由。
3. ShareGPT 单独确定摘要/普通回答/压力输入用途；提示与预算变化建立新版本，质量与执行测量分别判断。
4. 按计划核验 SemBench 作者版本、Movie 原始查询/evaluator，再单独定义 Movie-derived 全扫描任务；
   两种结果目标不可混称，当前均未适配或运行。

## 服务器恢复检查

用户提供新连接后，复用既有仓库外 runtime env，`core,text,workload-text` 只读预检通过；
可选 nvcc 缺失不影响本轮读取与 tokenizer 操作。数据盘仍有约 419 GiB 可用空间，两张 RTX 4090
初始及结束均为 1 MiB 占用，没有模型服务。完整机器报告留在服务器仓库外，公开
[预检与清理摘要](raw/reconnect-preflight-summary.json)不含连接信息。

- SQuAD 原始文件 SHA 与原清单一致；实际 serving 环境的 Transformers 5.14.1 对准备好的
  tuning/evaluation 各 64 行重新计数。范围分别为 92–257 和 118–228 tokens，全部满足输入加
  64 输出预算不超过 4096。模型 config/generation config/tokenizer 两文件共 4 项 SHA 与旧证据一致；
  模型权重本次未重新哈希，也未启动服务，见[tokenizer 记录](raw/reconnect-tokenizer.json)。
- 隔离 PostgreSQL 18.3 实例通过 128 行既有 SQuAD 输入及 2 行 Unicode/空白/敏感形态 fixture
  的写入、倒序流式读回及按 ID 比较：130/130 UTF-8 完整相同，见[读回报告](raw/reconnect-pg-readback.json)。
  这是普通 text 存储检查，没有调用 Map 或模型，也未运行未提交的新 recorder 实现。
- 首次 initdb 因父目录穿越权限失败，未启动 PG；原日志和目录保留。按 runbook 临时添加
  PostgreSQL 用户最小穿越 ACL，在全新目录复查后通过。临时 PG 已停止，原 ACL 已完整恢复，
  最终 PG/模型进程为 0。

新增真实模型请求仍为 0。新的私有 ShareGPT 准备、完整源→PG→出站一致性、新 recorder 实际接入、
服务 usage 核对以及双副本路由仍待验证；不能把本次普通 PG 检查称为模型端到端通过。

## 服务器完整验收

本节接续用户“在这个服务器上测试，然后没问题就提交推送合并到 main”的授权。
服务器从 Git 取得 `687eb8de` 并创建隔离 worktree，再应用待提交补丁；服务器原 main 工作目录未修改。
测试前后 682 个代码目录文件 SHA 均一致，基线、补丁 SHA 与工作树树对象见
[source manifest](raw/server-source-manifest.json)。这是未提交改动的可复核快照，不冒充已发布 commit。
验收后只补充文档和证据；提交前 669 个非 Markdown 代码目录文件仍与服务器测试版本逐项相同，
见[最终代码匹配](raw/final-code-match.json)。

### 源数据、实际 tokenizer 与 PG 记录

[Linux 相关测试](raw/server-related-tests.log.gz)为 168/168 通过，与本地套件有重叠，不相加成独立测试数。
新 CLI 对原始 SQuAD 生成两侧各 64 行、对原始 ShareGPT 生成 16 行首个 human 原文；ShareGPT
按源文件中的位置重新读取后逐字相同，保留原 SHA 与选样身份。完整消息 token 计数分别为：

| 数据 | 行数 | 输入 token 最小–最大 | 声明输出预算 | 输入加预算不超过 4096 |
|---|---:|---:|---:|---|
| SQuAD tuning | 64 | 92–257 | 64 | 全部满足 |
| SQuAD evaluation | 64 | 118–228 | 64 | 全部满足 |
| ShareGPT 原样准备 | 16 | 77–450 | 128 | 全部满足 |

这次 ShareGPT 计数针对重新准备的原文；128 仍只是原摘要配置的诊断预算，未据此宣称摘要质量通过。
模型 9 个文件重新哈希，与 revision `a09a35458c702b33eeacc393d103063234e8bc28` 的旧核验值一致，
见[模型记录](raw/server-model-verification.json)。PG 扩展二进制与对应源文件也已核对，见
[PG source binding](raw/server-pg-source-binding.json)；没有更改 PG 实现。

使用新 `record_pg_query` 的真实 PostgreSQL 18.3 检查共 7 项：146 行文本读回（128 SQuAD＋16 ShareGPT＋
2 fixture）、已消费 2 行后 SQL 错误、首行前错误、evaluator 错误、结果上限、statement timeout 和空结果。
预期错误均保留 execution/部分结果，evaluator 错误仍保留 completed 执行状态；每项结束后连接均可继续查询。
详细状态、行数和计时见[服务器离线验证](raw/server-offline-summary.json)。

### 真实 Map 与证据落盘

新账本 `semloom.data-evaluation.harness.20260910` 上限为 10，与旧 48/68 账本分开。
使用 GPU 0 的 Qwen2.5-7B-Instruct TP1/BF16/FCFS/eager 副本，关闭前缀缓存，
`max_model_len=4096, max_num_seqs=4, max_num_batched_tokens=4096`；完整
[服务参数及缓存环境](raw/server-service-command.json)记录本次实际设置。
模型请求超时 30 s，PG statement timeout 60 s，单次验收上限 15 min；没有语义重试。

| 路径 | 请求/返回行数 | EM / F1 | 执行与结果记录耗时（s，单次） |
|---|---:|---:|---:|
| 同步预检 | 2 / 2 | 100% / 100% | 0.226251549 |
| 同步参考 | 4 / 4 | 100% / 100% | 0.387735898 |
| 增量 v6，窗口 2 | 4 / 4 | 100% / 100% | 0.339140328 |

三条记录只覆盖 4 个不同的 SQuAD 样本，不是 10 个独立样本；不作总体质量或性能比较。
耗时包含新 recorder 的结果写盘开销，目的在于核对时间先于评价保存，不能据差值计算方法收益。
实际 complete messages/生成选项在发送前通过哈希次数清单核对，实际 token usage 与 tokenizer 一致。
全部输出为 stop，PG ID、payload 与原输出关联正确；本次增量恰好按 0/1/2/3 完成，乱序的缺陷回归
仍由单元用例和旧乱序运行证据支持，不能声称本轮实际观察到了逆序。

增量 HTTP 峰值为 2；held tasks、input bytes、result bytes、active requests、active work 最终均为 0。
[真实结果](raw/server-live-summary.json)、[账本](raw/server-ledger.json)、
[同步事件摘要](raw/server-openai-compatible-fixed-events.jsonl)、
[增量事件摘要](raw/server-incremental-map-events.jsonl)分别保存。
另按原始 POST 日志、账本、请求字节/值哈希、PG 原输出、标准答案和文件时间顺序独立重算，
确认实际 POST 为 10，见[独立审计](raw/server-independent-audit.json)；重审没有新增模型调用。

### 准备失败与清理

模型准备前两次失败分别由 FlashInfer 默认缓存目录权限和服务虚拟环境的 bin 未进入 PATH 引起，
均为 0 POST，原目录与完整日志保留。修正为数据盘缓存并显式加入已有服务虚拟环境/CUDA bin 后，
在全新目录继续准备，仍用同一 10 次账本；未安装依赖或改模型、提示、预算，见
[启动失败记录](raw/server-startup-failures.json)。这两次准备失败不属于重发模型请求。

模型正常退出、没有强制终止；PG/网关均停止，GPU 进程为 0，两卡各 1 MiB，临时 ACL 恢复。
清理时最初裸 bind 返回 EADDRINUSE，随后以连接拒绝和 SO_REUSEADDR 重绑确认端口无监听；
未把该探测结果误当成残留模型进程，见[最终清理](raw/server-cleanup.json)。
115 个私有文件（含索引）已独立归档，114 个内容文件哈希与归档 SHA 重新核对；
[归档身份](raw/server-archive-identity.json)只公开哈希、数量和大小。原文、预测与完整异常留在仓库外。
脱敏的[准备检查脚本记录](raw/server-validate-offline.py.txt)与
[真实验收脚本记录](raw/server-validate-live.py.txt)用于审计来源；它们是一次性控制器记录，
脱敏后不可当作可复用生产入口，原脚本保存在私有归档中。

本次可支持合并的是数据准备、token 检查、执行记录和观测核对修复。ShareGPT 任务质量、强静态容量、
组织策略收益、双 GPU 路由、多阶段 PG 方法桥接和 SemBench 适配仍未由本次验收证明。

## 主干集成

实现提交 `5085c6ff` 从 `687eb8de` 继续修复，已按用户授权快进合入 main，包含此前的数据执行切片。
合并没有改动通过服务器验收的实现；669 个非 Markdown 代码目录文件仍逐项匹配测试快照，
本节和状态入口的更新只登记集成事实。原始失败、旧账本及新 10 次账本均保留，私有材料不进入 Git。
