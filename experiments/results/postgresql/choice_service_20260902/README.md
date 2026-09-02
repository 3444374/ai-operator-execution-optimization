# Choice：真实 PostgreSQL → gateway → 模型服务验证

## 目的与结论

按[四 C 专项计划 C.5](../../../plans/completed/postgresql_choice_profile_engineering.md#c5-对照请求预算与资源保证)，
验证数据库保存的 choice 配置能进入真实模型请求，原始结果由 PG 严格解析并决定行是否保留。
这是 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化所需的执行基础检查，
不是质量、性能或调度策略比较。

**最终真实运行通过：14 次请求（旧配置 7 次、choice 7 次）及两个零调用 NULL 对照。**
计数工具首轮失败消费的 1 次也保留，整个四 C 累计 **15/100**，没有退款或隐藏重试。
结合此前[受控资源检查](../choice_resources_20260902/README.md)，四 C 的工程验证完成，已以 `2820dcb5` 合入本地 main；
**Filter reference 语义质量未通过，校准未恢复，第二物理路径与多算子组合未实现。**

## 身份、设置与合规自检

- 初始工具源码 `87b7963b`；最终工具 `0a1c12d301e41c1c0c1d3977d4002294879fcc69`。
  修复仅在实验采集工具和对应测试，不修改 PG C、SQL、wire、生产 gateway/HTTP Adapter 或模型参数。
- 明确使用 **PostgreSQL 18.3**，复用 `39007150d5d0f84904fcd0c36b7bab87de7c07c1` 已验证的安装。
  extension SHA-256 为 `534b1a5245999a85da941a4c2932e06582961f946732b490db98a1c5c9b0fcc6`。
  本轮**未重新构建、未重跑完整 TAP/regression**；历史 919/919、regression 1/1 不重新绑定。
- Qwen2.5-1.5B-Instruct、vLLM 0.25.1、Transformers 5.14.1；复用已有公开模型，7 个模型/配置/
  tokenizer 文件 SHA 前后核对。单 RTX 4090、单 endpoint、TP=1、BF16、FCFS、eager、prefix cache off，
  max length=4096、max sequences=1、batch tokens=4096、memory utilization=0.25。
- 两配置使用同一模型进程、相同 messages 和显式生成参数：temperature=0、top_p=1、max_tokens=8、
  n=1、stream=false、stop=[换行]；继承 repetition_penalty=1.1、top_k=20。完整继承配置见
  [service-before.json](raw/server/pg-real-r2/service-before.json)，前后身份完全相同。
- 唯一请求变量是 `structured_outputs.choice=[TRUE,FALSE,UNKNOWN]`；移除此字段后，实际 HTTP JSON
  按完整结构、值及类型相同，不仅比较几个参数。生产 Adapter 无重试，不修改模型原始回答。
- preflight 的 core/text-qwen15b 已通过；数据库仅 Unix socket，模型仅 localhost。
  使用三个公开合成输入，无公司代码/私有数据上传，无新下载、held-out 读取、拟合或校准 artifact。
  公司参考仍是主计划 §8.7 的请求消解和错误分类经验，不复制实现；自有计划与执行能力不因此收缩。
- PG 拥有取行与关系结果，gateway 执行一个同步 HTTP 请求，vLLM 拥有模型执行；无新增调度策略。

身份、66 个源码 SHA 和二进制哈希见 [qualification.json](raw/server/qualification.json)。
测试开始时本地 main 为 `939d1b54`，研发分支独立、未合并或推送；后续集成状态见下方收尾说明。

收尾补充：上述 main 身份是测试开始时的观察。随后主线新增 `dd8cbf89` 的 pgml 工程参照；用户要求
清理后合并，合并结果另记项目日志，不把后续文档或清理提交重新绑定为本次模型运行源码。
清理只删除工具未使用的 import，并把实际同时服务 v3/v4 的内部 `v3_adapter` 改名为
`completion_adapter`；兼容 wrapper、v3 路由与原始失败证据保留。依据主计划 §8.7/§8.8 保留现有
职责，不增加任务或框架；验证位置仍是既有合同和 PG→HTTP fixture，不另消耗真实模型预算。
模型原始日志的尾随空格通过本目录窄范围 Git 属性保留，内容及哈希不修改；代码/文档仍检查空白差异。

清理提交 `907eeb5f` 再次通过本地 94/94 与服务器 PG18.3 → HTTP fixture 的 14 次请求、两个 NULL
对照，真实 ledger 仍为 15。见 [清理复验](raw/cleanup/server/cleanup.json) 与 `raw/cleanup/local/`。
吸收主线文档与规则后的最终集成版本也通过本地 94/94，日志为 `raw/cleanup/local/merged-*.log`。
本次真实模型证据仍绑定 `0a1c12d3`，不改写为清理或合并提交；历史源码 SHA 按该提交读取验证。
v3/v4 继续共用 C `wire_semantic/wire_common`、Python semantic codec/session 和 PG runtime，
只保留各版本必要的入口、字段及身份检查。

推送前独立复核以 `dd8cbf89...2820dcb5` 为范围，核对四 C 计划和工程分工，重跑本地 94/94 合同，
并复验本目录 223 项公开 manifest、66 项原始源码及 2 项清理源码 SHA。未新增 PG 构建/TAP、
资源采样或真实模型请求；仅同步现役文档并移除源码中已过期的计划位置和阶段注释，不改执行逻辑。

## 全部尝试与修正

| 尝试 | 实际观测 | 处理与结论 |
|---|---|---|
| 模型启动 1 | IPC 路径超过 Unix socket 上限 | 缩短数据盘临时目录；未发模型请求 |
| 模型启动 2 | FlashInfer 调用 `ninja` 时找不到可执行文件 | 包已安装，但服务 PATH 未包含其目录；补正确 PATH，未安装新包 |
| 模型启动 3 | 日志已到 `Application startup complete`，编译缓存落在系统盘 | 主动停止，不是模型返回失败；未验证 health 或发送请求 |
| 模型启动 4 | 短 IPC、PATH 与数据盘缓存检查通过，health=200 | 相同模型/服务参数下完成下述两轮；全部启动日志保留 |
| `pg-real` / `87b7963b` | 第一次 old 请求返回 TRUE，PG 保留 1 行并记录 prompt=65/output=2；随后工具断言失败 | 工具把 BatchEncoding 的两个字段当成两个 tokens。该轮仍标失败，消费 1 次 |
| 离线反例与修复 | 两套环境返回相同 65 个 token IDs，BatchEncoding 的 `len` 却为 2 | `0a1c12d3` 显式请求 `return_dict=False`；保留红测试，修复后实际捕获请求离线复验通过 |
| `pg-real-r2` / `0a1c12d3` | 独立新目录执行原定 14 次，退出码 0 | 全部通过，累计从 1 增至 15；原轮不覆盖、不改判 |

主要日志：[首次启动](raw/server/model.log)、[ninja 失败](raw/server/model-r2.log)、
[第三次启动及主动停止](raw/server/model-r3.log)、[最终模型日志](raw/server/model-r4.log)、
[首轮工具失败](raw/server/real-run.log)、[tokenizer 离线证据](raw/server/tokens-driver.json)、
[最终调用与退出码](raw/server/real-invocation-r2.json)。缓存/路径环境修正没有改变模型、prompt 或生成配置。

准备阶段还发生过 Git bundle 尚未传完就读取的 EOF，传输完成并校验后才建立源码 worktree。
本地一次 unittest 模块路径错误、一次零项文件筛选，以及 socket bind 被沙箱拒绝均保留；
有效测试以正确 discover 路径和允许 localhost socket 的最终日志为准，不计这些尝试为通过。

## 逐输入观测与 PG 结果

指令固定为 `The input asks for writing, explaining, or debugging computer code.`。
先以第一条输入对 old/choice 各预热一次，再按输入顺序交错运行两次。下表 old/choice 每项分别列出
两次结果，均为 `finish_reason=stop`、output tokens=2，SQLSTATE 无错误。

| 合成输入 | old 两次 | choice 两次 | 每次 prompt tokens | 每次 SQL 输出行数 |
|---|---|---|---:|---:|
| Write a Python function that adds two integers. | TRUE / TRUE | TRUE / TRUE | 65 | 1 |
| Give me a recipe for tomato soup. | UNKNOWN / UNKNOWN | UNKNOWN / UNKNOWN | 64 | 0 |
| Can you explain this? | TRUE / TRUE | TRUE / TRUE | 61 | 1 |

两个预热均为 TRUE、65/2 tokens、输出 1 行。最终 14 次合计 prompt tokens=890、output tokens=28，
来自逐条服务 usage 求和；不是吞吐或成本测量。PG 每次 `Model Calls=1`、usage 与服务相同；
实际模型名、finish reason、全部 JSON 对照及 tokenizer 模板计数均核对。
两个 SQL NULL 对照 `Model Calls=0`、输出 0 行，ledger 与请求日志无增长。
完整顺序、原始 HTTP、completion、PG EXPLAIN 见 [逐请求汇总](raw/server/real-request-summary.json)
与 `raw/server/pg-real-r2/`，不把 14 次调用说成 14 个独立质量样本。

这轮真实响应没有 FALSE，不能声称真实模型覆盖了三个标签。原 parser 的 FALSE 分支由既有
deterministic/TAP 和本次 HTTP fixture 覆盖。汤食谱的 UNKNOWN 和含糊输入的 TRUE 也说明
“格式合法、PG 忠实处理”不能替代“模型选对标签”；本轮没有人工标签评测或准确率结论。

## 测试、资源与证据完整性

- 本地与服务器最终各 **94/94**：PG/protocol 68、gateway migration 5、calibration 10、预算/采集工具 11。
  [本地 PG](raw/local/postgres-r2.log)、[gateway](raw/local/gateway-r3.log)、
  [calibration](raw/local/calibration-r2.log)、[工具](raw/local/token-green-r2.log)；
  服务器见 `raw/server/*tests-r2.log`。红测试只证明新增计数问题，完整 PG TAP 仍引用历史证据。
- 修复前后各有一次实际 PG18.3 → production gateway/Adapter → HTTP fixture 的 14 次检查与两个 NULL
  对照，均通过；fixture 预算与真实模型账本分开。fixture 返回值为预定数据，不代表模型判断。
- 本轮未扩大 RSS/FD/线程测试。此前资源结论仍只绑定 `4464fe9b` 的受控规模与取消/DNS 设置。
- 结束时按 PID、启动 ticks 与进程组核对后停止自有模型；自有 gateway/PG 已退出，无本切片 listener，
  观测时 GPU 无计算进程。见 [清理观察](raw/server/recording-state.json)。不声称服务器无其他工作负载。
- 第三次退出记录一次 semaphore 清理 warning，不能凭 warning 推断持久泄漏。系统盘 FlashInfer
  sampling 目录观察到 6,454,772 bytes，因未确认全部文件归属而保留，未进行广泛缓存删除。
  后续启动已将对应缓存放在数据盘，不声称服务器所有临时文件已清空。
- 原始数据、失败日志、数据库目录、源码 worktree、Git bundle 与真实累计 ledger 在仓库外保留；
  Git 内保存脱敏副本及 [总 manifest](raw/manifest.json)。服务器原始 152 项、公开 156 项 SHA 核对通过，
  本地导入再核对公开与源码哈希；无凭据或实际服务器路径进入本结果。

## 复现与下一步

遵循 runtime runbook，先核对已有资产、服务参数、数据盘缓存和健康状态，再执行：

```bash
PYTHONPATH=code <driver-python> code/scripts/experiments/run_choice_service_checks.py \
  --repo <clean-worktree> --root <fresh-artifact-directory> --prefix <qualified-pg18.3-prefix> \
  --config <fixed-model-config> --ledger <existing-cumulative-ledger> \
  --identity <live-service-identity> --model-root <existing-model-directory> \
  --model-manifest <verified-model-file-manifest>
```

已有 ledger 不重新初始化；启动与清理脚本、实际命令、脱敏环境分别见 `raw/server/` 与 `raw/local/`。
这不是自动重跑许可。下一项由[主架构计划](../../../plans/postgresql_ai_semantic_operator_architecture_20260827.md)
定义：真实生成型 SemMap 驱动必要公共任务/结果整理，再扩展自有 PG 的多算子组合与有界多会话。
公司系统移植单独验证；本次未开发公司代码。Filter reference 的质量、成本校准和第二路径仍独立待做。
