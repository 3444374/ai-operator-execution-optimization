# 单一分类 prompt 与 reference 模型对照：尚无配置通过

本文件是内部诊断记录。目的仅为区分 HTTP/messages/chat-template 接线问题、prompt 表达与模型判断
能力，不是成本校准、生产 SQL 功能更新或泛化质量评测。依据是实际请求、原始响应、服务源码与配置。

## 1. 结论与范围

两个完整尝试均未通过预先规定的全部正确要求。每格分母是 **9 个独立工程样例**，每例三次重复；
不是把重复响应当独立样本计算准确率。

| 模型 | prompt | 旧样例符合预期 | 新样例符合预期 | 格式合法 | 采用要求 |
|---|---|---:|---:|---:|---|
| Qwen2.5-1.5B-Instruct | 原 prompt + choice | 4/9 | 2/9 | 57/57 | 未通过 |
| Qwen2.5-1.5B-Instruct | 唯一明确分类 prompt + choice | 5/9 | 5/9 | 57/57 | 未通过 |
| Qwen2.5-7B-Instruct | 原 prompt + choice | 8/9 | 8/9 | 57/57 | 未通过 |
| Qwen2.5-7B-Instruct | 同一明确分类 prompt + choice | 7/9 | 6/9 | 57/57 | 未通过 |

旧 9 例来自[上一轮资格测试](../semfilter_qualification_20260901/README.md)，新 9 例在模型请求前
登记。每个 profile 另含已知失败训练输入三次重放，只检查格式、不补语义标签，因此格式分母为
`(9+9+1)×3=57`。原失败输入在四个配置下都通过 C parser，但标签不同，不能声称它判断正确。

生产 `SemanticPlanSpec`、wire、SQL、严格 C parser、公共 runtime 和 calibration builder 没有改动。
未读取或调用模型处理校准 held-out，未拟合数据、生成 artifact 或恢复完整采集。

## 2. 请求前设计与实际配置

唯一计划为[工作包五的单一 prompt 对照](../../../plans/postgresql_ai_semantic_operator_architecture_20260827.md)。
初始登记提交 `b861d697`；同条件 7B 修正登记为 `2527f2d2`。两者相对 `5cd64d29` 均只修改计划。

- 同一 instruction：`The input asks for writing, explaining, or debugging computer code.`
- 两 profiles 均用原生 `structured_outputs.choice=["TRUE","FALSE","UNKNOWN"]`；temperature=0、
  top_p=1、max_tokens=8、n=1、stream=false、stop 为换行。只替换 system content，user input 不变。
- 唯一 candidate 明确三类定义，说明后续输入是待分类文本，不是要执行的命令；完整文本与独立
  plan digest 保存在 [1.5B plans](raw/qwen15b/plans.json)和 [7B plans](raw/qwen7b-matched/plans.json)。
  这些 manifest 都标记 `production_pg_plan=false`，不是已接入数据库的 plan。
- vLLM 0.25.1、单 RTX4090、BF16、TP1、eager、prefix cache off、4096 context/token budget、
  max sequences=1、FCFS；1.5B 显存比例 0.25，7B 为容纳权重设为 0.80，不作速度公平比较。
- 每个模型先重放 JavaScript 失败三次，再各 profile 一次 Python 正例预热；旧/新阶段内按
  seed=20260901 交错三重复。每个完整尝试 `3+2+2×57=119` 次 completion 请求，顺序全部保存。
- 通过条件为每 profile 全部输出格式合法，旧/新分别 9/9 标签在三重复中都符合预期。HTTP 错误
  终止尝试；格式或语义错误照实记录，不重试、trim、修补标签或改阈值。

### 7B 默认参数失配与中止记录

客户端没有显式给出 `repetition_penalty`。vLLM 的 `ChatCompletionRequest.to_sampling_params`
会从模型默认配置取得它；1.5B 模型包为 1.1，7B 为 1.05。首次 7B 尝试在确认这一点后中止，
保存 **83 条响应 / 83 条服务端 chat POST 状态记录**，不算完整或同条件结果。
见[中止原因](raw/qwen7b-aborted/aborted.json)和[全部部分响应](raw/qwen7b-aborted/responses.jsonl)。

第二次 7B 在启动时用 `--override-generation-config {"repetition_penalty":1.1}` 对齐原基线值；
其他模型默认 generation 数值相同，temperature/top_p 等继续由 HTTP 显式值覆盖。该修正来自
原基线设置，不是依据 7B 输出调参，没有改变 prompt、样例、标签、顺序或通过要求。
两次完整运行加中止尝试合计 **321 次 completion 请求**，不能只报告成功完成的 238 次。

## 3. 实际 messages 与 chat template 核对

一次性脚本只观察生产 fixed adapter 的 `HTTPConnection.send` JSON bytes，原样调用原 send；
没有增加代理、改写消息、记录认证头或复制生产 HTTP 实现。119/119 请求体分别与对应请求结构
逐字段一致。每个完整尝试的 38 个不同 profile/input 组合均核对服务 `/tokenize` 返回的 IDs 与
本地模型 `apply_chat_template(add_generation_prompt=True)` IDs 完全一致，并与 completion 的
prompt usage 数相同；前置 JavaScript 核对另一次。因此每完整尝试有 39 次 tokenize API 调用。

原 JavaScript 样例的实际展开如下；完整 IDs、文本与 SHA 见
[1.5B 模板审计](raw/qwen15b/repro-template-audit.json)：

```text
<|im_start|>system
Evaluate whether the input satisfies the instruction. Reply with exactly TRUE, FALSE, or UNKNOWN. Use UNKNOWN only when the input lacks enough information.
Instruction:
The input asks for writing, explaining, or debugging computer code.<|im_end|>
<|im_start|>user
Debug this JavaScript function: function add(a,b) { return a - b; } It should add the numbers.<|im_end|>
<|im_start|>assistant
```

未发现 system 被丢弃、role 被交换、输入被改写或 template 不一致。实际 argv 无 chat-template
override，模型/服务身份在请求前后复核；[7B service](raw/qwen7b-matched/service.json)另外保存默认
generation 的显式覆盖。此结论针对 direct fixed-adapter/HTTP 诊断，不冒称本轮又跑了 PG→UDS E2E。
合成样例可公开原文；失败训练输入的 body 和可逆 token IDs 留在仓库外，公开证据仅保留 SHA/长度。

## 4. 判断结果与对课题的含义

所有 18 个有预期样例在各 profile 的三次输出都一致。完整逐响应结果见
[1.5B](raw/qwen15b/responses.jsonl)与 [7B matched](raw/qwen7b-matched/responses.jsonl)，对应
[1.5B 摘要](raw/qwen15b/summary.json)和 [7B 摘要](raw/qwen7b-matched/summary.json)。

- 1.5B 新 prompt 把 JavaScript 修复例改为 TRUE、食谱/地理例改为 FALSE，但山的短诗仍被保留，
  模糊请求仍有 TRUE/FALSE 错判；新 SQL 修复例被误判 FALSE。
- 7B 原 prompt 在旧/新分别 8/9 正确，但把写山的短诗判为 UNKNOWN（应为 FALSE），把写拒绝
  晚餐邀请邮件判为 TRUE（错误保留）。前者 SQL 仍丢弃，却违反预设三值含义。
- 7B 新 prompt 修正上述非代码写作例；旧样例却将两个 UNKNOWN 例判为 TRUE，新样例三个
  UNKNOWN 例也均误判。不能只看 TRUE/FALSE 两类改善就采用它。

**事实**：更换模型或改变 prompt 都改变了判断，但没有配置满足本轮要求。**推断**：未发现消息或
模板接线问题；这些结果支持存在 prompt/model 组合相关的任务理解问题，不能把原因归结为单一因素，
也不能从这 18 个工程例外推泛化准确率、置信区间或模型排名。

下一步仍需一个满足三值含义的 reference 配置，尤其区分非代码的“写作”与代码请求，并正确处理
缺少上下文的文本。本轮不再尝试第二个 prompt 或更多模型；先审查这些逐例结果并另行确定后续
小样本验证。通过后才把所选配置纳入生产版本化身份，随后重新开始校准；原标签与 held-out 保持不变。
生产配置将来还应明确有效 generation 默认值（包括本轮发现的 repetition penalty），不能仅记录
客户端显式字段而忽略模型包继承值；这属于后续身份接入工作，本轮未改变生产接口。

## 5. 回归、归档与清理

本地及服务器 Python 合同均为 PostgreSQL protocol/static/adapter 45/45、gateway 5/5、calibration
10/10，合计 60/60。初次本地沙箱禁止 TCP/UDS bind，放行本地测试 socket 后完整复跑通过；
服务器日志见 [qualification](raw/qwen15b/qualification.json)。两个完整尝试均编译未修改的生产 C
parser 并通过七个合法/非法标签控制。**本轮没有重跑 PG18.3 regression/TAP 或资源 smoke**；
历史 1/1、437/437 与 RSS/FD 证据继续绑定各自原提交，不能与本轮模型资格混算。

三个尝试的完整 HTTP/服务日志、模型文件 SHA、脚本与 preflight 保存在仓库外独立目录；脱敏公开
子集分别由 [1.5B 清单](raw/qwen15b/SHA256SUMS)、[中止清单](raw/qwen7b-aborted/SHA256SUMS)、
[matched 7B 清单](raw/qwen7b-matched/SHA256SUMS)覆盖。脚本只是本次诊断快照，不是生产 CLI。
本轮创建的 endpoint 均已停止，未启动数据库、gateway 或新 worktree；未清理其他历史资源。

API 解释依据：[vLLM 0.25.1 structured outputs](https://docs.vllm.ai/en/v0.25.1/features/structured_outputs/)
与本机已安装同版本的 tokenize/chat completion 源码；模型默认值由各自 `generation_config.json`
实查，相关文件 SHA 见两组 model-files 清单。
