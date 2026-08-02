# 短/长 prompt 静态 request/work credit 筛选（2026-07-30）

## 1. 实验设置

本实验对应研究内容二，问题是：prompt 长度变化时，最佳静态 request credit
或 token-aware active-work credit 是否稳定迁移；如果固定 65,536 work /
endpoint 已同时覆盖短、长 workload，则不应继续为单 job workload 漂移增加
动态 K 控制器。

远端提交为 `f203257` 代码快照，硬件为 2×RTX 4090，模型为
Qwen2.5-7B，vLLM 0.25.1，`max_num_seqs=256`、
`max_num_batched_tokens=8192`，prefix cache 关闭。两组均使用 raw
Completions、request 粒度、1 actor/endpoint、256 concurrency/actor、
per-endpoint admission、manifest-pinned routing、immediate flush 和
30 秒 request SLO。

需要特别说明：配置未显式设置 async transport，`runs.csv` 实际记录为
`urllib`；也没有启用 `completion_return_token_ids`。因此本轮不是当前冻结的
async feeding 合同，只能作为静态 credit 的 screening / 机制审计，不能用于
项目峰值或正式动态 GO/NO-GO。

| workload | 行数 | server-observed prompt tokens/row | prompt P50/P95/P99 |
|---|---:|---:|---:|
| short (`<50`) | 512 | 16.95 | 14 / 42 / 48 |
| long (`>=150`) | 325 | 566.23 | 418 / 1,149 / 1,354 |

vLLM aggregate generation tokens/row 的中位数分别为 236.60 和 245.01。
per-request 实际 output token P50/P95/P99 未采到；request trace 中相关字段为
0/空值，不能用 whitespace count 冒充模型 tokenizer 的实际 token 数。

## 2. 实验设计

每个 workload 各运行六个臂：

- request count：`K={64,128,256}` / endpoint；
- active work：在 K256 safety cap 下使用
  `work={32768,65536,98304}` / endpoint。

每臂 1 warm-up + 3 formal，并在各 workload 内按 repeat 随机顺序执行。
总计 48/48 run 成功，其中 12 warm-up、36 formal。

本轮被远端描述为 “K×active-work”，但实际是两组一维臂，并非 K 与 work 的
笛卡尔积。short 全部执行完后才执行 long，也没有跨 workload 交错。因此它能
筛查候选区间，但不能隔离 K×work 交互或时间漂移。

复现材料：

- [`short/runs.csv`](short/runs.csv) 与
  [`long/runs.csv`](long/runs.csv)：正式运行主表；
- [`short/manifest.json`](short/manifest.json) 与
  [`long/manifest.json`](long/manifest.json)：解析后的配置、顺序和运行状态；
- [`config_short.json`](config_short.json) 与
  [`config_long.json`](config_long.json)：远端运行配置；
- [`formal_summary.csv`](formal_summary.csv)：按正式中位数口径重新汇总；
- [`decision_audit.json`](decision_audit.json)：可机读 GO/NO-GO 审计。

生成审计：

```bash
python3 code/scripts/analysis/summarize_static_credit_workload_surface.py \
  --surface short=experiments/results/static_credit_prompt_length_screen_20260730/short/runs.csv \
  --surface long=experiments/results/static_credit_prompt_length_screen_20260730/long/runs.csv \
  --output experiments/results/static_credit_prompt_length_screen_20260730/decision_audit.json \
  --summary-csv experiments/results/static_credit_prompt_length_screen_20260730/formal_summary.csv
```

## 3. 严谨性自检

通过项：

- 两组 manifest 均为 `completed`，48/48 run `status=ok`；
- 每臂 3 个 formal repeat，无 skip、incident 或 actor failure；
- 两组共享模型、协议、SLO、actor pool、routing、flush 和 cache 设置；
- prompt 长度差异足够大，vLLM aggregate prompt/output token delta 完整。

未通过项：

1. 当前项目正式选择使用中位数，而远端初始结论使用算术平均；两者对 short
   active-work oracle 给出不同答案。
2. short `w65536` 与 `w98304` 的 model-request throughput CV 分别为
   18.3% 和 33.9%，超过 5% 稳定性门槛。
3. short `k256/w65536/w98304` 均无 bounded wait，三臂都一次性放行 512
   个请求；work 高水位仅 49,318/endpoint，65K 和 98K cap 均未绑定。
   理应近似等价的三臂 model-request throughput 中位数却相差 48.5%。
4. `urllib` 不是冻结的 async transport；不能外推到项目最佳 feeding 路径。
5. 未启用 token IDs，缺少实际 per-request output token 分布。
6. 两个 workload 未交错；行数既不相同，总 offered token work 也未匹配。
7. 六臂不是 K×work full factorial，不能把差异唯一归因于 token-aware
   credit。

因此机器审计结果为 `inconclusive`，下一动作是
`rerun_controlled_static_credit_gate`。

## 4. 实验数据

以下均为 formal 中位数；吞吐使用项目正式的 model-request 时间边界。
SLO violation 为三次 formal 的均值，保留其不稳定性。

| workload | arm | model-request tok/s | E2E | P95 | SLO violation | SLO goodput | MFU |
|---|---|---:|---:|---:|---:|---:|---:|
| short | K64 | 4,434 | 30.84s | 29.83s | 5.40% | 16.47/s | 18.91% |
| short | K128 | 4,581 | 30.22s | 29.98s | 8.20% | 16.31/s | 19.53% |
| short | K256 | 2,785 | 48.72s | 48.54s | 82.16% | 0.74/s | 11.88% |
| short | W32K | 3,818 | 36.40s | 35.71s | 66.86% | 6.29/s | 16.28% |
| short | W65K | 4,949 | 27.19s | 26.98s | 5.92% | 18.83/s | 21.10% |
| short | W98K | 5,406 | 25.25s | 24.94s | 16.41% | 20.28/s | 23.05% |
| long | K64 | 8,503 | 33.02s | 32.82s | 18.56% | 8.02/s | 35.21% |
| long | K128 | 7,747 | 36.04s | 35.50s | 65.64% | 4.13/s | 32.07% |
| long | K256 | 7,256 | 38.74s | 37.85s | 87.79% | 1.19/s | 32.68% |
| long | W32K | 7,987 | 35.03s | 34.89s | 27.79% | 6.68/s | 33.07% |
| long | W65K | 9,340 | 30.06s | 29.94s | 13.44% | 10.81/s | 38.67% |
| long | W98K | 7,491 | 37.35s | 37.11s | 73.85% | 1.77/s | 31.01% |

远端最初表格使用 E2E `tokens_per_s` 的三次均值，因而把 short W65K
报告为 4,853、W98K 报告为 4,517，并选择 W65K。按预注册的
model-request 中位数口径，short 候选反而是 W98K，long 是 W65K；交叉
SLO-goodput regret 分别为 7.14% 和 83.65%。但这个“迁移信号”来自未通过
稳定性与等价臂审计的数据，不能触发动态实验。

## 5. 结果解释

### 事实

- long workload 中 W65K 三次 model-request throughput CV 为 3.18%，work
  高水位 65,532，说明 65K cap 确实绑定；它同时优于 long 的其他五臂。
- request-count K256 在两组都造成明显 SLO 退化，说明“把 request cap
  无限放大”不是可靠默认。
- short W65K/W98K 没有形成 active-work 背压，因此它们之间的性能差不能
  解释成 work limit 效果。
- 缺少 async transport 和 per-request output token IDs，违反了本轮原始
  指导要求。

### 推断

65K 是值得继续保留的共同静态候选，尤其在 long workload 上信号清晰。
token-aware work credit 也比单纯扩大 request K 更有希望成为稳定默认。
但是 short 侧的高方差和等价臂分裂说明服务状态、运行顺序或未观测路径仍在
影响结果；当前证据不足以证明 65K 已自动吸收全部 workload 漂移。

### 待确认

先执行一个最小机制门禁：同一 async 配置、同一 manifest，交错运行
K256、K256+W65K、K256+W98K；如果 W65K/W98K 均未绑定，三臂必须在 5%
内且至少 2/3 repeats 同向稳定。门禁通过后，再在 short/long 单一 runner
中交错扫描 W32K/W49K/W65K/W98K，并补齐 K×work 交互。

### 不能声称

- 不能声称 short-opt=long-opt=65K 或交叉损失为 0；
- 不能声称动态 K 已被最终否决；
- 不能声称中间 prompt 长度必然与两个端点相同；
- 不能声称 request 粒度结果一定推广到 batch 粒度；
- 不能用本轮绝对 tok/s 代表项目 async 上限。

## 6. 对课题的含义

这轮最有价值的结果不是一个过早的 NO-GO，而是明确了动态路线的判决顺序：

1. 先验证静态点迁移是否真实、稳定且有至少 5% 错配代价；
2. token-aware 静态 credit 是动态控制必须击败的强 baseline；
3. 不通过等价臂、稳定性和 token 观测门禁时，禁止增加 AIMD/PID/UCB；
4. 单 job workload 漂移若最终 NO-GO，研究内容二收敛到 shared credit、
   idle borrowing 和异质多 job fair queue。

## 7. 下一步

1. 使用 `httpx_async` 和 `completion_return_token_ids` 重跑最小等价臂门禁；
2. 在同一 runner 中交错 short/long，分别报告相同行数与等 offered work；
3. 增加 W49K，验证共同 65K 相对各 workload oracle 的 regret；
4. 只有静态最优稳定分离且错配损失 ≥5%，才运行 endpoint-local adaptive；
5. 无论单 job 动态是否晋级，继续异质 1/2/4-job shared credit、idle
   borrowing、weighted fairness 正式实验。
