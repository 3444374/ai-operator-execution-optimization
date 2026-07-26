# AI 算子端到端代价估计（2026-07-26）

## 复现入口

单个 grouped held-out split 使用：

```powershell
D:\Code\ai-operator-execution-optimization\.conda\pg-ai-profile\python.exe `
  code\scripts\estimate_operator_cost.py `
  --input-csv <合并后的真实 runs.csv> `
  --output experiments\results\operator_cost_estimation_20260726\e2e_cost_model.json `
  --target e2e_s `
  --test-fraction 0.25 `
  --seed 20260726 `
  --alpha 1.0
```

五个 seed 的 JSON 是独立重复执行得到的稳健性审计。输入只能使用真实 profile CSV；合并和过滤后必须保持 283 行、70 个配置组，并由脚本按配置组切分，不能先按行随机切分。

## 目标

基于已经采集的真实 profile 数据，验证仅使用执行前可知特征能否估计
AI_COMPLETE 算子的端到端时间。该模型用于后续编排决策的补充信息，不作为
独立研究贡献，也不声称带来调度加速。

## 数据与特征

输入为 `experiments/results/` 下 29 个 `runs.csv`：

- 原始 `status=ok` 行 285；
- 2 行缺少完整 packing 特征而排除；
- 最终 283 行、70 个唯一配置组。

配置组由模型、workload、batching、输出上限、token budget、K_max、flush、
arrival replay 等执行前配置签名定义。同一配置的 warm-up/formal/repeat 不会
同时进入训练和测试，避免重复运行泄漏。

特征共 15 个：

- 行数、prompt token 总量、completion 上限；
- token budget、组织后的 batch 数；
- batch 估计代价 P50/P95/max；
- K_max；
- flush timeout/max wait；
- arrival scale、是否 arrival replay；
- adaptive/immediate flush 指示变量。

实际输出 token、实测 E2E/service、vLLM 指标、能耗和 MFU 均不作为特征。
目标为 `e2e_s`。

## 模型与指标

模型为标准化特征上的 log1p ridge regression（alpha=1），对照为训练集目标
均值。主切分使用 seed 20260726、25% 配置组留出：

| 模型 | 测试行 | MAE (s) | MAPE | RMSE (s) | R² |
|---|---:|---:|---:|---:|---:|
| 训练均值 baseline | 87 | 29.427 | 255.35% | 55.805 | -0.004 |
| ridge cost model | 87 | 9.728 | 32.58% | 34.302 | 0.620 |

为避免只报告有利切分，又对连续 5 个固定 seed 做同样的分组留出审计：

| seed | 测试行 | MAE (s) | MAPE | RMSE (s) | R² |
|---:|---:|---:|---:|---:|---:|
| 20260726 | 87 | 9.728 | 32.58% | 34.302 | 0.620 |
| 20260727 | 58 | 15.058 | 68.16% | 29.682 | 0.788 |
| 20260728 | 85 | 8.957 | 30.76% | 16.905 | 0.833 |
| 20260729 | 39 | 13.578 | 90.60% | 29.045 | 0.858 |
| 20260730 | 85 | 11.091 | 30.92% | 19.502 | 0.781 |
| 均值 | — | 11.682 | 50.60% | 25.887 | 0.776 |

五个切分的 ridge MAE 均优于对应 mean baseline；mean baseline MAE 范围为
27.621–34.651s。

## 结果解释

**事实**：执行前特征对 E2E 具有可用但不稳定的解释力。主切分 R² 为 0.620，
跨 seed 范围为 0.620–0.858；MAPE 范围为 30.76%–90.60%，说明小目标和留出
配置组合会显著影响相对误差。

**事实**：五个切分上 ridge 均降低 MAE，平均 MAE 从 mean baseline 的
29.890s 降至 11.682s。

**推断**：当前模型适合做粗粒度容量/编排提示，不适合作为严格 SLO 预测器。
最大的不可观测因素是自然 EOS 下的实际输出长度；completion 上限只是执行前
代理变量。

**不能声称**：

- 不能把当前 MAPE 写成跨模型泛化精度；
- 不能使用测试集选择 alpha 或特征后再报告同一测试集；
- 不能把预测相关性等同于在线调度收益；
- 不能在没有模型/模态重训或校准时直接用于多模态。

## 工程产物

- `code/src/cost_estimation.py`：分组切分、ridge estimator、回归指标；
- `code/scripts/estimate_operator_cost.py`：从 profile CSV 构造可复现模型；
- `e2e_cost_model.json`：主切分完整特征 schema、系数、均值/尺度和指标；
- `e2e_cost_model_seed_*.json`：五个稳健性切分。

后续若用于在线编排，应先增加独立时间段/新 workload 留出，并输出预测区间或
保守上界，而不是只返回点估计。
