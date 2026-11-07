# 离线算子代价分析

本目录处理既有 profile CSV、离线估计方法和参考校准观测，不进入 records 单查询执行链。

- `profiles.py` 读取 CSV、选择 formal 成功记录，并保持原上下文、候选身份和 23 维特征。
- `FEATURE_NAMES` lives with `feature_vector` in `profiles.py`; it describes the existing offline CSV schema.
  The CLI imports those names directly, while the shared Ridge implementation accepts numeric matrices without a profile schema.
- `comparison.py` 复用原数学库，按已有方法名称执行 fit/predict；LightGBM 只在对应 fit 时导入。
- profile 和估计比较命令负责参数、评估及输出，不再加载其他分析脚本或改写另一模块的输入路径。
- [semfilter_reference_calibration.py](semfilter_reference_calibration.py) 从 training／held-out 汇总观测构建并校验
  exact SemFilter reference 产物，保留原系数拟合、可辨识性检查、误差评价及身份摘要。

[通用数学实现](../../planning/costs)暂留原处。运行链的 token/work 估算、资源记账和实测反馈保持独立。
CSV 中的 `completion_http_transport` 等字段仍表示历史输入身份，不代表当前 HTTP 执行能力。

从仓库根目录调用，输入须由调用方明确选择：

```sh
python -B code/scripts/analysis/estimate_operator_cost.py \
  --input-csv /path/to/runs.csv --output /path/to/ridge.json
python -B code/scripts/analysis/compare_cost_estimators_full.py \
  --data-csv /path/to/runs.csv
python -B code/scripts/analysis/compare_cost_estimators_contextloo.py \
  --data-csv /path/to/runs.csv --output /path/to/loo.json
```

两个比较 CLI 必须选择 `--data-csv` 或 `--ref-json` 之一。后者读取含 `source_csvs` 列表的 JSON；
绝对 CSV 路径保持原意，相对路径仍从仓库根目录解析。不存在的旧默认参考 JSON 不再用作输入。
context-LOO 还须显式指定 `--output`，避免将新输入的结果写入历史目录；原 `--overwrite` 规则保持。
Ridge CLI 原有 `--input-csv`、target、split、seed 和 alpha 参数保持。

参考校准的独立命令仍显式选择输入和输出，不访问数据库或模型：

```sh
python -B code/scripts/analysis/build_semfilter_reference_calibration.py \
  --source /path/to/observations.json --output /path/to/artifact.json
```

该产物保留 `semloom.exact_filter.reference-calibrated.v1` 和 `openai-compatible-fixed` 历史身份，
不表示当前 IPC 或 records 调度自动使用它。完整产物由构建器校验，CLI 只负责文件读写与退出状态。
原 `planning` 导入已移除；原模块、命令和专测按[准确历史来源](../../../../experiments/reproduction/native_baselines/README.md#semfilter-reference-calibration-sources)恢复。

context-LOO 的 `source_reference` 指向所选 CSV 或参考 JSON，`source_csvs` 继续记录 CSV 摘要。
`code_evidence` 记录实际参与计算的两个离线模块及原估计／指标模块，不再将另一 CLI 当成计算来源。
分组、候选 repeat 聚合、tie 选择和指标数值不因本次职责整理改变。

既有研究结果和失败记录保留。完整旧来源按[历史恢复说明](../../../../experiments/reproduction/native_baselines/README.md)
与对应清单读取；旧投影不导入当前离线模块。
The former `planning.costs.regression.FEATURE_NAMES` import is not retained; recover its exact source with the
[profile-schema recovery notes](../../../../experiments/reproduction/native_baselines/README.md#operator-cost-feature-schema-source).
