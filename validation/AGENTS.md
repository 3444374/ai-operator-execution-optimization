# validation/AGENTS.md

本目录维护可行性验证实验、benchmark 代码和实验结果。进入本目录前先读根目录 `AGENTS.md`。

## 作用

- 用最小实验验证技术路线是否有可观察瓶颈。
- 记录 Phase 0 的可复现实验、CSV 结果和自动分析报告。

## 代码规则

- 遵循 `karpathy-guidelines`：目标明确、改动克制、先验证再下结论。
- 新实验必须有明确问题、命令、CSV 输出和结果解释。
- 不混淆数据生成、序列化、`ray.put`、fan-in、写回等时间边界。
- warm-up 要忽略或明确标注。

## 当前证据

- Ray small task 不是当前最强瓶颈。
- Arrow IPC 本身不是当前明显瓶颈。
- Ray many-object fan-in 有放大。
- Arrow RecordBatch `N upstream -> P downstream` fan-in 是当前最贴近课题的本地证据。

## 下一步边界

本目录继续做可行性实验；端到端 AI 算子动机实验应放到 `motivation/` 或未来工程代码目录。
