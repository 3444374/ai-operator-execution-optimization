# experiments/AGENTS.md

本文件继承根 `AGENTS.md`，只增加正式研究实验规则。进入计划或结果子目录时继续读取其
`AGENTS.md` 和 `README.md`。

## 1. 职责与边界

- `plans/` 保存当前计划、已完成合同、长期参考和历史方案；`results/` 保存方法实验原始证据与解释。
- 本目录回答“方法是否有效、在什么条件下有效、代价和失效条件是什么”。
- `motivation/` 回答为什么值得研究；`feasibility/` 回答组件/环境是否可用；`code/` 保存实现；
  `figures/` 保存长期图资产。
- 数据组织和提交/路由/多 Job 调度是两项研究内容；代价估计是共同支撑，多模态是验证角色，
  写回是工程 baseline。不得因实验数量增加新的平级研究内容。

## 2. 实验授权与设计

- 每个新实验先有一个当前计划，明确研究问题、对应研究内容、系统边界、baseline、变量、消融、
  correctness、资源上限、指标、重复、停止条件和不能声称的结论。
- 先通过环境 preflight、数据/schema/exactly-once 和最小 correctness；只有计划明确授权后才运行
  rehearsal/formal。旧计划中的 `formal` 指令不自动获得当前执行授权。
- 优化臂必须与强静态点或同上限对照；动态策略只有显著优于同上限静态配置才可晋升。
- baseline 由被测系统拥有执行与调度。Project 实现的 FIFO/DRR/VTC-style/actor pool/credit 等只能
  作为 Project internal control 或 diagnostic，除非直接运行了相应官方系统。
- 正式 run 的具体 machine/model/service/protocol/workload 签名和阈值来自当前计划及 runtime 报告，
  不从历史结果或本规则文件继承。

## 3. 运行与落盘

- 按目标计划执行 warm-up、交错重复、健康/饱和/稳定性检查；任一强制条件失败时停止策略结论，
  将该 run 作为失败或诊断证据保留。
- 每个 run 保存 resolved config、manifest、upstream/provenance、command、环境报告、request/submission/
  resource trace、成功与失败记录；敏感内容按根规则脱敏。
- 统一区分 database/source、organization、serialization/put、admission/queue、submit、model、fan-in、
  sink 和完整 JCT；优先使用 time-series 聚合，不把单次 snapshot 当稳态指标。
- 质量、成本、能耗和 fairness 只在适用且观测合同完整时报告；不可用字段写 `unavailable + reason`，
  不填零、不事后猜测。
- 结果落到 `experiments/results/<方向>/<实验>_<日期>/{README.md,raw/}`；绘图脚本和长期图放
  `figures/`，结果目录只引用。

## 4. 报告与结论（结果边界）

结果 README 依次覆盖：目的、设置、合规自检、设计、全组件数据、事实/推断/不能声称、对课题含义、
下一步。所有主数字给出单位、公式/来源和全部重复值。

- 缺少对照臂时按实际路径数命名，不称完整排名；
- `NULL`、未采集和“未观察到错误”不写成审计为零；
- microbenchmark、单次 rehearsal 和不同 workload/签名的结果不合并成统一性能结论；
- 负结果和策略未晋升同样进入证据台账。

详细门禁、指标与措辞检查只从以下长期参考读取：

- `plans/baseline_reference.md`；
- `plans/reference/experiment_report_honesty_checklist.md`；
- `plans/experiment_status_and_gaps.md` 顶部当前摘要；
- 目标实验计划与结果 README。

实验结论变化后同步实验证据台账、`PROJECT_OUTLINE.md`、受影响的图/开题材料和 `PROJECT_LOG.md`。
