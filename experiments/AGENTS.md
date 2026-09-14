# experiments/AGENTS.md

继承根规则。设计/运行查 `plans/baseline_reference.md` 与目标计划；撰写/审查结果查
`plans/reference/experiment_report_honesty_checklist.md` 与原始证据；确定进度查
`plans/experiment_status_and_gaps.md`。局部文字修订只读受影响内容，入口不明时查 [README.md](README.md)。

## 1. 实验设计与授权

- 本目录研究方法的效果、成本和适用条件，保持根 §2 的两项研究内容；动机画像与组件验证进入各自目录。
- 新实验先有当前计划，写明问题、对应研究内容、系统职责、baseline、变量、消融、correctness、
  资源上限、指标、重复、停止条件和不能声称的结论。
- rehearsal/formal 需要用户授权覆盖计划、环境、资源预算和停止条件，并先通过环境 preflight、
  数据/schema/exactly-once 和最小 correctness。已有授权在范围内持续有效；计划文本不授予执行权限。
  失败后不自动重跑、重置预算或放宽阈值。
- 优化臂与强静态点或同上限配置对照；动态策略只有显著优于同上限静态配置才可作为更优方法。
- baseline 由被测系统拥有执行与调度；SemLoom FIFO/DRR/VTC-style/actor pool/credit 等仅作为
  internal control 或 diagnostic。历史 `Project`/`project_*` 按原 schema 解释，不改成官方系统身份。
- machine/model/service/protocol/workload 签名和阈值来自当前计划与 runtime 报告，不能从历史结果继承。

## 2. 运行与记录

- 按目标计划执行 warm-up、交错重复和健康/饱和/稳定性检查；强制条件失败时保留失败或诊断结果，停止策略结论。
- 每次运行保存 resolved config、manifest、upstream/provenance、command、环境报告、request/submission/
  resource trace 及成功/失败记录；按根 §8 脱敏。
- 区分 database/source、organization、serialization/put、admission/queue、submit、model、fan-in、sink
  与完整 JCT；优先用 time-series 聚合，单次 snapshot 不代表稳态。
- 质量、成本、能耗和 fairness 仅在适用且测量定义完整时报告；缺项写 `unavailable + reason`，不填零或猜测。
- 结果保存到 `results/<方向>/<实验>_<日期>/{README.md,raw/}`；长期图表放 `figures/`，结果目录只引用。

## 3. 报告与结论

- README 覆盖目的、设置、运行要求检查、设计、全组件数据、事实/推断/不能声称、课题含义和下一步；
  主数字有单位、公式或来源及全部重复值，按内容组织，不要求机械套用顺序。
- 按实际对照路径命名比较；缺臂不称完整排名，`NULL`、未采集和“未观察到错误”不写成审计为零。
- microbenchmark、单次 rehearsal 或不同 workload/签名的结果不合并为统一性能结论；负结果同样登记证据台账。
- 结论变化后按根 §7 同步证据台账、总纲及实际受影响的图、开题材料和日志。
