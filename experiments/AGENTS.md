# experiments/AGENTS.md

继承根规则。设计/运行查 `plans/reference/对照规范.md` 与目标计划；撰写/审查结果查
`plans/reference/报告核对.md` 与原始证据；确定进度查
`results/EXPERIMENT_EVIDENCE_REGISTRY.md`与对应报告。局部文字修订只读受影响内容，入口不明时查 [README.md](README.md)。

## 1. 实验设计与授权

- 本目录统一保存方法实验、动机画像和组件验证，保持根 §2 的两项研究内容；结果按实际问题与验证类型解释。
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
- 每次运行关联实际配置、数据与源码来源、命令、环境及成功/失败记录；保留支持所报告指标的
  request/submission/resource 原始观测。保存、共享与精简按 [结果保存规则](results/AGENTS.md)执行，按根 §8 脱敏。
- 区分 database/source、organization、serialization/put、admission/queue、submit、model、fan-in、sink
  与完整 JCT；优先用 time-series 聚合，单次 snapshot 不代表稳态。
- 质量、成本、能耗和 fairness 仅在适用且测量定义完整时报告；缺项写 `unavailable + reason`，不填零或猜测。
- 结果入口放 `results/<方向>/<实验>_<日期>/`，按需使用 `README.md` 与 `raw/`；已有证据可引用单一副本。
  长期图表放 `figures/`，结果目录只引用。

## 3. 报告与结论

- 报告说明问题、实际设置、运行与证据状态、结果及不能声称的结论；按问题补充所需阶段与组件数据，
  主数字有单位、公式或来源及全部重复值。简短检查可直接记录在已有入口，不逐次复制整套报告或摘要。
- 按实际对照路径命名比较；缺臂不称完整排名，`NULL`、未采集和“未观察到错误”不写成审计为零。
- microbenchmark、单次 rehearsal 或不同 workload/签名的结果不合并为统一性能结论；负结果同样登记证据台账。
- 结论变化后按根 §7 同步证据台账、总纲及实际仍维护的图、材料和日志。
