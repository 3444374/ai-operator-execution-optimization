# experiments/AGENTS.md

本目录维护课题正式研究实验。进入本目录前先读根目录 `AGENTS.md`、`PROJECT_INDEX.md`，再读本文件和 `README.md`。

## 作用

- 承接开题报告中的两项策略设计与多模态泛化验证：数据组织策略（研究内容一）、运行层调度与提交控制策略（研究内容二）、多模态泛化验证（正文实验）。写回瓶颈判定作为实验设置中的验证性内容，不作为独立研究内容。
- 记录优化方案、消融实验、对照实验、正式结果和小范围调优测试。
- 在动机测试已经说明“为什么值得做”之后，回答“具体方法是否有效、有效在哪里、边界是什么”。

## 与其他目录的边界

- `motivation/`：只负责课题动机、为什么值得做、初步系统画像和瓶颈信号；不承担完整研究实验规划。
- `feasibility/`：只负责环境、组件、脚本和连接可用性验证；不承担论文方法有效性结论。
- `code/`：保存可复用实现、脚本和测试代码；`experiments/` 保存实验计划、运行记录和结果解释。
- `figures/`：保存正式图资产；本目录结果需要画图时，图放到 `figures/data/` 的合适子目录，不能把图分散在实验结果目录里长期维护。

## 目录结构

- `plans/`：按研究内容组织的正式实验计划、变量、baseline、消融和评价指标。
- `results/`：正式研究实验结果、优化测试记录、CSV 说明和结论边界。

后续如果某一研究内容开始有稳定代码，可在 `code/` 中建立实现；本目录只记录实验设计和结果。

## 规则

- 每个实验必须写清楚研究问题、对应研究内容、运行命令、参数、CSV 路径、指标、结果解释和不能声称的结论。
- 优化实验必须有 baseline 和消融，不只汇报优化后数字。
- 小改动调优也要记录：改了什么、为什么改、对哪个指标有影响、是否可复现。
- GPU-backed E2E 链路优先于 CPU/fake 简化实验；CPU/fake 只能作为调试或历史对照。
- 不把动机测试结果写成方法贡献；动机测试只说明为什么值得继续做。
- 修改本目录后，同步检查 `README.md`、`PROJECT_INDEX.md`、`PROJECT_OUTLINE.md`、`overview/current_direction_and_plan.md`、`opening/report/opening_report.md` 和 `figures/README.md` 是否需要更新。
- 实验设计和结论遵循 `karpathy-guidelines`：不确定就标注不确定，先定义可验证目标，做最小实验，每个结论区分事实/推断/待确认。
- 实验图统一放在 `figures/`；做图前先读 `figures/AGENTS.md`。设计论文级核心图时参考 `figure-designer`，投稿级质检参考 `nature-figure`。

## 结果边界与归档（多路径 scale/calibration sweep）

下列是正式 run 在**报告措辞**与**落盘归档**上的硬性边界（复审第四轮确立；违反 = 过强结论 / 证据不可复现）。可勾选投影见 `experiments/plans/experiment_report_honesty_checklist.md` §8。

- **缺臂如实命名**：sweep 未含全部对照臂（如缺 `project_static`）时，称"N 条系统路径的 scale/calibration sweep"，**不**称"完整三臂正式排名"；只答所含路径的容量曲线/稳定性/规模拐点差异，**不**答"项目方法是否优于 baseline"（须补齐缺臂、同合同重跑后才能）。
- **指标必附代码公式 + 行号**：报任何派生指标（skew/CV/ratio）写明代码精确公式与行号，不给裸数字。例：后端平衡 skew = `_backend_skew` = `abs(a-b)/max(a,b)` = (max-min)/max（`code/scripts/baselines/multicard_scale_ramp.py:366`），127:129 = 1.55%；**不**用 (max-min)/sum（=0.781%，代码不用）。gate 阈值 10% 也对 /max。
- **finish_reason 措辞**：DuckDB-ai 扩展该字段为空 → 写"0 error/NULL、未观察到 max_tokens truncation error"，**不**写"已审计 0 length"（空 ≠ 审计非 length）。
- **跑完归档清单**（每次正式 run 落盘到 results 目录）：两个 vLLM 进程的完整 cmdline + strict-preflight 输出（证 declared==effective）；vLLM/model revision、dtype、tensor-parallel、gpu-memory-utilization；nginx conf SHA（gateway 轨）；每 cell warmup/formal 身份 + service counters + request-skew + token-work-skew（均 (max-min)/max）；query JCT 与 request E2E **分列**；失败 cell 完整落盘；reps≥3 用 sample CV(n-1) + 报告**全部单次值**（不只 mean）。
