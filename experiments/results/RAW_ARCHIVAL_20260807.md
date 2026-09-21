# 实验数据存放政策（2026-08-07 修订）

## 政策

- **raw（per-run requests/submissions/resources/flush CSV + stdout/stderr log + artifacts + during-cell gauges）**：
  存 AutoDL 服务器 `/root/autodl-tmp/{ai-operator/experiments/results, ai-operator/motivation/results, experiment-artifacts}`
  + 开发者本地机 `C:\Users\ays\Desktop\results\`（2026-08-07 全量镜像）。**不进 git。**
- **git 只放**：aggregated CSV（runs.csv / formal_summary.csv）、summary（ramp_aggregate.{json,md} / ramp_run.json）、
  manifest、README、代码、计划文档。

## 本地 raw 镜像（C:\Users\ays\Desktop\results\，2026-08-07）

3 个 transport tar.gz 从服务器打包下载，sha256 与服务器逐一校验一致：

| archive | size | sha256 | 内容 |
|---|---|---|---|
| experiments_results.tar.gz | 189M | `1c9e940d6a190f5cc6cc97008ed74d56dd825a0da6f2552edfb941f05a41377c` | experiments/results/ 全量（10082 entries，含全部正式 + screening 实验 per-run raw） |
| experiment_artifacts.tar.gz | 57M | `e3d586fcb3ba7399d4598a908f856a390f15d0e2fd34c85aebf3e0d0283769cc` | experiment-artifacts/ 去 retired-worktrees（7813 entries；含 320-run raw 67M、highcv-rerun 89M、gates、image gates） |
| motivation_results.tar.gz | 1.2M | `de4198e81c0fa4894be2da88ea71039315ab0a97de29147579a2896cf208eff4` | motivation/results/（174 entries，image CLIP/embed 实验） |

解压后约 1.2G，镜像服务器目录结构（`experiments/results/<exp>/`、`experiment-artifacts/`、`motivation/results/`）。

## 订正

- 2026-08-07 早些曾称"320-run per-request raw 已被服务器清理"——错误。raw 实际在
  `experiment-artifacts/dual_gpu_cost_profile_formal_v2_cache_on_20260807/`（67M），已随 experiment_artifacts.tar.gz 落本地。

## 已撤回的 git tarball（同日早些）

曾把 7 个实验 raw 打成 tarball 提交 git（commit 40471bf：07-29 dual_gpu batch 5 个 + enhanced ramps 2 个）。
按本修订政策已 force-push 撤回（reset 回 c250e19），raw 改存本地。formality 三审记录（6-dim 对抗式 workflow，
24 候选，T1/T2/T3 分类）见会话 workflow journal 与 PROJECT_LOG 2026-08-07。

## 已迁出的历史 raw（2026-09-21 执行）

`operator_cost_profile_pilot_20260804/v1_diagnostic_raw.tar.gz` + `v2_raw.tar.gz`（更早 commit 的 pilot/diagnostic
raw，被该实验 README 引用）已随下方迁出一并从 git 跟踪移除。

2026-09-21 按本政策补做历史迁出：2026-07 文本轨道、`rc1_*`、`multicard_*` 与上述 pilot 共 34 个结果目录的
2,380 个 per-run 原始文件（requests/submissions/resources/flush 等 trace、日志、raw/ 内容，git blob 合计约
339 MB）从 git 跟踪移除。迁出前以 git blob 内容（LF 归一化后）为准逐文件 SHA-256 核对，AutoDL
`/root/autodl-tmp/ai-operator/experiments/results/` 与本地 `C:\Users\ays\Desktop\results\experiments\results\`
两处镜像均 2,380/2,380 一致。受影响 README 已加注镜像位置；git 内保留聚合 CSV、summary、manifest、
README。历史内容仍可从 git 历史对象恢复，镜像为现行取用入口。
