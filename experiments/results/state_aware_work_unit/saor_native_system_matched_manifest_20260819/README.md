# SAOR native-system matched 两 Job 冻结 manifest（2026-08-19）

> **性质**：native-system matched comparison（五臂）的两 Job immutable identity 合同。
> 完整 prompt manifest 留在 Git 外；仓库只冻结行数与 SHA，避免 workload 文本中的个人信息进入版本历史。

## 文件

| 文件 | 行数 | SHA256 |
|---|---|---|
| combined（服务器 `experiment-artifacts/`，Git 外） | 1024 | `72dc51b7a63ce8a35c410d3050eb9b110cb08a68a9e45928770be428058bf56f` |
| 来源 Job0（bulk/long）：`opening_multijob_manifests_20260808_work_balanced/long_512.jsonl`（服务器 `experiment-artifacts/`，git 外） | 512 | `8e532819f045f85ff4e92b61c688e2d50f180d438dc577eed79c57e19cfce9c1` |
| 来源 Job1（foreground/short）：同目录 `short_512.jsonl` | 512 | `85b3f90cdc4045ae9fdb48f1d30772649c25d86375b72bab0fbd903f2a01c971` |

## 生成方式

在 Git 外执行 `cat long_512.jsonl short_512.jsonl > matched_long_short_1024.jsonl`（字节级拼接，Job 顺序 =
Job0 long/bulk → Job1 short/foreground，与 typed `job_release_schedule=[0,5]` 和 Job 内 eager 合同一致）。

两个来源 SHA 与 `saor_feeding_gap_diagnostic_contract.json`、`saor_active_set_formal.env.example`
引用的冻结合同值逐字节一致；该两份 manifest 已被 63d17300 六臂 rehearsal、feeding ceiling、
feeding-gap diagnostic 三批 GPU 实验验证过身份。

## 结构验证（项目自带 `read_manifest`）

- 1024 行，`doc_id` 全唯一
- Job0 endpoint 0/1 = 256/256；Job1 endpoint 0/1 = 256/256
- `max_output_tokens = estimated_output_tokens = 256` 全体一致
- `arrival_time_s`：Job0 185.0–66872.0，Job1 5.0–66880.0（Job1 首请求 5.0s 与 foreground offset 合同一致）

## 引用

- `deploy/autodl/saor_native_system_matched.example.json` 通过三个 runtime env 指向 Git 外文件，
  同时为 Job0/Job1 分别冻结 `job_id/rows/sha256`。
- readiness 先验证每份 Job 的 SHA、512 行、output cap、跨 Job `doc_id` 唯一，再验证 combined
  等于 Job0+Job1；Project 的 `rows_per_jobs` 也必须等于 `[512,512]`，任何一项漂移均不 dispatch。
- matrix root 会封存 combined 与两份 Job manifest 的副本及 SHA；原始 workload 不得提交回 Git。
