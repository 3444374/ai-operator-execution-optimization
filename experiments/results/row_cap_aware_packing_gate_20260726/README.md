# Row-Cap-Aware Packing 64-Row Gate

## 1. 实验设置

本门禁验证研究内容一的新装箱候选是否能在真实组件链路中正确运行，不用于判断性能优越性。链路为 PostgreSQL 18.4 + pgvector 0.8.2 → Daft native organizer → Ray task → vLLM 0.25.1 / Qwen2.5-1.5B BF16 → RTX 5070 12 GB；未使用 fake backend，未执行数据库写回。

vLLM 使用 `--enable-mfu-metrics`。排查发现该版本即使未启用此开关也会暴露值为 0 的 `estimated_flops_per_gpu_total`，因此门禁要求计数器必须在单请求前后增长，不能只检查指标名称存在。

## 2. 实验设计

三种数据组织策略使用相同的 64 个文档、固定输出上限 16 tokens、token budget 6144、每批最多 16 行、静态 `K_max=8` 和两个 Ray task worker：

- `seq_fixed`：顺序 token-budget；
- `bfd_fixed`：经典 best-fit-decreasing；
- `row_cap_fixed`：保留 decreasing order，但优先填充接近行数上限的可行批次。

每个场景运行一次 warm-up 和一次 formal，场景顺序由种子 `20260726` 固定。复现入口为 `scenario_config.json`，原始运行级数据为 `runs.csv`。

## 3. 严谨性自检

- 最终 manifest 为 `completed`，6/6 运行成功，0 incident；
- 384/384 请求完成，每轮均有 64 个唯一 request ID 和 64 个唯一 doc ID；
- request → submission 外键完整；
- 每轮 `batch_rows_max <= 16`，无超预算行时最大批成本不超过 6144；
- 每轮 resource trace 非空，最终 vLLM running/waiting 均为 0；
- 每轮 vLLM FLOP 增量大于 0，`mfu_status=ok`；
- 首轮因服务未启用 MFU 开关而产生的无效产物已废弃并重跑，不进入本目录最终 CSV。

## 4. 实验数据

以下仅列 formal 单次运行，不能据此做显著性或稳定性结论：

| 场景 | submissions | budget utilization | E2E (s) | request P95 (s) | rows/s | tokens/s | energy (J) | MFU |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| seq_fixed | 4 | 0.456787 | 2.822922 | 2.739219 | 22.672 | 3974.959 | 119.093 | 0.034409 |
| bfd_fixed | 5 | 0.365430 | 2.960790 | 2.893398 | 21.616 | 3791.556 | 115.236 | 0.032690 |
| row_cap_fixed | 5 | 0.365430 | 3.008667 | 2.935049 | 21.272 | 3731.220 | 114.636 | 0.032190 |

## 5. 结果解释与边界

事实：三种策略都满足完整性、行数上限、token budget、资源轨迹和 MFU 门禁；Arrow/Daft 共享的 row-cap-aware 实现已进入真实 GPU 链路。

推断：当前 64 行输入上，经典 BFD 与 row-cap-aware 形成了相同数量和平均大小的批次，未暴露纯函数反例中的批次数优势。

不能声称：单次 formal 运行不能说明 sequential、BFD 或 row-cap-aware 的性能排序，也不能说明该候选能修复 1024 行上的经典 BFD 退化。

## 6. 对课题的含义

基础设施已经能够把“经典 BFD 完整机制”和“decreasing order + row-cap-first placement”作为两个独立候选进行真实对照。策略默认值仍保持顺序 token-budget，是否采用任一 BFD 机制由后续规模实验决定。

## 7. 下一步

进入 512 行筛选：在相同文档、输出上限、提交控制和测量配置下，搜索 row cap `{16,32,64}`、token budget `{4096,6144,8192}` 与三种算法。任何正确性或 MFU 无效的配置立即淘汰；只有通过预先定义的吞吐、P95、能耗和 MFU规则的 row-cap-aware 候选才进入重复实验与 1024 held-out 确认。
