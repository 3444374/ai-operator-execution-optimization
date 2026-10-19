# 动机测试结果

本目录保存数据库 AI 算子动机测试的**唯一正式结果**和分析。`experiments/results/diagnostics/` 不再存放动机测试 CSV 副本。

## 目录结构

```text
experiments/results/motivation/
├── README.md                    ← 本文件
├── fake_cpu/                    ← CPU/fake 历史预研（仅作背景参考）
│   ├── README.md
│   ├── analysis.md              ← 早期动机测试分析（原 motivation_test_results_analysis.md）
│   ├── fake_embed_pipeline.csv
│   ├── workload_matrix.csv
│   ├── granularity.csv
│   └── backpressure.csv
├── cpu/                         ← CPU baseline 对照
│   ├── README.md
│   ├── ai_embed_cpu_profile.csv
│   └── cpu_vs_gpu_embed_comparison_20260712.md
├── gpu/                         ← GPU-backed E2E 主动机结果（当前最优先引用）
│   ├── README.md
│   ├── ai_embed_profile.csv / .md
│   ├── ai_embed_chain_breakdown_20260712.csv / .md
│   ├── ai_embed_multi_endpoint_20260712.csv
│   ├── multi_endpoint_ray_motivation_20260712.md
│   ├── ai_embed_pgai_integrated_key_20260714.csv
│   ├── pgai_integrated_key_rerun_20260714.md
│   ├── ai_embed_pgvector_writeback_20260714.csv
│   ├── pgvector_writeback_20260714.md
│   ├── image_host_path_screening_20260802/ ← 图像 host-path 木桶 screening/诊断
│   └── image_clip_transfer_ceiling_20260802/ ← R0/R1/R2 H2D ceiling 诊断
└── pg18_4_fake/                 ← PG18.4 本地同构预演（不代表真实平台结论）
    ├── README.md
    ├── baseline_matrix.csv / .md
    ├── system_profile.csv / .md
    ├── vector_writeback.csv / .md
    ├── pgvector_scaling.csv / .md
    └── ...
```

## 当前阅读优先级

正式论证优先引用 GPU-backed 结果：

1. `gpu/image_clip_transfer_ceiling_20260802/README.md` — R0/R1/R2 compute/H2D/ownership 最新机制诊断
2. `gpu/image_host_path_screening_20260802/README.md` — 图像 CLIP host-path 木桶诊断（screening 边界）
3. `gpu/ai_embed_chain_breakdown_20260712.md` — GPU-backed embedding 链路拆分
4. `gpu/pgai_integrated_key_rerun_20260714.md` — pgai-integrated GPU-backed rerun
5. `gpu/pgvector_writeback_20260714.md` — pgvector(384) 写回对比

`fake_cpu/analysis.md` 只用于了解早期为什么关注 task/object/invocation/fan-in/backpressure，不代表真实 GPU-backed 链路瓶颈。

`pg18_4_fake/` 下的结果只能作为 PG18.4 本地同构预演和历史信号；不能把 PG18.4 本地预演写成 PostgreSQL 18.3 内部平台结论。

## 历史复算

早期 CPU 模拟与专用汇总入口已退役，完整命令与实现见
[固定历史版本](https://github.com/3444374/ai-operator-execution-optimization/tree/b8b5f3d11576e4c06953e38732fdd3d010807a85/code/scripts/benchmarks)。
复算时使用同一提交的源码。`fake_cpu/` 的四份 CSV、原分析和现有绘图读取继续保留。

## 说明

fake_cpu 结果用于研究方向筛选和动机补强，不等价于最终论文实验。正式结论必须基于 GPU-backed 链路或真实数据库 AI 算子验证。
