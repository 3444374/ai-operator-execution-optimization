# 开题四张核心证据图：设计合同与审计

日期：2026-08-07

## 共同合同

- 类型：Experimental Results；范式为 quantitative grid。
- 输出：Python/Matplotlib 单一后端，主输出 SVG（文字可编辑），同时输出 300 dpi PNG。
- 画布：13.2 × 5.8–6.2 in，适配 16:9 幻灯片主体和报告双栏宽图。
- 配色：蓝色承载主要观察，灰色为参照；红/绿只用于风险或门槛。曲线同时使用不同 marker/线型，柱图同时使用空心/实心或 hatch，避免只靠颜色区分。
- 统计：只读正式重复；active-work 显示均值 ± sample SD，数据组织显示三次 formal 原始点 + 中位数，图像显示均值并由报告 CV 还原 SD，代价估计显示 20 个 context 的折级分布摘要。
- 完整性：不混入 warm-up、smoke、无效 run 或跨机器数字；所有数值由脚本从项目内 CSV/JSON 重建。
- 不能声称：四图说明当前硬件、模型、workload 与冻结合同下的容量、边界和机制信号，不证明跨系统普适最优，也不证明待研究的状态感知动态策略已经胜出。

## 图一：serving capacity 与 overload frontier

```text
Core conclusion: 提高 active work 能持续改善填充，但 65K/endpoint 已达最大均值的 97.80%，继续放大只换来约 2.2% 吞吐而 P99 从 36.78 s 升至约 40.05 s。
Figure archetype: quantitative grid
Target journal/output: 开题报告 + 开题 PPT + 后续论文实验部分
Backend: Python
Final size: 13.2 × 5.8 in
Panel map:
  a: 吞吐—active-work 曲线，均值 ± SD，标 65K 饱和区入口
  b: 请求 P99，展示饱和后的尾延迟恶化
  c: 30 s SLO goodput，展示有效吞吐趋于平台
Evidence hierarchy:
  hero evidence: 吞吐饱和曲线
  validation evidence: P99 与 SLO goodput
  controls/robustness: 8 档、每档 3 formal、统一合同
Statistics needed: mean, sample SD, P99 mean, goodput mean
Source data needed: dual_gpu_active_work_saturation_20260729/formal_summary.csv
Image-integrity notes: 无图像处理；SVG/PNG 由同一脚本直接导出
Reviewer risk: 65K 是最小近饱和点，不是全局最优或 vLLM 内部容量上限
```

## 图二：work-aware 组织的 regime dependency

```text
Core conclusion: 相同双卡硬件下，2-endpoint 低 KV 压力与 4-endpoint consolidation 高 KV 压力呈现不同策略排序；后者中破坏 prefix 局部性的重排序策略命中率塌到 0.06–0.07 并损失吞吐。
Figure archetype: quantitative grid
Target journal/output: 开题报告 + 开题 PPT + 后续论文实验部分
Backend: Python
Final size: 13.2 × 6.2 in
Panel map:
  a: 5 策略 × 2 压力 regime 的吞吐；柱为 formal 中位数、点为 3 次正式重复
  b: prefix_group_ratio—prefix cache 命中率机制散点；标注 4-endpoint 高 KV 压力
Evidence hierarchy:
  hero evidence: 4-endpoint 饱和 regime 下 39–50k tok/s 的分化与排名反转
  validation evidence: prefix_group_ratio 与 hit-rate 同步塌缩
  controls/robustness: 相同 workload/合同，2-endpoint 大池作为干净对照
Statistics needed: median and all three formal values
Source data needed: rc1_data_organization 两个 topology 的 raw/runs.csv
Image-integrity notes: 无图像处理
Reviewer risk: 2-endpoint feeding-saturation 严格门未过，故图只支持 regime-dependent 机制信号，不支持全局性能排名
```

## 图三：图像 matched-resource 结构收益

```text
Core conclusion: 在 CPU/GPU 资源匹配后，项目静态分级 actor 路径仍把图像算子 JCT 降低 12.8%/15.1%，独立复测在两档 CPU 上保持同向。
Figure archetype: quantitative grid
Target journal/output: 开题报告 + 开题 PPT + 后续论文实验部分
Backend: Python
Final size: 13.2 × 5.8 in
Panel map:
  a: CPU8/CPU16 下 Ray Data staged 与项目静态路径的 JCT，均值 ± SD
  b: 两次实验的 matched-CPU JCT 降幅，标 5% 预注册门槛
Evidence hierarchy:
  hero evidence: 主实验 matched-CPU JCT
  validation evidence: schema-v12 独立复测方向一致
  controls/robustness: 两档 CPU、同 batch/GPU/source/sink/质量语义
Statistics needed: mean, CV, n=3 formal per cell
Source data needed: summary.csv and summary_schemav12.csv
Image-integrity notes: 无图像处理
Reviewer risk: GPU busy 仍低，结论是执行结构收益与跨模态可复用性，不是 GPU 饱和或动态策略胜出；45.7% 旧口径禁止使用
```

## 图四：cost-model decision quality

```text
Core conclusion: Hybrid 是首个同时满足候选级 pairwise、median/macro/max regret 门槛的估计器，但 max regret=14.72% 距 15% 线仅 0.28 pp，属于 marginal pass。
Figure archetype: quantitative grid
Target journal/output: 开题报告 + 开题 PPT + 后续论文实验部分
Backend: Python
Final size: 13.2 × 5.8 in
Panel map:
  a: 6 个估计器的 median/macro/max decision regret 与冻结门槛
  b: candidate pairwise accuracy—max regret 决策平面，标出通过区
Evidence hierarchy:
  hero evidence: Hybrid 的 0/2.90/14.72% regret 合同
  validation evidence: candidate pairwise=0.808；Ridge/LightGBM 的 max 门失败
  controls/robustness: context-LOO，429 formal rows，20 context × 4 candidate
Statistics needed: fold median/mean/max and candidate-pairwise mean
Source data needed: ce_context_loo_rerun_20260807.json
Image-integrity notes: JSON 含模型输出文本的非 UTF-8 字节，脚本仅以 replacement 解码读取结构化 estimator summary；图中数值字段不受影响
Reviewer risk: 不能称稳健通过、不能外推到未见模型/硬件，也不能把估计器实验写成系统 baseline 胜负
```

## 预渲染完整性门禁

- [x] 四张图均为实验结果图，图型与数据关系匹配。
- [x] 每图只有一个核心结论，panel 各自承担独立问题。
- [x] 标签使用真实系统/指标名称，无 Module A/X/Y 占位符。
- [x] Matplotlib 与项目真实数据源匹配，未手工编辑导出物。
- [x] 轴含单位，数值轴从 0 起；无 3D、阴影、渐变或装饰性图表元素。
- [x] SVG 保留 `<text>`，PNG 只作为 PPT/预览兼容副本。
- [x] 渲染后逐图人工复核字体、裁切、重叠、颜色和小尺寸可读性；组织图与代价图的首版标签重叠已修复并复核。

## 渲染后 QA

- 4/4 SVG 可由 XML parser 打开，分别含 41–56 个 `<text>` 节点，文字保持可编辑。
- 4/4 PNG 可由 Pillow 完整校验；尺寸为 3994×1774 或 3994×1894，RGBA，适合报告与 PPT 缩放。
- 逐图视觉复核：标题、panel label、坐标、图例、门槛线和注释均无裁切；没有残留中文缺字方框。
- serving-capacity 图保留从 0 开始的三条纵轴；组织图保留 formal 原始点；图像图显式呈现 5% 门槛；代价图显式呈现 5%/15%/0.75 三项合同门槛。
- 数据组织图的三个低命中率标签已错位排布；代价图把同坐标的 Mean/Lookup 合并标注。只改标注，不改数据。

最终审计：0 CRITICAL，0 MAJOR，0 MINOR；四图可进入开题材料。
