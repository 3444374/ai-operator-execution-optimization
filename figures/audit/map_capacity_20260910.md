# 单卡 Map 容量图审计

文档角色：内部图资产审计。类型：实验结果图，属于当前校准记录的辅助材料，不进入开题主图。

- 图的任务：分别展示短查询并发诊断、固定并发下的规模检查、窗口诊断和完整重复值，避免混用规模。
- 数据来源：`experiments/results/postgresql/map_capacity_20260910/raw/queries.csv`；每一行对应实际查询记录。
- 使用位置：同目录结果报告；图源为 `figures/scripts/plot_map_capacity_20260910.py`。
- 不能推出：动态组织收益、全局最优并发、任意 workload 的 GPU 饱和、长期 RSS 上限或所有问题回答正确。

绘图脚本从 CSV 的完整查询 JCT 计算完成行吞吐；失败查询没有吞吐值，另在结果报告保留。
诊断曲线各点为单次值；重复图显示全部五个值及中位数，无伪造误差棒或显著性。
第一个 direct 对照来自被 PG 资源拒绝中断的首组，图下注明；其推理配置未改变，未按结果优劣挑选。

```sh
python figures/scripts/plot_map_capacity_20260910.py \
  --input experiments/results/postgresql/map_capacity_20260910/raw/queries.csv \
  --output-stem figures/data/backup/map_capacity_20260910
```

导出与目视检查：使用服务器 driver 环境的 Matplotlib 3.11.0 生成 SVG（文字保留为 text）与
300 dpi PNG（3450×2280）。实际打开 PNG 检查，四面板标题、坐标、图例、全部重复点与脚注均无裁切；
曲线、单位、零起点和配色清楚。程序要求重复面板每组恰好五次、每次 4,000 行且执行/评价成功，
短诊断与完整重复分别筛选，未补值或合并失败吞吐。SVG/可见文字未出现项目内部阶段代号。
词法检查唯一的 `P1` 子串来自 `TP1`，表示张量并行度 1，已在报告图注解释，保留为技术术语。
该图是当前校准的辅助图；高并发长查询平台尚未证明，因此不进入论文主图或宣称强 D0 已完成。

入库时仅移除 SVG 行尾空白，逐元素核对属性词元和全部文本不变；PNG 保持原导出。公开 CSV 转为 LF 后逐单元格核对相同，原始 CRLF 文件仍在私有归档。
