# ECharts 图表生成规则

本文件记录开题材料中使用 ECharts 生成实验结果图的经验。核心原则是：图表由脚本可复现生成，PPT 和 Word 只消费生成好的图片资源，不在 PPT 里手工画和手工改图。

## 依赖位置

ECharts 依赖放在项目外，避免把 `node_modules` 放进仓库。

当前约定外部依赖目录：

```text
D:\Tools\echarts
```

历史环境中安装过：

```text
echarts 6.1.0
sharp 0.35.3
```

后续如果本项目需要重新生成 ECharts 图，优先复用该外部依赖目录；如果环境不存在，再记录新的安装位置和版本。

## 推荐输出目录

本项目开题图表统一放在：

```text
opening/assets/charts/
```

建议每张图同时输出两种格式：

| 格式 | 用途 |
|---|---|
| `.svg` | 给 Word / 报告使用，清晰可缩放 |
| `.png` | 给 PPT 使用，兼容 PowerPoint / WPS 更稳 |

## 推荐渲染流程

ECharts 作为服务端图表渲染器使用，不通过浏览器截图。

```text
ECharts option
  -> SSR 渲染 SVG
  -> 保存 SVG
  -> sharp 转 PNG
  -> PPT / Word 引用图表文件
```

核心 JS 逻辑：

```js
const chart = echarts.init(null, null, {
  renderer: "svg",
  ssr: true,
  width: 1920,
  height: 1080
});
chart.setOption(option);
const svg = chart.renderToSVGString();
```

SVG 转 PNG：

```js
await sharp(Buffer.from(svg)).resize(WIDTH, HEIGHT).png().toFile(pngPath);
```

## 画布和字号

开题 PPT 图表优先按 16:9 大图生成：

```text
1920 x 1080
```

推荐字号范围：

| 元素 | 字号 |
|---|---|
| 主标题 | 40-46 |
| 副标题 | 24-28 |
| 坐标轴 | 23-28 |
| 数据标签 | 24-28 |
| 图例 | 24-28 |

不要生成小图再拉伸到 PPT，否则投影时会糊，字体比例也容易变形。

## 样式规则

- 所有实验图使用统一 `baseOption`，统一标题、字号、颜色、网格、图例位置。
- 图表要自带标题和副标题，插入 PPT 后即使页面只放图，也能看懂图想表达什么。
- 图表只放图表头、坐标、图例、颜色含义和必要数值。
- 解释文字放报告正文、PPT 页面备注或飞书实验分析中，不塞进图里。
- 颜色要克制，主色跟随开题模板，强调色只用于关键对比或关键结论。
- 坐标轴范围必须诚实，不用截断坐标夸大效果。

## PPT 嵌入规则

PPT 图表页优先插入 PNG，而不是让 PPT 运行 ECharts。

如果基于学校模板替换指定页面内容，脚本应只删除正文区域旧内容，保留页眉、页脚、校徽和模板装饰。

历史做法的关键逻辑：

```python
remove_body_shapes(slide)
add_title(slide, title)
add_chart(slide, chart_file)
```

删除正文区域时要保留模板区域，例如：

```python
if top < Inches(0.95) or top > Inches(6.65):
    keep.append(shape)
    continue
slide.shapes._spTree.remove(shape._element)
```

这类阈值需要根据当前学校模板实际安全区重新确认，不能直接照抄旧项目数值。

## Word / 报告嵌入规则

- Word / 报告优先使用 SVG，保证缩放清晰。
- 报告中每张图要有图题和正文解释。
- 图表文件名应能看出实验、变量和日期，例如：

```text
gpu_embed_4096_executor_e2e_20260712.svg
gpu_embed_4096_executor_e2e_20260712.png
```

## 命令记录

后续本项目如新增 ECharts 脚本，建议命名为：

```text
scripts/opening/generate_echarts_experiment_charts.js
```

运行命令需要写入对应 README 或实验报告，例如：

```powershell
node scripts/opening/generate_echarts_experiment_charts.js
```

如果还需要把图表嵌入 PPT，另建 Python 脚本并记录输入输出：

```powershell
python scripts/opening/build_opening_ppt_charts.py
```

## 质量检查

- 图表来自真实 CSV 或明确的数据表。
- 图表文件同时生成 SVG 和 PNG。
- PPT 使用 PNG，Word / 报告使用 SVG。
- 图表尺寸为 16:9 大图，不能小图拉伸。
- 图表标题、坐标轴、图例、颜色含义完整。
- 生成脚本、输入数据和输出路径可追溯。
- 插入 PPT 后没有压住页眉、页脚、校徽。
- PPT 图表页有 `汇报讲稿` 和 `答辩备注`。

