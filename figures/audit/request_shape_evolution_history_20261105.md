# 旧请求流程学习图的历史状态

这是请求形成、token 切分、提交及 vLLM 推理的概念示意，采用八条示例 prompt；数字不是实验结果。
图中旧 PG profiler 和 HTTP metrics 轮询已退出维护，不能用来描述当前 SemLoom 的执行、容量或性能。
旧轮询准确源码从[历史恢复入口](../../experiments/reproduction/native_baselines/README.md#retired-ray-http-polling)核对。

本次只更正[脚本](../scripts/generate_request_shape_evolution.py)的历史标注及两级图入口，
没有运行生成器、修改布局或覆盖[普通 PNG](../learning/request_shape_evolution.png)／[两倍 PNG](../learning/request_shape_evolution@2x.png)。
没有新的 SVG／PNG 导出或新视觉检查，不把本次索引整理作为新图验收。
两份旧图保留原字节，仍是历史学习原件；全仓维护文字未查到引用它们的当前报告。
开题历史材料未改，当前实现从代码 README 与实现状态读取。
