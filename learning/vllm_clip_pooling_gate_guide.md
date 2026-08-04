# vLLM CLIP pooling 门禁怎么读

## 为什么要测它

项目图像链路是 PostgreSQL 图像行经过 Daft/Ray 后交给 CLIP actor。vLLM pooling
baseline 则把图像直接交给 vLLM，由服务内部负责预处理、batching 和 CLIP forward。
它绕过数据库与项目调度，因此若能运行，代表一条“强服务上限”，用来判断项目链路
离纯服务容量还有多远。

它不是数据库 AI 算子系统 baseline。Daft built-in 和 Ray Data native graph 才保留
数据库读取、数据引擎与执行图，回答“完整系统怎么跑”。两个层级不能只按 images/s
混在同一排名中。

## 这次实际发生了什么

vLLM 成功识别 `CLIPModel` 和 pooling runner，但识别配置不等于已经执行模型。两次
离线进程都在 600 秒超时，退出码 124，没有 `result.json`，因此没有 embedding、
吞吐或正确性数据。第二次关闭 FlashInfer sampler 后仍超时，只能排除“该 sampler
开关单独决定结果”，不能把根因写成已经找到。

## 为什么不继续跑 5K/60K

能力门禁像启动前的刹车检查：连一张图都没有产生合法输出时，扩大到 5K/60K 只会
增加等待并生成无法解释的空数据。正确顺序是：

```text
1-image offline capability
  → 1-image online API
  → 256-image semantic/parity
  → 5K calibration
  → long formal
```

任一级失败就停止。当前停在第一级，所以 A 线状态是 `blocked`，不是性能差，也不是
vLLM+CLIP 在所有环境中都不可行。

正式机器证据和不能声称的边界见
`../feasibility/results/vllm_clip_pooling_gate_20260804/README.md`。
