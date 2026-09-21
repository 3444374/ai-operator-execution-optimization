# 文本 AI 算子的 baseline 到底在比什么

## 1. 先把四种角色分开

同一张双 4090 服务器上，文本请求最终都可能进入 vLLM，但“谁把请求送进去”不同：

```text
服务上限：          vLLM Bench ───────────────→ vLLM
直接客户端 control：项目 bounded HTTP ───────→ vLLM
框架原生 baseline： Daft prompt / Ray Data ──→ vLLM
数据库产品 baseline：OceanBase AI_COMPLETE ──→ vLLM
项目方法：          PostgreSQL→Daft→Ray策略 ─→ vLLM
```

- `vLLM Bench` 像测发动机台架上限，目标是让项目尽量接近它，不是“打败”它。
- bounded HTTP 是本项目写的强客户端，用来判断是否因为客户端太弱而没喂饱服务；它不是
  市面系统，也不叫原生 baseline。
- Daft/Ray Data 原生 baseline 必须让框架自己负责 batching、backpressure、task/actor
  调度。本项目只把一行 prompt 转成一次请求并保存结果。
- OceanBase 必须真的从 SQL `AI_COMPLETE` 执行，不能写 Python HTTP 循环后贴上
  OceanBase 标签。

## 2. 一行数据经过什么

以 Daft 原生 Chat arm 为例：

```text
冻结 manifest 中的一行 prompt
  → Daft DataFrame 的一行
  → daft.functions.prompt 内置表达式
  → OpenAI-compatible /v1/chat/completions
  → vLLM continuous batching
  → output_text 回到 Daft collect barrier
```

Ray Data arm 则由 `HttpRequestProcessorConfig + build_processor` 建立官方 graph；项目的
preprocess 只构造 payload，postprocess 只解析 response。只要在这里加入 active-work、
自定义 router 或项目 actor pool，它就不再是 Ray Data 原生 baseline。

## 3. 为什么 Chat 和 Completions 不能直接比

Chat 请求通常是一行一次 HTTP request；项目原始 Completions 路径可能把多行 prompt
放进一个 HTTP body。两条路径的 chat template、HTTP request 数、返回结构和可用产品
算子不同。因此：

- Chat 轨比较 vLLM Bench、Daft、Ray Data、OceanBase 和项目 Chat 路径；
- Completions 轨只解释 multi-prompt packing、token-budget、length-align、flush 等机制；
- 即使 Completions tokens/s 更高，也不能说它击败了 Chat 轨的数据库产品。

## 4. 为什么还要 64、512、4096 三个规模

- 64 行 validity gate：只回答“能不能正确运行”，检查 exactly-once、双 endpoint、空队列。
- 512 行 calibration：各系统只调自己真实暴露的参数，找到稳定运行点。
- 4,096 行 held-out formal：参数冻结后运行至少 60 秒，1 次 warmup + 3 次交错重复，
  才回答谁的 JCT、吞吐、资源效率更好。

短 gate 很容易被冷启动、task 数不足和 GPU 缓存状态支配，不能直接当论文性能排名。

## 5. 结果表应该怎么看

先看正确性与工作量，再看性能：

1. manifest/hash、模型、输出上限、调用数、exactly-once 和失败率是否相同；
2. 双 endpoint 的服务端 prompt/generation token counter 差分是否一致；
3. 用两端 token 总量除以共同 group wall 得到双 GPU headline throughput；
4. 同时看 JCT、P95/P99/SLO、CPU、GPU/MFU/显存/能耗、vLLM running/waiting/KV；
5. Daft 当前只有整 shard barrier，不能把同一个 barrier 时间复制成每行 P99。

最终要回答的不是“谁的一个 tokens/s 数字最大”，而是：在相同输入、模型、质量、资源和
计时边界下，原生系统的木桶在哪，项目是否用更低排队、更少 GPU 空转或更好的多 job/SLO
表现获得可重复的系统收益。
