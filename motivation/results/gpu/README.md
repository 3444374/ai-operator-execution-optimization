# GPU-Backed E2E Results

本目录保存生产式 GPU-backed 端到端主动机结果，优先用于回答：

> 为什么数据库 AI 算子的外部执行链路值得优化？

## 当前结果

| 文件 | 含义 |
|---|---|
| `ai_embed_profile.md` / `.csv` | 第一组真实 GPU-backed embedding 端到端画像 |
| `ai_embed_chain_breakdown_20260712.md` / `.csv` | 真实 embedding 链路拆分结果：PostgreSQL fetch、Arrow/batch、operator wall、HTTP model request wall、fan-in、writeback |
| `multi_endpoint_ray_motivation_20260712.md` / `ai_embed_multi_endpoint_20260712.csv` | 两个本地 GPU endpoint 下的 Ray task/actor 初步动机测试 |
| `ai_embed_chain_breakdown_draft_20260712.csv` | 早期草稿结果，含计时字段修正前的行；不要用于正式分析 |

## Endpoint

当前 GPU-backed embedding endpoint：

```text
http://localhost:8000/v1/embeddings
```

多 endpoint 动机测试额外使用：

```text
http://localhost:8001/v1/embeddings
```

模型：

```text
sentence-transformers/all-MiniLM-L6-v2
```

复现实验前先检查 8000 端口；若未启动，按 `ai_embed_profile.md` 或 `ai_embed_chain_breakdown_20260712.md` 中的命令启动。

## 结果边界

- 本目录只放真实 GPU-backed 模型端点结果。
- CPU-only、fake-model、连接验证结果不能放在这里。
- 当前真实模型返回 384 维 embedding；`ai_embed_chain_breakdown_20260712` 使用 JSON text 写回，不是 384 维 pgvector 写回。
- `model_service_s` 是请求耗时加和；阶段占比优先看 `model_request_wall_s`、`operator_wall_s` 和 `writeback_s`。
- `multi_endpoint_ray_motivation_20260712` 是 Ray 价值的初步动机测试，不是最终 Ray Serve / vLLM / 多 GPU 结论。
