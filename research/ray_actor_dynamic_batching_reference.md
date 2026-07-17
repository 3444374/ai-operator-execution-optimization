# Ray Actor, Dynamic Batching & vLLM Integration Reference

> 研究日期: 2026-07-16
> 用途: 为构建 vLLM 上游自适应提交系统 (adaptive submission system) 提供 Ray 技术栈参考
> 来源: Ray 官方文档 (docs.ray.io)、Ray GitHub (ray-project/ray)、Anyscale 博客、学术论文

---

## 目录

1. [Ray Serve Dynamic Batching](#1-ray-serve-dynamic-batching)
2. [Ray Core Actor Patterns (构建自适应提交系统)](#2-ray-core-actor-patterns)
3. [Ray Core Scheduling Architecture](#3-ray-core-scheduling-architecture)
4. [Ray Observability](#4-ray-observability)
5. [Ray + vLLM Integration Patterns](#5-ray--vllm-integration-patterns)
6. [Related Papers & Technical Reports](#6-related-papers--technical-reports)

---

## 1. Ray Serve Dynamic Batching

### 1.1 `@ray.serve.batch` Decorator

Ray Serve 的 `@serve.batch` 装饰器实现动态请求批处理。它将单个请求排队，直到满足 `max_batch_size` 或 `batch_wait_timeout_s` 两个条件之一。

**核心参数:**

| 参数 | 默认值 | 说明 |
|---|---|---|
| `max_batch_size` | 10 | 单批次最大请求数 |
| `batch_wait_timeout_s` | 0.01 | 首个请求到达后等待的最长时间(秒) |
| `max_concurrent_batches` | 1 | 每个 replica 可并发处理的批次数 |
| `batch_size_fn` | None | 自定义批大小计算函数 (2024 新增) |

**基本用法:**

```python
from typing import List
from ray import serve
from ray.serve.handle import DeploymentHandle

@serve.deployment
class Model:
    @serve.batch(max_batch_size=8, batch_wait_timeout_s=0.1)
    async def __call__(self, samples: List[int]) -> List[int]:
        # 批量推理逻辑
        import numpy as np
        return (np.array(samples) * 2).tolist()

    async def handle(self, sample: int) -> int:
        # 单个请求入口，内部调用批量方法
        return await self.__call__(sample)

handle: DeploymentHandle = serve.run(Model.bind())
results = [handle.handle.remote(i) for i in range(100)]
```

### 1.2 Custom Batch Size Function (`batch_size_fn`) -- 2024 新增

当计算开销取决于内容（token 数、节点数、像素数）而非请求计数时使用。`batch_size_fn` 接收整个待处理列表，返回整数表示有效批大小。

**按 Token 数批处理:**

```python
@serve.deployment
class TokenBatcher:
    @serve.batch(
        max_batch_size=512,  # 最大总 token 数
        batch_wait_timeout_s=0.1,
        batch_size_fn=lambda sequences: sum(len(s.split()) for s in sequences),
    )
    async def process(self, sequences: List[str]) -> List[int]:
        return [len(seq.split()) for seq in sequences]

    async def __call__(self, sequence: str) -> int:
        return await self.process(sequence)
```

**按图节点数批处理 (GNN):**

```python
@serve.deployment
class GraphNeuralNetwork:
    @serve.batch(
        max_batch_size=10000,
        batch_wait_timeout_s=0.1,
        batch_size_fn=lambda graphs: sum(g.num_nodes for g in graphs),
    )
    async def predict(self, graphs: List[Graph]) -> List[float]:
        ...
```

### 1.3 运行时动态重配置

无需重新部署即可调整批处理参数:

```python
class Model:
    @serve.batch(max_batch_size=8, batch_wait_timeout_s=0.1)
    async def __call__(self, samples: List[int]) -> List[int]:
        ...

    def reconfigure(self, user_config: Dict):
        self.__call__.set_max_batch_size(user_config["max_batch_size"])
        self.__call__.set_batch_wait_timeout_s(user_config["batch_wait_timeout_s"])
```

### 1.4 Batch 内部机制

- 批处理运行在 asyncio event loop 上，请求在队列中累积
- `batch_size_fn` 返回的值超过 `max_batch_size` 时，溢出的请求推迟到下一批次
- 被装饰的方法必须是 `async def`，接受 `List` 输入，返回等长 `List`
- 当 `max_batch_size > max_ongoing_requests` 时，Ray Serve 会输出警告日志——速率限制器不感知批大小，batch 可能永远无法填满

### 1.5 Ray Serve Autoscaling

Ray Serve 支持基于请求负载的自动扩缩容:

```python
@serve.deployment(
    autoscaling_config={
        "target_ongoing_requests": 2,      # 每个 replica 的目标并发请求数
        "min_replicas": 1,                 # 最小 replica 数 (可设为 0)
        "max_replicas": 100,               # 最大 replica 数
        "initial_replicas": 2,             # 初始 replica 数 (可选)
    },
    max_ongoing_requests=5,                # 每个 replica 的硬上限
)
class MyDeployment:
    ...
```

| 参数 | 默认值 | 说明 |
|---|---|---|
| `target_ongoing_requests` | 2 (Ray 2.32+) | 自动扩缩容目标。长请求/严格延迟目标设更低 |
| `min_replicas` | 1 | 缩容下限。无流量时段可设为 0 |
| `max_replicas` | 100 (auto 模式) | 扩容上限。建议设为峰值预估的 120% |
| `max_ongoing_requests` | 5 (Ray 2.32+) | 硬上限。应设为 target_ongoing_requests 的 1.2-1.5 倍 |

来源: https://docs.ray.io/en/latest/serve/autoscaling-guide.html, https://docs.ray.io/en/latest/serve/api/doc/ray.serve.batch.html

---

## 2. Ray Core Actor Patterns

### 2.1 Stateful Actor 基础

Actor 是 Ray 中的有状态计算单元，实例化后会创建专属 worker 进程，所有方法调用在该 worker 上串行执行，可读写内部状态:

```python
@ray.remote
class Counter:
    def __init__(self):
        self.counter = 0

    def inc(self):
        self.counter += 1
        return self.counter

counter = Counter.remote()
ray.get(counter.inc.remote())  # => 1
ray.get(counter.inc.remote())  # => 2
```

### 2.2 Async Actor (Asyncio Event Loop)

Ray 原生集成 asyncio。若 actor 定义 `async def` 方法，所有方法在单个 event loop 中并发执行。关键特性:
- 一次只有一个 task 执行，但通过 `await` 协作式多路复用
- 默认 `max_concurrency=1000`
- 不允许在 async 方法内调用阻塞的 `ray.get()` 或 `ray.wait()`

```python
@ray.remote
class AsyncActor:
    async def run_concurrent(self):
        await asyncio.sleep(1)
        return "done"

actor = AsyncActor.remote()
refs = [actor.run_concurrent.remote() for _ in range(100)]
results = ray.get(refs)  # 100 个任务在单个 event loop 中并发
```

**控制并发度:**

```python
actor = AsyncActor.options(max_concurrency=10).remote()
# 最多 10 个异步任务并发，其余排队
```

### 2.3 ObjectRef 转 asyncio.Future

在 async actor 内部，可以 `await` ObjectRef，或转换为原生 `asyncio.Future`:

```python
@ray.remote
def some_task():
    return 42

# 直接 await ObjectRef (推荐)
result = await some_task.remote()

# 转为原生 asyncio.Future (Python 3.11+)
ref = some_task.remote()
fut: asyncio.Future = asyncio.wrap_future(ref.future())
print(await fut)
```

### 2.4 Internal Buffer + Async Submission Loop (核心模式)

这是构建自适应提交系统的关键模式：async actor 维护内部缓冲区，通过 `while True` 循环持续消费/提交，同时通过 `await` 让出控制权使其他方法可并发执行。

**异步长轮询模式 (Long Polling):**

```python
@ray.remote
class LongPollingActorAsync:
    def __init__(self, data_store_actor):
        self.data_store_actor = data_store_actor
        self.buffer = []  # 内部状态缓冲区

    async def run(self):
        """内部提交循环: 持续拉取数据，通过 await 让出控制权"""
        while True:
            data = await self.data_store_actor.fetch.remote()
            self.buffer.append(data)
            if len(self.buffer) >= BATCH_THRESHOLD:
                await self._flush_buffer()

    async def other_task(self):
        """其他方法可以在 run() 循环期间并发执行"""
        return len(self.buffer)

    async def _flush_buffer(self):
        batch = self.buffer
        self.buffer = []
        # 提交批次到下游...
```

**Queue 缓冲模式 (使用 `ray.util.queue.Queue`):**

```python
from ray.util.queue import Queue

@ray.remote
class BufferActor:
    def __init__(self, output_queue: Queue):
        self.output_queue = output_queue
        self.internal_buffer = []

    async def push_chunk(self, chunk):
        """外部调用者通过 async method 提交数据"""
        self.internal_buffer.append(chunk)
        if self._should_flush():
            await self._flush()

    async def run(self):
        """内部提交循环: 定时检查并 flush"""
        while True:
            await asyncio.sleep(0.01)  # 或更复杂的条件等待
            if self._should_flush():
                await self._flush()

    async def _flush(self):
        batch = self.internal_buffer
        self.internal_buffer = []
        await self.output_queue.put_async(batch)
```

### 2.5 Threaded Actor (替代方案)

对于 CPU 密集型或阻塞操作（不通过 `await` 让出控制权），使用 threaded actor:

```python
@ray.remote
class ThreadedActor:
    def heavy_work(self, data):
        # CPU 密集型操作，在线程池中执行
        return process(data)

actor = ThreadedActor.options(max_concurrency=4).remote()
```

### 2.6 Actor Resource Specification

```python
@ray.remote(num_cpus=2, num_gpus=0.5, memory=1024 * 1024 * 1024)
class ResourceActor:
    ...
```

| 参数 | 说明 |
|---|---|
| `num_cpus` | 需要的 CPU 数 (可为小数) |
| `num_gpus` | 需要的 GPU 数 (可为小数) |
| `memory` | 需要的堆内存 (bytes) |
| `resources` | 自定义资源标签，如 `{"special_hardware": 1}` |

### 2.7 Inter-Actor Communication

**Actor Handle 传递:** Actor handle 可以作为参数传递给其他 actor 或 task:

```python
@ray.remote
class Worker:
    def __init__(self, coordinator_handle):
        self.coordinator = coordinator_handle

    async def do_work(self, data):
        result = await process(data)
        # 向协调者汇报结果
        await self.coordinator.report.remote(result)

@ray.remote
class Coordinator:
    def __init__(self):
        self.results = []

    async def report(self, result):
        self.results.append(result)

coordinator = Coordinator.remote()
workers = [Worker.remote(coordinator) for _ in range(4)]
```

**`ray.wait()` 模式:** 用于处理异构耗时的并发任务:

```python
refs = [actor.do_work.remote(data) for data in dataset]
MAX_IN_FLIGHT = 10
pending = []

for data in dataset:
    if len(pending) >= MAX_IN_FLIGHT:
        ready, pending = ray.wait(pending, num_returns=1)
        # 处理 ready 中的结果...
    pending.append(actor.do_work.remote(data))
```

来源: https://docs.ray.io/en/latest/ray-core/actors/async_api.html, https://docs.ray.io/en/latest/ray-core/patterns/concurrent-operations-async-actor.html

---

## 3. Ray Core Scheduling Architecture

### 3.1 历史演进

Ray 原始架构有两级调度: 每个节点的 **local scheduler** + 中心化的 **global scheduler** (通过 Redis task table)。**此设计已在 PR #4549 中完全移除**，替换为现代 **raylet-based** 架构。

### 3.2 当前架构: Raylet + Peer-to-Peer

- **每个节点一个 raylet**，作为该节点的唯一调度器
- 调度是 **完全分布式、点对点** 的: 无中心化全局调度器
- 任务在 raylet 之间直接转发，不通过中心化组件

### 3.3 调度状态机

当 raylet 接受资源请求 (`RequestWorkerLease` RPC):

| 状态 | 说明 |
|---|---|
| **Granted** | 客户端获得资源和 worker，可执行 actor/task |
| **Reschedule** | 发现更合适节点，任务被转移；本地节点可观察全集群资源使用 |
| **Canceled** | 资源无法满足 (目标机器宕机、runtime env 创建失败) |

### 3.4 Task 调度策略

| 策略 | 行为 |
|---|---|
| **Hybrid (默认)** | 优先本地执行。本地资源使用率超过阈值(默认 50%)时溢出到远程节点。选利用率最低的节点 |
| **Spread** | 跨节点轮询分布，保证负载均衡但可能造成资源碎片 |
| **Node Affinity** | 用户显式指定目标节点。`soft=True` 时节点不可用则 fallback；`soft=False` 则失败 |
| **Data Locality** | 优先调度到参数对象已存在的节点，最小化数据传输 |
| **Placement Group** | 在预留资源组内调度 task/actor |

### 3.5 Actor 调度

- Actor 是 **有状态的**，被固定(pin)到特定 worker 进程
- Actor 实例化时: 选择节点 -> raylet 创建 worker 进程 -> 在该 worker 上创建 actor 对象
- Actor method 调用直接发送到该 actor 所在的 raylet，不跨集群负载均衡
- Actor 的 state 存在于其特定 worker 上，不迁移

### 3.6 Placement Groups (Gang Scheduling)

Placement groups 允许原子性地跨多节点预留资源组:

| 策略 | 行为 |
|---|---|
| **STRICT_PACK** | 所有 bundle 必须在单节点。最大化数据局部性 |
| **PACK (默认)** | 优先单节点打包，不可行时跨节点 |
| **STRICT_SPREAD** | 每个 bundle 必须在不同节点 |
| **SPREAD** | 优先跨节点分布，不可行时允许重叠 |

```python
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

pg = placement_group([{"CPU": 2}, {"CPU": 2, "GPU": 1}], strategy="STRICT_SPREAD")
ray.get(pg.ready())

actor = Actor.options(
    scheduling_strategy=PlacementGroupSchedulingStrategy(
        placement_group=pg,
        placement_group_bundle_index=0,
        placement_group_capture_child_tasks=True,
    )
).remote()
```

### 3.7 Backpressure Mechanisms

Ray Core 提供多层反压机制:

**1. `ray.wait()` 手动反压 (推荐用于 actor task 提交):**

```python
MAX_PENDING = 100
pending_refs = []

for item in data_stream:
    if len(pending_refs) >= MAX_PENDING:
        ready, pending_refs = ray.wait(pending_refs, num_returns=1)
        process_results(ready)
    pending_refs.append(actor.process.remote(item))
```

**2. Streaming Generator Backpressure (PR #40285):**
- 通过 `streaming_generator_backpressure_size_bytes` 参数控制
- Executor 端: 当 `total_object_generated - total_object_consumed > threshold` 时暂停执行
- 客户端: 推迟回复直到对象被消费
- 限制: 不支持 async actor

**3. CoreWorker Direct Actor Task Queue Backpressure (PR #19936):**
- `CoreWorkerDirectActorTaskSubmitter` 检查 max pending task 阈值
- 队列满时返回错误而非无限排队

**4. ConcurrencyCapBackpressurePolicy (Ray Data streaming executor):**
- 基于输出队列增长速率的动态并发控制
- 使用非对称 EWMA 计算 deadband
- 队列增长 -> 降低 concurrency cap；队列缩短 -> 提升 concurrency cap

### 3.8 与 Autoscaler 和 GCS 的关系

- **GCS (Global Control Store):** 持有集群状态快照（资源可用性、阻塞任务、worker 节点配置）
- **Autoscaler:** 定期读取 GCS 快照，运行 bin-packing 算法计算需要的节点数，通过云提供商接口调整集群
- **RuntimeEnvAgent:** 每个节点一个 gRPC server，确保 runtime env 依赖就绪后才调度

来源: https://docs.ray.io/en/latest/ray-core/scheduling/index.html, https://github.com/ray-project/ray/pull/4549

---

## 4. Ray Observability

### 4.1 Ray Dashboard

默认地址 `http://localhost:8265`，提供 Web 监控界面:

| 页面 | 功能 |
|---|---|
| **Actors View** | 列出所有 actor 的 ID、类名、状态(ALIVE/DEAD)、PID、IP、重启次数、日志链接 |
| **Actor Detail** | 元数据、当前状态、所有执行过的 task、CPU profiling (stack trace / flame graph) |
| **Jobs View** | task/actor 按状态分组，按类名嵌套显示 |
| **Metrics View** | actor 数量、状态、CPU/内存的时间序列图 (需 Prometheus + Grafana) |

### 4.2 Programmatic Actor State Query (Ray State APIs)

```bash
# CLI
ray list actors
# 输出: ACTOR_ID, CLASS_NAME, NAME, PID, STATE

# Python SDK
from ray.experimental.state.api import list_actors
actors = list_actors()
for actor in actors:
    print(actor["actor_id"], actor["state"], actor["name"])
```

### 4.3 Prometheus Integration

架构:

1. **Dashboard Agent** 运行在每个节点，收集 raylet、GCS 和 worker 进程的 metrics，在动态端口暴露 `/metrics` endpoint
2. **Service Discovery:** head node 的 `PrometheusServiceDiscoveryWriter` 每 5 秒调用 `ray.nodes()`，将所有节点的 metrics endpoint 写入 `/tmp/ray/prom_metrics_service_discovery.json`
3. **Grafana Dashboards:** Ray 自动生成 Grafana dashboard JSON 到 `/tmp/ray/session_latest/metrics/grafana/dashboards/`

**手动配置方式:**
使用 `ray.nodes()` 获取每个节点的 `NodeManagerAddress` 和 `MetricsExportPort`，然后在 Prometheus 中配置 static targets。

### 4.4 Custom Application Metrics

在 actor 内部定义自定义 metrics:

```python
from ray.util.metrics import Counter, Histogram

@ray.remote
class MonitoredActor:
    def __init__(self):
        self.request_counter = Counter(
            "my_actor_requests_total",
            description="Total requests processed",
            tag_keys=("status",)
        )
        self.latency_histogram = Histogram(
            "my_actor_request_latency_seconds",
            description="Request latency",
            boundaries=[0.01, 0.05, 0.1, 0.5, 1.0]
        )

    async def process(self, request):
        start = time.time()
        try:
            result = await do_work(request)
            self.request_counter.inc(tags={"status": "success"})
            return result
        except Exception:
            self.request_counter.inc(tags={"status": "error"})
            raise
        finally:
            self.latency_histogram.observe(time.time() - start)
```

来源: https://docs.ray.io/en/latest/ray-observability/key-concepts.html, https://docs.ray.io/en/latest/ray-core/ray-dashboard.html

---

## 5. Ray + vLLM Integration Patterns

### 5.1 Native Ray Serve LLM APIs (Ray 2.44+, 2025)

Anyscale 发布 `ray.serve.llm` 模块，提供与 vLLM 的一等集成:

```python
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="qwen-0.5b",
        model_source="Qwen/Qwen2.5-0.5B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(min_replicas=1, max_replicas=4),
    ),
)
app = build_openai_app({"llm_configs": [llm_config]})
```

**关键组件:**
- **`LLMConfig`** — 声明模型、engine kwargs、autoscaling、accelerator type
- **`LLMServer`** — 管理 vLLM engine 实例 (placement groups 处理 TP/PP)
- **`OpenAiIngress`** — OpenAI 兼容的 FastAPI ingress
- **`LLMRouter`** — 多模型路由

### 5.2 Custom Request Routing (Ray 2.49+)

Ray Serve 引入了完全可编程的 Python 路由层:

**PrefixCacheAffinityRouter (旗舰路由):**

```python
from ray.serve.llm.request_router import PrefixCacheAffinityRouter
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="deepseek-r1",
        model_source="deepseek-ai/DeepSeek-R1",
    ),
    deployment_config=dict(
        autoscaling_config=dict(min_replicas=1, max_replicas=8),
        request_router_config=dict(
            request_router_class=PrefixCacheAffinityRouter,
        ),
    ),
)
```

效果: **TTFT 降低 60%**, 端到端吞吐提升 **40%+**。原理: 维护字符级 prefix tree，路由到 prefix 匹配最长的 replica。

**自定义 Router:**

```python
from ray.serve.llm.request_router import RequestRouter

class LatencyAwareRouter(RequestRouter):
    async def choose_replicas(self, pending_request):
        # 获取所有可用 replica 的延迟统计
        replicas = self.select_available_replicas(pending_request)
        # 按延迟排序，选最快的
        replicas.sort(key=lambda r: r.get_latency_p50())
        return [replicas[0]]
```

可组合 mixins: `FIFOMixin`, `MultiplexMixin`, `LocalityMixin`

### 5.3 Disaggregated Prefill/Decode (PxDy)

```python
from ray.serve.llm import build_pd_openai_app, PDConfig

pd_config = PDConfig(
    prefill_config=LLMConfig(...),
    decode_config=LLMConfig(...),
)
app = build_pd_openai_app(pd_config)
```

- Prefill 和 decode 阶段独立扩缩容
- 通过 NIXL connector 自动传输 KV cache
- **已知限制 (2025 Dec):** vLLM v1 engine 不兼容 `build_pd_openai_app` (嵌套 placement group 冲突)；workaround 使用 `distributed_executor_backend="mp"`

### 5.4 Wide Expert Parallelism (MoE 模型, Nov 2025)

```python
from ray.serve.llm import build_dp_deployment

deployment = build_dp_deployment(config)
```

- 自动创建 data-parallel + expert-parallel deployment group
- 集成 DeepEP 和 DeepGEMM 通信库
- Nebius 上达到 2,400 TPS/H200 (InfiniBand)

### 5.5 Ray Data + vLLM (离线批推理)

**原生 vLLM 集成 (推荐):**

```python
from ray.data.llm import vLLMEngineProcessorConfig, build_llm_processor

config = vLLMEngineProcessorConfig(
    model_source="unsloth/Llama-3.1-8B-Instruct",
    engine_kwargs={
        "enable_chunked_prefill": True,
        "max_num_batched_tokens": 4096,
        "max_model_len": 16384,
    },
    concurrency=1,
    batch_size=64,
)

processor = build_llm_processor(
    config,
    preprocess=lambda row: dict(
        messages=[{"role": "user", "content": row["item"]}],
        sampling_params=dict(temperature=0.3, max_tokens=250),
    ),
    postprocess=lambda row: dict(answer=row["generated_text"]),
)

ds = processor(ds)
```

**通过 map_batches 直接使用 vLLM:**

```python
class LLMPredictor:
    def __init__(self):
        from vllm import LLM, SamplingParams
        self.llm = LLM(model="meta-llama/Llama-2-7b-chat-hf",
                       tensor_parallel_size=2)
        self.sampling_params = SamplingParams(temperature=0.8, top_p=0.95)

    def __call__(self, batch):
        outputs = self.llm.generate(batch["text"], self.sampling_params)
        return {"prompt": [...], "generated_text": [...]}

ds.map_batches(LLMPredictor, concurrency=4, num_gpus=1, batch_size=32)
```

### 5.6 构建自适应提交系统的关键模式

**模式 1: Actor Pool + 自定义 Router + Backpressure**

```python
@ray.remote(num_gpus=0.5)
class vLLMWorker:
    def __init__(self, model_id):
        from vllm import AsyncLLMEngine
        self.engine = AsyncLLMEngine.from_engine_args(...)
        self.queue_depth = 0

    async def submit(self, request):
        self.queue_depth += 1
        result = await self.engine.generate(request)
        self.queue_depth -= 1
        return result

    def get_queue_depth(self):
        return self.queue_depth

@ray.remote
class AdaptiveRouter:
    def __init__(self, worker_handles):
        self.workers = worker_handles

    async def route(self, request):
        # 查询每个 worker 的队列深度
        depths = await asyncio.gather(*[
            w.get_queue_depth.remote() for w in self.workers
        ])
        # 选负载最低的
        best_worker = self.workers[depths.index(min(depths))]
        return await best_worker.submit.remote(request)
```

**模式 2: 使用 Ray Core 的 `ray.wait()` 实现发送端反压:**

```python
MAX_IN_FLIGHT_PER_WORKER = 10

async def adaptive_submit(worker, requests):
    pending = []
    for req in requests:
        # 检查队列深度
        if len(pending) >= MAX_IN_FLIGHT_PER_WORKER:
            ready, pending = ray.wait(pending, num_returns=1, timeout=0.05)
            if ready:
                fetch_results(ready)
        pending.append(worker.submit.remote(req))
```

来源: https://www.anyscale.com/blog/ray-serve-llm-anyscale-apis-wide-ep-disaggregated-serving-vllm, https://www.anyscale.com/blog/ray-serve-faster-first-token-custom-routing, https://docs.ray.io/en/latest/data/batch_inference.html

---

## 6. Related Papers & Technical Reports

### 6.1 "Optimizing Distributed LLM Inference Agents via Ray-based Preemptive Scheduling" (Master's Thesis, 2026)

**作者:** Haoran Zhang, 数据科学学院

最直接相关的研究。提出了基于 Ray 分布式框架的多租户 LLM 推理 agent 的端到端协同优化架构。集成三层解耦:

- **Ray orchestration** — 可抢占的微批处理、故障容忍边界上的恢复点
- **Inference runtime** — KV 层级缓存管理
- **Tool service** — 动态批处理

关键成果:
- 亲和/反亲和部署减少跨节点通信瓶颈
- **p50 和 p99 尾延迟降低两个数量级**
- 在请求尖峰、迁移带宽受限、checkpoint 间隔波动等极端场景下保持稳定

### 6.2 "Prophet: An LLM Inference Engine Optimized for Head-of-Line Blocking" (Stanford CS244B, 2024)

完全使用 **Ray actors** 构建的 LLM inference engine，特性:
- Disaggregated prefill/decode 在不同 GPU 上 (inspired by DistServe)
- **SRPT scheduler** for prefill, **MLFQ scheduler** for decode (均含防饥饿机制)
- **KV cache coordination** 通过 NCCL + Ray Collective Communication library
- 评估: Llama3-8B + ShareGPT，显著减少 head-of-line blocking

来源: https://www.scs.stanford.edu/24sp-cs244b/projects/Prophet_An_LLM_Inference_Engine_Optimized_For_Head_of_Line_Blocking.pdf

### 6.3 "DiLLeMa: An extensible and scalable framework for distributed LLMs inference on multi-GPU clusters" (SoftwareX, Feb 2026)

**作者:** Robby Ulung Pambudi, Ary Mazharuddin Shiddiqi, et al.

基于 **Ray Serve + vLLM** 的分布式 LLM 部署框架:
- Ray actor model: 每 GPU 一个 model replica
- 三种部署模式: single-node single-GPU, single-node multi-GPU, multi-node multi-GPU
- FastAPI backend + React frontend + vLLM engine + Qdrant RAG pipeline
- 自适应 GPU 扩展

来源: https://scholar.its.ac.id/en/publications/dillema-an-extensible-and-scalable-framework-for-distributed-larg/

### 6.4 "Rosa: A Robotics Foundation Model Serving System" (NVIDIA/Stanford, arXiv 2026)

基于 **Ray Serve** + vLLM/PyTorch/JAX 的分布式 serving 系统:
- **Factory-objective-driven scheduling** (最大化 SLO 合格吞吐而非最小化单个延迟)
- 启发式 + ILP 组合进行模型放置、请求路由和批处理配置
- 共享 GPU-pool serving 架构

来源: https://arxiv.org/abs/2607.01088

### 6.5 "Enabling Efficient ML Inference in SigmaOS with Model-Aware Scheduling" (MIT M.Eng Thesis, 2025)

**作者:** Katie Liu

将 **RayServe** 集成到 SigmaOS 云操作系统，提出模型感知调度器 (Model Colocation + Centralized Model Registry) 减少冷启动。多租户设置下平均推理延迟降低约 50%。

来源: https://pdos.csail.mit.edu/papers/liu-katieliu-meng-eecs-2025-thesis.pdf

### 6.6 "The Streaming Batch Model for Efficient and Fault-Tolerant Heterogeneous Execution" (2025)

Ray Data 中的 streaming batch model，是 batch 和 streaming 的混合模型:
- 使用 partition 作为弹性的执行单元
- Partition 可动态创建和在 operator 间流式传输
- 异构批推理管道吞吐提升 **2.5-12x**
- Stable Diffusion 等多模态模型训练吞吐提升 **31%**

来源: https://arxiv.org/abs/2501.12407

---

## 附录: 参考 URL 清单

| 主题 | URL |
|---|---|
| Ray Serve Dynamic Batching | https://docs.ray.io/en/latest/serve/advanced-guides/dyn-req-batch.html |
| Ray Serve Batch API Reference | https://docs.ray.io/en/latest/serve/api/doc/ray.serve.batch.html |
| Ray Serve Batching Guide | https://github.com/ray-project/ray/blob/master/doc/source/serve/batching-guide.md |
| Ray Serve Autoscaling | https://docs.ray.io/en/latest/serve/autoscaling-guide.html |
| Ray Core Async Actors | https://docs.ray.io/en/latest/ray-core/actors/async_api.html |
| Ray Core Concurrent Operations Pattern | https://docs.ray.io/en/latest/ray-core/patterns/concurrent-operations-async-actor.html |
| Ray Core Limit Pending Tasks (Backpressure) | https://docs.ray.io/en/latest/ray-core/patterns/limit-pending-tasks.html |
| Ray Core Scheduling | https://docs.ray.io/en/latest/ray-core/scheduling/index.html |
| Ray Core Placement Groups | https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html |
| Ray Observability Key Concepts | https://docs.ray.io/en/latest/ray-observability/key-concepts.html |
| Ray Dashboard | https://docs.ray.io/en/latest/ray-core/ray-dashboard.html |
| Ray Serve LLM Architecture | https://docs.ray.io/en/latest/serve/llm/architecture/overview.html |
| Ray Serve LLM Roadmap (Q2 2025) | https://github.com/ray-project/ray/issues/51313 |
| Ray Serve Custom Router RFC | https://github.com/ray-project/ray/issues/53016 |
| Ray Serve Prefill/Decode RFC | https://github.com/ray-project/ray/issues/53257 |
| Ray Data Batch Inference | https://docs.ray.io/en/latest/data/batch_inference.html |
| Ray Data LLM APIs | https://docs.ray.io/en/latest/data/working-with-llms.html |
| Anyscale: Wide-EP & Disaggregated Serving | https://www.anyscale.com/blog/ray-serve-llm-anyscale-apis-wide-ep-disaggregated-serving-vllm |
| Anyscale: Custom Request Routing | https://www.anyscale.com/blog/ray-serve-faster-first-token-custom-routing |
| Anyscale: Autoscaling & Custom Routing | https://www.anyscale.com/blog/ray-serve-autoscaling-async-inference-custom-routing |
| Streaming Generator Backpressure (PR #40285) | https://github.com/ray-project/ray/pull/40285 |
| Custom Batch Size Function (PR #59059) | https://github.com/ray-project/ray/pull/59059 |
