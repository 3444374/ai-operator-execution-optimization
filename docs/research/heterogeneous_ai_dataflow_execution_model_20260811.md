# 数据库 AI 的 CPU–GPU 异构分阶段执行模型（静态核心已实现）

> 状态：`static-core-implemented / gpu-gate-pending`。本文给出已有架构迁移审计、项目执行
> 模型、数据合同、数学模型和实验门禁；真实 ready broker 与 static Ray adapter 已实现，
> 但 packed-uint8/pinned/DALI、动态 SAOR 与 GPU 性能门仍未完成，不构成性能或新颖性结论。工作名
> **Heterogeneous Staged Execution（HSE）** 只用于工程沟通，投稿前必须继续做相关工作检索。

## 1. 为什么需要执行模型，而不只是继续调 actor 数

### 1.1 本地实验事实

现有 2×RTX 4090、PostgreSQL→Daft→Ray→CLIP 证据把问题限定得很清楚：

- `torchvision_tensor_pt` 的 CPU prepare 仍为 4.44–4.78 ms/image，是模型 actor
  阶段的 13.8–31.2 倍；只替换 processor API 没有消除阶段失衡；
- 60K×2 matched-resource 正式实验中，project CPU8→CPU16 从约 1039 提升到
  1666 image/s，但 GPU busy mean 仍只有约 6%→10%；
- source thread 1→4 的冷吞吐只提高约 2.4%，当前不是简单的 PostgreSQL reader 木桶；
- active batches 16→32 的 post-setup 吞吐只增加约 2%，64 已同时损害吞吐和完成延迟；
- R0 GPU-resident 双卡容量参照约 19K image/s；R1 pinned FP16 只比 R0 低约 10%；
  R2 pageable FP32 的 ownership copy、FP32→FP16 和 H2D 将单卡吞吐降到约 2K image/s；
- Daft built-in 路径在 12K–20K 行之间撞 object-store/memory materialization 上限，
  streaming Ray Data/project 可以执行约 120K 行。

因此当前瓶颈应表述为 **CPU decode/resize/normalize + host representation conversion +
driver/Ray submission 的组合**，不是 GPU forward、PCIe 或数据库读取的单点问题。只提高
model active window 会让未完成的 prepare dependency 更早进入 GPU actor，却不会创造
ready tensor。

来源类型：本地实验事实。证据分别见
`../motivation/results/gpu/image_clip_preprocess_variants_20260801/README.md`、
`../motivation/results/gpu/image_host_path_screening_20260802/README.md`、
`../motivation/results/gpu/image_clip_transfer_ceiling_20260803/README.md` 和
`../experiments/results/image_ai_embed_operator_formal_20260803/README.md`。

### 1.2 研究问题

在不修改 PostgreSQL、Daft、Ray Core、vLLM 或模型 kernel 的条件下，能否通过：

1. 显式分离 CPU prepare 与 GPU model 阶段；
2. 让阶段间传递紧凑、连续、带版本语义的 typed block；
3. 用 bytes/work 有界的 ready buffer、事件驱动补位和多 Job 控制协调两阶段；
4. 在相同 CPU/GPU、输入、模型和质量语义下，优于冻结的最强静态 overlap；

同时保持 exactly-once、任务质量、内存安全、公平与可恢复性？

## 2. 已有架构可以直接借鉴什么

| 来源 | 已有机制 | 本项目的使用边界 |
|---|---|---|
| Ray Data | block 化 shared-memory object store、streaming operator、out queue 和 backpressure；可按 CPU/GPU/concurrency 配置 `map_batches` | Ray Data baseline 由 Ray 自己调度；project 只借鉴 block/streaming，不向 baseline 注入 SAOR |
| Daft | batch UDF、`@daft.cls` 持久实例、CPU/GPU resource request、concurrency 和 Ray placement 参数 | Daft 负责数据库数据引擎与分区；不把自写 broker 冒充 Daft Native |
| DALI | CPU/GPU/mixed image pipeline、parallel external source、prefetch queue、pinned-memory 预分配和 nvJPEG | 作为 work-reduction/data-path baseline；mixed decode 仍可能用 CPU 且占 GPU，必须实测 |
| Apache Arrow | variable binary、FixedSizeList、canonical fixed-shape tensor、CUDA host/device 类型和 device interface | 用于稳定的逻辑/物理表示合同；Arrow Device Interface 仍标 experimental，不预设 Daft/Ray 已端到端支持 |
| PyTorch/CUDA | pinned host memory、nonblocking H2D、独立 CUDA stream 和双缓冲 | 只在 GPU actor 本地实现并实测；`non_blocking=True` 本身不等于 H2D 与 kernel 已重叠 |
| StarPU | task graph、异构 implementation、数据副本/位置管理、性能模型和 data-aware scheduling | 借鉴“任务+数据+代价一起调度”，不在项目中引入 StarPU runtime |
| HetExchange | 用 exchange 封装 CPU/GPU 数据并行与数据移动 | 借鉴显式异构边界；本项目不做关系算子 GPU coprocessing |
| MaxWeight / backpressure | 用相邻队列差驱动多跳服务，在显式假设下可证明 capacity-region 内稳定 | 作为 HSE/SAOR 控制律理论基础；实际估计误差与有限 action set 仍需单独桥接 |

一手依据：

- Ray Data internals：<https://docs.ray.io/en/latest/data/data-internals.html>
- Ray `map_batches`：<https://docs.ray.io/en/latest/data/api/doc/ray.data.Dataset.map_batches.html>
- Ray serialization/zero-copy：<https://docs.ray.io/en/latest/ray-core/objects/serialization.html>
- Daft UDF：<https://docs.daft.ai/en/stable/api/udf/>
- DALI `external_source`：<https://docs.nvidia.com/deeplearning/dali/main-user-guide/docs/operations/nvidia.dali.fn.external_source.html>
- DALI image decoder：<https://docs.nvidia.com/deeplearning/dali/main-user-guide/docs/operations/nvidia.dali.fn.decoders.image.html>
- Arrow fixed-shape tensor：<https://arrow.apache.org/docs/format/CanonicalExtensions.html>
- Arrow C Device Interface：<https://arrow.apache.org/docs/format/CDeviceDataInterface.html>
- PyTorch pinned/nonblocking transfer：<https://docs.pytorch.org/tutorials/intermediate/pinmem_nonblock.html>
- StarPU：<https://doi.org/10.1002/cpe.1631>
- HetExchange：<https://www.vldb.org/pvldb/vol12/p544-chrysogelos.pdf>
- Tassiulas–Ephremides MaxWeight：<https://hdl.handle.net/1903/5346>

### 2.1 代码来源与可主张边界

当前实现没有复制或移植 StarPU、HetExchange、DALI、Ray Data 或其他异构系统的实现代码。
项目直接调用的第三方能力是 Daft batch stream、Ray actor/`ObjectRef`、NumPy/Torch 与现有
CLIP backend；`StageBlockDescriptor`、`BoundedStageBroker`、block lease 状态机、双字节/
work 预算、ready 后才提交 GPU 的静态执行循环和 exactly-once 对账均为本项目新写代码。

因此允许主张的是“针对数据库 AI 外部链路实现并验证了项目级 staged execution/control
contract”；不允许主张发明 actor、object store、backpressure、异构流水线、pinned memory
或 MaxWeight。若后续接入 DALI/pinned ring/Arrow device buffer，必须保留独立 adapter 与
upstream 版本/provenance，并分别与官方路径比较，不能把第三方执行增益归入 SAOR。

## 3. HSE：分级数据面 + 应用控制面

HSE 不是一个新的底层 runtime。它是项目路径中的执行合同：**Daft/Ray 拥有资源放置与
任务执行，typed data plane 拥有表示和生命周期，SAOR 拥有项目侧 admission、Job 选择和
有界中间态。**

```text
PostgreSQL / lakehouse
  │ predicate + projection + immutable row/version id
  ▼
Daft encoded-block stream                         representation R0
  │
  ├── CPU prepare actor pool ─────────────────── representation R1
  │       decode / resize / crop
  │
  └── optional DALI mixed path ──────────────── representation R1/R2
          │
          ▼
bounded ready-block broker
  │ packed uint8 or pinned FP16; byte/work credit
  ▼
resident Ray GPU actor                            representation R2
  │ async H2D + normalize/cast + CLIP forward
  ▼
Arrow embedding blocks                            representation R3
  │ ordered fan-in / exactly-once
  ▼
PostgreSQL + pgvector sink
```

### 3.1 两个控制时间尺度

- **慢时间尺度（校准/部署）**：冻结 CPU actor 数、每 actor 线程数、GPU actor 数、NUMA/
  placement、processor backend、batch action set 和 pinned-memory 上限。actor 创建成本高，
  不允许在线控制周期反复扩缩池。
- **快时间尺度（每 completion/固定观测周期）**：只调整
  `(prepare_inflight, ready_bytes, model_inflight, Job-head release)`；completion 立即补位，
  周期 snapshot 只修正窗口和债务。

这种分工保留 Ray actor 的 stateful/async 特性，也避免把项目写成“重做 Ray scheduler”。

### 3.2 为什么采用 pull-ready，而不是提前排 dependency

当前路径可以把未完成的 `preprocess_ref` 直接传给 GPU actor。Ray 能正确解析依赖，但项目
控制器会同时把该 batch 视作 prepare/model active，失去真实 ready 状态。HSE 改为：

1. broker 仅向 CPU pool 发出 bounded prepare lease；
2. CPU completion 产生 `ReadyBlock` 并原子进入 ready queue；
3. 空闲 GPU actor 从 ready queue 拉取完整 block；
4. model completion 释放 ready-byte/model-work credit；
5. 失败、重试和取消按 block lease 做 exactly-once 对账。

runtime 同时维护显式 idle-actor 集合：prepare/model 提交时领取具体 actor slot，只有对应
future 完成或提交失败才归还。仅满足 `inflight ≤ actor_count` 不够，因为朴素 round-robin
可能把新任务排到仍繁忙的慢 actor 后面；显式 slot lease 使 broker inflight 对应真实可执行
actor，而不是 actor mailbox 中不可见的二级排队。

项目可见的 source pull 也不是无界 look-ahead：runner 从冻结的 PostgreSQL manifest 读取
单图最大编码字节数，以 `batch_size × max_encoded_bytes` 作为单块最坏上界，在调用 Daft
iterator 的 `next()` 前先检查 encoded capacity；实际 block 超过该不可变上界时 fail closed。
该约束覆盖 driver 接管 block 后的 materialization 与 broker 生命周期；Daft 执行器内部的
stream buffer 仍由 Daft 自己拥有，不纳入项目 broker 的安全定理，实验中单独记录 source
buffer 配置与进程内存。

GPU actor 因而只接收真正 ready 的数据，控制器可以区分 CPU starvation、ready-buffer
膨胀和 model congestion。

## 4. GPU 友好型数据合同

### 4.1 不是“所有东西都变成 GPU tensor”

数据在不同阶段有不同最优表示；错误做法是过早展开为逐图 FP32 tensor并长期放进 Ray
object store。建议定义下列物理表示：

| 表示 | 内容 | 推荐布局 | 生命周期 |
|---|---|---|---|
| `EncodedBlock` | row id + JPEG/PNG bytes | Arrow `binary/large_binary`，多行一个 block | DB/Daft→prepare |
| `PreparedU8Block` | resize/crop 后像素 | C-contiguous `uint8[N,C,H,W]`；逻辑上 `arrow.fixed_shape_tensor` | CPU cache/ready queue |
| `PinnedInputBlock` | GPU 即将消费的输入 | actor-local pinned `uint8` 或 FP16 NCHW ring slot | 短生命周期，H2D 完成后复用 |
| `DeviceInputBlock` | GPU 输入 | actor-local contiguous FP16 NCHW | 单 GPU actor/stream 生命周期 |
| `EmbeddingBlock` | row id + vector | Arrow `FixedSizeList<float32>[D]` | fan-in/writeback |

224×224 RGB 每图的理论 payload：

- uint8：`224×224×3 = 150,528 B`；
- FP16：`301,056 B`；
- FP32：`602,112 B`。

所以 ready-buffer 必须按 bytes 记账，不能按 batch 数；在质量等价时，CPU 阶段优先输出
packed uint8，让 normalize/cast 在 GPU actor 内完成。是否比 pinned FP16 更快由实测决定。

### 4.2 `StageBlockDescriptor`

每个物理 block 都应携带不可变描述符，策略层只读元数据，不 import Arrow/Ray/Torch：

```text
block_id, job_id, ordered_sequence
row_ids / row_version_ids
representation = encoded | prepared_u8 | pinned_fp16 | device_fp16 | embedding
shape, layout, dtype, logical_bytes, physical_bytes
content_digest, transform_signature, model_signature
source_work, prepare_work, transfer_work, model_work, result_work
object_ref/locality_key/device_id
created_at, ready_at, lease_id, retry_count
```

`transform_signature` 至少覆盖 decoder、resize/crop、normalization、processor revision 和
输出 layout；`model_signature` 覆盖模型 revision、dtype、projection 和 embedding
normalization。任何签名不匹配都必须 cache miss/fallback，不能静默复用。

### 4.3 零拷贝的诚实边界

- Ray object store 中同节点 NumPy 可零拷贝只读访问，但把 pageable shared-memory array
  变成 pinned tensor 通常仍需要一次显式 copy；不能写成“Ray→GPU 零拷贝”。
- pinned ring 适合留在 GPU actor 或同节点 staging actor 内；跨进程生命周期必须由 lease/
  event 明确保护，不能把临时地址塞进普通 Python descriptor。
- Arrow CUDA/Device Interface 支持 device buffer/IPC 的表达，但 Python、Daft、Ray、Torch
  的实际版本组合未验证前，只列为 held-out engineering candidate。
- GPUDirect Storage 面向 file/storage→GPU direct DMA；当前 source 是 PostgreSQL/network +
  encoded JPEG，且 source-thread 不是木桶，因此暂列 `NO-GO`，不进入当前实现。

## 5. 数学模型

### 5.0 先承认串联系统的吞吐上界

HSE 可以让各阶段重叠并吸收服务时间波动，但不能仅靠 buffer 或 admission 突破瓶颈阶段。
令 source、prepare、model、result 的长期有效服务率分别为
$\mu_s,\mu_p,\mu_g,\mu_o$，则任何稳定、无丢失串行流水线都满足：

$$
X_{E2E}\le \min\{\mu_s,\mu_p,\mu_g,\mu_o\}.
$$

当 prepare 是瓶颈且 GPU 每个输入只执行一次时，长期 GPU 可获得的输入率不超过 $\mu_p$，
因此近似有：

$$
U_{GPU}\le \min\left\{1,\frac{\mu_p}{\mu_g}\right\}.
$$

现有 project CPU16 吞吐约 1,666 image/s，而 GPU-resident 双卡参照约 19K image/s，二者比值
约 8.8%，与约 9.6% 的 GPU busy mean 同量级。这项交叉验证说明当前低 GPU busy 主要由
prepare supply 上界解释：增加 ready buffer、model inflight 或 SAOR K 不可能把 GPU 长期喂满。

因此 HSE 的两类机制必须分开归因：

- **flow efficiency**：真实 ready queue、overlap、completion 补位和有界 buffer，目标是逼近
  当前 $\min_s\mu_s$，减少 bubble、spill 和排队；
- **work/rate improvement**：packed uint8、GPU normalize/cast、DALI mixed path、derived-image
  cache 或更有效的 CPU backend，目标是提高 $\mu_p$ 或减少每图 prepare/copy work。

如果 static HSE 仅改变 flow 而现有 static pipeline 已接近 prepare ceiling，则其合理结果可以
是近似中性；不能为了证明动态调度有效而继续扩大 buffer。只有 work-reduction 提高 $\mu_p$
后，stage balance 和 admission 才可能出现新的最优点。

### 5.1 Tandem queues 与约束

Job $j$ 在控制周期 $t$ 维护：

- $Q^e_j(t)$：encoded、等待 prepare 的 normalized service quanta；
- $Q^r_j(t)$：ready tensor、等待 model 的 normalized service quanta；
- $Q^o_j(t)$：等待 fan-in/writeback 的 normalized service quanta；
- $Z^f_j(t),Z^s_j(t)$：公平和 SLO 虚拟债务。

每个 block 的 native stage work 先除以独立校准的 stage scale/rate，映射到共同的
service-quanta 口径；不能直接拿 `tensor_values` 减 `pixels`。当 prepare 完成时，从 $Q^e$
扣除该 block 的 prepare quanta，同时按 descriptor 中的 model work 向 $Q^r$ 加入对应 model
quanta。令 $I^r_j(t),I^o_j(t)$ 分别为这种跨阶段转换后的实际流入。

动作 $a(t)$ 从离线校准的有限安全集合中选择，预测各 Job 的
$\widehat\mu^p_j(a),\widehat\mu^g_j(a),\widehat\mu^o_j(a)$。令
$D^p_j(t),D^g_j(t),D^o_j(t)$ 为该周期实际完成的 prepare/model/result service quanta，
队列更新为：

$$
Q^e_j(t+1)=\left[Q^e_j(t)-D^p_j(t)\right]^+ + A_j(t),
$$

$$
Q^r_j(t+1)=\left[Q^r_j(t)-D^g_j(t)\right]^+ + I^r_j(t),
$$

$$
Q^o_j(t+1)=\left[Q^o_j(t)-D^o_j(t)\right]^+ + I^o_j(t).
$$

$D^p,D^g,D^o,I^r,I^o$ 均由实际完成 block 及其 descriptor 生成，不用“已提交”代替。
所有动作还必须满足：

$$
\sum_j c_j^p(a)\le C_{CPU},\quad
\sum_j c_j^g(a)\le C_{GPU},\quad
\sum_j B^r_j(t)\le M_{ready},\quad
\sum_j B^o_j(t)\le M_{result}.
$$

其中 $B^r,B^o$ 使用 physical bytes；CPU/GPU actor 和 ready/result buffer 是不同约束，
不能压成一个 active-batch 标量。

ready buffer 的作用是覆盖服务抖动和有限 burst，不创造长期 capacity。设一个 prepared block
的 physical bytes 为 $b_r$，则硬上限只允许
$n_r\le\lfloor M_{ready}/b_r\rfloor$ 个 block；buffer sizing 应由独立 trace replay 选择满足
目标 starvation probability 的最小值，并同时报告 excess residence time。若提高
$M_{ready}$ 只降低瞬时 GPU starvation、却不提高 correct E2E throughput 或 tail，则保留更小
内存点。

### 5.2 有限动作上的 differential MaxWeight / DPP

第一版候选在每个周期最小化等价 score：

$$
\begin{aligned}
\Psi(a)=
&-\sum_j (Q^e_j-\kappa_jQ^r_j)\widehat\mu^p_j(a)
-\sum_j (Q^r_j-\eta_jQ^o_j)\widehat\mu^g_j(a)\\
&-\sum_j Q^o_j\widehat\mu^o_j(a)
-\sum_j Z^f_j\widehat s_j(a)
-\sum_j Z^s_j\widehat g_j(a)\\
&+V\,[c_{tail}(a)+c_{copy}(a)+c_{energy}(a)+c_{memory}(a)+c_{switch}(a)].
\end{aligned}
$$

$\kappa,\eta$ 把不同 stage work 映射到可比较的 service-time/normalized-work；它们必须来自
独立 profile，不能凭经验硬编码。该 score 的直觉是：

- encoded backlog 大、ready 少 → 增加 prepare；
- ready 已堆积 → 抑制继续物化，优先 model；
- result 堆积 → model 不再无界推进，先释放 fan-in/sink；
- fairness/SLO debt 决定多 Job 中先释放谁；
- copy、memory、energy 和 switch cost 防止用无界缓存或抖动换吞吐。

### 5.3 已完成的 broker 安全定理

定义一个 block 的活跃状态集合

$$
\mathcal S=\{E,P,R,G\}
$$

分别表示 encoded queue、prepare lease、ready queue 与 model lease。终态为 completed/failed。
broker 在发出 prepare lease 时，按 descriptor 的 `ready_bytes_estimate` 和 model work **预留**
容量，而不是等 prepare 完成后再记账。

**定理 1（状态唯一与硬容量安全）**。设初始 broker 为空，所有状态变化只经
`enqueue_encoded`、`lease/complete/fail_prepare` 和 `lease/complete/fail_model`；descriptor
immutable identity 校验通过，且每个 prepared block 的实际 bytes/work 不超过 prepare lease
时的预留值。则任意有限执行前缀后：

1. 每个未终结 block 恰好属于 $E,P,R,G$ 中一个集合；
2. encoded held bytes、ready held physical bytes 与 ready held model work 分别不超过
   $M_e,M_r,W_r$；
3. prepare/model lease 数分别不超过 $K_p,K_g$；
4. 只有 $R$ 中的 block 能取得 model lease；同一 row id 不能被重复 admission/completion。

**证明**。对状态转换次数归纳。空初态显然成立。`enqueue` 在写入 $E$ 前检查 block/row
唯一性和 $M_e$；`lease_prepare` 将 block 从 $E$ 原子移入 $P$，并且只有在加入其
`ready_bytes_estimate`/model work 后仍满足 $M_r,W_r,K_p$ 才执行；因此并发 prepare 全部完成
也不会突破预留上界。`complete_prepare` 先验证 immutable identity、实际 bytes/work 不超过
预留，再将 $P\to R$ 并释放估计与实际的差额；`fail_prepare` 释放全部 ready 预留并使
$P\to E/F$。`lease_model` 只从 $R$ 取 block，并在 $K_g$ 下执行 $R\to G$；ready bytes/work
在 model completion 前继续持有。`complete_model` 先验证输出 row 顺序和未完成集合，再执行
$G\to C$ 并释放实际 ready bytes/work；`fail_model` 执行 $G\to R/F$，分别保留或释放额度。
每个转换只移动一个集合成员且同步更新其唯一 state，故四条性质保持。证毕。

这一证明覆盖的是**执行安全与可观测性**，不是吞吐最优、公平或尾延迟定理。代码中的
`_assert_invariants()` 还独立重算各容器持有的 bytes/work 与账本值，测试覆盖成功、失败、
超限、身份变化、重复 row、model-before-ready 和 drain；测试是证明实现与抽象对应的检查，
不是数学证明本身。

### 5.4 尚未完成的稳定性/最优性定理

可尝试证明的 oracle theorem：若 arrivals/service 有界且平稳、真实 service 已知、action set
完整覆盖可行资源组合、arrival vector 严格位于 capacity region 内、descriptor/lease 无丢失，
则基于 quadratic Lyapunov 的 MaxWeight/DPP 控制稳定物理/虚拟队列；在标准 Slater 条件下，
长期 penalty gap 为 $O(1/V)$，平均 backlog 为 $O(V)$。

这仍不是当前已经完成的定理。正式证明还必须补：

1. 三段实际完成量与队列更新的一致性；
2. byte constraints、ordered Job-head 和不可撤销 Ray task 对 capacity region 的影响；
3. finite safe action set 是否为原系统的精确/近似 oracle；
4. service estimation error 的 multiplicative/one-sided/buffered 条件；
5. actor setup/switch cost 的慢时间尺度处理；
6. fairness/SLO virtual queue 的可行性和更新式。

无法完成估计误差桥接时，只能保留 oracle theorem + empirical controller，不能声称实际
HSE/SAOR throughput-optimal。

## 6. 工程模块边界

```text
modalities/image/
  descriptor_builder        # encoded row → neutral StageBlockDescriptor
  prepare_backend           # CPU fast path / derived cache / DALI adapter
  tensor_contract           # representation、shape、dtype、signature、quality
  model_backend              # typed GPU actor; owns CUDA streams/ring

scheduling/runtime/
  stage_broker               # leases、real ready queue、byte/work credit、completion
  saor_pipeline              # pure finite-action decision; no Ray/Daft/Torch import

infrastructure/
  Daft source / Ray placement / PG sink
```

框架/native baseline 与 project 必须继续隔离：Ray Data/Daft Native 使用其官方 graph 和
scheduler；project HSE 才使用 `stage_broker + saor_pipeline`。公共部分只限 source manifest、
processor/model semantics、sink、质量审计和指标。

### 6.1 对当前代码的复用审计

| 当前模块 | 结论 | HSE 最小改动 |
|---|---|---|
| `planning/work.py::WorkDescriptor` | 复用；已经表达 source/prepare/model/result work 与 calibration signature | 不加入 Ray/Arrow/Torch 类型；由 block descriptor 引用 |
| `modalities/image/contracts.py::ImageEmbeddingBatch` | 复用为 model backend 输入，不扩张成全流水线对象 | ready broker 完成后才构造；继续保持完整 batch/row id |
| `build_image_runtime_snapshot` | 当前是 observe-only 近似，把 submitted 同时算入 prepare/model | HSE adapter 接线后由真实 prepare/ready/model/result ledger 取代 |
| `run_project_ray_pipeline` | 当前直接把未完成 preprocess ref 排给 GPU actor | 拆成 source pump、prepare completion、ready queue、model completion 四个小循环 |
| `scheduling/runtime/saor_pipeline.py` | 复用纯控制核；已经无 Ray/Daft/CLIP 依赖 | action 增加 ready physical-byte hard cap；arm service 继续来自离线 profile |

当前代码已按这条约束落地：`planning/blocks.py` 定义中性 descriptor，
`scheduling/runtime/stage_broker.py` 维护 lease/真实 snapshot，
`modalities/image/staged.py` 负责图像签名与物理表示校验，
`modalities/image/staged_execution.py` 是生产消费者。CPU actor 使用 Ray 静态多返回值分别产生
小 descriptor ref 与大 tensor ref；driver 只读取 descriptor，tensor ref 在 broker 判定 ready 后
才作为 GPU actor 顶层参数提交。原 `direct_dependency` 路径保留为强静态对照。

## 7. 最小实施顺序与停止门

### 7.1 先修可观测性，不立即换数据结构

1. ✅ `pending-prepare / ready-block / pending-model` 已变成真实队列与 lease；result 由 driver
   立即审计，尚未加入独立 sink queue；
2. ✅ 每个 block 已记录 identity/signature/work、ready/lease 时间和 logical/physical bytes，
   CSV 新增 queue/residence、limit/peak；
3. ✅ 保持当前 NumPy/Torch FP32 tensor，实现 static broker；
4. ⏳ 与 current direct-dependency project static 的同资源 GPU gate 尚未运行；若只拆队列就
   回退 >5%，先修执行开销，不接动态控制。

### 7.2 再逐项做 data-path 消融

按单因素顺序比较：

1. per-image Python/list → packed block；
2. FP32 prepared → uint8 prepared + GPU normalize/cast；
3. pageable → actor-local pinned ring；
4. single stream → copy/compute double buffer；
5. CPU fast path → DALI mixed；
6. cold raw → signed derived-image cache（单列冷建库/refresh 成本）。

每步未达到 E2E/JCT ≥5% 且方向可重复，就不进入下一组合；不能把六项叠加后再反推贡献。

### 7.3 最后加入动态控制

只有 static HSE 达到或超过当前冻结 project static，且能准确观测 real ready queue 后，才比较：

1. static HSE 最小饱和点；
2. 简单 threshold/differential controller；
3. SAOR-HSE（加 Job ordered release、公平/SLO debt）；
4. oracle/offline action sequence（仅作 regret 上界）。

## 8. 实验合同

### 8.1 Baseline

- Ray Data native：官方 streaming graph，自有 backpressure；
- Daft built-in：单列容量/物化诊断，不跨规模排名；
- current project frozen-static：CPU actor + GPU actor + 固定 active window；
- static HSE：真实 ready queue + typed/byte-bounded block，无动态策略；
- DALI path：相同 encoded input、processor/model/quality 的 data-path baseline；
- SAOR-HSE：只在前述门通过后加入。

### 8.2 必须固定

PostgreSQL source manifest、row/version id、processor/model revision、layout/dtype、batch candidate
set、CPU/GPU 数、actor thread budget、quality semantics、sink、warmup/formal 次数和计时边界。

### 8.3 指标

- 主指标：verified operator/system JCT、images/s、first-output、P95/P99；
- 阶段：DB/source、prepare queue/service、ready residence、H2D、model queue/service、fan-in/sink；
- 资源：CPU-core-s/image、GPU-s/image、GPU busy/MFU（若可得）、pinned/object-store/heap/device
  memory、spill、bytes/image、copy count、energy/image；
- 控制：每 stage backlog/work/bytes、action、lease、stale/fallback、overshoot/recovery、switch；
- 多 Job：weighted attained service、normalized slowdown、Jain、max service gap、SLO goodput、
  starvation；
- 正确性：exactly-once、row-order mapping、embedding shape/finite、recall@k/nDCG、失败/重试。

### 8.4 Fatal-flaw audit

- 只比串行 baseline：无效；必须超过同资源的强 static overlap。
- 只降低 CPU time、把工作搬到 GPU：无效；必须报告 GPU model capacity/energy 是否受损。
- cache 不计 build/refresh：无效；冷查询与热查询分轨。
- 使用更多 CPU/pinned/object-store memory：无效；matched-resource + bytes cap。
- Ray NumPy zero-copy 当成 GPU zero-copy：无效；逐边记录 copy ownership。
- GPU 利用率提高但 JCT/tail/quality 变差：不能晋级。
- 只在 CLIP/224 生效：可以作为条件性结果，但不能声称通用 AI 数据系统。

## 9. 暂缓候选：prompt 变化感知、结果复用与增量推理

这三项与 HSE 的 descriptor/signature 能自然衔接，但不进入当前关键路径。

### 9.1 Prompt 变化感知

- 把 prompt 拆成 template/system/context/row-value 等**逻辑 segment**并计算版本签名，不把单行
  prompt 拆成多个互相隔离的 vLLM 请求；
- 用 `(template_revision, segment_hashes, tokenizer_revision)` 判断 exact unchanged、prefix-only
  change、suffix change 和 full change；
- 先用于 prefix-affinity routing、tokenization/result cache lookup 和代价估计；
- 需对比简单 full-hash，只有决策收益覆盖签名/索引成本才实现细粒度结构。

### 9.2 结果复用

- 第一阶段只做 exact cache，键至少包含 source row/version、完整 prompt/input digest、模型/
  tokenizer/processor revision、sampling/decoding 参数和输出语义；
- derived image cache 与 exact AI result cache 分层，前者复用输入变换，后者复用最终结果；
- semantic cache 会改变结果语义和质量，只列长期候选，必须有 similarity threshold、任务质量、
  false-hit 和失效策略；不能与 exact cache 混报 hit rate。

### 9.3 增量推理

- **数据库级增量**：优先做 change-data/version aware execution，只对新增/更新行重跑，并复用
  未变化行结果；这是最符合数据库/湖仓背景且无需修改模型的路线；
- **prompt/prefix 级增量**：先利用 vLLM APC/prefix-affinity 的已有能力，不声称项目实现 KV
  增量计算；
- **模型内部增量**：任意 prompt 中部修改后的 KV 修补、跨请求 KV 生命周期或增量 attention
  需要 engine/model 支持，当前列 `parked-conditional`。

触发条件：HSE static data path 和 SAOR dynamic gate 完成后，真实 workload 中 exact/derived
reuse opportunity ≥10%，且离线 oracle 显示扣除 lookup/refresh 后 E2E 或成本潜力 ≥5%。

## 10. 当前结论

HSE 的 static core 已实现，但仍只是图像主路径的**执行底座候选**，不应上升为新的第三研究内容。它把研究内容一
的 work-unit/数据表示和研究内容二的 admission/多 Job 调度连接起来，并为文本 vLLM 与图像
typed actor 提供同一 staged descriptor。当前最小待验证增量是“同资源 direct-dependency
static vs real-ready/byte-bounded static broker”；packed uint8、pinned ring、DALI 与动态 SAOR
都必须在该 gate 后逐项进入。

只有 static HSE 在 matched-resource 下超过 current project frozen-static，动态 SAOR-HSE 又超过
static HSE/简单 controller，才能分别声称 data-path 与动态控制增量。否则保留 Ray/Daft 原生
执行和当前静态 project，收缩方案而不是继续叠加机制。

<a id="frontend-stage-audit"></a>
## 11. 从语义查询核对准备复用：2026-10-13独立切片

本节研究 PostgreSQL 内置 AI 语义算子的外部分布式物理执行与调度优化中的数据准备问题。
来源为固定源码、既有原始图像记录和本地无模型检查；没有执行新的数据库、HTTP模型或GPU实验。
专用分支为`exp/semloom-stage-execution`，从native提交`62445a7e7774e51108cfda194210cf8b72951002`建立。
它与stability提交`9bb6c4154a3fa321c49a84f45eaeb6c7e12b129c`双向均非祖先，共同祖先为`2fad623e`。
未合并stability；正式配对前由主任务协调共同来源及仍未整合的稳定性改动。
仓库内未发现项目专属`.agents/skills`，本切片使用现有规则和下面的设计/源码。

### 11.1 前端能力与实际接点

| 固定来源 | 已核实的语义入口 | 准备与执行接点 | 本切片尚不能声称的能力 |
|---|---|---|---|
| LOTUS1.2.4，`b1a85fd7a66fabed8a1585d44d7597d592b4433f` | `ImageArray`列进入`sem_filter`和`sem_map`；图像可与普通文本列一起成为消息 | `df2multimodal_info`先取每行图像，再由formatter产生`image_url`内容列表，随后LM调用；现有SemLoom接点在`_process_uncached_messages` | 当前[SDK适配](../../code/src/semantic_methods/lotus/sdk.py)只接受字符串`content`，图像替换执行尚未接入；固定目录树未发现视频/音频类型或原生抽帧/窗口入口 |
| DuckDB AI0.4.14，`9b7b16a5d5bfa97180b8be48d69bd9a4a4106419`；DuckDB1.5.4 | `ai_complete`/`ai_try_complete`及文本embedding；SQL completion输入是VARCHAR | `ChatMessagesJson`将prompt写为字符串`content`；[已有补丁](../../code/integrations/duckdb_ai/duckdb_ai_semloom.patch)在`PrepareCompletion`之后、`RunProviderJobs`之前交接完整调用 | 该固定completion路径不接受图像内容列表；把图片URL或base64当字符串并不增加视觉输入能力 |
| Sema公开产物，仓库来源`3f2c7182bdaa26c1e8925f486585da25337e687e`，二进制`v0.0.1 8c2d3bd2` | 文本`WHERE s'…'`和`SELECT s'…'`，作者材料列出Filter/Map及其组合 | [现有请求服务](../../code/src/execution_provider/adapters/sema_request_service.md)在完整HTTP请求到达后接入，原SQL、请求池和提示已执行 | 尚无可核验图像/视频入口；OpenAI兼容格式不证明视觉支持。当前公开目录API返回404，复用已保存的二进制/归档核验，不补造新实库检查 |

LOTUS依据：[图像示例Map](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/examples/op_examples/multimodal_ops/map.py)、
[Filter](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/examples/op_examples/multimodal_ops/filter.py)、
[ImageArray](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/dtype_extensions/image.py)、
[图像加载](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/utils.py)、
[消息格式化](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/templates/task_instructions.py)。
DuckDB依据：[provider](https://github.com/leonardovida/duckdb-ai/blob/9b7b16a5d5bfa97180b8be48d69bd9a4a4106419/src/duckdb_ai_provider.cpp)
§`ChatMessagesJson`/`RequestPayload`/`EmbeddingPayload`与
[函数登记](https://github.com/leonardovida/duckdb-ai/blob/9b7b16a5d5bfa97180b8be48d69bd9a4a4106419/src/duckdb_ai_extension.cpp)。
下载的provider与extension摘要分别匹配既有[source_identity.json](../../code/integrations/duckdb_ai/source_identity.json)，
LOTUS的LM、Map和formatter摘要匹配既有adapter；没有复制供应商实现进项目。
Sema依据：[固定README](https://github.com/BITQiKangK/SemaSystem/blob/3f2c7182bdaa26c1e8925f486585da25337e687e/README.md)
及上述局部核验记录，公开README的示例与当前未取得的目录树分别解释。

### 11.2 一个主场景和一个外部有效性场景

**主场景：图库或商品条目的图文一致性审核。** 普通关系处理先得到`row_id, asset, listing_text`，
同一资产可以关联多个条目；每条记录调用原生`sem_map`判断图片是否符合文字描述，分别保留其结果。
最小原生形态为`ImageArray`图像列加文本列，指令引用`{image}`与`{listing_text}`。
可优先用已有COCO图像和标签构造身份明确的图库核验输入；商品数据只是应用场景，不声称已有商品实测。
初次比较使用一个Map，不引入代理、级联、少做调用或不同抽帧规则。
Filter→Map可以作为后续消费者寿命验证，但须保留实际Filter选择和后继调用，当前原型未接入该组合。

**外部有效性场景：文本记录审核。** LOTUS Map、Sema语义投影、DuckDB AI completion均有既有入口。
比较各自原方法的native执行与SemLoom执行，保留原提示、SDK转换、NULL及parser。
这里确定性准备可能很轻，预期可以得到“无收益”的反例；不为补齐表格给Sema或DuckDB增加视觉接口。
跨前端只有输入、任务质量和必要模型工作可对齐时才比较；原提示或JSON解释不同则只报告各前端内部配对。

共性问题是：同一有限合法输入中，多个逻辑调用如何共享确定性准备而保持独立调用与结果寿命。
不同前端可以表达这个问题，但作用位置不相同：LOTUS视觉准备在LM之前，DuckDB完整调用在原池之前，
Sema当前只能在收到正文之后管理留存，无法通过该接点减少已做过的前端解码或提示准备。
未来轨迹帧筛选也可能共享相同帧/变换；须先定义必要帧、时序和随机变换语义，不能以逐图Map代替轨迹窗口。

### 11.3 缺口、假设与反例

现有[编码块](../../code/src/modalities/image/staged.py)的摘要包括`row_ids`，`block_id`又包括Job/序号，
因此同一图片在不同逻辑行中的身份不同。[阶段broker](../../code/src/scheduling/runtime/stage_broker.py)
已按块预留encoded和ready字节、区分prepare/model、处理lease及结果；这些已有能力不作为本切片新增贡献。
LOTUS自己的`ImageArray._cached_images`按`(index,image_type)`缓存，复制可带缓存，`take`产生新数组；
不能从“有缓存”推出不同条目/算子已按共同资产共享准备。

**可证伪假设：** 当真实准备成本显著、资产复用能落在有限字节额度内，且producer能便宜地提供
“内容身份＋确定性变换＋实际消费者寿命”，按资产管理准备可减少重复decode/编码和无消费者留存，
并可能改善完整查询时间。新增信息是资产到逻辑调用的对应与未结束消费者；当前scheduler只知道任务/块身份、
工作量和队列，HTTP正文也不天然说明谁还需要本地主机表示。
producer取得内容摘要、版本及寿命信息的读取/计算成本必须计入。只有超出同信息的静态内容缓存和原生引擎，
才能讨论额外决策价值；这仍是已有[准备复用候选](优化方法依据.md#query-specific-execution-opportunities)的小切片。

| 需要分别测量的对象 | 当前实际情况与反证 |
|---|---|
| encoded与decode膨胀 | LOTUS`fetch_image`实际解码/转RGB、存PNG再base64，并非原JPEG的直接base64；压缩长度不能决定RGB暂存，PNG大小又依内容而变 |
| frame与token工作量 | 一帧的编码字节、原分辨率、转换成本和模型视觉token并不相同；CLIP的固定224像素工作也不能代表原图decode成本 |
| host、pinned、device驻留 | 现有CLIP actor显式处理Ray只读NumPy的主机复制，再`torch.as_tensor`入设备；没有本切片可控制的pinned/device复用。黑盒视觉HTTP服务内部decode/H2D不可观测，不补造GPU驻留信息 |
| 背压与转换峰值 | 既有ready额度覆盖固定输出表示；原图decode、临时转换、PNG/base64及完整请求副本仍需另测。原型仅对调用者声明的host输出/临时空间预留，不能证明RSS或任意decoder分配受控 |
| 数据移动与结果聚合 | 每行仍序列化并发送相同完整模型消息，base64共享不减少网络字节或必要模型调用；LOTUS仍整阶段合并。原型不持有模型输出，也未改变原聚合等待 |

无重复资产、复用距离超出额度、摘要/lookup比准备更贵、准备远小于模型服务、原生缓存已覆盖、或新增消息副本
抵消准备节约时，应无收益或更慢。原生Ray Data/Daft已经支持异构stage、资源配置和背压；普通内容缓存也能
减少重复确定性准备。若同信息静态方案达到相同结果，应采用该方案，不再声称动态调度增量。

### 11.4 最小原型、证据与后续配对

[PreparedAssetWindow](../../code/src/data/materializers/prepared_assets.py)仅管理一个producer已许可的有限窗口，
以内容摘要和变换签名独立标识准备资产；每个消费者有独立lease，模型请求/work仍由原有执行者逐项处理。
准备前预留输出上限加临时空间，活跃lease不可淘汰，只有闲置数据按LRU（最近使用顺序）淘汰，最后一个实际
消费者结束即归还；取消未取得lease的需求不释放已取得的使用者。跨Job/快照共享、随机变换、异步worker集成、
真实内存上限及pinned/device管理均未实现。它没有接入生产入口，未改公共调度、HTTP或观察设施。

本地证据：12项[资产检查](../../code/tests/data/test_prepared_assets.py)、2项
[LOTUS源码函数fixture](../../code/tests/modalities/image/test_lotus_stage_preparation.py)、
原4项stage descriptor及8项broker回归，共26项通过，无跳过，模型调用/GPU启动为0。
源码fixture仅编译已核对摘要的`fetch_image`、`context_formatter`、`user_message_formatter`，没有运行完整LOTUS
DataFrame、LM或供应商SDK；两文件摘要为`2d8d38af8230f447a354f460e25a8f523f1dc1aa5edebf977a8297ed04458ebe`
与`059909938902f3fa44ae1fc9666ce91452266f1e4e8d57c5c81134c24e51a8a3`。
4条不同图文消息使用2个本地小图，原逐行准备4次，原型和同信息静态缓存均2次；完整消息值逐字节相同，
逆序释放仍保留独立消费者。**静态缓存反例成立，不能据此宣称新增调度收益。**
同一fixture还调用当前adapter的实际消息检查：文本通过，原生图像列表被明确拒绝；没有绕过该检查执行模型调用。
另生成1024×1024纯色PNG，编码小于16KiB而RGB为3MiB；只有最终输出能容纳时仍拒绝启动需要更大暂存的准备。
首次unittest模块路径命中了Python自带`code`模块，在测试加载前失败；使用既有目录的discover入口后上述检查全部通过。

复核命令使用已有Python、NumPy/Pillow，不安装LOTUS或其他重依赖：

```sh
PYTHONPATH=code python3 -m unittest discover -s code/tests/data -p 'test_prepared_assets.py' -v
PYTHONPATH=code SEMLOOM_LOTUS_SOURCE_ROOT=<pinned-checkout>/lotus \
  python3 -m unittest discover -s code/tests/modalities/image -p 'test_lotus_stage_preparation.py' -v
```

以下为`92bcd05a`时点的后续需求，已完成的独立接线见§11.5：为现有SDK增加独立版本的图文内容profile及`utils.py`/ImageArray摘要核对，
由实际SDK产生并重放完整图像调用；再在图像格式化前增加query-local准备接点，不只改LM之后的执行器。
保留原native入口、模型/processor修订、PNG/RGB与采样配置，核对全消息、响应、usage、错误、关联及退出。
现有[151次真实CLIP前向](../../experiments/results/postgresql/capacity_organization_image_validation_20260920/raw/image-summary.json)
及[清理](../../experiments/results/postgresql/capacity_organization_image_validation_20260920/raw/image-cleanup.json)
确有`real_forward_attempts=151`，模型`openai/clip-vit-base-patch32`、修订`3d74acf9a28c67741b2f4f2ea7635f0aaf6f0268`、
FP32/224/512维；只能复用该受限图像语义/生命周期依据，不能代替LOTUS视觉接入、质量或性能资格。

后续先做零模型SDK与完整查询fixture。若通过且主任务另给视觉服务预算，建议最多32条图库记录、4个执行配置：
原native、同方法SemLoom、SemLoom强静态有界内容缓存、显式消费者寿命候选；另保留原生Ray Data/Daft的相同方法
执行图作分批扩展对照，不向它们注入本原型。输入资产/帧、原提示、模型修订、必要调用和结果规则完全相同。
每配置预热1次/测量3次，加两条配对正确性检查，总计576个必要模型调用，上限640次/15分钟，单固定服务；
这是建议预算，当前未获新模型运行授权，也未执行。小规模只作资格与stage画像，不作吞吐/P99结论。
测完整查询、准备/PNG/base64、序列化、排队、服务、fan-in、释放，记录所有副本及实际内存；服务内部GPU时间
不可取得时明示。先有资格与可解释准备占比，再决定是否扩大；收益仅来自静态缓存或完整查询更慢时停止候选。

### 11.5 图文接线与实际前端检查

`607b9f88`是可独立集成的通用接线patch，只改`lotus/sdk.py`、`lotus/batch.py`并增加测试，
不包含准备缓存、公共HTTP或调度器变化。显式`lotus-image-text-v1`沿用原SDK处理原PNG data URI与文本列表，
保留原生Map/Filter及完整JSON请求值、内容/行顺序、parser与usage；默认text仍保持。
固定SDK9项无模型配对通过，旧文本54项中50通过、4项失败均在原`92bcd05`复现；
范围、失败、双摘要和代码入口由[唯一检查报告](../../experiments/results/diagnostics/lotus_image_text_compatibility_20261013/README.md)维护。
接续诊断发现JSON副本会将原SDK拒绝的surrogate规范化并提交，已加编码预检；更新其余旧断言后，
55项文本和11项图文实际SDK检查全部通过，首次失败原件保持。已有12个COCO资产摘要重核一致，
JPEG为62,406–204,829字节、RGB逻辑占用562,500–983,040字节、PNG data URI为269,962–871,878字节；
这证明不同阶段表示须分别计量，不是准备耗时、allocator峰值或GPU驻留测量。
同信息静态缓存反例没有改变，没有取得超静态收益。

固定Sema二进制的函数目录及2次localhost探针表明，当前公开普通VARCHAR语义投影将PNG路径和data URI
放进字符串content，不产生视觉内容列表；不把它加入图文native性能配对。
实际生成式VLM尚未核实，4个已有模型config中的CLIP只能作为图像embedding能力，不能承担本图文Map。
这轮不运行真实模型，后续阶段画像和质量比较仍需协调共同基线与视觉服务能力。
