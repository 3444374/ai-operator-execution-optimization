# vLLM 0.25.1 CLIP pooling 本机能力门禁（2026-08-04）

## 1. 实验设置

本实验只回答一个问题：当前 AutoDL 环境能否用 vLLM pooling 对一张本地图像返回
合法的 CLIP embedding，从而允许继续做在线门禁和容量测试。

- 机器：2× NVIDIA GeForce RTX 4090（每卡 24,564 MiB），驱动 595.58.03。
- Python 3.12.3，vLLM 0.25.1，PyTorch 2.11.0+cu130，CUDA runtime 13.0。
- 模型：服务器本地 `clip-vit-base-patch32`；`config.json` SHA256 见
  `environment_snapshot.txt`。
- 输入：COCO val2017 的固定 JPEG；文件 SHA256 见同一快照。
- runner：vLLM offline `LLM(..., runner="pooling", enforce_eager=True)`。
- 代码：`d97dfca` 的 capability worker 与进程组超时监管器；后续 `main` 只增加
  B 线证据修正，没有改变本轮 A 线执行语义。

`environment_snapshot.txt` 是两次门禁结束后补采的只读环境快照，不冒充运行时资源
trace。两次运行的命令、起止时间、退出码和环境变量以各自
`process_status.json` 为准。

## 2. 实验设计

成功条件预先固定为：监管器退出码为 0、没有超时、worker 生成 `result.json`，且其中
图像和文本 embedding 的维度、有限性与非零范数检查全部通过。只有满足这些条件，才
允许启动在线 pooling server；在线门禁通过后，才允许 5K calibration 或 60K formal。

本轮执行两个最小对照：

| run | 唯一变化 | 目的 |
|---|---|---|
| `vllm_clip_offline_1` | sampler 使用 vLLM 默认值 | 验证默认本机能力 |
| `vllm_clip_offline_no_flashinfer_sampler` | `VLLM_USE_FLASHINFER_SAMPLER=0`，固定 GPU 0 | 判断 top-p/top-k sampler 是否是必要阻塞条件 |

两臂都使用 eager 模式，明确关闭 torch.compile 和 CUDA Graph，单次超时 600 秒。第二臂
不是性能配置，只是一个单变量诊断。

复现命令入口：

```bash
/root/autodl-tmp/venvs/vllm-4090/bin/python \
  code/scripts/profiling/run_vllm_clip_pooling_gate.py \
  --output-dir <new-output-dir> --timeout-seconds 600 -- \
  --model /root/autodl-tmp/models/clip-vit-base-patch32 \
  --image /root/autodl-tmp/data/raw/coco_val2017/val2017/000000212226.jpg
```

## 3. 严谨性自检

- 输入文件和模型配置存在性在 worker 启动前 fail closed；报告保存二者 SHA256。
- 每臂使用不同的新输出目录，拒绝覆盖；监管器保存 stdout、stderr 和机器可读退出状态。
- 600 秒到期后终止整个进程组，而不是只杀父进程；退出码 124 明确表示 timeout。
- `result.json` 不存在时不从日志推测 embedding、吞吐或正确性。
- 第二臂只改变 sampler 环境变量；它不能隔离 vLLM 初始化中的所有其它内核、通信、
  权重加载或多模态处理因素。
- 本轮没有 profiler stack：容器禁止 ptrace。故障位置只能由最后一条可见日志限定，
  不能写成已定位到某个函数或 JIT kernel。
- 两次运行均为 capability gate，不包含 warmup/formal repeat，不是性能实验。

## 4. 实验数据

| run | 墙钟 | 最后可见阶段 | exit | result JSON | 判定 |
|---|---:|---|---:|---|---|
| 默认 sampler | 600.12 s | V1 EngineCore 完成单进程 rank 分配；随后记录使用 FlashInfer sampler | 124 | 无 | timeout/blocker |
| 禁用 FlashInfer sampler | 600.12 s | V1 EngineCore 完成单进程 rank 分配；明确记录 sampler 已禁用 | 124 | 无 | timeout/blocker |

共同日志事实：vLLM 成功解析 `CLIPModel`，选择 pooling runner，最大模型长度为 77，
并确认 eager 模式关闭 compile/CUDA Graph。两次运行都没有产生模型 embedding，也没有
出现可以证明模型权重已完成加载和首个 forward 的日志。

机器可复算材料：

- `summary.csv`：两臂紧凑状态表；
- `raw/*/process_status.json`：命令、时间、环境变量、退出码和结果文件状态；
- `raw/*/stdout.log`、`stderr.log`：未裁剪日志；
- `MANIFEST.sha256`：raw 文件完整性校验。

## 5. 结果解释

### 实验事实

1. 当前 `vLLM 0.25.1 × PyTorch 2.11.0+cu130 × 当前容器 × CLIP` 组合在两次
   600 秒离线门禁中都未返回 embedding。
2. 禁用 FlashInfer top-p/top-k sampler 没有解除超时，因此该 sampler 不是本轮
   blocker 的充分解释。
3. 代码注册和配置解析证明 vLLM 认识 CLIP pooling 接口，但不证明该组合可运行。

### 合理推断

阻塞发生在 V1 EngineCore 初始化期间或其后、首个可观测 embedding 之前。范围仍包含
权重加载、worker/EngineCore 同步、多模态 processor 初始化、其它 kernel warmup 等多个
候选，现有日志不能进一步唯一归因。

### 待确认

- 在官方已验证的 vLLM/PyTorch/CUDA 版本矩阵或独立新 venv 中是否能够通过同一门禁；
- 开启允许 stack/profiler 的容器后，EngineCore 子进程实际等待点是什么；
- 若 capability 通过，在线 pooling 的输入语义、L2 normalization 和项目 CLIP actor
  是否一致。

### 不能声称

- 不能声称“vLLM 永久不支持 CLIP”或“4090 不支持 vLLM pooling”；
- 不能声称已证实是 FlashInfer JIT、CUDA Graph、torch.compile 或模型文件导致；
- 不能报告 images/s、GPU 利用率、embedding 质量或与 project/Ray Data 的排名；
- 不能把本门禁当作数据库 AI 算子 baseline。它只是一条 direct-service ceiling 候选。

## 6. 对课题的含义

vLLM CLIP pooling 的角色与文本轨的 `vLLM bench serve` 类似：如果可运行，它用于测量
绕过 PostgreSQL、Daft、Ray 数据链路后的强服务上限，回答“服务本身最多能处理多少”。
它不是 Daft built-in、Ray Data native graph 这类系统 baseline，也不是项目策略的直接
对手。

当前结论是 **baseline candidate unavailable on this environment**。因此图像系统比较继续
使用已经过 provenance 门禁的 Daft built-in 与 Ray Data native graph；不得用缺失的
vLLM pooling 数字填表或用其它机器公开数字替代同机测量。

## 7. 下一步

1. 保持当前文本 vLLM 环境不变，另建隔离 venv，选择 vLLM 官方支持矩阵中的一组版本，
   先重复同一 1-image gate；不在现有环境原地升级。
2. 若仍超时，在允许 ptrace/Nsight Systems 的容器保存 EngineCore 父子进程 stack 和
   CUDA API timeline；只有新证据出现才更新根因。
3. capability 通过后依次做在线 1-image gate、256-image 语义/parity gate、5K
   calibration；任何一级失败都停止，不直接跑长 formal。
4. 在此之前，A 线状态固定为 `blocked`，不消耗 GPU 时间反复运行等价 flag 组合。
