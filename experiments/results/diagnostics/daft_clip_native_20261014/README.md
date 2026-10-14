# Daft 本地 CLIP 原生批次与独立资格

`DaftClipExecutor` 已实现；真实 Daft 0.7.21 的 CPU kernel 替身检查 13 项通过、0 跳过，
匹配 Linux 环境同样 13 项通过，相关旧图像回归 39 项通过。独立设计与代码审核已完成，初测失败及后续修订保留于
[软件身份](raw/software.json)、[SDK 输出](raw/sdk.log.txt)、[旧路径回归](raw/image_regression.log.txt)和[首次失败摘录](raw/initial_failure.txt)。
服务器源码传输曾被自动审批拒绝，两次操作均未执行；收到用户既有明确上传原文授权后，
同一操作重试成功，三个源码摘要核同。首轮 9 图真实 CLIP 数值通过，但清理判据失败保留；
修订验证器后 [gpu-02](raw/gpu_passed.json) 四次完整向量逐位同 Direct，模型释放及物理退出核验通过。
完整 LOTUS 集成由独立负责人接续，本报告只支持批次后端资格。

## 接点与所有权

文件为 [`daft_clip.py`](../../../../code/src/baselines/image/frameworks/daft_clip.py)。

```python
from src.baselines.image.frameworks.daft_clip import DaftClipExecutor

executor = DaftClipExecutor(
    model_path=model_root, processor_path=processor_root,
    dtype="float32", batch_size=32, model_workers=1, gpus_per_worker=1,
)
result = executor.embed_batch(doc_ids, encoded_images)
executor.close()
```

- `doc_ids` 是非空字符串组成的唯一 tuple；`encoded_images` 是逐 ID 对应的 `Sequence[bytes]`。
  空批次返回 `float32[0,512]`，不启动图或模型；普通路径字符串不作为图片输入。
- LOTUS 负责人将 RGB PIL 图像编码为无损 PNG 后调用本接点。Daft 原生 `decode_image(..., RGB, on_error="raise")`
  拥有解码；显式慢 `CLIPImageProcessor` 拥有 resize／crop／rescale／normalize，复用现有 `ClipTensorActor`
  的 projection 与 FP32 L2 normalize。输出为既有 `ImageEmbeddingResult`，完整 `float32[N,512]`，按输入 ID 恢复行序。
- `semantics` 是既有 `EmbeddingSemantics`。两个 revision 字段为实际本地 model／processor root；
  `identity` 另记录 weights／model config／processor config 的 SHA-256、软件、计算精度、batch、模型并发上限、GPU 声明及调度所有者。
  loader 只读取本地资产。运行期间这些目录必须保持不变；哈希记录不等同文件系统 snapshot。
- 原生 `daft.cls`／`method.batch` 拥有有状态模型、批次、资源声明与执行图。没有 HTTP、SemLoom 调度或自写请求池。
  UDF 和图由本项目编写，身份为 `diagnostic_reference`，不称 Daft 官方内置 `embed_image`。
  现有内置、JPEG 文件读取及 fast processor 路径未改。
- 单个执行器拒绝同时活动的调用。默认一次至多 4096 行，结果收集仅属于调用方有限批次；
  这不是 Daft 内部内存或总缓冲上限。0.7.21 native runner 没有把 `to_arrow_iter` 的结果缓冲参数传入执行器。

## 生命周期与计时

每次调用构建新图并完整消费，然后核对 ID、shape、finite 和范数。相同有状态 class wrapper 可被
Daft 跨图复用；本机 CPU 实测为 driver 进程内实例，两图共 6 个 UDF 批次只加载一次，`close()` 后销毁。
`use_process_requested=True` 是声明，不能据此声称进程位置；本次 GPU 轨迹同样核实为 driver 内实例。
首次模型载入、实例复用后的查询消费与关闭分别记录，复用后的短查询不等同持续稳态或饱和点。

成功调用后 `close()` 释放本执行器的 class wrapper，并拒绝后续调用；活动调用中拒绝 close。
原生图失败后拒绝复用；SDK 没有公开 worker join 接点，因此 close 会报告未知退出。
模型异常先尝试确认自有 CUDA 工作，同步失败保留原错误并附注。该处理不把迭代器关闭当成完整退出证明。
验证器的 lease 释放若失败，保存已完成结果及 `unconfirmed` 后再报错，不删除他人的 lease。

默认不启用详细计时，模型沿用 `detailed_stage_timing=False`。
若设置 `SEMLOOM_DETAILED_TIMING_DIR`，借用正在开发的 `src.experiments.execution_timing`，须先集成其分支；
当前基线本身没有该模块，显式开启会报告缺依赖。
[借用探针](raw/optional_timing.json)通过临时加载该原文件核验五个观测点、10 条事件，未复制或合入计时平台。
这些事件表示可观察调用，不推断 Daft 内部排队或 GPU kernel 时间。

## 独立核验入口与 GPU 配置

[单测](../../../../code/tests/baselines/image/test_daft_clip_executor.py)可用固定 SDK 运行：

```sh
SEMLOOM_DAFT_IMAGE_SDK_TEST=1 DAFT_ANALYTICS_ENABLED=0 DAFT_PROGRESS_BAR=0 \
  PYTHONPATH=code python -m unittest tests.baselines.image.test_daft_clip_executor -v
```

相关旧路径回归的命令为：

```sh
PYTHONPATH=code python -m unittest \
  tests.modalities.image.test_image_contracts tests.modalities.image.test_image_execution \
  tests.baselines.image.test_image_baseline_contract \
  tests.baselines.image.test_image_vendor_baseline_manifest -v
```

计时探针用固定 `bbbd8a1b3ed2456e97f0d1b766484991be4ac5c9` 的原模块，可按下面复算。
`TIMING_MODULE_FILE` 指向该提交的 `code/src/experiments/execution_timing.py`，`NEW_OUTPUT_ROOT` 是仓库外新目录。

```sh
PYTHONPATH=code DAFT_ANALYTICS_ENABLED=0 python - <<'PY'
import importlib.util, os, sys
spec = importlib.util.spec_from_file_location(
    "src.experiments.execution_timing", os.environ["TIMING_MODULE_FILE"])
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
from tests.baselines.image.test_daft_clip_executor import DaftClipSdkTests
case = DaftClipSdkTests("test_empty_batch_does_not_load_a_model")
case.setUp()
os.environ["SEMLOOM_DETAILED_TIMING_DIR"] = os.environ["NEW_OUTPUT_ROOT"]
os.environ["SEMLOOM_TIMING_RUN_ID"] = "daft-cpu-sdk-optional-timing"
try:
    image, _ = case.png()
    case.executor.embed_batch(("timed:one", "timed:two"), [image, image])
    case.executor.close()
    module.close_timing()
finally:
    os.environ.pop("SEMLOOM_DETAILED_TIMING_DIR", None)
    case.tearDown()
PY
```

首轮假设每图重载模型而失败（`1 != 2`）；实际生命周期核实后修订测试，失败记录保留。
上述 CPU kernel 为明确替身，真实 CLIP／HTTP／GPU 调用次数为 0；单独的 GPU 运行见下文，不能合并计数。

独立真实模型入口为 [verify_daft_clip_gpu.py](../../../../code/tests/baselines/image/verify_daft_clip_gpu.py)：

```sh
PYTHONPATH=code python -m tests.baselines.image.verify_daft_clip_gpu \
  --model "$MODEL_ROOT" --processor "$PROCESSOR_ROOT" --image-root "$IMAGE_ROOT" \
  --artifact-root "$ARTIFACT_ROOT" --output "$NEW_OUTPUT_ROOT" \
  --source-hash "$SOURCE_TAR_SHA256" --base-sha "$BASE_SHA"
```

实际 gpu-02 配置：固定 Daft 0.7.21、Transformers 4.57.6、Torch 2.12.1+cu130、单 RTX 4090、FP32，
9 张固定 COCO RGB 图片无损 PNG；native batch=32、model_workers=1、gpus=1，9 图实际为一个未满批次。Direct 同 batch 形状，检查全部 512 维。
Direct 用相同权重、慢 `ClipImagePreprocessor` 和 `ClipTensorActor`，先要求有效 pixel tensor 完全一致。
预先规定所有向量最大绝对差 ≤1e-5、最小 cosine ≥0.99999、单位范数误差 ≤1e-5。
执行首次 native 查询及 3 次复用；失败保存并停止，不扩大规模。GPU 按已有共享 lease 串行，禁止恢复他人 lease。
成功须核实完整消费、自己持有的 model／398 个 parameter／全部 buffer 按实例身份终结，
同步确认自己的 GPU 工作并核对自己的 child PID 退出，才释放 lease。进程全局 allocated bytes 继续观测，
不作为自有模型释放的代理；不修改数学、向量容差或 GPU 资源额度。
preflight 已通过，用户明确授权后同范围源码传输成功。失败和复测各有独立 source 摘要及运行目录。

该规模仅作数值和生命周期资格。P99／P99.99、吞吐平台、完整 LOTUS 查询耗时均 `unavailable`。
原生 LOTUS 文本查询和公共 CLI 由独立负责人拥有；stock SentenceTransformers 与 cached OpenAI 权重／processor
等价性由其单独资格核验，本后端不从模型名称或序列化 hash 推断等价。

工程来源核对：固定 [Daft image API](https://github.com/Eventual-Inc/Daft/blob/v0.7.21/daft/functions/image.py)
与 [stateful UDF API](https://github.com/Eventual-Inc/Daft/blob/v0.7.21/daft/udf/udf_v2.py)，
现有 `build_daft_clip_embedder` API 形态、`ClipTensorActor` 计算函数与共享结果类型。
架构计划 §8.7–8.8 的执行名额与实例寿命分开原则用于接口判断；本次没有使用公司源码或改动上游实现。

首轮 [gpu-01 失败记录](raw/gpu_first_failed.json) 保留原 `failed`：batch4 产生4／4／1，四次查询
pixels 与完整512向量对同批形状 Direct 的最大差均为0，范数误差1.192e-7。
原生实例在 driver 内加载／销毁各一次，无 child；验证器因整个进程 allocated=34,078,720 bytes 而报错。
Direct 清理后也有8,519,680 bytes。残留的具体归属未分解，不能据此判断有未知模型执行。
原 CLI 已物理退出且当时 GPU compute 空；没有修改随后他人的 lease。
该轮完整 vector 包在清理判定后才保存，所以失败时未落盘；逐 ID 的所有维度误差项仍保留，不能补称原向量已保存。

独立审核后新增正常释放和故意持有 model／parameter／buffer 引用的 CPU 反例，确认新判据能够拒绝未释放对象；
终结 callback 只带纯标量身份，禁用进程退出时 callback。新版先保存完整 vector 包及 hash，再检查关闭。
[PyTorch 官方分配源码](https://github.com/pytorch/pytorch/blob/v2.12.1/aten/src/ATen/cuda/CublasHandlePool.cpp)
也存在经同一 allocator 管理的 workspace；此源码仅说明全进程分配不是自有模型归属证明，不归因本轮残留。


## 修订后的 GPU 结果与交接

[完整比较与关闭观测](raw/gpu_passed.json)对应 `daft-native-20261014-b/gpu-02`，
source tar SHA-256 为 `4b12f83cc9cf4113f6550f2dd75410e84513081bfbac98dde086944a24a39417`；
三份源码 SHA-256 逐文件核同[软件身份](raw/software.json)，生产后端与首轮逐字节相同，修订只在验证器及其反例。
两个环境的软件、输入 JPG／PNG、有效 pixels、模型配置及权重、完整输出包、原 report／trace／exit 的 SHA-256 均保留。
公开副本把实际 revision 路径和命令路径换成占位符；执行器运行时仍返回实际本地 root。

有效 pixel tensor 形状 `[9,3,224,224]`，native 与 Direct SHA-256 相同、最大绝对差 0。
四次调用的所有 9 个 ID、512 维 float32 输出均逐位相同，最大绝对差 0、最小 cosine 1，
单位范数最大误差 `1.1920928955078125e-7`。同样本不同 batch 形状未在本轮比较，不声明 batch 不变性。
完整 `embeddings.npz` 在关闭检查前保存，SHA-256 为
`137ba510df51786d6532cbe2bd7eb5a087896feb2afea65cf89758ce2c69642a`；数据和完整包留在仓库外。

| 调用阶段 | 调用耗时（秒） | 模型状态 |
|---|---:|---|
| 首次查询 | 0.413005 | 本次图载入一次，载入观测 0.271735 秒 |
| 复用 1 | 0.168782 | 同实例 |
| 复用 2 | 0.137774 | 同实例 |
| 复用 3 | 0.140095 | 同实例 |

调用耗时覆盖批次输入检查、图构造、解码、慢预处理、首次载入、完整消费及输出检查。
构造执行器时的文件 SHA 计算另计 `0.587809` 秒；表中不含它。
`service_s` 从图构造开始，另保留全部原值；Direct 的 prepared tensor 调用不含这些阶段，不能据此排名。
没有单独 warm-up，首次查询单列，之后三次只称实例复用；详细计时默认关闭。
Direct 数值对照先执行，因此首次 native 载入不代表整机磁盘缓存或 CUDA 库冷启动。

四个图全部消费，1 个原生实例在 driver 内载入／销毁各一次，模型 module、398 个 parameters 和 2 个 buffers
按 `(pid, model_instance_id, kind, tensor_index)` 全部恰好终结一次，无自有 child。
本轮全进程 allocated bytes 为 `25,559,040`，仅作观测；自有模型释放资格来自对象身份与同步确认。
验证器记录的 close `0.215283` 秒包括约 400 条终结轨迹写入、gc 与同步，不是生产 close 的成本。
CLI 退出码 0，原子共享 lease 正常释放，退出后的采样为 lease 不存在、GPU compute 空。
原始 407 条生命周期事件压缩保存了所有 tensor index（重复次数不丢失），时间顺序留在带 SHA 的原 trace。
独立 GPU 审核另读实际 NPZ 和服务器源码核对，确认上述数值和释放结果。

后续接入使用 `src.baselines.image.frameworks.daft_clip.DaftClipExecutor` 作为外部 factory，
读取 `.semantics`／`.identity`，调用 `.embed_batch(doc_ids, encoded_images)`，完整消费后 `.close()`。
原生图库与在线图像 query 的 RGB PIL→无损 PNG、索引及完整查询计时由 LOTUS 负责人实现，文本查询仍使用 stock RM。
本轮配置外的更大规模、异常路径的物理退出及完整 LOTUS P99／P99.99 均不借本资格外推。
