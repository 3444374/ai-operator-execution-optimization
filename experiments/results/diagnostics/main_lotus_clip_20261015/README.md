# main 的原生 LOTUS CLIP 查询接入

本批代码固定 `8834761416912c71fac58b8e715f5d635c8ebc57`，底稿为 main `3b1eb789`。
引入原 LOTUS 1.2.4 的 RM、FAISS `sem_index`／`sem_search` 完整接点及显式单卡资源配置。
本批完成 CPU／SDK 组合检查，**新候选的 GPU 资格仍 pending**；没有增加模型调用或借用服务 auto 的模型结果。
文件摘要、检查与边界见[来源记录](provenance.json)，使用方式见[脚本入口](../../../../code/scripts/README.md#原生-lotus-clip-查询)。

## 必要闭包与排除项

实现从固定 `d2fa8c9b83470057cd3bd10118ba7e44339695eb` 按实际调用抽取，
其报告固定在[资源交付 `95f8266f`](https://github.com/3444374/ai-operator-execution-optimization/blob/95f8266f9245b4e30f337ea7a5a957dc86f1095d/experiments/results/diagnostics/resource_auto_20261015/README.md)。
baseline 的 `d93dbdbb` 是融合记录，实际模型源继续是 d2。main 原本没有图像 RM／建库／检索消费者，单独增加 UUID 文件不能形成可用入口。

| 范围 | 必要性与采用边界 |
|---|---|
| `lotus/image_{source,identity,rm,workflow,backend}.py` | 固定原生源码／模型内容，原 RM、FAISS 持久化与检索，完整行关联、矩阵及独立结果所有权 |
| 三个公开脚本、`file_source.py`、`data/coco.py` | 从 JPEG 内容 manifest、模型身份资格到完整查询；旧 COCO 导入器复用相同选择函数，数据库事务职责保留 |
| `daft_clip.py`、Ray 两个 CLIP predictor | 原生 UDF／Data graph 拥有模型批次与执行；旧 Ray graph builder 保持 |
| `clip/contracts/staged/execution/staged_execution.py` | 实际预处理身份、v1 兼容、部分启动 handles、ready-first、失败／取消 fences 与 unknown 禁复用 |
| `worker_identity.py`、`resource_budget.py` | 完整 ASCII Torch／NVML UUID 对齐，启动前限制已具备来源资格的资源范围 |
| `image_reuse.py` | 仅由显式 `--observe-model-reuse` 消费的构造身份与调用计数；默认关闭 |

共21个生产／公开脚本文件、22个测试文件。main 的四个服务文件、timing、Daft runner、Map transport 和 Broker 保持底稿字节。
未带入新 dispatcher／路由、HSE 自动深度、多设备策略、Broker 历史计数优化、external factory、Serve、PZ／IMLane、临时控制器或原始模型产物。
HSE 只保留本闭包所需的静态身份与生命周期基础；未将整份 `bdae61bb` 修复或其旧设备资格转为本批资格。

## 显式资源范围

`--resource-config` 只允许 `semloom_static`、Ray fused／staged、`daft_native`，CPU worker为1，
要求该进程外置 `NVIDIA_TF32_OVERRIDE=0`。auto 在部署允许集合与继承 UUID mask 的交集内解析；解析出多卡会拒绝，不截取一张。
HSE scoped、额外 stage 深度和多卡在模型构造前失败。无资源配置时保留来源的 stock 默认及固定单 worker 路径，
原 Ray 非资源图保持原生重试策略；显式资源图关闭重启／任务重试，unknown 不自动重发。

图库使用 RGB PIL 图像，原生 FAISS 行位置与文档身份一致；缓存关闭，已有 index 不覆盖。
有限图库最多5000行，私有完整向量捕获最多256行。预处理内容签名、逐字节输入复核与完整矩阵校验保留。
文件 manifest 的 load 只核 size／mtime／ctime，不是文件系统快照；CLI 在解码及查询前另核实际文件 SHA。
模型身份 producer 会真实运行编码器，`--device cpu` 也会执行模型；本轮只检查它的公开入口，没有运行该 producer。

## 来源模型证据与失败

d2 在一张4090上各执行一次 auto／explicit1 的原生建库及两次检索。static、Ray fused／staged、Daft 的完整向量、
排名、worker身份及物理退出只在上述固定精度环境下通过。HSE 的输出／身份／退出通过，explicit1 的额外环境验收仍失败。
默认环境 static 的图库最大差 `1.182854175567627e-4` 超过原 `1e-5`，失败及6行诊断保持；没有证明 TF32 根因，也没有放宽阈值。
来源累计66/66图像行、模型HTTP0，完整报告与原件恢复摘要由95交付拥有。本批不增加图像预算，不把 d2 的结果重标为8834或最终main。

## 本批检查与剩余资格

Mac／Linux 检查的固定源、原 LOTUS／FAISS 确定性 embedding、真实 CPU Ray／Daft、旧 Map／LOTUS／服务兼容结果及原始失败见来源 JSON。
| 固定8834检查 | 结果 | 范围 |
|---|---|---|
| Mac完整定向／兼容 | 463pass／20条件skip，250subtests | 含原SDK／FAISS、软件HTTP及旧image／Map／服务，localhost fixture获准 |
| Linux原生图像独立进程 | 320pass／0skip，116subtests | 真Ray2.56.1／Daft0.7.21 CPU、真实slow processor、确定性embedding原SDK查询 |
| Linux旧路径独立兼容 | 136pass／8条件skip，132subtests | 覆盖原超时Map子项、旧LOTUS软件HTTP和服务校验 |

计数有重叠，不相加。所有模型实现均为替身或仅processor准备；CUDA隐藏，新增真实模型／GPU／lease操作为0。
此前混合native组385pass／178subtests但有1个旧Map子项超时；未改生产代码或放宽原超时，随后将native与旧Map夹具分进程执行。
两组同源检查分别通过，混合fixture顺序下的根因未隔离，原失败不删除。

实际 CPU 进程退出按 PID／出生 ticks 观察；这些 fixture 不证明 GPU 退出或真实模型质量。
首次测试工具缺项、Torch mock 导入顺序、localhost 沙箱绑定失败、ctime 分辨率用例及混合套件的 Map 等待超时均保留。
ctime 用例已改为确定性元数据边界，并新增 CLI 实际字节不符时拒绝查询的检查；旧 Map 在同源独立兼容进程复查。

仍待：新合并源 GPU 完整查询、默认精度、HSE 环境补验、多卡／DP>1、Daft多卡 mask、Serve、真实扩缩、
GPU失败／取消后的下一查询及稳定性能。混合故障 fixture 之后同一原生 Daft 进程内跨入口继续查询也不在本批资格内。
原 Movie／Animals 与服务 auto 继续由[各自固定来源](../main_service_auto_20261015/README.md)拥有。


仓外CPU归档`main-clip-cpu-evidence-20261015.tar.gz`为293476字节，SHA-256
`94942fcb1192084456928c13fd9d30b78f3b37719ebad1ae568cfaf28c793e29`；28份证据加manifest共29成员，Mac与独立服务器副本均逐成员bytes／SHA核对。
按run ID `main-clip-cpu-20261015`和摘要向项目owner取得原件；不含权重、runtime env、venv或完整数据集。
