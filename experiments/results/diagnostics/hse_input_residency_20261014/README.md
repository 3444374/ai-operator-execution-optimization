# CLIP actor 输入驻留与复用检查

源码起点`d307bb9265a32e516052f3bea7d8206f8906c288`，职责分支`feat/hse-input-residency`。
初版CPU/确定性替身检查见[机器记录](checks.json)，其模型/GPU运行均为0。
随后完成[实际单卡验收](#actual-device-results)：真实Torch/CLIP/Ray生命周期通过，完整5,000图及重复两遍均0-hit，
缓存增加时间与内存，保持默认关闭。设备间直接搬运和HSE选择接线仍pending；[计划](../../../plans/数据组织.md)保留后续范围。

## 已有需求与采用范围

[source](../../../../code/src/modalities/image/source.py)的`dataset_passes`在同次执行重复图像bytes，但每次产生新行ID。
[runner](../../../../code/scripts/experiments/run_image_clip_e2e.py)在warmup后结束旧actor，再为正式执行重建；
matrix各项也使用独立进程。小容量整批缓存是否能命中取决于实际内容、表示、顺序及actor分派，不预设收益。
既有StageBlockDescriptor摘要包含行ID，shape locality也不表示内容相同，均不能直接用作此缓存键。

原型`92bcd05ae6714b563b8937c80f3276ff38ca6bd7`的
[PreparedAssetKey/PreparedAssetWindow](../../../../code/src/data/materializers/prepared_assets.py)与对应测试原样吸收。
本次只复用内容/变换身份；已有host window仍是有限消费者bytes原型，未另接到HSE或冒称设备驻留。
新增[DeviceInputCache](../../../../code/src/modalities/image/input_residency.py)只持有tensor对象和引用，
[ReusableClipTensorActor](../../../../code/src/modalities/image/reusable_clip.py)在原actor的输入转换接点使用它。
原CLIP forward、float32 normalization、完整embedding和本次doc_ids保持；没有结果缓存、额外消费者或自动重试。

## 接口、字节与完成责任

- 显式构建`ReusableClipTensorActor(..., input_cache_bytes=0, input_cache_entries=8)`；非零bytes才启用。
  默认及标准worker builder继续使用原actor，HSE借用池可由整合owner显式选择新actor。
- `input_residency_snapshot()`返回不可变的cache_id、actor本地location、预算/逻辑峰值、条目、引用和命中/淘汰统计，无tensor payload。
  cache_id随actor重建改变；location不能证明物理卡号或独立GPU。
- `input_transfer_cost(key, host_bytes=..., device_bytes=...)`只读返回命中与host reload/device copy逻辑字节。
  无实测秒数时`transfer_s=None`，`p2p_supported=False`；不会访问其他actor或移动数据。
- 共同HSE选择回调`input_residency.estimate(descriptor, destination, now_s)`当前返回`None`，
  因选择时没有verified prepared内容身份；不读取、拷贝或hash整tensor，也不另定义调度estimate类型。
- actor键按实际prepared tensor内容及shape/layout/dtype/processor/目标dtype生成。内容/表示变化重新转换；
  只淘汰空闲条目，byte和entry上限同时生效，超出缓存预算的输入使用原uncached转换。
- `input_reuse_observation()`单列identity_s、identity_copy_bytes、host_ownership_copy_bytes、已开启详细计时的h2d_s和settle_s。
  identity时长包含临时连续拷贝和hash；缓存逻辑峰值包含加载预留，额外完成确认与归还进入actor service_s。
  host/object-store payload仍会交给actor，命中不能据此认领decode、Ray网络传输或整查询收益。
- 逻辑预算仅覆盖缓存持有的输入，不覆盖模型权重、forward临时内存、Torch allocator、host/RSS或原uncached输入，不能解释为物理HBM。
  缓存加载报错保留预留；使用缓存输入的模型异常/取消遇到失败确认保留引用且停用该条目；后续embed拒绝，只有本actor成功synchronize才结清。
  clear仅取消复用，不删除活跃或未知引用；其他actor确认不能释放本actor输入。

工程依据：PyTorch 2.14的[as_tensor文档](https://docs.pytorch.org/docs/2.14/generated/torch.as_tensor.html)
说明同dtype的CPU NumPy转换可共享内存，因此CPU retained输入独立取所有权；
[synchronize文档](https://docs.pytorch.org/docs/2.14/generated/torch.cuda.synchronize.html)说明设备完成等待范围。
这些是API来源；初版未运行本机Torch/CUDA，随后实际资格使用下文固定服务器版本。LRU、逻辑budget与保守异常引用属于本次工程决策。

## 本地结果与复现

35项复用/已有host原型检查，以及71项旧HSE、broker、图像输出/执行/预处理检查，共106项通过、0跳过。
其中两次相同内容/不同row ID的2行调用只做1次输入转换，完整forward仍为2次、每次输出为`(2,3)` float32。
版本变化重新转换，96-byte逻辑容量的峰值保持96；未知完成仍持有96 bytes与1个引用，成功同owner确认后才归还。
这些数字来自受控替身断言，不能解释为实际H2D、物理显存、GPU时间或性能改善。

```sh
PYTHONPATH=code python3 -B -m unittest \
  tests.data.test_prepared_assets \
  tests.modalities.image.test_input_residency \
  tests.modalities.image.test_reusable_clip \
  tests.modalities.image.test_staged_image_execution \
  tests.modalities.image.test_hse_static_lifecycle \
  tests.modalities.image.test_image_execution \
  tests.modalities.image.test_image_contracts \
  tests.modalities.image.test_image_operator \
  tests.modalities.image.test_image_clip_preprocess_variants \
  tests.scheduling.test_stage_broker -q
```

首次运行用了`code.tests`模块前缀，碰到stdlib的code模块，3项导入失败；未执行行为测试。
修正为`PYTHONPATH=code`与`tests.*`后33项通过，补充服务时间检查后组合105项通过。失败身份保存在机器记录。
独立审查发现完成fence本身被取消时引用未记为未知；修正所有中断先登记未知责任，再传播原中断。
新增取消/KeyboardInterrupt fence用例后组合106项通过，独立复查23项驻留测试通过且未发现新增问题。
源码/文档/隐私检查由同一机器记录定位。

初版未占用共享lease或启用默认策略。随后实际Ray/单GPU检查见下文；双卡容量/速率/选择、跨卡直接搬运、
decode复用和选择前prepared身份传播仍pending。当前保持actor本地组件，调度主循环接线由选择owner维护。

## 实际设备验收准备

接续`42186dbed22c9c70cf0d22285afe4698c97bc714`，输入驻留owner承接实际Torch、完整CLIP、Ray生命周期、
CUDA完成及物理内存核验。当前IMLane/图像baseline先执行，本项继续完成[最小单卡计划](../../../plans/数据组织.md#clip-input-device-validation)
与独立审核；单/双卡实测和设备间直接搬运状态分别登记，不把计划或CPU替身当作物理证据。

2026-10-14 08:28 UTC只读核对：1张RTX4090、设备总显存24,564 MiB、已用1 MiB、GPU利用率0%；
CPU quota16，共享lease不存在，未见Ray/模型进程。已有COCO val2017共5,000 JPEG、CLIP权重605,247,071 bytes。
driver的Torch2.12.1+cu130、torchvision0.27.1+cu130、Transformers5.14.1、Ray2.56.1、Daft0.7.21可复用；
这次快照没有启动模型、建立CUDA context或取得执行lease。依赖/资产与图像owner固定CLI的准备记录对齐，
实际开跑前仍核对其baseline结果、runtime preflight和资产摘要。

正常缓存完成从全设备同步改为当次current-stream CUDA事件等待，事件/引用在确认失败后继续持有；
未知恢复仍使用明确的设备完成确认。相关24项CPU替身检查通过，CUDA事件用法依据
[PyTorch 2.12 Event API](https://docs.pytorch.org/docs/2.12/generated/torch.cuda.Event.html)，尚未在设备上执行。
主off/on比较使用完整5,000图与已有dataset_passes，单独保留无重复输入成本及0-hit结果；
主计时不启用逐阶段全设备同步，CUDA阶段事件诊断另跑，全部hash、拷贝、管理、forward及完整返回计入原operator耗时。
原CLI最终审计摘要、文件落盘和worker/Ray退出另列，wrapper的整次调用墙钟同时保留，不能将两种口径混用。
Torch allocated/reserved/峰值、NVML设备总体与缓存逻辑bytes分别记录；详细参数、累计工作上限及停止方式由上述计划维护。

验收工具[`validate_clip_input_residency.py`](../../../../code/scripts/profiling/validate_clip_input_residency.py)
分别提供实际64图资格、仅SemLoom的原CLI wrapper和只读拓扑声明。wrapper没有改变原厂商代码或标准builder，
缓存off/on均调用完整forward，模型和processor从已有本地目录离线加载。
[准备机器记录](device_preparation.json)保存本轮120项本地检查与实际载入源码摘要；独立审查覆盖24项生产fixture、
11项不写文件的工具fixture及未完成/已完成事件快照，写者完整13项工具测试通过，各组有重叠。
准备阶段的实际Ray/cloudpickle/Torch/CUDA为pending。09:04 UTC只读确认先行图像15次调用complete、进程退出已确认、
原lease已释放且当前无GPU/Ray进程；后续以执行时的原子lease领取为准，不从空闲快照推断已获执行名额。


<a id="actual-device-results"></a>
## 实际单卡结果

测量驻留源码为`9f1b4ef5eeab493bf38cb26147a371ac5483d567`，原图像CLI与依赖图来自
`45da0a5451bfcf785d98b9ab5336054bdbaf199f`。逐文件摘要、模型/清单、完整重复值和物理责任由
[实际机器记录](device_actual.json)保存；[证据归档](device_evidence.tar.gz)有79个成员，SHA256为
`29986373d3c80cf0816ca7831bdfbfcb908a884a293a67dc81b549aec5361a7f`。
原件分别保留在仓库外运行`clip-input-residency-20261014`和`clip-input-residency-20261014-attempt2`，
公开副本替换单机路径与私有网络信息，各成员的原件/公开bytes与SHA256及取回归档摘要写入归档的`provenance.json`。

实际一张RTX4090、总显存24,564 MiB、CPU quota16；Torch2.12.1+cu130、Transformers5.14.1、Ray2.56.1、
Daft0.7.21。模型为CLIP ViT-B/32，固定revision `3d74acf9a28c67741b2f4f2ea7635f0aaf6f0268`；权重/config/processor
均测量SHA256且与先行baseline一致，runtime core/image/image-clip检查通过。原CLI用CPU prepare4/source2、
GPU actor1、batch上限64、window4，每次独立进程并在warmup64图后重建formal actor。标准builder、HSE主循环及厂商baseline均未改。

64张真实JPEG资格中，四个off/on配对及真实异步完整forward的9个矩阵均为64×512 FP32、finite、归一误差最大
`1.1920928955078125e-7`；四对及异步参考逐元素最大差为0。不同row ID复用相同输入，完整forward仍执行；
单像素变化重新转换并只改变对应输出行，淘汰后的原输入重载。两个完成确认中断各保留19,267,584逻辑bytes、1个未知引用和真实CUDA事件，
clear继续持有，同owner显式同步后归还。协作取消在确认阶段观察到真实Ray取消旗标及`CancelledError`；ack故障为明确的控制注入。
真实异步CLIP事件首次query为False，两份引用继续计费，clear后仍持有，事件完成与两个归还后为0；未添加人为CUDA延迟。

主任务继续使用同一5,000图清单，以下为原CLI operator耗时。缓存预算154,140,672逻辑bytes、最多8条；
原source的实际分批产生90次forward（5k）与179次（10k），完整输入/输出行数一致，未按理论整批数改变source。

| passes / 配对次序 | cache off 秒 | cache on 秒 | on增加 |
|---|---:|---:|---:|
| 1 / off→on | 21.057897 | 21.802446 | 3.54% |
| 1 / on→off | 21.420140 | 21.720510 | 1.40% |
| 2 / on→off | 29.800668 | 31.287357 | 4.99% |
| 2 / off→on | 30.437731 | 32.335908 | 6.23% |

两组均值分别增加2.46%和5.62%；只解释此固定单卡/工作负载的诊断。每个cache-on完整任务均0-hit，
miss/eviction分别90/82与179/171，hash累计为2.841/2.885秒与5.217/5.771秒；identity临时连续拷贝为0。
host ownership copy仍为每遍3,010,560,000 bytes，缓存未减少该完整任务的复制。全部10次CLI的512维、finite、
exactly-once、归一检查通过，同passes的逐行ID＋rounded5向量摘要相同；主任务未另存全5k矩阵作逐元素比较。
所有调用墙钟、worker设置和GPU/CPU汇总在机器记录与原manifest中分别保留，不能与operator时间混用或减去累计阶段来解释净收益。

| 指标 | off | on |
|---|---:|---:|
| 缓存逻辑峰值 MiB | 0 | 128.625 |
| Torch allocated峰值 MiB | 386.017 | 498.564 |
| Torch reserved峰值 MiB | 424 | 532 |
| 原CLI NVML采样最高 MiB | 900 | 1008 |

逻辑峰值低于147 MiB预算，byte与8-entry两种限制及实际分批共同作用；它不表示物理显存。
Torch峰值从actor模型加载后reset开始，NVML是0.5秒采样汇总及停止时snapshot；逐点NVML时序未保存，不能据此认领设备整体真正瞬时峰值或稳态。
hash时长、host copy及CUDA事件各自保留，主时间不启用逐阶段全设备同步。单独passes2事件诊断off/on为
30.435130/33.160000秒，as_tensor流区间累计1.763/1.605秒，forward区间1.471/1.501秒，全部事件已完成；
这些区间包含分配/转换及提交间隙，不能当作总线字节计数、浮点效率或主性能重复。

首次协调器在环境变量合并处报TypeError，子进程及模型0，物理退出/lease归还已确认；修正后使用独立attempt2，旧证据保留。
attempt2的2项准备、64图资格、拓扑及10次CLI共14项退出码0，每项自身进程退出均确认；累计81,344图，未超过83,000。
最终09:26:48 UTC确认全部自身进程与GPU计算结束后归还lease，09:34:13只读复查controller/自有lease/GPU计算均空。
独立审核复算全部manifest/CSV、源摘要、矩阵、责任及内存；本地后置标签修正另验，不改变已固定的测量源码。
[最终本地检查](device_final_checks.json)记录121项通过/0跳过、独立25项受影响软件检查、归档逐成员摘要及78文本成员隐私核对。

测量工具9f的命中trace顶层`host_ownership_copy_bytes`按只读源预测，命中时会误记非零；本报告与复算使用内层
`input_reuse_observation`的实际0-byte记录及无as_tensor事件。未来工具已改为在真实转换接点核对，readonly miss/hit、
off readonly/writable的本地回归通过；生产驻留源码与9f相同，修后的观测工具未重跑GPU，原件未改写。

当前采用决定：保持默认关闭和只读metadata接点。单批复用的真实正确性已核验，当前完整任务没有收益，
不扩大缓存/缩小任务来制造命中。shared locality estimate仍返回`None`；HSE集成、选择前内容身份传播、跨actor重用、
decode复用、双卡容量/速率/选择及设备间直接搬运继续pending。单卡拓扑检查的peer-access方向为空，硬件声明和未来验收计划不代表搬运实现。

复算公开副本，不启动模型或服务器：

```sh
python3 -B code/scripts/analysis/summarize_clip_input_residency.py \
  --evidence experiments/results/diagnostics/hse_input_residency_20261014/device_evidence.tar.gz \
  --output /tmp/new-clip-residency-summary.json
```
