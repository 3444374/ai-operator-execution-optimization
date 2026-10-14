# CLIP actor 输入驻留与复用检查

源码起点`d307bb9265a32e516052f3bea7d8206f8906c288`，职责分支`feat/hse-input-residency`。
本项是CPU/确定性Torch与模型替身的软件检查；模型、GPU、服务器及厂商执行库运行均为0。
[计划](../../../plans/数据组织.md)与[机器记录](checks.json)区分已检查的行为和待验证项。

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
这些是API来源，未运行本机Torch/CUDA资格；LRU、逻辑budget与保守异常引用属于本次工程决策。

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

真实Ray、单GPU CLIP及性能、双卡容量/速率/选择、跨卡直接搬运、decode复用和选择前prepared身份传播均为pending。
没有占用共享lease、改变厂商baseline或启用默认策略。当前保持actor本地组件；调度主循环接线由选择owner维护。
