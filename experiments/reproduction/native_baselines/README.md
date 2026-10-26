# 原生基线与具名 Core 历史复现

共享结果模块迁移与e2bc汇合的本地候选将九份原公共来源保存在`historical_sources/`：五份结果合同/消费者旧文件、旧环境预检和Movie矩阵说明，以及旧RayHTTP与importer。MANIFEST相对45d共新增43条override，保留双方既有路由，原expected SHA、projection ID、source revision及既有variant override保持；还原树继续使用原字节，不导入当前shim或新模块。此目录属于历史复现，不是生产共享实现。


维护代码在 `code/`；这里保留五种**已实测来源的实际差异文件**和逐文件摘要，不仅保留 patch，也不把不同 Core 策略合成新默认。没有模型、数据、机器配置或原始日志。

```sh
PYTHONPATH=code python3 -B code/scripts/experiments/materialize_native_baseline.py list
PYTHONPATH=code python3 -B code/scripts/experiments/materialize_native_baseline.py prepare \
  --variant core_local_window_pairs --output-dir /tmp/semloom-named-reproduction
```

此命令只从维护代码与具名覆盖文件构造独立代码树，逐项验证来源；不运行模型、不扫描参数、不分配调用、不连接服务器。生成的 `REPRODUCTION.json` 明确这是代码投影，原内容指纹不是Git提交。每一种覆盖文件匹配旧冻结代码，包含其当时的观测边界；不得借维护代码后续观测修订追认历史MFU。

维护代码的Core原评价器整合测试读取显式`SEMBENCH_CHECKOUT`，缺少时只跳过该整合用例。五份历史投影通过各自override保留旧测试字节及不变的代码投影ID；历史相对packet路径只属于原私有封包，不是新目录的可移植运行保证。

维护版`FilterMethod`要求显式非空instruction，Movie提示仅留在任务配置。为保持历史来源逐字节可重建，
`compatibility/explicit_instruction/`保存受影响原文件的共享精确副本，manifest只增加对应override映射；
已有五份variant文件、`code_files_sha256`和代码投影ID不变，旧隐含默认只属于历史版本。

独立树真实入口是 `code/scripts/experiments/run_movie_records_matrix.py`。完整运行需要新的显式封包、输入/token/模型/环境绑定、账本与资源owner；旧私有packet不随此目录发布，不复用旧账本或未知调用。`--help`不运行模型；不以源码还原或CPU通过冒称新树已获GPU资格。

- `native_isolated_and_imlane`：Native、Ray isolated、Daft isolated和IMLane作者worker路径。
- `core_original_dynamic`：cfdc具名原动态；实际已测source79e8f103。
- `core_local_window_pairs`：ee39具名局部窗口配对；实际已测source5007e95b。
- `ray_runtime_reuse`：只复用runtime，每查询仍可创建原生模型。
- `daft_single_thread_session`：显式不同Daft原生单线程session配置。

Kalypso为独立作者原生服务，不是LOTUS后端；见 `code/src/baselines/text/frameworks/kalypso_native/` 的适配、命令和未绑定项。

历史variant中的README按原字节保留，其相对链接属于当时文档语境；差异覆盖目录不是完整源码/文档树。materialize只保证manifest列出的代码投影与摘要，不承诺未列历史文档链接可用。当前维护入口与CPU资产路径说明见[代码入口](../../../code/README.md)及[CPU绑定](../../../code/tests/records_cpu.md)。
