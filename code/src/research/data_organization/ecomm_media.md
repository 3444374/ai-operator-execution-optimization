# Ecomm Q11 同资产表示与消费次序原型

这是隔离的 CPU 可行性原型。它研究真实算子依赖允许的次序与主机图片表示寿命，
尚无原生 SDK／真实图片运行结果，也没有 GPU 或方法收益结论。本地检查见
[证据报告](../../../../experiments/results/diagnostics/ecomm_media_consumers_20261109/README.md)。

## 真实查询与可以调整的动作

来源是 SemBench `c814e3807e72d4cf876b852b17e77f3cc94575c2` 的
[Ecomm Q11](https://github.com/SemBench/SemBench/blob/c814e3807e72d4cf876b852b17e77f3cc94575c2/files/ecomm/queries/dialects/lotus/q11.py)。
原查询先合并商品与图片映射，再执行鞋、下装、上装三个完整关系 Filter，以及价格不超过 500 的配饰 Filter。
前三个分支共用同一输入；配饰分支是普通 pandas 切片。
随后原查询执行鞋×配饰、下装×上装、两份组合间的三个原生语义 Join，最后按原代码组成四件商品的 ID。
Join 等待原有输入分支；原型不实现 Join 算法。

可调整的是四个独立初始 Filter 内尚未调用的行次序，以及其相同资产表示的持有时间。
每个合并后源行的位置是 occurrence（物理出现项）身份，重复商品 ID、重复图片与相同请求均保留。
指令、消息内字段与媒体次序、每项模型／参数、调用数量、依赖及最终装配不变。
分块结果恢复为原关系顺序后，未经修改的 Q11 源码继续运行原三个 Join。

| 内部对照 | 次序与图片准备 |
|---|---|
| `branch_native` | 原 Q11 分支顺序，保留 LOTUS 原生 ImageArray 缓存 |
| `tile_native` | 按首次出现的资产引用分块，每块依次消费四分支；保留原生缓存，块完成后释放块内缓存表示 |
| `tile_shared` | 相同分块／清理，再让块内数组共享相同源引用的原生 base64 字符串 |

分组仍保留所有行；共享只用于不可变字符串，实际 miss 调用原生 `fetch_image`。
原始引用不改，缓存只活到同步块消费结束；失败也清理块内表示并恢复 SDK 函数。
真实异步模型在途引用如何交接尚未实现，不能从同步 replay 推定其安全或收益。
本轮没有 CPU/GPU 重叠、新调度评分或生产接点。

## 最近邻与尚未回答的问题

源码依据：LOTUS 参考提交 `b1a85fd7a66fabed8a1585d44d7597d592b4433f` 的
[ImageArray](https://github.com/lotus-data/lotus/blob/b1a85fd7a66fabed8a1585d44d7597d592b4433f/lotus/dtype_extensions/image.py)
已经按数组下标和表示种类缓存；`take`／切片建立冷数组。
因此原 Q11 前三个分支并非重复解码三次，须实测 native 命中和额外 miss。
`df2multimodal_info` 只加载指令引用的图片列，故 Q12 文本 Filter 前延迟图片解码本来就存在，未作为候选实现。
实际安装版本和源码哈希由既有 `source_report` 记录，参考提交不代表已安装字节相同。

[vLLM v0.25.1 EncoderCacheManager](https://github.com/vllm-project/vllm/blob/v0.25.1/vllm/v1/core/encoder_cache_manager.py)
已经按多模态哈希跨请求复用 encoder 输出，引用释放后的条目可在内存压力下被替换；
其 EncoderDecoderCacheManager 的相关检查始终返回未命中。
本原型不实现 encoder／KV 缓存。假设仅是合法兄弟请求靠近后，现有服务缓存可能减少重算；
实际模型、processor、服务配置与缓存容量是否支持、原生批处理是否已消除该差距，均待实测。
资产距离、CPU miss 或逻辑缓存字节都不等于 GPU 命中、显存或空转。

[Optimizing LLM Queries in Relational Data Analytics Workloads](https://arxiv.org/html/2403.05821v2)
已有行与字段重排以增加 KV 复用；本原型不改字段或消息，普通行局部排序不是新算法。
[Kalypso: Relational LLM Serving](https://arxiv.org/html/2607.23815v2)
已有依赖流水、KV 复用、在线基数／fanout（后继数量）及显存预算协调。
待检验的增量是原生准备缓存与服务缓存之间，同资产表示寿命和合法次序是否还有可测的完整查询收益；
目前只尝试最小机制，不宣称文献空白或创新成立。

## 本地检查与协调会话命令

本地只需 Python 标准库：

```bash
PYTHONPATH=code python3 -m unittest discover \
  -s code/tests/research/data_organization -p 'test_ecomm_media*.py' -v
```

原生检查由现有服务器协调会话执行，先按 runtime runbook 完成只读环境检查。
需要已有 pandas、Pillow、LOTUS 及其原生依赖、上述 SemBench Git 对象，以及已有两份 CSV／Parquet 与 `images/`。
不下载、不安装、不启动模型。先跑同一测试命令并设置 `SEMLOOM_NATIVE_TEST_CHECKOUT`，
再用原有样本执行下列命令，`PATTERN` 分别为 `all_true` 和 `mixed`，输出目录每次新建。

```bash
CUDA_VISIBLE_DEVICES='' HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 \
OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 PYTHONPATH=code \
timeout 120s "$DRIVER_PYTHON" -m src.research.data_organization.ecomm_media_profile \
  --sembench-checkout "$SEMBENCH_CHECKOUT" --data-dir "$ECOMM_DATA_DIR" \
  --styles-file "$ECOMM_STYLES_FILE" --mapping-file "$ECOMM_MAPPING_FILE" \
  --image-rows 2 --tile-assets 1 --max-logical-requests 128 \
  --model openai/CPU-vision-fixture --max-tokens 8 --pattern "$PATTERN" \
  --warmup 1 --repeats 3 --output-dir "$PRIVATE_CPU_RUN_DIR"
```

最多读取首两个图片映射及其全部匹配商品出现项；不按结果挑选。原合并的重复项可能使行数大于二，
预先计算全 True 时的完整调用上界，超过显式 128 就停止，不静默删行。
每次 invocation 最多 12 个 case、1536 个逻辑请求；两种 pattern 合计最多 3072，真实模型调用为零。
每种 pattern 的进程时限 120 秒。缺源、缺 SDK、无图片、请求／分支／Join 不一致或网络尝试立即停止并保留失败。

`requests_*.jsonl` 私有文件保留完整逐项消息、媒体、模型／有效参数、重复项、依赖和固定响应。
按 occurrence 校验每项输入／响应哈希，另记提交次序；逐分支原生 DataFrame 及最终 Series 严格比较。
没有运行视觉语言模型 processor，因而没有 token ID，replay 不能证明 GPU 输出逐位相同或真实质量。

`profile.json` 保留全部重复、warm-up 标记、源／查询／实现／SDK 哈希、请求字节分布与图片尺寸／大小。
`query_cpu_s` 包含捕获、分组、切片、原生准备、固定响应审计、分支装配及原三个 Join；
候选为捕获前四分支额外重放一次原查询前缀，该成本也计入，当前实现不适合作生产性能估计。
源读取／预检单列；两者之和是组合 CPU 口径，不是 GPU 端到端结果。
`df2multimodal_info_s` 包含图片准备，`filter_formatter_s` 是提示构造；原生 fetch、审计及观测采样另记，
这些嵌套时间不能相加。峰值仅覆盖被观察 ImageArray／块池中唯一字符串对象，按阶段分列，
不覆盖临时编解码、pandas、完整消息、解释器、RSS、锁页内存或显存。
`observed_cache_samples` 逐次保存缓存占用发生变化时的相对秒数、阶段、字符串字节和 Python 对象字节，
包括块清理后的采样；这是被观察缓存的留存轨迹，不是全部图片对象或进程物理内存的寿命。

先判断 native 是否已有全部复用，以及剩余 fetch／表示留存是否值得处理；
若额外复用不存在，或潜在节省小于组织／捕获成本及重复波动，停止 CPU 共享候选。
即使 CPU miss 减少，也需另有服务缓存资格、容量压力与完整查询比较计划，才由主会话排期 GPU。
公开结果仅保存经审核的汇总、哈希和必要观测；完整消息、样本路径与图片留在私有目录。
