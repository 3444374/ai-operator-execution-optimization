# main 可选 vLLM 资源分配资格

固定候选 `7b20feb507f64af27354b1d4c39c0606c7160725` 提供显式部署范围内的 `auto`／GPU数量配置。当前单RTX4090的auto与显式1各完成6次原LOTUS SDK请求，实际PID／出生时间、GPU UUID、请求及物理退出通过；它是服务接入资格，不是SemBench质量或性能结论。

## 最小闭包和来源

底稿main `464de926`；服务三文件来自固定资源源码 `88f9b08c`（baseline CPU接纳来源`7bac93ad`），移植后重新做本批检查。另补当前main严格sidecar reader兼容，四个生产文件共同构成闭包：

- `gpu_allocation.py`：只解析明确分配的设备，与继承UUID mask相交后冻结；不枚举或推断整台主机。
- `vllm_resources.py`：把数量／TP映射到同节点原生vLLM DP及内部负载均衡，检查官方parser和声明；纯reader校验不探卡。
- identity launcher：可选`--resource-config`，模型初始化前冻结CUDA UUID并核parser，原子sidecar保留requested／resolved。
- 现有`vllm_preflight.py`：旧精确八字段仍接受；资源两键必须成对、严格类型／数量／UUID／rank自洽，并核live argv、既有PID／出生／Python／模型身份。

main不因test import而引入CLIP/HSE/RM/index、PZ、replica router、新Ray actor范围或其他集成历史。资源owner后续`d2fa8c9b`的CLIP数值pair失败独立保留，既不阻断本服务闭包，也不升级为本轮或main资格。

## 使用与保持的默认

[现有launcher](../../../../code/scripts/services/launch_vllm_with_identity.py)新增可选参数：

```bash
PYTHONPATH=code VLLM_PYTHON=/path/to/vllm-python
"$VLLM_PYTHON" code/scripts/services/launch_vllm_with_identity.py --identity-output /path/to/private/service.json --port 8000 --resource-config /path/to/private/resources.json -- --model /path/to/local-model --host 127.0.0.1 --port 8000
```

资源文件只写`gpu_count`（`"auto"`或1至256整数）、`allowed_gpus`和`tensor_parallel_size`。推荐显式完整UUID列表；未写列表时仅从`GPU_IDS`或UUID形式的`CUDA_VISIBLE_DEVICES`取部署分配。`auto`使用交集中的全部已分配卡，显式数量取有序前缀；TP须整除卡数，DP=卡数／TP，PP1。继承的数字CUDA ordinal含义不明确时拒绝；声明外设备不先探测。资源配置拥有全部拓扑参数，不接受CLI／环境的平行覆盖，官方版本固定0.25.1。

没有`--resource-config`时，launcher参数、精确八字段sidecar及既有默认路径保持。配置声明的rank映射不是实际GPU进程绑定；本轮只另外验证所用单卡的实际自有进程／UUID，不声称通用多卡证明。

## 检查与实际12请求

Mac30项、Linux30项均通过且无skip；官方vLLM0.25.1 parser的auto1／explicit1两case在CUDA隐藏下通过，模型初始化0。原生SDK localhost六请求fixture、5项物理证据guard和两轴独审分列于[机器摘要](checks.json)。reader还覆盖producer CSV字符串与列表往返、单键／null／bool／UUID／count／rank／live参数漂移，以及新旧schema的PID／出生／venv错误。

| 固定候选单卡服务 | actual POST | 完整200响应／OK输出 | 实际HTTP峰 | 启动s | 服务退出／剩余 |
|---|---:|---:|---:|---:|---|
| auto | 6 | 6／6 | 4 | 57.027041 | 0／空 |
| 显式1 | 6 | 6／6 | 4 | 55.645477 | 0／空 |

同一张显式分配卡，TP1／DP1，vLLM0.25.1、Qwen2.5-7B-Instruct BF16、maxlen4096、seq4／batched2048、显存比例0.80、eager、prefix cache关闭。服务内部仍由原生vLLM管理；原LOTUS1.2.4／LiteLLM1.95.0／OpenAI2.50.0拥有批次与返回，maxbatch4、生成上限8、无重试／缓存。两组相同6个固定无敏感诊断请求，实际请求值多重集合相同、原SDK输出均OK，vLLM成功计数各6；未以向量近似阈值或CLIP结果替代服务检查。

启动／退出分别计量，一次运行不提供加速或稳定等价性统计。累计12全部返回、unknown0、HTTP错误0；正常共享lease、recover_stale=False，结束时owned／forced／actions／survivors及inventory error均为空，observer停止后正常释放。完整请求body／响应、实际NVML与进程出生记录、780运行文件／六helper绑定及预算保留在原件。

独审修掉派生控制器遗留的lease前worker调用，改为零HTTP／零LM的源码校验；CPU演练将acquire替换为刻意OfflineStop，到达边界并以0模型正常收尾后才运行实机。该准备修复不改变生产源码或预算。原始main464查询复算及成绩见[独立报告](../main_queries_20261015/README.md)，不作为新服务配置的原查询资格。

原件run id `main-auto-service-probe-r1`，146112bytes，SHA256 `cc0ea63e88e36a41a1bf6f3393b87355dfa7ed40cbb6c99efbd82b3a46cf7556`；49份证据加manifest共50文件，Mac独立副本逐份校验。项目维护者可按run id／SHA提供。恢复时controller／worker来自`controls/`，`controls/helpers/`恢复为同根`main-model-helpers/`，先核计划与helper哈希；仅审查可直接使用原请求／响应和生产声明校验函数，无需启动模型。

## 合并后固定main原查询

PR25合并后的源码固定`2041da67d883f54285952042482493a4a76a6ba1`，780个运行文件逐字核对；Mac／Linux各30项回归通过且无skip。原Movie Q2每臂120行、Animals Q1每臂64图，四执行owner分别在auto与显式1独立服务下运行，共16完整查询、1472实际POST／完整返回，warm-up、重试和结果缓存均为0。固定单RTX4090、TP1／DP1／PP1、每路径HTTP上限4；模型、原数据、SDK及服务参数沿用上文与[main464报告](../main_queries_20261015/README.md)的固定身份，本轮自行核验和计数，没有转用旧源码结果。

完整查询时间单位为秒；所有原值、ready-query和请求P50／P95见[results.json](results.json)。

| 原查询／单卡配置 | LOTUS | Daft | Ray Data | SemLoom |
|---|---:|---:|---:|---:|
| Movie Q2／auto | 5.213270 | 5.299987 | 19.861628 | 5.325647 |
| Movie Q2／显式1 | 5.227021 | 7.296496 | 19.521591 | 4.906483 |
| Animals Q1／auto | 38.595305 | 44.127305 | 60.899405 | 39.963959 |
| Animals Q1／显式1 | 37.729624 | 42.970365 | 60.498006 | 40.145637 |

Movie完整窗口从查询准备至完整结果，owner初始化／退出证据保留原件；本runner没有更宽的Movie application wall。Animals查询窗口保留原ImageArray→sem_filter→COUNT，外部执行器准备、评价／归档和关闭在更宽的application_s中单列。各值均不包含外部模型服务启动，不能把不同窗口相减当阶段成本。

| 服务组 | actual／returned POST | 启动s | 退出／自有剩余 |
|---|---:|---:|---|
| movie-auto | 480／480 | 56.864150 | 0／空 |
| movie-explicit1 | 480／480 | 54.111028 | 0／空 |
| animals-auto | 256／256 | 79.939907 | 0／空 |
| animals-explicit1 | 256／256 | 80.281271 | 0／空 |

两种配置／四臂的实际请求值多重集合和逐发生项判定相同；原OpenAI／LiteLLM转换、Filter parser、row_id及PNG像素关联均从本轮完整响应复算。Movie Q2的LIMIT 5官方precision／recall／F1均1.0，但LIMIT前分类仍TP7、FP0、TN106、FN7，不能写成全量Filter正确。Animals八格COUNT3／真值3、TP3／TN61、FP／FN0，64图逐项相同。它们只认证本数据与本次原查询，不证明更广质量。

每个cell只有一次查询，样本请求数120或64，故不报告请求P99、查询P99或P99.99，不合并不同cell凑尾部样本，也不给auto、显式1或四owner稳定排名。GPU UUID、live argv、PID／出生时间／Linux ticks与自有descendant在四次启动均有原件并经CPU回读；声明rank不代替这些物理证明。RSS每0.5秒采样，client加controller容量8GiB是监督门限，采样和逻辑额度均不是硬内存界；完整观测保留原件。

两次正常lease竞争在发送前退出，累计模型调用0；一次目录已存在保护同样在任何调用前退出。初次busy的stderr被该目录错误日志覆盖，原controller-result、预检及后续guard日志／过程记录保留并注明限制。随后仅增加仓外可取消的取锁前等待和物理证据导出，双审及15项Mac／9项Linux零模型guard通过后再运行；生产源码与1472预算未变。等待不持lease，锁消失后仍只进行一次normal acquire，竞态失败不自动重试，recover_stale=False。

四服务均自然exit0／remaining空，最终owned／forced／actions／survivors／errors为空、observer停止、inventory error为null，正常释放lease。结束后的现场无GPU计算进程；该快照不预留后续窗口。完整原件run id `main-auto-model-r1`，657225086bytes，SHA256 `4b7b2942dc9c73e1bfb4386275185535e14c29d8da1c4558622c89bb624ae6f7`，444份证据加manifest共445文件；Mac差量恢复的445份独立文件副本全部通过字节数／SHA核对，压缩归档本身保留服务器；恢复状态见[checks.json](checks.json)。原件按run id／SHA由维护者提供，包含全部实际请求／响应、预算、服务／资源、两次busy及首日志限制，不上传大量PNG或私有机器路径。

[复算入口](recompute.py)与[共享原解析／关联函数](../main_queries_20261015/recompute.py)按报告目录相对位置恢复，使用固定SDK环境与main2041代码，仅CPU运行：

```bash
CUDA_VISIBLE_DEVICES="" PYTHONPATH=code python /path/to/main_service_auto_20261015/recompute.py --root /path/to/main-auto-model-r1 --checkout /path/to/sembench --movie-manifest /path/to/movie-manifest.json --movie-csv /path/to/Reviews.csv --animals-manifest /path/to/animals-manifest.json --output /path/to/results.json
```

原件`controls/reports/`保存这两份脚本；controller／worker与`controls/helpers/`分别恢复到运行根及其`main-model-helpers/`，先核plan中的6份哈希。复算不需要启动模型；重执行原控制器仍受原预算、正常lease和环境前置要求约束。

本轮未重跑旧Movie Q1／Q3；其结果仍绑定main464。CLIP／HSE、PZ／IMLane、Serve、多GPU、TP>1／DP>1及多节点的实机资格分别解释，本报告不覆盖全项目auto。
