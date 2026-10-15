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

合并后固定新的main，用原Movie Q2／Animals Q1四臂、auto／显式1共16查询／1472请求做有限整合复验；不重跑旧Q1（2000行）和Q3（120行），不继承旧源码的模型资格。多GPU、TP>1／DP>1、Serve及多节点实机验证仍pending，不阻塞单卡可选服务配置。
