# Records CPU 检查的本地资产绑定

本页仅用于已有CPU/SDK与合成响应回归，不加载模型、不启动GPU或SSH。使用已配置的driver解释器；本页不安装依赖或下载数据。

`SEMBENCH_CHECKOUT`指向固定SemBench `c814e3807e72d4cf876b852b17e77f3cc94575c2`源码目录。
`SEMBENCH_Q3_MANIFEST`指向本机新生成的manifest。路径只由测试环境提供，不写入生产代码或公开配置。
固定历史CSV的SHA-256为`ad5def7df4ece425c741c82d73687b8cb4a61952e1580c0427879cbef3033aaf`，
全2000行、Q3普通谓词命中120个发生项；这是historical_sf2000 profile，不声称官方整表身份。

旧封包manifest可能记录原机器绝对路径。保留旧封包与CSV字节，用现有prepare入口在新的仓库外目录重新绑定本地路径：

```sh
export CUDA_VISIBLE_DEVICES=''
export HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 PYTHONDONTWRITEBYTECODE=1
export LITELLM_LOCAL_MODEL_COST_MAP=True
export PYTHONPATH=code
export SEMBENCH_CHECKOUT="$PINNED_SEMBENCH_CHECKOUT"
"$DRIVER_PYTHON" -B code/scripts/experiments/run_lotus_records_movie.py prepare \
  --input-csv "$RECORDS_CPU_INPUT_CSV" \
  --input-sha256 ad5def7df4ece425c741c82d73687b8cb4a61952e1580c0427879cbef3033aaf \
  --max-rows 2000 --role sembench_movie_subset --task movie-q3 \
  --output-dir "$NEW_CPU_INPUT_OUTPUT"
export SEMBENCH_Q3_MANIFEST="$NEW_CPU_INPUT_OUTPUT/manifest.json"
"$DRIVER_PYTHON" -B -m unittest \
  tests.baselines.text.test_lotus_native_local_model.NativeLocalTests.test_full_120_occurrence_native_http_profiles_match_original_q3 -v
```

调用者须提供真实CSV、固定源码及新目录变量。prepare验证CSV摘要，再由`MovieRecordSource`核对原CSV、records、references、行序和120个发生项；不伪造缺失资产，不修改旧manifest或参考标签。资产或固定SDK缺失时，既有可选用例明确skip；一旦提供无效路径/摘要，应失败，不能把失败改成skip。

该用例使用原LOTUS和SemBench方法/评价器，但响应来自`httpx.MockTransport`与`FixtureService`。
它对比并发4/8下的120发生项、消息、原parser、调用关联及合成usage；不认证模型质量、真实token、GPU性能或正式预算资格。
如需保存脱敏CPU回执，可显式设置`NATIVE_CPU_EVIDENCE_DIR`，调用者须先创建新的私有目录；省略该变量不写该份回执。

Daft session回归使用真实Daft CPU图和合成kernel。其本机Unix socket须由运行环境允许；沙箱拒绝bind是环境错误，应保留失败记录并在获准的CPU环境重跑，不能替换原生图或跳过断言取得通过。

普通合成fixture无需这份真实CSV。与本页相关的全部CPU通过仍不替代当前生产源码的独立source/input/token/model/env/ledger/owner绑定和GPU资格。

<a id="resource-allocation-cpu"></a>
## 2026-11-03：CPU 配额与 GPU 身份传递

本地独立候选基于 `5ff54bfe774f5cdabb5b67bb5a2696dca2b84e08`，未推送或合并，独审 pending。
运行标识为 `resource-attribution-cpu-20261103`；仅使用临时文件、计数替身、线程与真实本地进程通信。
真实 Ray、模型和 GPU 调用为 0，没有整查询性能、模型质量或多卡调度结论。

源码行为与采用依据：

- `observe_cpu_capacity()`复用机器观测，取 affinity 与可见 cgroup v1/v2 逐级 CPU 配额的较小值，保留小数；读取失败不授予整机核数。
- SemLoom 普通 records、常驻 session 和已有 cascade 启动声明在 `ray.init()` 前检查；声明仍为准备 actor＋模型 actor＋调用方 1 核，actor 各自的 1 CPU 保持。
  原生 Ray Data／Daft 的配置与调度仍归对应系统。校准的 `hardware.cpu_limit` 接受正数配额，候选比较沿用相同求和规则。
- cgroup CPU 用量按有效额度归一化，并检查有配额的可见祖先用量，保留最大占用比例；初读、重置、组／配额变化或读失败保持缺测。
  v1 的 CPU 与 cpuacct 分开编组时无法证明相同成员，用量缺测；主机 affinity 计数和独立主机采样保持自己的含义。
- 主机 RAM 不作容器额度。容器的 limit/headroom 另列，余量同时考虑可见祖先已占用的字节。挂载之外的隐藏祖先与未采集的内核内存仍不可确认。
- 原生反馈启用时用现有 vLLM `collective_rpc` 向实际模型 worker 读取一次 UUID，经 readiness 和原 sampler 传递；只读已初始化的单设备 CUDA。
  关闭反馈不增加该 RPC；超时、错 UUID、MIG、多设备或缺回执均保留缺测。没有修改 vLLM 源码、token 调度、KV cache、双 50% 规则、B／C、模型参数或 baseline。

最终相关检查 321 项：316 项通过、3 项错误、2 项跳过。3 项错误均为缺少 LOTUS 导入；同一解释器下对原基线单独复核这 3 项，得到相同错误。
用例为 `CoreFeedbackTests.test_actual_new_method_prompts_Q1_Q3_Q2_Q3_reuse_one_model`，以及 `OnlineConfigurationTests` 中的 isolated/cache 与 resident/cache 两项。
不能把这 3 项写成通过。新用例覆盖嵌套 v1/v2、小数配额、未知读取、命名空间／挂载歧义、共享父级压力、回退隔离、错误设备和实际本地 readiness 通信。

使用已有 Python 3.12 driver 与只读依赖路径运行，没有安装或下载。初次使用系统解释器时缺少 PyArrow；添加父进程依赖路径仍不能传给重设 `PYTHONPATH` 的测试子进程，相关失败全部保留。
随后改用已有含 PyArrow 的 driver 解释器，父进程从既有系统路径读取 psutil；上述 3 项 LOTUS 错误仍存在。
一次定向检查 139 项中 138 通过、1 跳过是较早快照；最终以 321 项为准，不累加重复运行。

可在具备依赖的已检查 driver 环境重跑：

```sh
CUDA_VISIBLE_DEVICES='' HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 PYTHONPATH=code \
"$DRIVER_PYTHON" -m unittest \
  tests.infrastructure.test_cpu_allocation tests.infrastructure.test_model_gpu_identity \
  tests.observability.test_allocated_resources tests.observability.test_host_cpu_counter_source \
  tests.observability.test_records_telemetry tests.infrastructure.test_environment \
  tests.infrastructure.test_environment_capabilities tests.experiments.test_core_calibration \
  tests.experiments.test_records_cpu_budget tests.serving.test_allocation_identity_ipc \
  tests.serving.test_async_engine_process tests.serving.test_async_engine_batch \
  tests.serving.test_native_resource_feedback tests.execution_provider.test_lotus_records_async_actor \
  tests.execution_provider.test_lotus_records_async_core tests.execution_provider.test_lotus_records_actor_owner \
  tests.execution_provider.test_lotus_records_core tests.execution_provider.test_core_feedback_cadence \
  tests.execution_provider.test_core_resource_feedback tests.execution_provider.test_resource_rpc_rejection_audit \
  tests.experiments.test_movie_records_session tests.experiments.test_records_online_config \
  tests.infrastructure.test_gpu_allocation
```

脱敏输出保存在候选工作树的忽略目录 `tmp/resource-attribution-checks/`；`suites.json`保留完整清单。
全部早期输出保留；最终输出 `final-verified.log` 的 SHA-256 为 `b167e35e961180b5d307f8319627d533c78c540b2eb600441534381dcf3211d9`，
原基线复核 `baseline-three.log` 为 `b3ce460f5158aa2dbd56f5e3989a1450558c75e8948b6a2d8bc3c2fc9e38a78f`。
通过运行标识与摘要从本地工作树取得；这些输出不作为 GPU 资格或性能证据。
