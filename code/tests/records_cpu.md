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
## 2026-11-03：具名 GPU 身份 RPC 与历史源码修订

修订以已审 `9b731ab9f1fa51e1b73820941586e11a46e9f012` 为父提交，保留该提交和[原 CPU 记录](#resource-allocation-cpu-9b)，等待复审，未推送或合并。
运行标识为 `resource-attribution-repair-20261103`。真实模型、GPU、Ray 调用仍为 0。

已复现并修复的两项问题：

- 使用固定 vLLM `v0.25.1` 的准确 `MsgpackEncoder` 定义与真实 msgspec 0.20.0，9b 原观察函数在默认设置下返回 `unavailable:TypeError`，worker 执行数为 0。
  修订在 `AsyncEngineArgs` 注册具名扩展，只传方法名 `semloom_gpu_identity` 和普通值，返回值也经过默认编码器；不传函数、代码或 pickle 数据，不改变安全设置。
  探针覆盖固定源码的扩展注册、方法冲突检查、请求／回执编码和方法分派；这属于源码片段与真实 codec 的 CPU 检查，未运行完整 vLLM。
- `calibration/core.py` 的维护改动曾新增五处旧投影来源差异。现保存准确 `5ff54bfe` 原字节，五个 variant 各追加一条 override；
  原文件 SHA 为 `81bfbc605eec72c74f4045b5609b4131ea9ced46e163b46b920de9a603e84515`，旧 expected SHA、投影 ID、来源版本及既有映射均未改。
  全 3010 个绑定的差异从 15 处恢复为原基线已有的 10 处，新增差异为 0；完整历史还原仍受这 10 处影响，不写成全部通过。

定向 21 项全部通过。相关回归 330 项中 325 项通过、3 项缺 LOTUS 导入错误、2 项跳过；跳过项为可选数据库解析与缺固定 SemBench checkout。
3 项错误与 9b 原本地结果相同；独审在已有 LOTUS 1.2.4 环境中对 9b 的 321 项得到 318 通过、1 项原基线同样失败的断言、2 项跳过。
该断言为 `test_actual_new_method_prompts_Q1_Q3_Q2_Q3_reuse_one_model` 的 `actual={}`，本次不改判或修写旧失败；有 SDK 的新树回归仍待复审环境核对。
21 项包含在相关回归中，不累加为独立数量。

测试文件：[固定来源探针](serving/test_vllm_identity_protocol.py)、[准确来源与 CPU 替身](serving/vllm_rpc_source_fixture.py)、
[设备身份](infrastructure/test_model_gpu_identity.py)、[历史校准映射](experiments/test_native_baseline_reproduction.py)。
源码哈希不匹配或显式指定探针却缺 msgspec 时检查失败；普通环境不提供源码目录时，该专用探针明确跳过，不下载依赖或源码。
命令中的 `CPU_PROTOCOL_PYTHON` 须具备常规 CPU 测试依赖和 msgspec；本机解释器与只读依赖路径由运行交接清单提供。

```sh
CUDA_VISIBLE_DEVICES='' HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 PYTHONPATH=code \
VLLM_0251_SOURCE_ROOT="$PINNED_VLLM_SOURCE" "$CPU_PROTOCOL_PYTHON" -m unittest \
  tests.infrastructure.test_model_gpu_identity tests.serving.test_vllm_identity_protocol \
  tests.serving.test_native_resource_feedback.NativeFeedbackTests.test_public_vllm_constructor_enables_custom_stats_without_default_logs_or_http \
  tests.experiments.test_native_baseline_reproduction.HistoricalProjectionTests.test_historical_calibration_keeps_exact_pre_fractional_CPU_source
```

本轮先按现有环境工具做纯 CPU 预检与安装预览，在独立临时环境安装唯一新增包 msgspec 0.20.0，原 driver／模型环境未改。
首个联网安装因 Python 默认 CA 路径不可用失败；随后通过正常 TLS 获取并校验官方 CPU wheel 摘要，再离线安装。没有关闭证书验证。
固定标签的官方源码只用于临时探针，Git 保存准确摘要与检查代码。原始复现、测试失败、来源清单、前后预检和检查输出保留在修订工作树的忽略目录
`tmp/resource-attribution-repair-20261103/` 与仓库外 CPU 探针目录，通过运行标识取得。

RPC 的 `timeout=1.0` 在当前 uni executor 中不是 worker 执行时限；外层 1.5 秒只限制客户端等待，不证明 worker 停止。
回执缺失时原显式 UUID 或唯一 NVML 设备的合法解析仍可用，`binding_source` 明确区别其与实际 worker 回执；多设备数字映射仍缺测。
真实 Ray／vLLM 进程、CUDA 设备归属和 GPU 性能仍未验证，不据本次 CPU 结果扩大资格。

<a id="resource-allocation-cpu-9b"></a>
## 2026-11-03：CPU 配额与 GPU 身份传递

本段保留 9b 本地候选当时的检查，底座为 `5ff54bfe774f5cdabb5b67bb5a2696dca2b84e08`，未推送或合并。
随后独审确认的编码与历史绑定问题及修订见[当前记录](#resource-allocation-cpu)。
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


### Unspecified R with existing complete grants

The CPU suite `tests.execution_provider.test_records_unspecified_requests` covers
None declarations through ordinary/rolling factories, CLI checks, session, query
reuse and offline identity. Missing grants reject before actor startup. Related
checks total167 successful methods; no model, real Ray or GPU ran. Default-R and
automatic-resource work remain pending. [Summary](../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/unspecified_request_checks.json).


### Native pressure with unknown work

`tests.execution_provider.test_records_request_feedback_safety` runs accepted Core
tasks, retained CPU receipts, typed observations and the current request policy.
It covers fresh pressure with unavailable work, sequence deduplication and unknown
responsibility. [Combined scope](../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/capacity_pressure_composition_checks.json)
records the exact source and skipped real-runtime checks.


### Retired one-off tools and retained regression tests

The maintained organizer and DuckDB adapter tests remain active. Existing native
projection tests restore all five historical trees and verify every original file
digest, including the retired tools. No test or fixture was removed by this cleanup.
[Checks](../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/entry_slimming_checks.json)
separate current code checks, exact historical restoration and untested model work.


### Session request-capacity preflight

`tests.experiments.test_session_capacity_preflight` checks resident/isolated static
None rejection before frameworks, private output creation or Ray. It also checks
unchecked Core startup, explicit static values and actual ordinary/rolling public
factories with complete independent budgets. Ray and model startup remain inert.
[Scope and outputs](../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/static_capacity_preflight_checks.json)
retain the original reproduction and the separate geometric handoff investigation.


### Synchronous native-context readiness

`tests.serving.test_sync_model_context` checks native-API stub values, spec/owner
identity, unknown or changed context, output allowance and retained cleanup after
ready failure. It also sends the real ready packet through the existing local
private pipe and confirms failed startup collects the CPU child. Unconfirmed
shutdown rejects ready until cleanup is confirmed; closed metadata remains available.
Related checks selected146 methods and ran143 successfully, with one three-method class skipped
for its missing upstream checkout. [Evidence](../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/sync_context_checks.json)
keeps source/API checks distinct from real vLLM and model validation.


### Session batching preflight

`tests.experiments.test_session_batching_preflight` checks direct startup with
invalid batch concurrency, incompatible rolling options and a missing batching
policy. Rejection precedes private directory creation and any inert Ray call.
A positive case retains deferred model startup and the actual factory settings.
[Evidence](../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/session_batching_preflight_checks.json)
keeps the original failure and a command containing a nonexistent test-module name.


### Prepared supply with independent declarations

The reviewed ready/event/supply and backend-progress modules are ported from
`b258c575`; inherited rolling cases remain distinct from new methods.
`tests.execution_provider.test_prepared_supply_budgets` exercises the actual
public factory, an unspecified R with complete finite declarations, independent
Method row and byte grants, and a single held-task grant with RPC notifications.
The two existing clock fixtures now patch the current query module; assertion
ASTs are unchanged. A new ordinary positive fixture uses its actual Filter stage.
[Evidence](../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/prepared_supply_composition_checks.json)
retains initial failures and original performance limitations.


### Combined follow-up source

The final combination includes prepared-supply notifications, independent
resource grants, both reviewed readiness/preflight repairs, and direct session
batching validation. It also runs the existing allocated-resource and cgroup
CPU fixtures; these checks do not allocate automatic capacity or measure the
target server. [Combined evidence](../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/system_followup_checks.json)
keeps all earlier failed runs, skips and the new exact-source result separately.


## 2026-11-04 legacy 等待中帧交接

[确定性用例](execution_provider/test_core_pending_frame_handoff.py)用 Future 与手动时钟检查未消费帧、
原时间／序号／一次消费、RPC 到期／错误、模型／查询变化和缺少序号。保留帧仍可被控制器按其原年龄规则拒绝。
该组及原来源身份／几何 trace 合计 34 方法通过；
[交接摘要](../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/pending_frame_checks.json)
另列相关完整复查和原 geometric 集成失败。新修正不扩大观察时长、不改动态阈值、不启动真实 Ray 或模型。


## 2026-11-04 Movie 准备模式接线

[入口用例](experiments/test_movie_preparation_mode.py)覆盖默认等价、非法模式、固定 rolling 限定、
直接 start 提前拒绝、check／worker CLI、resident 同模型复用、校准身份及 None 加完整独立预算。
后期上下文错误另核对原 CPU 子进程的成功事件与保留回执／用量，并保持查询失败。
[检查摘要](../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/preparation_cli_checks.json)
保留原反例及两处新增 fixture 修正，不把某一 fixture 中零前缀推成所有默认路径零提交的要求。


## 2026-11-04 同步逐调用实际上下文

[逐调用用例](serving/test_sync_generation_context.py)检查省略与显式上下文、小／大模型解析值、
没有先读取 ready 的调用、上下文变化、调用数不被拒绝项消耗及关闭／不确定／忙碌状态。
这是 CPU 原生 API stub；构造和 chat 参数保持，真实模型与完整提示长度资格尚未获得。
[检查摘要](../../experiments/results/diagnostics/records_runtime_refactor_20261104/raw/sync_generation_checks.json)
保留第一轮反例和本地 psutil 进程身份读取受限的原错误，后续本机 CPU 回归不改判这些记录。


优先级路径的延迟关闭检查改为复用已有“确认模型成功后延迟交付”的用例，保留 successor_first 设置，
核对结果／用量、未完成查询、Core 持有和无 parser／后继重放。原接纳确认不能证明模型成功，
旧完整回归的一次失败和父候选／当前候选的单次对照均保留；接纳但仍计算的关闭情况仍未由此用例证明。
