# PostgreSQL AI 算子画像脚本

跨机器环境入口：`environment/manage_environment.py`。它按
`deploy/runtime/profiles/*.json` 与 `deploy/runtime/assets.json` 只读检查机器、Python
能力、模型和数据；安装/下载是独立显式子命令。它不导入 PostgreSQL workload，也不
替代各实验 runner 的正确性门禁。完整流程见 `deploy/runtime/README.md`。

## 文件定位

脚本按职责分为七组：

| 子目录 | 只负责 |
|---|---|
| `data/` | workload 导入 |
| `services/` | 本地调试服务与受审计的模型服务启动器 |
| `baselines/` | 原生 baseline/gate 薄入口 |
| `profiling/` | 数据链路画像与机制诊断 |
| `experiments/` | 场景、矩阵和多 job 正式编排 |
| `analysis/` | 离线汇总、代价估计和 calibration 选择 |
| `environment/` | 机器、依赖、资产与仓库安全检查 |

入口脚本只解析参数并调用 `src/`；不得因为移动目录而复制生产逻辑。历史结果目录里的
raw manifest 保留执行时旧路径作为不可变证据，README 中的复现命令使用当前新路径。

## PostgreSQL semantic execution-provider gateway

`services/run_execution_provider_gateway.py` 是外部 semantic execution-provider 的 canonical CLI。
同步 recording wire v2、exact wire v3、choice wire v4、deterministic golden adapter、fixed OpenAI-compatible
adapter 和 UDS server 的实现位于 `src/execution_provider/`。从仓库根运行 recording profile：

```bash
python3 code/scripts/services/run_execution_provider_gateway.py \
  --socket /absolute/path/semloom-recording.sock
```

`postgres/semloom_pg/gateway/recording_gateway.py` 与同目录 `protocol.py` 只为既有 TAP 和 import
保留自定位兼容入口；它们不要求调用方额外设置 `PYTHONPATH`，也不保存协议或 server 逻辑。
golden profile 使用 `--golden-fixture`，fixed profile 使用仓库外 `--fixed-model-config`；endpoint、
model、timeout 和 bearer-token 环境变量名不进入仓库。

## SemMap resource measurement

`experiments/run_semmap_resource_checks.py` creates a new, exclusively owned result directory before
preflight and compilation. Existing directories are refused without writing into them. It compiles the
source-managed C client; the obsolete `--client` argument has been removed.

```bash
python code/scripts/experiments/run_semmap_resource_checks.py \
  --repo /path/to/repository --root /path/to/new-artifact-directory \
  --prefix /path/to/postgresql-18.3-install --commit <source-commit> --diagnostic
```

The diagnostic performs an actual 1×100 fixture workload with 100000-byte input and 65536-byte output.
It sends no real model requests. Run the runtime preflight first; keep the artifact/socket path short enough
for AF_UNIX. Each phase preserves baseline, operation, cleanup, session events and its own report/hash list.
Case and run reports use the same assessment; diagnostic qualification is always `not_evaluated` (exit 2).
Formal results use exit 0 for all required phases/cases passing, 1 for valid failed checks, 2 for incomplete
measurement, and 3 for runner/preflight failure. Interrupts preserve available evidence and propagate.
Fault/recovery connections use a separate experiment fixture that waits up to five seconds for both endpoints
to be observed before releasing the handshake. Pressure timing is unchanged; these fault timings are not performance evidence.
See the [Map contract](../../experiments/plans/postgresql_semmap_generation_contract.md) for thresholds and
current authorization. Formal 3×2000 remains unavailable until a valid current diagnostic and separate authorization.

## Choice resource qualification tools

`experiments/run_choice_resource_checks.py` 是内部、仅限 Linux 的 fixture 资源验证入口。它使用指定的
PG18.3 安装创建独立集群，测旧/新配置、取消恢复和阻塞 DNS；不启动或调用真实模型。
运行前遵循 runtime preflight，并使用新的仓库外产物目录，采样与判定条件见
[choice 专项计划 C.5](../../experiments/plans/completed/postgresql_choice_profile_engineering.md#c5-对照请求预算与资源保证)。

```bash
python code/scripts/experiments/run_choice_resource_checks.py \
  --repo /path/to/repository --root /path/to/new-artifact-directory \
  --prefix /path/to/postgresql-18.3-install
```

实验侧 `src/experiments/choice_attempt_ledger.py` 在 POST 前持久预留最多 100 次尝试，失败不退款；
已有 ledger 不重新初始化。`choice_gateway_observer.py` 只在独立验证进程中记录实际请求/完成，
不记录认证头，也不改变生产 gateway 或 PG port。真实 smoke 必须复用同一外部 ledger；这些工具
本身不证明真实服务支持 choice 或通过模型质量验证。

`experiments/run_choice_service_checks.py` 以独立 PG18.3 集群执行预先登记的 14 次 old/choice 请求与
两个 NULL 对照。它要求已有持久 ledger；真实模式核对 live service、模型文件及继承参数，fixture 模式
必须显式指定。记录实际 HTTP JSON、raw completion、SQLSTATE、EXPLAIN 行数/usage，以及前后身份；
仅删除 choice 字段后仍有值或类型差异则拒绝通过。程序不启动模型、不创建真实预算、不读取 held-out，
也不评定标签质量；先按同一 [C.5 计划](../../experiments/plans/completed/postgresql_choice_profile_engineering.md#c5-对照请求预算与资源保证)
完成 preflight、模型服务与账本核验，再使用 `--help` 中的路径参数运行。

## Exact SemFilter reference calibration

`analysis/build_semfilter_reference_calibration.py` 将离线收集的 exact-reference training/held-out
观测转换为 planner 可消费的严格 JSON artifact。输入固定 semantic/physical digest、provider profile、
model/role、workload/service SHA-256 签名、允许的 held-out 最大相对误差，以及两组观测；每条观测分别
记录 semantic input rows、output rows、model calls、prompt/output tokens 和 service milliseconds。
它不连接 PostgreSQL 或模型服务，也不在线采样：

```bash
python3 code/scripts/analysis/build_semfilter_reference_calibration.py \
  --source /absolute/path/reference-observations.json \
  --output /absolute/path/reference-calibration.json
```

builder 分开拟合 output selectivity、calls/input、prompt/output tokens/call 和 fixed/call/prompt/output
四项 service-time 系数，并用 held-out 数据检查误差后才以独占创建方式写出 artifact。PostgreSQL planner
再通过 superuser-only `semloom_pg.reference_calibration_file` 显式选择该文件，严格校验 schema、identity、
semantic/physical/model/role/provider 匹配并把 workload/service 签名复制进 plan。签名是外部运行编排选择
正确 artifact 的审计身份，不是 PostgreSQL 对 gateway endpoint 或硬件的在线探测；更换模型、服务或
workload 分布时必须离线生成并重新选择 artifact。缺失、损坏或失配只保留 uncalibrated exact reference，
不会生成 optimized path。

`analysis/audit_saor_formal_readiness.py` 在不发送请求的前提下 fail-closed 校验固定包络 SAOR
十 scenario formal，或用 `--profile priority_reachability` 校验 static/SAOR/foreground
strict-priority 三臂诊断；共同检查 1+3、FCFS 声明、calibration selection、manifest
行数/SHA/endpoint 覆盖。formal profile 还检查 direct/project 请求等价字段。正式 runner 的
`--rehearsal` 只产生每 scenario 一个 warmup identity；通过后才可用新目录运行配置中的 formal。

`analysis/summarize_saor_active_set.py` 要求十个 scenario 各 1 warm-up + 3 formal、0 incident，
并把六臂 workload lifecycle 与四个 credit 策略 mechanism gate 分开审计。rehearsal 本身
fail-closed：metrics/resources、lifecycle 或适用的 borrow/reclaim/work-conserving-drain
机制任一失败都会写 failed manifest 并返回非零。它分别以 project
solo 和 direct solo 复算 work-rate slowdown/Jain，输出 `formal_summary.csv`、
`per_job_slowdown.csv` 与 `validation.json`；不把 direct 的 request bound 误标为 work-credit
等资源 envelope，也不产生 theorem 或 dynamic-K claim。

同一汇总器的 `--mechanism-only` 只从 compact `group_runs.csv` 回放 credit mechanism。
post-drain 完成间隔低于 250 ms trace 周期且区间内没有样本时记为不适用；有时间窗/样本时仍
fail-closed。输出 `mechanism_gate_replay.json` 明确不升级完整 formal validation。

`analysis/summarize_saor_priority_reachability.py` 汇总 static、SAOR 与 non-preemptive
foreground strict-priority 三臂 1+3 诊断。它要求 group evidence 中 priority 动作为 `[0,1]`，
并以 foreground P99≤30.7s、SLO violation≤1% 判 release-only 上界是否可达；吞吐只作语境，
该诊断不构成 SAOR 或 reservation 策略胜出。

`analysis/summarize_saor_bounded_priority_gate.py` 是 bounded-priority SAOR
候选的 fail-closed 本地开发门。它只接受两轮干净 rehearsal，每轮固定包含 static、
release-only SAOR、0.125K 和 0.25K debt-cap 四臂，并重算正确性、前台尾延迟/SLO、
吞吐、bulk 保护和机制门。机制证据只来自无损 release-event ledger；账本缺失、为空、
序号缺口或重复都会失败。采样 credit snapshot 仅供诊断，既不能否决短于采样周期的真实
转换，也不能单独满足机制门。通过只得到 `formal_registration_candidate`，不构成正式性能结果。
同一脚本的 `--profile bounded_ready` 只接受新 `saor_bounded_ready` 两档 scenario，并额外
要求 submission lifecycle 与 actor-side release-event request join 完整、foreground
register→grant
区间非空、区间内 foreign fallback=0；旧 bounded-priority profile 保持不变。

`analysis/audit_saor_formal_readiness.py --profile matched_ready_selector_ablation`
审计六臂项目内部归因合同：project frozen-static 不使用 bounded-ready；FIFO、DRR、
external VTC-style、strict-priority 与 proposed 对所有 Job 使用同一 bounded-ready
request/work/logical-bytes 上限。该 profile 不包含、也不能替代任何原生系统 baseline；
通过只允许 1--2 轮 development rehearsal。
`analysis/summarize_saor_matched_ready_ablation.py` 随后 fail-closed 校验六臂身份、
correctness、五个 bounded-ready 臂的全 Job ready lifecycle 和 proposed 的 guarded-debt 机制证据；
frozen-static 生产路径不经过 shared-credit ledger，其 completion service lag 明确标为
`not_applicable`，但仍用共同可见的 JCT/P99/SLO/吞吐评价，输出原始臂指标，
但固定写明 `selector_victory_decided=false`、`formal_authorized=false`；效应量与
non-inferiority 边界未预注册前，工具不会替研究者宣布 proposed 胜出。

`experiments/run_saor_project_mechanism.py` 是独立 Project mechanism 矩阵的 audit-aware
入口。它绑定 `saor_project_mechanism_formal_contract.json`，运行前复用 matched-ready
readiness audit，并把合同 SHA 与 readiness 写入 output root。最终 rehearsal 已通过并登记证据
SHA，独立审核后又由当前签名 ceiling 确认 feeding=92.898%<95%，合同已进入
`locked_failed_feeding/formal_authorized=false`，因此 wrapper 仍只允许
`--rehearsal`；即使有人
遗漏命令行约定，非 rehearsal 也会 fail closed。正式配置使用位置平衡种子，使六臂在三次
formal 中各占三个不同序位，而不是让 proposed 连续固定在首位。授权 validator 逐字段绑定已审核
root 的 validation SHA、commit、root ID、archive SHA 与 valid flag；不是“任意 SHA 存在”即可放行。

`analysis/summarize_saor_project_mechanism_formal.py` 只接受经过上述 wrapper、完整 1+3、18 个
formal cell 的证据。static 的 registered-ready fairness 为不适用；五个 bounded-ready 臂必须
有 completion-accounted service lag/最长无服务证据；proposed 还必须闭合 recovery grant→request
completion→debt below-cap episode。工具将 evidence validity 与 claim gate 分开：有效实验即使
未过 5% headline、吞吐/bulk JCT/SLO/no-service non-inferiority 或 repayment 门也保持有效负结果，
不会把性能失败伪装成无效运行。

`experiments/run_saor_feeding_ceiling.py` 只运行一个 `direct_no_job` cell，并在发请求前逐字段比较
ceiling 与六臂 reference 的 endpoints、服务元数据、K/W、common args、typed work cost、
calibration、manifest、rows 和 arrival。它明确禁止 bounded-ready/credit，调度所有权属于 direct
HTTP semaphore + vLLM FCFS。`analysis/summarize_saor_feeding_ceiling.py` 再区分 evidence validity
与 ≥95% feeding gate：合法的 92% 结果写为 `failed_feeding` 且退出码仍允许归档，不会被伪装成
基础设施失败或自动授权 formal。当前冻结结果为 direct 13,684.90 tok/s、SAOR 12,713.03 tok/s、
ratio=92.898%，因此当前合同不再等待 ceiling，而是终止 formal。
汇总 CLI 现在强制传入 evaluation contract 与 project/ceiling 两个完整 archive；只有 group CSV、
manifest commit/config/root、运行时合同快照、rehearsal validation 和 archive SHA 全部匹配，才会
设置 `evidence_valid=true`。可复用 runner 参数解析和 endpoint idle gate 位于
`src/experiments/shared_vllm/{cli,preflight}.py`，feeding CLI 不再跨 `scripts/` 导入。

`experiments/run_saor_feeding_gap_diagnostic.py` 是终止 formal 后唯一允许的文本差距归因入口。
它验证旧合同仍为 `locked_failed_feeding` 且 SHA 未变，再执行 D0 direct K-only、D1 direct K+W、
P0 bounded-ready FIFO K+W 的平衡 `1+3` diagnostic matrix。D1 使用 endpoint-local typed-work
reservation/completion release，但不读取 Job 身份、权重或 ready window，因此是 Project diagnostic
control，不是原生 baseline。入口会先结构化保存 PG/Ray/全部 endpoint clean gate；失败 root 不得
续跑或覆盖。

`analysis/summarize_saor_feeding_gap_diagnostic.py` 从 group、direct admission ledger、P0 credit trace
和 Job runs 重算 W/request occupancy、admission wait、Ray submit/actor-ready、vLLM、MFU、TTFT/ITL、
JCT/SLO 与 energy，并对三个 measured repeat 配对计算 D1/D0 和 P0/D1。缺任一证据族只输出
`invalid_evidence`；四种 0.95 分类均显式保留旧负判决，不能授权 SAOR formal。

`analysis/audit_chat_prompt_overhead.py` 从 `jobs/*.requests.csv` 与
`jobs/*.submissions.csv` 按 submission ID 独立重算
`service total - raw prompt - actual output`。它只接受一请求一 submission、全完成、非负且
全体一致的开销，并可冻结 expected overhead/request count；输出包含每个输入文件 SHA。该值绑定
模型 revision、chat template 与 completion protocol，不是跨服务常数，也不能由 runtime
summary 自证。request trace 保留 raw prompt token；只有 admission/credit 的 effective work
加入该校准项。正式 fixed-cap 合同还要求每条 `estimated_output_tokens` 严格等于冻结的
`completion_max_tokens`；客户端对输出文本的事后重分词只作诊断，不能充当 admission estimate。

`analysis/audit_saor_formal_readiness.py --profile ready_observation_bridge`
审计三臂 Project observation bridge：frozen-static/single-head、shared FIFO/single-head、
shared FIFO/bounded-ready。第一段隔离 static partition→shared capacity，第二段在 FIFO 固定时
隔离 single-head→bounded-ready。配置入口为
`deploy/autodl/saor_ready_observation_bridge.example.json`。
`analysis/summarize_saor_ready_observation_bridge.py` 要求一到两个干净 rehearsal root，输出
`bridge_metrics.csv` 与 `bridge_effects.csv`，同时固定 native baseline 数为 0、两个效应均为
`decided=false`、`formal_authorized=false`；该桥是项目内部归因，不是原生系统比较。

`experiments/run_saor_native_system_matched.py` 是本地系统级 matched matrix 编排入口。它在
dispatch 前联合加载 matched/native/Project 三份配置，并调用
`src/experiments/saor/native_system_bindings.py` 执行无副作用的执行器绑定审计，随后
平衡编排 5 个唯一物理臂（3 个原生系统臂、Project frozen-static 和 proposed）。
`--rehearsal` 只运行每个物理臂一次 warm-up，不产生 formal
cell。非 rehearsal 还必须显式传入独立 `--formal-authorization` artifact，以及实际 rehearsal 的
validation/root/archive；授权精确绑定 repository commit、三份实际 config identity、resolved-config
fingerprint、manifest/Job SHA、rehearsal matrix-index/validation/archive SHA。runner
在创建输出目录、获取 host lease 或调用 executor 之前完成校验。当前 native-system GPU/formal
仍停止，仓库不随模板提供有效授权；merge/rehearsal 均不能替代独立审核后的 formal 决定。
readiness 同时把 native calibration 的 SHA/adapter/concurrency/batch 与实际 executor 对齐，并
要求三类执行器访问同一冻结 endpoint pair，Project 两臂真实执行完整 512+512 行。授权通过后，每次物理矩阵生成唯一
`matrix_instance_id`，并绑定 contract snapshot、index 与所有 cell，跨 output root 替换 cell
会被汇总器拒绝。原生 shard 与 Project Job 摘要必须提供一致的实际 PostgreSQL/pgvector 版本；
这些字段进入每条 `all_runs.csv`，不能用配置默认值代替。离线汇总还会按原始命令重验原生
adapter/concurrency/batch/endpoint 与 Project endpoint，避免只信任运行前配置检查。
该入口的 `--native-runner` 是每个 shard 真正调用的 official adapter CLI，必须传
`code/scripts/baselines/run_official_baseline.py`；不要传
`run_text_native_multijob.py`，后者已经是另一层 multi-job 编排器。native shard 的请求证据位于
`jobs/<job_id>/shard_<n>/requests.csv`，Project 请求证据仍位于
`jobs/<job_id>.requests.csv`。Project lifecycle trace 不包含输出正文；同一命令必须另产
`jobs/<job_id>.completions.csv`，由 profiler 直接从 in-process operator results 写出，统一 sink
用它和 PostgreSQL readback 独立核对内容 digest。native shard 则直接用包含正文的
`requests.csv`；两条路径都不能从数据库读出 expected text 再与数据库自比。

`analysis/audit_vllm_0251_source.py` 必须由冻结 vLLM Python 执行，逐项比较 package version、
dist-info 和五个关键 installed-source SHA；缺 expected SHA 只会 blocked。
`analysis/audit_saor_native_system_matched.py` 可从仓库根直接运行，联合加载三份实际 config；默认只
报告 `static_config_passed/rehearsal_ready=false`。外层始终由包含 Ray/Daft/psycopg 的
`DRIVER_PYTHON` 运行；`--vllm-python` 只作为子进程重哈希冻结 install，并将 live PID、start time、
未解析 argv0、`sys.prefix`、package path/version 与 endpoint launcher sidecar 绑定。服务身份通过仍不
置 ready；还必须依次绑定 system-preflight 与 correctness-smoke evidence，四阶段全部通过才允许
rehearsal。五臂 runner 同样必须由 `DRIVER_PYTHON` 运行，driver/vLLM 环境相同会 fail closed。

`analysis/run_saor_native_system_preflight.py` 是实际 system producer：从 DRIVER 环境读取 endpoint
health、PostgreSQL/pgvector、Ray actor/placement-group 状态，并用 `nvidia-smi` 拒绝不属于已绑定
vLLM 进程树的 CUDA compute PID；随后深校验一份在当前 vLLM
sidecar 之后产生的 bounded HTTP passed root。readiness 不只读其布尔字段，而会重跑探针并要求
结果完全一致。五臂 runner 的 `--correctness-smoke` 模式使用显式 fresh
`--correctness-smoke-root` 跑一轮完整五臂；输出的 `matrix_index.json` 是第四阶段唯一接受的
evidence，且不占用 canonical rehearsal root。

`analysis/validate_saor_native_system_rehearsal.py` 只读完成的 rehearsal root 与 archive，验证恰好
五个 warmup cell、全部 exactly-once、live readiness、commit/config identity 后封存 validation。
它还逐文件验证 root 与 archive 完全一致，并重算每臂 raw artifact SHA、sink digest 和 native
upstream/adapter provenance；CLI 必须同时传 `--config`、`--native-config` 与 `--project-config`，
formal identity 显式包含三份 config SHA。该 validation/root/archive 是 formal authorization 的必填前置，
不由仓库自行授权。

`services/launch_vllm_with_identity.py` 由 `deploy/autodl/start_endpoints.sh` 使用 `VLLM_PYTHON`
调用；它在 `exec` API server 前原子写入 PID/start-time/argv0/`sys.prefix`/package sidecar，使不同
venv 即使共享同一底层解释器也不能被 readiness 混同。

`analysis/summarize_saor_native_system_matched.py` 是该矩阵的薄 CLI；可复用的纯离线、
fail-closed 核心位于 `src/experiments/saor/native_system_summary.py`，不连接服务。CLI 要求同一个
独立 formal authorization artifact。核心在生成排名前重算 authorization/contract snapshot/config
fingerprint/manifest/service signature/scheduler owner/schedule/index/cell identity。通过时输出
`all_runs.csv`、五臂
`system_summary.csv`、`job_summary.csv`、`resource_summary.csv` 和固定边界的
`validation.json`。历史 Project selector rehearsal 不由该入口生成，也不进入系统排名。
原生 request P99/SLO 无共同真实 request clock 时必须保留字面值 `unavailable` 和
非空原因；P99 与 SLO 分别输出 status/value/reason，任一不可用时不得生成跨系统排名。Job JCT
按预注册的 nominal release→completion 计算，actual launch/offset/deviation 仅保留为启动抖动与
overlap 诊断。Project 的 actual launch 是父 runner 越过绝对 release barrier 后、紧邻 `Popen`
的 launcher epoch；子进程完成冷启动/DB fetch 后的首条 lifecycle arrival 另存为
`source_arrival_epoch_s`，二者不得混用。Task3 normalizer 把 legacy flat unavailable tail 转为中性 nested
`request_p99/slo → status/value/reason`，并把 `_snapshot_mapping` 产生的五类 JSON-encoded per-Job
live 容器解码后再存入 evidence。终态校验按真实 native `queue_final` 与 Project
`shared_credit_final` schema 只检查实时 active/waiting，不把 K/W 限额或历史峰值误判为残留工作。
六个输出先写相邻 staging；发布 CSV 前先原子写入非 passed 的 `publishing` marker，五个 CSV 逐个
替换，最后才原子发布 passed `validation.json`。消费者只可把 `validation.json.status=passed` 视为
有效代次；失败/篡改矩阵会删除旧性能表，但保留含所有已记录 cell 的 `all_runs.csv`
及经统一脱敏的失败原因。passed validation 仍固定 `formal_authorized=false`，仅以独立字段
`formal_authorization_verified=true` 表示本次运行 artifact 已通过身份核验。
工具不产生 winner；passed 只表示提供的独立 formal authorization 与封存证据身份一致，不会自行
启动实验。manifest 封存进 matrix root，resource/output artifact 只记录经逃逸检查的 root 相对路径；
native Job raw summary 同步封存 per-Job manifest 并相对化 shard locator；matrix index 使用 curated
schema，不透传 executor 的未知路径/异常字段。完整 root 归档搬迁后仍可离线复核。

`analysis/summarize_opening_short_job_interference.py` 对 exact-short 项目
full/half 控制、项目 short/long static/shared、Daft Native/Ray 与 Ray Data
single/two-job 原生观察做统一 fail-closed 汇总。它显式保留 request P99 仅项目可用、
native short 不足 60s 只作表征、interval MFU 无 counter 因而不可用等边界，并输出
项目 pre-long/overlap/drain 三段状态数据。它还从服务器逐请求 raw 分解
`arrival span + post-last-arrival drain = JCT`，汇总 buffer、flush→submit、
submit→service、service 与 request E2E 分位数；profiler 的 pipeline stage 字段可能
重叠，明确禁止相加。不绘图、不把 group throughput 当 short 专属吞吐。

`analysis/summarize_opening_fourjob_interference.py` 对
`short@0s → {long1,long2,long3}@5s` 补充矩阵做 fail-closed 汇总。它要求四个
manifest 全流程身份一致、每场景恰 3 次 formal、每 Job 512 条 exactly-once 且实际
发生 overlap；输出所有 Job 的 JCT/work rate/Project request P95-P99、相对各自
single-full slowdown、replay barrier lateness、arrival→first-submit 和
first-submit→completion 分解、三个 long 的离散度与完成顺序、组级公平性/资源，以及 Project
short-only/four-job-overlap/long-only-drain 三段状态。native request tail 因 barrier
timestamp 保持不可用；短 cell 只作 slowdown 基线，不作容量排名。
shared-credit 的阶段量先在同一采样时刻跨 endpoint 求和，再沿时间求均值，避免把
per-endpoint 平均误标为全局 active/waiting request/work 总量。

`analysis/summarize_project_short_all_at_t0.py` 审计同一 512-row short manifest 的
Project all-at-t0 1+3 raw，并冻结 T0 full-pipeline、T1 offered-work JCT、T2 framework
execute、T3 model-request window、T4 vLLM request mean 五层计时。它只在 T3/T4 下做
Project/Daft short 诊断；Daft 缺失的外层 T0 保持为空。Project-only eager 多 Job 仍使用
原 manifest，通过近零正数 arrival scale 压缩 DB source arrival；manifest 中的 arrival
字段不覆盖数据库列，不能把另存 manifest 误当成 offered-load 控制。

当前连接与测试流程集中在：

```text
code/scripts/profiling/postgres_ai_operator_profile.py
```

pgai SQL trigger-surface profile entry:

```text
code/scripts/profiling/pgai_sql_operator_profile.py
```

Daft text DataOrganizer smoke entry:

```text
code/scripts/profiling/daft_text_organizer_smoke.py
```

Shared-vLLM K_max interference runner:

```text
code/scripts/experiments/run_kmax_interference_experiment.py
```

Seeded scenario runner:

```text
code/scripts/experiments/run_ai_operator_scenarios.py
```

该 runner 在输出目录持有 `.runner-lease.json` 原子租约，禁止两个进程同时写
同一 manifest/CSV。中断恢复必须复用原配置和输出目录；只有确认旧 owner
已经消失后，才可同时传 `--resume --recover-stale-lease`。不要手工删除租约。

它既是当前 Phase 1 的实验驱动脚本，也是后续拆分正式 worker 之前的最小端到端实现。当前没有另一份隐藏的连接代码。

本目录只放实验主体、服务启动、数据采集和 profiling 入口。绘图、图表复现和素材筛选
脚本统一放在 `figures/scripts/`。

图像 CLIP 当前有三类不同入口，不能混读：

- `profiling/profile_image_clip_bottleneck.py`：历史 slow-pt 单进程阶段画像；
- `profiling/profile_clip_preproc_stages.py`：slow processor method-wrapper 诊断，未归因时间
  不能解释成具体转换步骤；
- `profiling/profile_image_clip_preprocess_variants.py`：当前 production-np、历史 legacy-pt、
  torchvision+PIL 和 torchvision+tensor-decode 的交错受控复测，经过同一
  `ClipTensorActor` 合同并做 embedding parity gate。它仍不是
  PG→Daft→Ray→pgvector E2E runner。

图像正式链路另有两个入口：

- `experiments/run_image_clip_e2e.py`：单个 vendor-native/diagnostic/project arm 的 operator-E2E、
  资源、正确性与 schema v12 原始记录；v12 在 unique/pass/processed rows 之外记录
  implementation provenance、scheduler owner 和 formal eligibility。Daft 内置
  `embed_image` 与 Ray Data native graph 可作 baseline；项目自写 Daft UDF formal
  默认拒绝，不能冒充官方实现；
  Daft built-in 只使用公开原生 `batch_size`，GPU 并发由 provider/Daft 推断，dtype
  记录为 `provider_default`，不会伪装成命令行 `--dtype` 已生效。正式跨系统排名必须传
  `--embedding-output-contract l2_normalized`；Daft 的 adapter-side L2 成本位于计时边界
  内，CSV 同时记录 requested/effective contract、归一化归属和是否计时；
- `data/build_image_multijob_manifest.py`：从 PostgreSQL 冻结 short + 3×matched-long
  图像 Job manifest，保存互斥 source range、doc-id digest、encoded-byte digest 与统一
  arrival offset；native/project 配置只引用该文件，不各自选数据；
- `experiments/run_image_native_multijob.py`：在同一外部 Ray 资源池上编排 Daft built-in
  和 Ray Data 的四个 single-full 与四个独立应用并发；只对齐 ready/start barrier，禁止
  project credit、active-work 和 router；`gate` 只跑两个四作业原生 arm 各一次，
  `run` 才执行 1+3 正式矩阵；
- `experiments/run_image_project_multijob.py`：在同一 immutable manifest 上运行四个
  single-full、frozen static partition 和稳定角色 `proposed`；算法变化只需更新
  `policy_revision` 并重跑 project，不改原生配置；`gate` 只跑 static/proposed 四作业
  各一次，`run` 才执行 1+3；
- `analysis/summarize_image_multijob.py`：fail-closed 检查 1+3 重复、manifest SHA、
  exactly-once 和 short/long 实际 overlap，输出逐 Job slowdown/阶段分解与组级资源；只做
  系统内 single→four-job 和 project static→proposed 对比，不作跨框架绝对排名；
- `data/prepare_vtc_compatible_workload.py`：按冻结 VTC suite 参数生成 seeded Poisson
  多 client 到达，用冻结 ShareGPT/SQuAD 的 256-token 邻近内容物化 append-only PostgreSQL workload，
  输出每 Job immutable manifest、行数与 SHA audit；明确不是 S-LoRA/VTC runtime 复现；
- `experiments/run_vtc_compatible.py`：从准备 audit 自动恢复逐 Job rows、首到达 offset 与
  manifest 环境，复用 shared-vLLM runner 执行同版本、可 resume 的正式矩阵；
- `analysis/summarize_vtc_compatible.py`：要求每个 isolated/static/shared-FIFO/shared-work
  场景 1 warm-up + 3 formal，检查异速 Job exactly-once/零失败，输出 actual-work、TTFT/P99/
  SLO、solo-normalized progress Jain、持续 backlog service disparity 与资源状态；
- `analysis/augment_image_observability.py`：不改 raw CSV，给历史 schema-v11 图像结果
  旁置补算 first-output/E2E 比例、post-first-output、60s duration gate、J/1K-images、
  GPU-seconds/image、images/CPU-core-second 与 host I/O bytes/image；它不补造 Daft
  内部 batch、逐图 latency 或 spill 数据；同时生成 `*.metrics.json`，逐字段记录中文
  含义、单位、公式、测量来源、比较范围和已知误差；
- `../configs/image_vendor_baselines.json`：固定 Daft 官方 image-classification
  benchmark 的 commit、入口 SHA256 与允许适配白名单；vendor-code parity 不通过
  项目 runner 重写其 batching、actor 或 backpressure；
- `experiments/run_image_clip_matrix.py`：读取 JSON 场景矩阵，用固定 seed 做 warmup + formal
  block 内交错，持有输出目录租约，并对 unique rows、exactly-once 与最小稳态时长
  fail closed。原始 CSV、逐 run manifest/stdout/stderr 和外层 schedule 必须保存在
  同一结果目录，不能只摘录汇总数字。

`data/import_coco_images.py` 同时支持 `--dir` 与 `--zip`；ZIP 模式直接顺序读取成员并在
单事务内写 PostgreSQL，不落地完整解压目录，适用于 COCO train 正式规模。
导入前强制表主键为 `(workload_name, doc_id)`；legacy 全局 `doc_id` 主键需先执行
`deploy/autodl/image_documents_workload_key.sql`，不能给某个 split 人工加 ID offset。

`profiling/profile_clip_transfer_ceiling.py` 是 H2D 机制诊断：R0 GPU-resident、R1 pinned
FP16、R2 pageable FP32 分别保存每个 batch/repeat 的 CUDA-event H2D、forward、
ownership copy 和同步 wall。它不含数据库/Daft/Ray queue，不能作为系统 E2E baseline。

`profiling/gate_vllm_clip_pooling.py` 是单图离线 CLIP pooling capability worker；
`profiling/run_vllm_clip_pooling_gate.py` 是跨 macOS/Linux 的超时监管器。监管器负责
不可覆盖的 stdout/stderr、退出码和 timeout 证据，worker 负责输入、输出维度/finite 与
版本门禁。两者都不是吞吐 benchmark，离线通过后仍需独立在线 API gate。

`analysis/compare_cost_estimators_contextloo.py` 对完整 decision context 做 leave-one-out，
将逐 fold 候选预测、宏平均/中位数/范围、pooled regret、repeat 合并后的候选 ranking、
源 CSV SHA256 和配置覆盖缺口写入 JSON。它验证的是 unseen-context generalization，
不能改称 unseen-config，也不能只抄控制台的 2.14% 均值。

`analysis/summarize_opening_database_e2e.py` 只汇总冻结的开题文本 database-E2E
矩阵。它要求 SQuAD 均匀控制组与 ShareGPT controlled-skew 各具备三条静态路径、
每条路径 1 次 warmup + 3 次 formal；任一单元状态、source/sink exactly-once、
workload identity 或 GPU feeding 门禁不一致都会写入 `audit.json` 并返回失败。
退出门禁只把 `project_frozen_static` 的 ≥95% direct service feeding 和 GPU mean ≥80%
作为项目有效性要求；产品 baseline 的 feeding 只报告、不反向调参，也不替项目门禁。
输出同时保留 raw rows/s、correct rows/s、service tokens/s 与 cap-semantic failure，
避免把产品语义不兼容误写成纯性能差异。该脚本不启动实验、不补跑缺失单元，也不
改变 runner 冻结合同。

`analysis/summarize_opening_project_feeding_calibration.py` 审计同 manifest 的 bounded
direct 三次重复与 project K32/64/128/256 三次重复，重新用服务端 token delta 与正式
`group_service_wall_s` 计算 direct group 吞吐，并用两 shard 最大 JCT 交叉检查 group join
开销，再逐格检查 exactly-once、行数、worker failure、终态空队列、
manifest SHA 和 resource metrics。只有全部门禁通过时，才冻结同时达到 direct 中位数 95%
与已测 project 峰值中位数 97% 的最小 K；否则显式输出 `audit_failed` 或
`active_work_scan_required`，不允许人工选点。选择文件同时从 vLLM estimated-FLOPs counter
恢复每个 repeat 的 BF16 MFU：direct 使用双 endpoint 聚合峰值，project 使用 profiler 的
per-GPU delta；峰值假设固定 4090 165 TFLOPS/GPU，MFU 只作资源证据、不替代 feeding 门。
若 primary root 含失败 cell，可用一个或多个 `--repair-root` 合并同配置 replacement；只有
成功 cell 总数对每组恰好等于预注册三次才通过，原失败记录、错误和路径仍保存在合同中。

`analysis/summarize_opening_multijob_minimal.py` 只汇总开题 short/long 两作业的
staggered 最小矩阵。它要求 static partition 与 shared DRR 两场景各 1 warm-up + 3 formal、资源与
MFU 状态完整、0 worker failure/incident、两个 job 使用各自 manifest-selected doc_id 集合、
zero source offset 和不同已验证 manifest SHA；runner 已逐 job验证 exactly-once
request/submission trace。输出
`formal_runs_compact.csv`、`scenario_summary.csv`、`pairwise_comparison.csv` 和
`audit.json`，比较相同 endpoint-shared K/work 上限下 static partition 与 shared DRR 的
吞吐、JCT、P99、SLO token goodput、Jain fairness 和 normalized service。该实验以完整
结果 gather 为终点，故意不含 sink；写回不属于多 job 调度因果变量。

`analysis/summarize_text_native_multijob.py` 审计 Daft Native、Daft Ray 与 Ray Data
三个 vendor-owned 两 Job 错峰观察臂各 1 warm-up + 3 formal。吞吐统一使用 vLLM
prompt+generation service-counter delta / arm barrier JCT；native 请求文件的 output-token
覆盖不一致，因此 runner 的 manifest-derived group token/s 明确排除出排名。汇总同时保留
short/long barrier JCT、实际重叠时长、running/waiting/KV、GPU 利用率、能耗与 MFU；原生
adapter 没有采集 request P95/P99，审计文件明确禁止从 barrier JCT 推断尾延迟。

`baselines/opening_database_e2e_matrix.py` 的替换正式模式允许 SQuAD 与 ShareGPT 分别
绑定上述校准 JSON 和选中的 project K；加载配置时会 fail-closed 核对 manifest SHA、
三次重复、双吞吐门槛、token budget、active work 与 actor slots。该 workload-specific
参数只作用于 `project_frozen_static`，direct/DuckDB 继续固定每 endpoint 32，避免把项目
校准误传到对照臂。
矩阵本身也必须由 `${VENV_ROOT}/text-baselines/bin/python` 启动，因为 DuckDB
community `ai` 扩展冻结在 DuckDB 1.5.4；base Python 中的 DuckDB 1.5.5 不属于该
baseline 运行合同。

## 流程与函数映射

```text
PostgreSQL documents/job table
  -> DataSource (arrow_postgres or daft_postgres)
  -> ArrowOrganizer / DaftOrganizer
  -> typed BatchRequest + endpoint topology
  -> SynchronousScheduler + RaySubmissionAdapter
  -> Ray task/actor -> model backend (fake, compatible_http, or ollama)
  -> sink.write / finish_job
  -> metrics append
```

| 环节 | 函数/对象 | 作用 |
|---|---|---|
| 数据库连接 | `connect` | 使用 psycopg 和 `--database-url` 建立连接 |
| 平台识别 | `database_metadata` | 读取真实 PG 和 pgvector 版本并写入 CSV |
| 建表 | `setup_schema` / `SCHEMA_SQL` | 创建 documents、jobs、embeddings、completions 表 |
| 任务触发替身 | `create_job` / `finish_job` / `fail_job` | 用 job table 模拟数据库 AI 算子触发，并记录成功或失败终态 |
| 数据读取 | `PostgresArrowSource` / `DaftPostgresSource` | 从 PG 基线路径或 Daft SQL 入口读取并返回 Arrow Table |
| 批划分 | `ArrowOrganizer` / `DaftOrganizer` | 按策略决定 actor 输入粒度；Daft 后端通过 `code/src/data/materializers/text.py` 接入 |
| AI 算子 | `FakeEmbeddingActor` / `CompatibleHTTPEmbeddingActor` / `FakeCompletionActor` / `CompatibleHTTPCompletionActor` / `OllamaCompletionActor` | `fake` 只用于离线 smoke 和控制变量；`compatible_http` 用于 vLLM-compatible embedding 或 completion endpoint；`ollama` 用于本地 Ollama `/api/generate` completion smoke |
| 并发与反压 | `submit_ray_tasks` / `submit_with_backpressure` → `SynchronousScheduler` | 静态 task/actor 路径统一执行 K_max、路由、等待和 fan-in；旧 queue-adaptive 分支暂时隔离保留 |
| 数据写回 | `code/src/data/sinks/postgres.py::write_embeddings` / `write_completions` | embedding 支持 `none`、JSON 文本和 pgvector；completion 支持 `none` 和 JSON 文本 |
| 指标输出 | `code/src/observability/metrics/::preflight_metrics_schema` / `append_metrics` | 正式工作前用 dry-run keys 拒绝旧 schema；追加时要求已有 header 与当前 row keys 精确一致 |
| 场景单写者 | `code/src/infrastructure/runner_lease.py::acquire_runner_lease` | 原子占用输出目录，校验 owner、进程启动身份与 config fingerprint，显式记录 stale recovery |
| completion 粒度 | `profiling.replay::_service_quantum_envelopes` | 在 planning batch 内按预测 work 切完整行，分别生成 HTTP/Ray completion 与 credit 释放单元；不拆单行 prompt |
| actor worker pool | `ActorWorkerPoolSubmitter` / `RaySubmissionAdapter` | 每个 endpoint 显式限制 worker slots，按 round-robin 或 least-active-work 分配，completion/failure 后由 canonical handle 精确释放 |

## 当前本地运行

```bash
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/ai_operator" \
.venv/bin/python code/scripts/profiling/postgres_ai_operator_profile.py \
  --setup --seed-rows 256 --total-rows 256 \
  --db-fetch-rows 128 --ray-batch-rows 64 \
  --model-workers 2 --max-inflight 4 \
  --strategy coalesced \
  --output feasibility/results/pg18_4_connection_smoke_256_rows.csv
```

`--submission-granularity service_quantum --service-quantum-tokens N` 同时
适用于 offline 与 arrival replay。planning batch 仍由 token-budget、
length-align 等组织策略决定；service quantum 只改变下游完成与补位粒度。
汇总 CSV 分开记录 organization batch 和 service quantum 的 count/rows/work，
submission trace schema 4 记录两级 ID、oversized 标记、credit-held 与
Ray-to-service 时间，避免把“更小 completion 单元”误写成“更好的数据组织”。

`--actor-workers-per-endpoint W --ray-actor-max-concurrency C` 的物理上限是
每 endpoint `W × C` 个 driver-owned slots；即使 `--max-inflight` 更大，
effective endpoint admission 也不会越过该上限。使用
`--actor-worker-routing least_active_work` 时，只在仍有空 slot 的 worker 中按
active work、running 数和稳定 worker index 选择。汇总里的
`actor_worker_slot_held_utilization` 包含 Ray/HTTP 等待时间，不是 GPU compute
utilization；submission trace 另记 worker ID/index/PID 供归因。

双 GPU 饱和后门禁使用两份隔离模板：

- `deploy/autodl/dual_gpu_actor_pool_shape.example.json` 固定每 endpoint
  256 slots 和 0.5 Ray CPU reservation，比
  1×256/2×128/4×64/8×32/16×16；按 97%-ceiling 选择最小 actor 数，
  16×16 只用于确认平台和方差；
- `deploy/autodl/dual_gpu_service_quantum.example.json` 固定所选 pool、active
  work 与 planning budget，比 batch、512/1024/2048/4096 quantum 和 request
  diagnostic。

不能把 actor 总 slots 随 arm 改变。当前 8192 quantum 大于已观测组织批次
最大 work（约 5892），不会切分任何批次，故不进入正式矩阵。

## 结果位置

- 原始数据：`feasibility/results/pg18_4_connection_smoke_256_rows.csv`
- 设置、过程、表核对、严谨性与结论：
  `feasibility/results/pg18_4_connection_validation.md`
- PostgreSQL 18.4 + pgvector 数据库部署：`deploy/postgres18.4/README.md`
- pgai SQL 算子触发面预演：`deploy/pgai/README.md`

## Daft text organizer smoke

`profiling/daft_text_organizer_smoke.py` is the smallest script-level entry for the
organizer abstraction in `code/src/data/materializers/text.py`. It does not connect to
PostgreSQL or vLLM; it verifies that text rows can pass through either
`ArrowOrganizer` or `DaftOrganizer` and return downstream Arrow batches. Use
`--runner ray` when checking Daft `into_partitions` or `repartition`;
NativeRunner reports these partition operations as no-op. The default output is
under `tmp/` because this is a local smoke result, not a formal experiment
result.

```powershell
.conda\pg-ai-profile\python.exe code\scripts\profiling\daft_text_organizer_smoke.py `
  --organizer arrow --rows 256 --batch-size 64 `
  --output tmp\daft_text_organizer_smoke.csv

.conda\pg-ai-profile\python.exe code\scripts\profiling\daft_text_organizer_smoke.py `
  --organizer daft --runner ray --rows 32 --batch-size 8 `
  --partition-mode into_partitions --partitions 4 `
  --output tmp\daft_text_organizer_smoke.csv
```

当前结果只证明 PostgreSQL 18.4 同构外部链路连通，不是已经验证 `REL_18_4` planner-visible
semantic operator，也不是性能优化结论。

## 正式对照实验

2026-07-11 已为 `profiling/postgres_ai_operator_profile.py` 增加可重复对照实验参数：

- `--executor python|ray_task|ray_actor`
- `--data-source arrow_postgres|daft_postgres`
- `--source-order doc_id|arrival_time`
- `--source-max-prompt-tokens`
- `--arrival-replay`
- `--arrival-time-scale`
- `--flush-policy immediate|fixed_timeout|queue_adaptive`
- `--flush-timeout-ms`
- `--flush-max-wait-ms`
- `--flush-trace-output`
- `--submission-trace-output`
- `--resource-trace-output`
- `--resource-sample-interval-s`
- `--model-flops-per-token`
- `--gpu-peak-tflops`
- `--mfu-precision`
- `--request-trace-output`
- `--request-slo-ms`
- `--scenario-id`
- `--random-seed`
- `--batching-policy fixed_rows|token_budget|best_fit_token_budget|length_align_fixed_rows|length_align_token_budget|prefix_aware_fixed_rows|prefix_aware_token_budget`
- `--token-budget`
- `--output-cost-mode prompt_only|fixed_output_cap|trace_target_output`
- `--cost-model-id`
- `--cost-tokenizer-id`
- `--scheduling-policy static|queue_adaptive|aimd|ewma_aimd|pid`
- `--adaptive-min-inflight`
- `--adaptive-max-inflight`
- `--adaptive-queue-threshold`
- `--adaptive-running-threshold`
- `--adaptive-kv-threshold`
- `--adaptive-poll-interval-s`
- `--controller-min-window` / `--controller-max-window`
- `--controller-initial-window`
- `--adaptive-sample-interval-s`
- `--ewma-alpha`
- `--pid-proportional-gain` / `--pid-integral-gain` / `--pid-derivative-gain`
- `--control-trace-output`
- `--endpoint-routing round_robin|least_queued|prefix_affinity`
- `--pool-routing none|request_cost`
- `--endpoint-pool-ids` / `--endpoint-gpu-ids`
- `--long-request-token-threshold`
- `--operator ai_embed|ai_complete`
- `--organizer arrow|daft`
- `--organizer-partition-mode none|into_partitions|repartition`
- `--organizer-partitions`
- `--daft-runner native|ray`
- `--model-backend fake|compatible_http|http_openai|ollama`
- `--embedding-endpoint-url`
- `--embedding-model`
- `--embedding-api-key`
- `--completion-endpoint-url`
- `--completion-model`
- `--completion-api-key`
- `--completion-max-tokens`
- `--completion-return-token-ids`
- `--completion-prompt-format raw|chatml`
- `--completion-temperature`
- `--model-metrics-url`
- `--writeback-mode none|json_text|pgvector`
- `--write-batch-rows`
- `--warmup-runs`
- `--repeats`
- `--run-phase warmup|formal`
- `--run-repeat-index`
- `--experiment-id`

运行级 CSV 现在直接记录 `tokens_per_s`，计算口径为 vLLM Prometheus 的
`(prompt_tokens_delta + generation_tokens_delta) / e2e_s`。该字段是服务端
实际 token 增量，不是 organizer 的 token cost 估计。

`queue_adaptive` flush 使用双窗口：`--flush-timeout-ms` 是低负载和指标
缺失时的 fixed-timeout fallback，`--flush-max-wait-ms` 是 waiting、KV 或
running 压力下的扩展窗口。running 压力阈值使用本次运行的
`--max-inflight`，不使用独立硬编码常量。窗口在 pending batch 打开时选择
一次，并写入 flush trace 的 `selected_wait_s` 和 `window_reason`。

`--source-order doc_id` is the offline throughput mode: PostgreSQL already
contains the workload rows, and the profile scans them in stable document-id
order before Daft organization. `--source-order arrival_time` reads rows by
`arrival_time_s NULLS LAST, doc_id`, but sorting alone is not arrival replay.
K_max experiments may use the sorted stream, while online flush experiments
must also pass `--arrival-replay`. Replay requires `daft_postgres` and a Ray
task/actor executor, preserves the observed inter-arrival gaps on a monotonic
clock, and rejects missing or decreasing arrival values.
`--arrival-time-scale` multiplies normalized replay offsets while leaving raw
database timestamps and flush timeouts unchanged. It defaults to `1.0`; values
below one are controlled trace acceleration and are recorded in every run.

The three runtime decisions are separate:

1. `--batching-policy` and `--token-budget` determine batch membership.
2. `--flush-policy` determines when a pending partial batch closes.
3. `--scheduling-policy` and its K_max/controller options govern closed-batch
   admission.

`--admission-scope global` preserves the historical meaning: one K_max is
shared by every endpoint. For a static multi-endpoint run,
`--admission-scope per_endpoint --max-inflight K` gives each endpoint an
independent K-credit cap and sets the scheduler-wide safety ceiling to
`K * endpoint_count`. The CSV records the configured K, per-endpoint cap, and
effective global ceiling separately. Per-endpoint scope is intentionally not
accepted for adaptive controllers yet: those controllers still maintain one
global window, so labelling them per-endpoint would be false.

`best_fit_token_budget` applies deterministic best-fit-decreasing packing to
complete rows visible to one organizer call. It is an offline organization
policy and is rejected with `--arrival-replay`, because replay must preserve
arrival order. `--output-cost-mode` controls only organization and scheduling
cost estimates; it never changes the backend `--completion-max-tokens` cap:

All token-budget policies enforce both `--token-budget` and
`--ray-batch-rows`. The latter is a hard per-submission row cap for sequential
and best-fit packing, so algorithm comparisons do not silently change maximum
request fan-out.

- `prompt_only` uses zero estimated output tokens
  (`output_cost_source=configured_zero`);
- `fixed_output_cap` uses the configured completion cap for every row
  (`output_cost_source=backend_completion_cap`);
- `trace_target_output` reads each row's `target_output_tokens` and caps the
  estimate at `completion_max_tokens`
  (`output_cost_source=burstgpt_unpaired_trace_metadata`).

The current trace targets are unpaired BurstGPT metadata, not oracle output
lengths for the configured prompt/model. Formal outputs therefore also record
`cost_model_id`, `cost_tokenizer_id`, `packing_scope`, the explicit packing
algorithm, budget utilization, oversized rows, and batch cost-unit
percentiles. A global-BFD claim is valid only when the full compared workload
is visible in one organizer call and `packing_scope=organizer_input`.

If `--flush-trace-output` is omitted, replay writes
`<output-stem>_flush_trace.csv` beside the main CSV. Queue-adaptive replay reads
vLLM metrics through a background sampler so metric I/O cannot block the hard
maximum wait.

`flush_trace_status` distinguishes an observed replay trace from a trace that
is not applicable. Offline non-replay execution has no arrival-driven flush
decision loop, so it records `not_applicable_non_replay`, an empty path, and
zero events. Zero here must not be interpreted as an observed count of flushes.

Formal runs should also set `--submission-trace-output` and
`--resource-trace-output`. The first records one row per closed batch with
an explicit `submission_id`, document identity, token counts, and service
timestamps (schema 2). The second samples
GPU utilization/memory and vLLM running/waiting/KV signals every 250 ms without
blocking the submission loop.

The main run row aggregates the resource trace into GPU utilization
mean/P50/P95/max, low-utilization time ratio, memory mean/max, vLLM
running/waiting/KV distributions, and (when `nvidia-smi` exposes
`power.draw`) power, integrated energy, and energy per 1,000 observed tokens.
`--resource-sample-interval-s` must remain identical across compared
scenarios.
For Ray endpoint experiments, `--endpoint-gpu-ids` also scopes `nvidia-smi`
sampling to the GPUs serving those endpoints. This prevents a single-endpoint
control on a multi-GPU host from averaging in an idle, out-of-scope device.

MFU is an explicitly labelled estimate, not a renamed GPU-utilization value.
It is left empty unless `--gpu-peak-tflops` and the matching
`--mfu-precision` are configured. The preferred numerator is the delta of
vLLM's `estimated_flops_per_gpu_total` counter. On vLLM 0.25.1 the service
must be started with `--enable-mfu-metrics`: the counter name can exist while
remaining permanently zero when the flag is absent, so a preflight must
verify a positive single-request delta rather than only metric presence.
Older vLLM versions may fall
back to `--model-flops-per-token` multiplied by observed prompt+generation
tokens. The time basis is `operator_wall_s`; output rows retain the selected
method, status, and all inputs for audit. The fallback scalar FLOP/token
estimate approximates prefill and decode jointly, so formal reports must
describe both paths as estimated MFU.

After repeated runs, generate plot-ready long-form statistics with:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\analysis\summarize_output_aware_bfd.py `
  --runs experiments\results\<experiment>\runs.csv `
  --output experiments\results\<experiment>\summary.csv
```

The summary includes row/token throughput, E2E/tail/SLO metrics, stage times,
batch and packing shape, GPU/memory, vLLM pressure/latency, energy, and MFU.
It excludes warm-ups and failed runs and reports `n`, mean, sample standard
deviation, P50, min, and max per scenario. Older CSVs remain readable: metrics
that were not recorded are emitted with `n=0` instead of rejecting the file.

`--request-trace-output` additionally writes one row per complete input prompt
on typed static/AIMD/EWMA/PID Ray paths. Arrival replay rows use
`request_time_origin=replayed_arrival`; offline organization rows use one
`offline_job_start` origin so their E2E includes source fetch, organization,
submission, and model completion. Each row records buffer/organization,
submit, service, completion, client E2E, endpoint/GPU identity, and optional
SLO status. A multi-prompt endpoint response exposes only submission-level
completion timing, so these rows use `latency_granularity=submission`; they
are client-observed per-prompt E2E values, not vLLM internal per-sequence
completion timestamps. Request trace schema version 3 records the time origin
and finish reason explicitly so offline and replay latency distributions
cannot be conflated.
Client lifecycle timestamps share one stable clock. Backend service epochs use
`service_clock_domain=backend`; when backend/client clocks cannot be ordered
reliably, `submit_to_service_s` is left empty instead of inventing queue time.

Aggregate compatible-endpoint token usage is never split into fabricated
per-request values. For vLLM, `--completion-return-token-ids` opts into genuine
per-choice token IDs and finish reasons; generic compatible endpoints keep the
extension disabled. `client_estimated_output_tokens` remains an explicitly
labelled whitespace-token estimate, while `actual_output_tokens` is populated
only when the backend supplies per-choice token IDs.
Replay timestamps use one epoch anchor plus monotonic elapsed time, so wall
clock adjustments cannot invert arrival and flush ordering.

## Seeded scenario runner

`experiments/run_ai_operator_scenarios.py` executes each profiler run in a separate
process. Warm-ups preserve configuration order; formal scenarios are shuffled
once per repeat with the recorded seed. Before every run, the runner requires
the model health endpoint to return HTTP 200 and the vLLM running/waiting
gauges to both equal zero. It stops at the first failed process or missing run
CSV row, and atomically updates `manifest.json` after every completed run.

`--resume` verifies that the config, seed, schedule, manifest, and successful
CSV rows still agree before skipping completed runs. A recovered failure remains
in the incident history. `--skip-failed-scenarios` is available only with
`--resume`; it records every omitted schedule item instead of fabricating a
successful CSV row.

The JSON configuration contains shared profiler arguments and scenario-specific
arguments. Output paths and run identity are owned by the runner and cannot be
overridden by the configuration. Persisted commands redact API credentials,
authentication tokens, secrets, passwords, and database URL passwords while
retaining performance controls such as token budgets.

Optional top-level `service_metadata` contains JSON scalar values such as the
vLLM version, prefix-cache state, and MFU-metrics state. It is persisted in the
redacted manifest and therefore participates in resume compatibility checks.
Secret-like metadata keys are redacted.

```powershell
.conda\pg-ai-profile\python.exe code\scripts\experiments\run_ai_operator_scenarios.py `
  --config experiments\results\request_lifecycle_gate_20260725\scenario_config.json `
  --profiler code\scripts\profiling\postgres_ai_operator_profile.py `
  --python-executable .conda\pg-ai-profile\python.exe `
  --output-dir experiments\results\request_lifecycle_gate_20260725 `
  --health-url http://localhost:8000/health `
  --metrics-url http://localhost:8000/metrics `
  --idle-timeout-s 60
```

Single-GPU smoke configuration:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\profiling\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --data-source daft_postgres --source-order arrival_time --arrival-replay `
  --arrival-time-scale 0.0005 `
  --executor ray_actor --operator ai_complete `
  --model-backend compatible_http `
  --completion-endpoint-url http://localhost:8000/v1/completions `
  --model-metrics-url http://localhost:8000/metrics `
  --batching-policy token_budget --token-budget 6144 `
  --scheduling-policy static --max-inflight 8 `
  --flush-policy fixed_timeout --flush-timeout-ms 25 `
  --warmup-runs 1 --repeats 1 `
  --output experiments\results\arrival_replay_smoke\runs.csv
```

Run the same smoke once for `immediate`, `fixed_timeout`, and
`queue_adaptive`. Do not start formal repeats unless the run CSV,
request/submission trace, flush trace, control/resource time series, and
manifest are all non-empty. Contract tests and dry-runs do not satisfy this
gate.

`--batching-policy fixed_rows` preserves the original row-count batching path.
`--batching-policy token_budget --token-budget N` greedily forms upstream
submission batches using `prompt_tokens + completion_max_tokens` as the
estimated model cost. This only changes the Ray/vLLM submission units; it does
not modify vLLM continuous batching or Ray's internal scheduler. CSV rows record
`batching_policy`, `token_budget`, and `model_request_timeout_s`.

Length- and prefix-aware variants reorder rows before creating the upstream
submission batches:

```text
length_align_fixed_rows
length_align_token_budget
prefix_aware_fixed_rows
prefix_aware_token_budget
```

The length-aware variants sort by `prompt_tokens`. The prefix-aware variants
sort by `prefix_key`, then `prompt_tokens`. CSV rows record
`organization_policy_family`, `batch_prompt_token_spread_mean`, and
`prefix_group_ratio`. These are organization signals only; prefix-cache benefit
still requires APC/cache metrics or a controlled prefix-share workload.

`--scheduling-policy static` uses the configured `--max-inflight` as a fixed
admission window through the typed scheduler and Ray adapter for both task and
actor execution. `--scheduling-policy queue_adaptive` currently follows the
isolated legacy branch and polls the vLLM metrics
endpoint and switches between `--adaptive-min-inflight` and
`--adaptive-max-inflight` according to queue/running/KV thresholds. CSV rows
record `adaptive_downshifts`, `adaptive_upshifts`, and
`adaptive_limit_mean`.

`aimd`, `ewma_aimd`, and `pid` use the typed dynamic admission gate. They
require a Ray executor and `--model-metrics-url`. Sampling is cached and does
not sleep in the submission loop. If `--control-trace-output` is omitted, the
trace is written beside the main CSV with `_control_trace.csv` appended to the
stem. UCB is not a CLI choice yet: its policy/reward core is tested, but formal
online use still requires reward-epoch aggregation and a static-K8 reward
baseline. The request E2E/SLO trace needed for that aggregation is now
available.

Actor pools and task endpoints share the same routing configuration. Pool and
GPU lists contain one value per actor/endpoint. `request_cost` routing requires
an explicitly resolved long-request threshold (the tuning-workload P75), which
is stored in the run CSV. Multiple logical endpoints on GPU `0` validate
routing behavior only, not multi-GPU scaling.

The service endpoint and Ray actor worker counts are separate. Each HTTP
endpoint may have multiple client actors, so the configured actor concurrency
ceiling is:

```text
endpoint_count * actor_workers_per_endpoint * ray_actor_max_concurrency
```

HTTP client actors/tasks use `ray_worker_num_cpus` and always record
`ray_worker_num_gpus=0`; GPU ownership stays with the external vLLM service.
Formal completion retries remain disabled (`max_retries=0`,
`max_restarts=0`, and `max_task_retries=0`) so a completed request is not
silently duplicated. Main CSV rows record `ray_version`,
`actor_workers_per_endpoint`, `ray_actor_max_concurrency`,
`ray_worker_num_cpus`, `ray_worker_num_gpus`, `endpoint_count`,
`actor_worker_count`, and semicolon-separated
`actor_worker_submission_counts`. Python executor rows use an empty
`ray_version`, `ray_actor_max_concurrency=0`, and
`ray_worker_num_cpus=0.0` as explicit non-applicable sentinels. Ray task rows
have no actor workers, record effective task-worker CPU, and also use
`ray_actor_max_concurrency=0` because that field describes actors only.
Internally, task definitions still resolve safe `RayWorkerOptions`. Fake Ray
task/actor definitions now receive the same CPU, zero-GPU, and disabled-retry
options, but remain debug backends rather than HTTP workers. Multi-GPU
performance testing remains pending and must use independent GPU-backed
service endpoints rather than multiple logical URLs or actors aimed at one
endpoint.

`append_metrics` writes a header for a new or empty CSV. Before appending to a
non-empty CSV it reads the existing header and requires an exact ordered match
with the current row keys. A stale/legacy schema raises `ValueError` before any
bytes are appended; use a new output file or explicitly migrate the old CSV.

`experiments/run_kmax_interference_experiment.py` is a small orchestration wrapper around
`profiling/postgres_ai_operator_profile.py`. It starts a background bulk `AI_COMPLETE`
job and then starts a foreground small job against the same vLLM endpoint. Use
it when testing the admission-control motivation for `K_max`: bounded
background inflight versus unbounded background inflight under shared service.
It supports static background `K_max` sweeps, the legacy `queue_adaptive`
admission baseline, typed AIMD, deterministic per-repeat scenario shuffling,
independent request/submission/resource/flush/control traces, and selectable
fixed or queue-adaptive flush. Its default outputs are:

```text
experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_small_20260726.csv
experiments/results/local_vllm_qwen15b_baseline/sharegpt_burstgpt_kmax_interference_bulk_20260726.csv
```

脚本现在会拆分 `db_fetch_s` 与 `arrow_build_s`，支持普通 Python baseline，并且只在
`--executor ray_actor` 或 `--executor ray_task` 时按需导入 Ray。2026-07-12 增加了
OpenAI-compatible endpoint 后端，用于后续连接本地 vLLM、Ray Serve 或其他
GPU-backed model service。推荐参数名是 `compatible_http`，旧的 `http_openai`
只作为兼容别名保留。`fake` 仍是默认值，只能作为脚本调试、PG18.4 同构预演或
历史对照，不能写成 GPU-backed 结论。`AI_COMPLETE` 当前使用
`--completion-endpoint-url` 指向 vLLM-compatible `/v1/completions`，并将 JSON 文本写回
`document_completions`；也支持 `--model-backend ollama` 连接本地 Ollama
`/api/generate` 做 completion smoke。它还不是 token-aware/prefix-aware 策略实现。

示例命令：

```powershell
.conda\pg-ai-profile\python.exe code\scripts\profiling\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup --seed-rows 4096 --total-rows 4096 `
  --db-fetch-rows 512 --ray-batch-rows 256 `
  --embedding-dim 128 --model-workers 2 --max-inflight 8 `
  --executor ray_actor --strategy coalesced `
  --warmup-runs 1 --repeats 3 `
  --experiment-id pg18_4_fake_4096 `
  --output motivation\results\pg18_4_fake\system_profile.csv
```

完整矩阵、CSV 位置与结果解释：

```text
motivation/results/pg18_4_fake/system_profile.md
```

GPU-backed embedding endpoint 配置检查示例：

```powershell
.conda\pg-ai-profile\python.exe code\scripts\profiling\postgres_ai_operator_profile.py `
  --dry-run `
  --executor ray_actor `
  --model-backend compatible_http `
  --embedding-endpoint-url http://localhost:8000/v1/embeddings `
  --embedding-model local-embedding `
  --experiment-id gpu_ai_embed_config_check `
  --output feasibility\results\gpu_ai_embed_config_dry_run.csv
```

AI_COMPLETE vLLM-compatible completion endpoint 配置检查示例：

```powershell
.conda\pg-ai-profile\python.exe code\scripts\profiling\postgres_ai_operator_profile.py `
  --dry-run `
  --operator ai_complete `
  --executor ray_actor `
  --model-backend compatible_http `
  --completion-endpoint-url http://localhost:8000/v1/completions `
  --completion-model local-llm `
  --completion-max-tokens 128 `
  --experiment-id ai_complete_config_check `
  --output feasibility\results\ai_complete_config_dry_run.csv
```

Local vLLM + Qwen2.5-1.5B startup on the current Windows/WSL Docker machine:

```powershell
docker run -d --name ai-operator-vllm-qwen --gpus all `
  -p 8000:8000 `
  --ipc=host `
  -e VLLM_WSL2_ENABLE_PIN_MEMORY=1 `
  -e VLLM_USE_V2_MODEL_RUNNER=0 `
  -v D:\Code\ai-operator-execution-optimization\models\Qwen2.5-1.5B-Instruct:/models/qwen:ro `
  vllm/vllm-openai:v0.25.1-cu129-ubuntu2404 `
  --model /models/qwen `
  --served-model-name qwen2.5-1.5b `
  --dtype auto `
  --max-model-len 2048 `
  --gpu-memory-utilization 0.75 `
  --enforce-eager
```

Minimal `AI_COMPLETE + Daft + vLLM` smoke:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\profiling\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup --seed-rows 4 --total-rows 2 `
  --db-fetch-rows 2 --ray-batch-rows 1 `
  --operator ai_complete `
  --executor python `
  --model-backend compatible_http `
  --completion-endpoint-url http://localhost:8000/v1/completions `
  --completion-model qwen2.5-1.5b `
  --completion-max-tokens 8 `
  --data-source daft_postgres --organizer daft `
  --writeback-mode json_text `
  --experiment-id vllm_local_qwen15b_daft_ai_complete_smoke `
  --output tmp\vllm_local_qwen15b_ai_complete_smoke.csv
```

Legacy 07-25..07-28 `AI_COMPLETE + Daft + Ray + vLLM` baseline workload (sharegpt_burstgpt, 1024 rows, doc_id 1000000). The CURRENT main workload is sharegpt_multiturn (2048 rows, doc_id 300000-302047, target_output 1-256) — substitute `--workload-name sharegpt_multiturn` / `--source-workload-name sharegpt_multiturn` (and the corresponding doc-id/row-count flags) in the commands below for new runs:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\data\import_ai_complete_workload.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --workload-name sharegpt_burstgpt `
  --start-doc-id 1000000 `
  --max-rows 1024 `
  --batch-rows 500 `
  --tokenizer-path models\Qwen2.5-1.5B-Instruct `
  --max-model-len 2048 `
  --completion-max-tokens 16
```

Then profile the imported workload:

```powershell
.conda\pg-ai-profile\python.exe code\scripts\profiling\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup `
  --total-rows 128 `
  --db-fetch-rows 128 --ray-batch-rows 8 `
  --operator ai_complete `
  --executor ray_task `
  --model-backend compatible_http `
  --completion-endpoint-url http://localhost:8000/v1/completions `
  --completion-model qwen2.5-1.5b `
  --completion-max-tokens 32 `
  --model-metrics-url http://localhost:8000/metrics `
  --source-workload-name sharegpt_burstgpt `
  --source-order doc_id `
  --data-source daft_postgres --organizer daft `
  --writeback-mode none `
  --experiment-id vllm_qwen15b_sharegpt_burstgpt_ray_task_batch_8 `
  --output experiments\results\local_vllm_qwen15b_baseline\sharegpt_burstgpt_ray_baseline.csv
```

AI_COMPLETE Ollama native completion smoke 示例：

```powershell
.conda\pg-ai-profile\python.exe code\scripts\profiling\postgres_ai_operator_profile.py `
  --database-url postgresql://postgres:postgres@localhost:5432/ai_operator `
  --setup --seed-rows 4 --total-rows 2 `
  --db-fetch-rows 2 --ray-batch-rows 1 `
  --operator ai_complete `
  --executor python `
  --model-backend ollama `
  --completion-endpoint-url http://localhost:11434 `
  --completion-model qwen2.5:1.5b `
  --completion-max-tokens 16 `
  --data-source daft_postgres --organizer daft `
  --writeback-mode json_text `
  --experiment-id ollama_daft_ai_complete_smoke `
  --output tmp\ollama_ai_complete_smoke.csv
```

正式 GPU-backed 结果应输出到：

```text
motivation/results/gpu/ai_embed_profile.csv
```

只有在 `--model-backend compatible_http` 连接到真实 GPU-backed endpoint 时，结果才可放入
`motivation/results/gpu/`。

本地真实模型 endpoint 可用 `services/local_embedding_server.py` 启动：

```powershell
$env:HF_HOME="D:\Code\ai-operator-execution-optimization\.cache\huggingface"
$env:HF_HUB_CACHE="D:\Code\ai-operator-execution-optimization\.cache\huggingface\hub"
$env:TRANSFORMERS_CACHE=$env:HF_HUB_CACHE
$env:TORCH_HOME="D:\Code\ai-operator-execution-optimization\.cache\torch"

.conda\pg-ai-profile\python.exe code\scripts\services\local_embedding_server.py `
  --model .cache\models\all-MiniLM-L6-v2 `
  --device cuda `
  --batch-size 64 `
  --port 8000
```

该服务提供 OpenAI-compatible `/v1/embeddings` 接口，供
`postgres_ai_operator_profile.py --model-backend compatible_http` 调用。
2026-07-12 的首轮 GPU-backed profile 中，该 endpoint 是用户手动启动的。

## 2026-07-14 GPU key rerun

Latest GPU-backed key rerun after pgai SQL trigger-surface validation:

```text
motivation/results/gpu/pgai_integrated_key_rerun_20260714.md
motivation/results/gpu/ai_embed_pgai_integrated_key_20260714.csv
```

This rerun uses `services/local_embedding_server.py` on ports 8000 and 8001 with
`--device cuda`. It keeps pgai SQL surface validation separate from the
job-table GPU timing profile.

## 2026-07-14 pgvector(384) writeback support

`postgres_ai_operator_profile.py --setup --embedding-dim 384` now creates
`document_embeddings.embedding_vector` as `vector(384)`. If an old
`embedding_vector` column has a different dimension, the script drops and
recreates that column only; it does not delete Docker volumes or the
documents/job tables.

Latest GPU-backed sink comparison:

```text
motivation/results/gpu/pgvector_writeback_20260714.md
motivation/results/gpu/ai_embed_pgvector_writeback_20260714.csv
```

## 2026-07-26 Workload materialization and cost estimation

`data/import_ai_complete_workload.py` can use a live vLLM-compatible `/tokenize`
endpoint when a local tokenizer checkout is unavailable. Controlled-prefix
materialization clones complete rows, chooses an exact nested subset
deterministically, preserves the original prompt suffix, and fails rather than
truncating a row that exceeds the model context.

For a disjoint held-out suffix, `--max-prompt-tokens` expresses a workload
eligibility boundary independently from `max_model_len`, and
`--source-row-offset N` skips `N` rows only after all ShareGPT/BurstGPT,
prompt-token and tokenizer/context filters. Never use a new `start_doc_id`
alone: that would relabel the first prompts instead of selecting new prompts.
The safe append contract is:

1. reuse the verified raw-file hashes, tokenizer and explicit workload filter;
2. set `--source-row-offset N --start-doc-id N`;
3. set `--verify-existing-prefix-rows N --append-only --dry-run`;
4. remove only `--dry-run` after every field of doc IDs `0..N-1` matches;
5. keep `--append-only`, so any conflicting new doc ID aborts instead of
   updating existing rows.

Prefix verification is read-only and returns `status=verified_dry_run`.
Without an exact match, the importer fails before the suffix is written.

`analysis/estimate_operator_cost.py` fits a grouped held-out cost model from one or more
profile CSVs. It uses only pre-execution features and writes the feature schema,
split groups, coefficients, normalization values, Q-error percentiles,
Spearman correlation, and plan-selection pick-rate/runtime/regret to JSON.
The current schema includes active-work/per-endpoint K, actor concurrency,
endpoint count, service quantum and per-GPU capacity. Decision-context identity
also includes database/runtime protocol plus normalized GPU model/memory, so a
new machine cannot silently join an old machine's context. Profiles from a new
machine still require their own calibration/holdout evidence; the identity guard
does not claim zero-shot cross-hardware generalization.

`analysis/summarize_formal_repeats.py` consumes a **new-schema** formal CSV and
adds sample standard deviation, coefficient of variation, Student-t 95% confidence
intervals, and optional paired performance-regression counts. Never append a new
run to an older profiler CSV after the observability schema changes; start a new
result directory so the fail-closed header check can protect the comparison.

`analysis/evaluate_embedding_retrieval.py` consumes a diagnostic `.npz` produced
by `run_image_clip_e2e.py --save-embeddings` plus an explicit
`query_id,relevant_id` CSV. It excludes self matches and reports Recall@K, MRR,
and nDCG@K. It is a quality gate, not a timed performance arm; checksum and norm
checks are not substitutes for retrieval relevance.

## 2026-07-29 Shared-vLLM multi-job runner

`experiments/run_shared_vllm_experiment.py` is the shared-endpoint multi-job group runner. Unlike
`experiments/run_ai_operator_scenarios.py`, one scheduled run contains multiple concurrent
profiler processes. It requires one explicit Ray address, gives every job an
  independent summary/request/submission trace, records group-level vLLM/resource
  metrics and MFU once, and uses one uniquely named Ray credit actor for
  `shared_drr`. A common replay epoch plus lateness/skew checks prevents startup
  jitter from becoming a hidden fairness variable. Durable per-group records
  rebuild the compact CSV on resume instead of appending duplicate rows.

Committed templates:

- `deploy/autodl/dual_gpu_shared_vllm_gate.example.json`
- `deploy/autodl/dual_gpu_shared_vllm_formal.example.json`
- `deploy/autodl/dual_gpu_shared_vllm_j4_gate.example.json`
- `deploy/autodl/dual_gpu_shared_vllm_j4_formal.example.json`

The AutoDL formal template runs 1/2/4 jobs after a separate four-job gate.
The former `ray_task` path expanded to more than 200
Ray workers and exhausted the container's `vm.max_map_count=65530`. Shared
multi-job templates now use one persistent async actor per endpoint per job;
the loader rejects an explicit four-or-more-job `ray_task` configuration before
any output directory or external request is created. The j4 gate must pass
before the 1/2/4 formal template or the j4-only isolation template is eligible.

The config must not contain `--setup`, reset, output/trace, Ray-address, or
credit flags. The runner owns them so concurrent jobs cannot race schema setup,
append to one CSV, or silently connect to different Ray clusters. Use the full
startup, gate, resume, evidence-preservation, and cleanup procedure in
`deploy/autodl/README.md`.

## 2026-07-29 同条件官方 baseline 入口

`baselines/run_official_baseline.py` 为同条件 baseline 提供薄执行入口。它不决定实验
矩阵，只执行已经固定的 manifest shard，并把不同实现统一到
`requests.csv + summary.json`：

- `bounded_http`：无 Daft/Ray 的强 AsyncIO 因果对照；其 httpx
  `max_connections/max_keepalive_connections` 显式等于全部 endpoint 的配置
  并发总量，禁止由客户端默认连接池暗中截断 C128/C256；fixed-output workload
  显式使用 `--ignore-eos`，多逻辑 Job 可通过共享 direct-client 控制合并各自的
  immutable arrival trace，但不得加入 per-job credit、fair queue 或项目 routing；
- `vllm_bench`：官方 serving ceiling，先保存详细原始结果，再显式归一化；
- `daft_native` / `daft_ray`：官方 `daft.functions.prompt()`；
- `ray_data_http`：官方 Ray Data HTTP Processor；
- `oceanbase`：仅在 OceanBase CE 能力门禁通过后启用。

所有 arm 使用一行一个完整 Chat Completions 请求、`temperature=0`、
相同模型和相同输出上限。两张卡先通过不可变 manifest 做
largest-work-first 固定分片；执行器不得自行重新洗牌。示例：

```bash
python code/scripts/baselines/run_official_baseline.py export-postgres-manifest \
  --database-url "$DATABASE_URL" \
  --workload-name "$SOURCE_WORKLOAD_NAME" \
  --row-count 64 \
  --row-offset 0 \
  --max-output-tokens 256 \
  --estimated-output-mode trace_target \
  --endpoint-count 2 \
  --output /absolute/path/to/official_baseline_gate_manifest.jsonl

python code/scripts/baselines/run_official_baseline_gate.py \
  --config deploy/autodl/dual_gpu_official_baseline_gate.example.json \
  --driver-python /absolute/path/to/driver-python \
  --vllm-python /absolute/path/to/vllm-python \
  --manifest /path/to/manifest.jsonl \
  --output-root /path/to/fresh-gate-output
```

校准时不得复制或临时改写远端 JSON。只选择已提交配置中的实验臂，并显式覆盖
各 arm 的 per-endpoint concurrency；例如在同一份 256 行 manifest 上运行
vLLM Bench 与 bounded HTTP 的 C64：

```bash
python code/scripts/baselines/run_official_baseline_gate.py \
  --config deploy/autodl/dual_gpu_official_baseline_gate.example.json \
  --driver-python /absolute/path/to/driver-python \
  --vllm-python /absolute/path/to/vllm-python \
  --manifest /path/to/immutable-256-row-manifest.jsonl \
  --rows-total 256 \
  --output-root /path/to/fresh-c64-output \
  --include-cell vllm_bench \
  --include-cell bounded_http \
  --concurrency-override vllm_bench=64 \
  --concurrency-override bounded_http=64
```

每个更高并发档必须使用新的输出根目录。未知 cell、重复或非正并发、以及对未选
cell 的覆盖都会在启动请求前失败；`resolved_config.json` 保存最终选择和有效
并发。C64/C128 是校准压力点，不是默认值，更不能据单次 gate 直接得出正式
性能结论。

`baselines/run_official_baseline_gate.py` 是可复现的双 endpoint core gate runner：
每个 cell 都先同时启动两个 shard，再等待二者完成；逐 endpoint 保存命令与
日志，轮询 vLLM queue 归零后才归一化和执行 gate。任一 shard、归一化或 gate
失败都立即停止后续 cell，写 `run_status.json` 并保留现场；输出根目录已存在
时拒绝运行。`project_profiler` cell 显式记录为 blocked，仍由现有 profiler
执行，不能被 core runner 中的近似实现替代。
vLLM Bench 必须从独立 vLLM venv 启动，并在该 venv 安装与服务完全同版本的
`vllm[bench]` extra；仅安装 serving 包会在 CustomDataset 读取阶段失败。

`run_official_baseline.py --dry-run` 仍用于单 shard 接口检查且不创建输出目录。
单 cell 校验由 `validate-gate` 合并两份 summary 和 request CSV；任一
exactly-once、预测 work 偏斜、endpoint 未使用、服务元数据不一致、worker
failure 或 vLLM 最终队列非空都会 fail closed。

`baselines/run_text_native_matrix.py` 只用于已有独立 calibration selection/
fingerprint 的原生 Chat 单 job 矩阵。它为每次 repeat 派生一份单 cell
core-gate 配置，执行 1 warmup + N 确定性交错 formal，保留失败和时长不足的
`not_rankable` 证据，并保存逐 GPU 资源 CSV 与 vLLM gauge/latency delta，后续
MFU 必须从 estimated-FLOPs delta、GPU 数和 service wall 按冻结口径计算。
`baselines/run_text_native_multijob.py` 只编排 Daft Native/
Ray 和 Ray Data 的两个错峰独立 job；每 job 同时启动两个现有
`run-shard` 子进程。它不实现框架调度、不注入项目 credit，只报
job/group barrier JCT、服务计数、vLLM running/waiting/KV/TTFT delta 与
逐 GPU 利用率/功耗时序；不把这些观测伪装成框架内部调度指标。配置中的
`process_timeout_s` 是两个 shard 共享的单一 wall deadline；超时后先 TERM、再 KILL
仍存活子进程并保存 `process_timed_out`，避免一个 CLOSE_WAIT shard 无限阻塞矩阵。

配置边界见：

- `deploy/autodl/dual_gpu_official_baseline_gate.example.json`
- `deploy/autodl/dual_gpu_official_baseline_calibration.example.json`
- `deploy/autodl/dual_gpu_same_condition_project_equivalence_gate.example.json`
- `experiments/plans/baseline_reference.md`
- `experiments/plans/completed/text_native_baseline_rerun_20260802.md`

模板是预注册规格，不是允许远端临时拼接 formal 命令的替代品。64 行 gate
通过前不得启动 calibration；calibration 通过前不得启动 2,048 held-out。

Project profiler 的 512 行 broad calibration 之前还有一道更窄门禁：
`static_k256` 与 `work98304_nonbinding` 各运行一次同压力 warm-up 和三次
formal repeat。actor-ready barrier 不计入 measured E2E，耗时写入
`actor_ready_s`；submission trace schema 5 记录 HTTP request、headers 和
body-read 边界。两臂 throughput/JCT 没有收敛到 5% 内时必须停止，不能以
单次最佳结果选择参数。

`analysis/select_strategy_calibration.py` 把通过门禁的 Completions feeding、
direct bounded gate、token-budget 和同协议 actor-shape formal CSV 合并为
`selection.json + calibration.env`。它按 95% feeding parity、
至少三次 formal repeat、97%-ceiling 和下一档增益小于 3% 的预注册规则冻结
token budget、per-endpoint K、active work；actor shape 在总 slots 固定时
选择达到峰值 97% 的最小 actor 数。后续
data-organization、submission-policy 和 shared-vLLM formal runner 会核对
该选择文件；旧 8K/K64、缺失 actor-pool 证据或环境漂移会在外部请求前失败。

`analysis/summarize_static_k_workload_surface.py` 读取
`dual_gpu_static_k_workload_surface.example.json` 的 formal CSV，先用
95% capacity floor 排除欠喂点，再按 SLO goodput（缺失时用 JCT）选择各
workload 的静态 K。只有最佳 K 至少迁移 2×或 97% 可接受集合不重叠、错配
损失至少 5%，且至少 2/3 paired repeats 同向时才输出 `passed`。
`--require-pass` 在不存在动态优化空间时返回 2，供远端 runner fail closed。

`analysis/summarize_static_credit_workload_surface.py` 用于 prompt 长度等 workload
变化下的 request/work credit 审计。输入为重复的
`--surface workload=/path/to/runs.csv`，统一输出 formal 中位数、均值、CV、
SLO goodput/JCT、observed/configured limit、无准入压力标志和交叉 regret。
如果候选臂 CV 超过 5%、未绑定等价臂相差超过 5%，或缺少 per-request output
token IDs，结果固定为 `inconclusive`，不能用算术平均表触发 adaptive
GO/NO-GO。07-30 short/long screening 正是因这些审计失败而被降级。

## 2026-08-05 SQuAD v1.1 dev capability gate（DuckDB-ai arm）

`baselines/squad_capability_gate.py` 是 bounded-output 主对比轨的 DuckDB-ai 单臂
capability gate：验证 DuckDB community `ai` 扩展在固定 cap 下能正确跑通 SQuAD 短答案
（EM/F1 可独立复算），**不是性能排名**（operator-only 计时边界，database-E2E runner
尚未实现）。修复过六轮 codex review 后的合同：

- `--mode {sampled,full}`：full 模式 fail-closed 校验 10570 行 + unique doc_id +
  unique source_example_id + 非空 reference_answers + canonical content hash 对齐
  importer provenance（两种模式都校验 workload 完整性）。
- sampled 模式用 largest-remainder 配额 + 多答案 max **SQuAD-normalized** 词数分桶 + 桶内均匀间距的
  确定性分层抽样；sample/workload hash 均为结构化 JSON-per-row SHA256（与 importer
  `compute_content_hash` 同定义，单测钉死一致），并写出包含 id/prompt/references 的
  `sample_manifest.jsonl`，使 sample hash 可离线复算。
- vLLM counter 归因门禁：endpoint 运行前/后必须 idle（running==waiting==0）、scrape
  非空、counter 单调、`request_success_delta == requests_sent`；任一不满足则 token/cache
  指标标记 `attribution=unavailable`（`--strict-attribution` 则整轮失败）。
- full-set exactly-once（result id set == input id set）、`output_chars`（字符数非 token）、
  失败结构化归档（`failure_report.json` + 非零退出）、命令与异常文本经
  `src/baselines/common/redact.py` 脱敏。

示例（服务器 text-baselines venv，单 endpoint idle 时跑）：

```bash
python code/scripts/baselines/squad_capability_gate.py \
  --database-url "$DATABASE_URL" \
  --workload-name squad_v11_dev_short_answer \
  --mode sampled --sample-count 256 \
  --importer-provenance feasibility/results/squad_v11_dev_import_20260805/provenance.json \
  --endpoint-url http://127.0.0.1:8000/v1/chat/completions \
  --metrics-url http://127.0.0.1:8000/metrics \
  --model qwen2.5-7b-instruct --max-tokens 64 \
  --service-prefix-caching enabled \
  --output-dir feasibility/results/squad_capability_256_v3_20260805 --force
```

输出：`report.json`（完整指标 + identity + 归因块）、`sample_manifest.jsonl`
（精确样本与 hash 复算输入）、`per_row_evidence.csv`
（source_example_id/status/error/output_chars/prediction/reference_answers，EM/F1 可复算）、
失败时附 `partial_results.csv` + `failure_report.json`。修改 stratified_sample /
integrity / attribution / redact 后运行
`python -m unittest tests.baselines.test_squad_capability_gate tests.baselines.common.test_redact`。

`baselines/squad_truncation_diagnostic.py` 是**单行定点截断诊断**（不是门禁、不进正式排名）：
对某个 `source_example_id` 的归档 prompt，在 direct vLLM 与 DuckDB `ai_try_complete` 两条路径上、
cap {64,128,256} × `--repeats` 次、cache/retry 关，重放并记录 `finish_reason`/completion_tokens/
`{response,error}`，并用 `ai_completion_request_json` 证明两路径请求体语义等价（direct 多显式
`stream=false`，非字节相同）。仅用于坐实/证伪某行的截断是稳定属性还是偶发；高 cap **绝不回灌**
正式 cap=64 门禁。示例：

```bash
python code/scripts/baselines/squad_truncation_diagnostic.py \
  --source-example-id 572700c8dd62a815002e976d \
  --database-url "$DATABASE_URL" --workload-name squad_v11_dev_short_answer \
  --endpoint-url http://127.0.0.1:8000/v1/chat/completions \
  --endpoint-base-url http://127.0.0.1:8000/v1 \
  --model qwen2.5-7b --caps 64,128,256 --repeats 3 \
  --output feasibility/results/squad_truncation_diag_<id>_<date>/diagnostic.json --force
```

`baselines/squad_database_e2e_runner.py` 是 SQuAD bounded-output 的 **database-E2E 顶层 runner**
（三臂均已实现：duckdb_ai/direct_client 进程内，project_static 经 profiler 子进程）。duckdb_ai/direct_client 把
operator-only 的 adapter 包进一个 E2E 计时墙：持久表扫描 → prompt 构造 → adapter 调用（DuckDB-ai 用
`run_duckdb_ai_complete`，direct_client 用 `code/src/baselines/text/products/direct_client.py` 的 `run_direct_client`；
operator-only 时间戳保留）→ 统一 sink（`write_completions` → `document_completions`，`json_text`）。
**`project_static` 臂结构不同**：runner 在通用 scan 前分流，子进程调用 `postgres_ai_operator_profile.py` 跑显式冻结的
静态合同（token budget / per-endpoint K / per-endpoint active-work / actor topology），profiler 独占 scan+organize+
model+sink；wrapper 读取 profiler 的 completion evidence 与实际 source-scan fingerprints，再由 runner 用独立 DB
完整性/评分读取核对 scan 身份与 sink，计时段来自 profiler `--output` CSV。
runner 层算主 headline `correct_rows_per_s`（= EM 行 ÷ `database_e2e_wall_s`）、`successful_rows_per_s`、`failure_rate`，
并对所有臂统一报 `truncation_count`/`truncation_rate`（`finish_reason=='length'` 或 error 含 `max_tokens`，arm-agnostic）；
状态字段解耦为 `single_run_valid` / `formal_run_gate_passed`（单次 runner 恒 false）/ `comparison_admission`；
进程内臂为 `pending_formal_repeat`，project_static 在统一计时墙完成前为 `blocked_unified_timing_boundary`。
这不削弱 zero-error validity。各臂共享 `--request-timeout-s`
（默认 120s），sink 失败或 readback 不匹配即 fail-closed。进程内臂不注入项目 credit/actor/backpressure；
project_static IS 项目调度（经 profiler）。冻结服务配置见
`deploy/autodl/single_endpoint_squad_database_e2e.example.json`（含支撑 `--service-config-hash` 的单 endpoint
vLLM 配置，REPLACE_ME 字段正式前填）。该 runner 只有一个 `--endpoint-url`；报告显式记录
`active_endpoint_count=1`、`multi_endpoint_method_exercised=false`。它不能作为双 GPU 项目方法证据。示例：

```bash
python code/scripts/baselines/squad_database_e2e_runner.py --arm duckdb_ai \
  --database-url "$DATABASE_URL" --workload-name squad_v11_dev_short_answer \
  --importer-provenance feasibility/results/squad_v11_dev_import_20260805/provenance.json \
  --endpoint-url http://127.0.0.1:8000/v1/chat/completions \
  --metrics-url http://127.0.0.1:8000/metrics \
  --model qwen2.5-7b --max-tokens 64 --max-concurrent-requests 32 \
  --service-prefix-caching enabled --service-config-hash <vllm_config_hash> \
  --metrics-settle-s 5 --strict-attribution \
  --writeback-mode json_text --write-batch-rows 500 \
  --output-dir feasibility/results/squad_database_e2e_duckdb_ai_REPLACE_ME --force
```

输出：`report.json`（E2E timing 块 + runner 指标 + identity + 3 状态字段）、`per_row_evidence.csv`
（含 `server_version`/`pgvector_version`，EM/F1 可复算）、失败时 `failure_report.json`。修改 runner
或 `_results_to_sink_payload`/`_runner_metrics` 后运行
`python -m unittest tests.baselines.test_squad_database_e2e_runner`。

## Project-derived phase-change state-aware experiment

`data/prepare_phase_change_workload.py` 从 SQuAD 短 prompt 与 ShareGPT 长 prompt
池构造 OFF-first 的两 Job 到达轨迹。它默认只写不可变文件合同，只有显式
`--apply` 才导入 PostgreSQL；不允许覆盖目录、重复源行、扩大 token 距离或复用
已有 workload。`experiments/run_phase_change.py` 在发请求前重新核对 canonical
manifest、SHA、导入回执、两 endpoint 与四阶段合同。

`analysis/audit_phase_change.py` 提供 `a-only`、`pressure`、`action`、`formal`
四个 fail-closed 审计模式。它不是通用 VTC 汇总器；这里只验证项目动态容量控制
是否在离线标定的上下界内形成合法双向闭环。完整顺序与硬停止条件见
`deploy/autodl/phase_change_state_aware_RUNBOOK.md`。

A-only 的 backlog 证据使用 request trace 中 replayed arrival 到 submit 的延迟，并要求
lower arm 持续占据；不能把 `organizer_queued_work` 当作 Daft source backlog——该字段
当前来自 shared-credit waiting work，单 Job 的等上限本地 admission 会先于它截流。
adaptive job 的本地 request/work 只作为安全 ceiling，固定为已标定候选的最大值；
实际在线容量仍由 shared coordinator 在候选臂之间调整。action/formal 审计还要求
每次上调后 2--20 s 窗口的 active-request P50 真正超过 lower K，避免把控制器动作计数
误当成有效扩容。

phase-change 独立确认曾在 tail drain 稳定复现 `httpx.ReadError`，同时 vLLM 健康且
服务日志均为 200。`CompatibleAsyncHTTPCompletionActor` 因此将客户端 idle keep-alive
expiry 默认设为 4 s，先于 Uvicorn/vLLM 常见的 5 s server expiry 淘汰连接；正式 SAOR
模板通过 `--completion-http-keepalive-expiry-s` 显式冻结并让 Ray actor 与 direct control
共用，实际值出现在 actor readiness、profiler summary 和 direct evidence。该修正不启用
重试。失败目录保留，修复后必须从新 output 运行，不能 resume 拼接。

`--arm project_static` 结构不同：runner 在通用 scan 前分流，子进程调用 `postgres_ai_operator_profile.py`
跑显式冻结的静态合同，profiler 独占 scan+organize+model+sink。wrapper 无连接——所有 per-doc 证据来自 profiler
输出文件（run-scoped completion-evidence CSV，`output_text` 从 in-process `operator_results` 展平，独立于
`document_completions` sink，因此 sink readback 是两个独立来源的核对，不是循环自证）。必须传冻结值
`--token-budget`/`--project-max-inflight`/`--project-max-active-work-per-endpoint`/
`--project-actor-workers`/`--project-ray-actor-max-concurrency`
（且 `actor_workers × ray_actor_max_concurrency >= max-inflight`，否则有效 K 会被静默夹到 slot 数——config
会 fail-closed）、带项目依赖的 `--project-python`、`--writeback-mode json_text`（database-E2E 合同要求 unified sink；
completion evidence 本身不依赖 sink）。请求语义被冻结为 raw chat / `temperature=0` /
`http_transport=httpx_async` / fixed-output-cap cost（默认 urllib 会破坏与 direct 臂的请求等价）/
`--service-prefix-caching`。**请求 manifest guard 是 2-endpoint
pinned-comparison 机制（要求 endpoint_count>=2），单 endpoint 臂不用**；workload 身份使用 profiler 从实际 Arrow
source scan 生成的 prompt fingerprints，再与独立 DB 完整性读取及 importer 结构 hash 核对。示例（smoke 用 `--limit`）：

```bash
python code/scripts/baselines/squad_database_e2e_runner.py --arm project_static \
  --database-url "$DATABASE_URL" --workload-name squad_v11_dev_short_answer \
  --importer-provenance feasibility/results/squad_v11_dev_import_20260805/provenance.json \
  --endpoint-url http://127.0.0.1:8000/v1/chat/completions \
  --metrics-url http://127.0.0.1:8000/metrics \
  --model qwen2.5-7b --max-tokens 64 \
  --token-budget <frozen-token-budget> --project-max-inflight 8 \
  --project-max-active-work-per-endpoint 65536 \
  --project-actor-workers 8 --project-ray-actor-max-concurrency 1 \
  --project-python /root/miniconda3/bin/python \
  --service-prefix-caching enabled --service-config-hash <vllm_config_hash> \
  --metrics-settle-s 5 --strict-attribution \
  --writeback-mode json_text --write-batch-rows 500 \
  --limit 256 \
  --output-dir feasibility/results/squad_database_e2e_project_static_smoke_REPLACE_ME --force
```

**当前禁止跨臂正式排名**：project_static 的 `database_e2e_wall_s` = profiler `e2e_s`，是比进程内臂
runner-measured wall **更宽**的边界（含 post-loop vLLM metrics scrape + trace CSV IO + finish_job，排除
actor-ready/Ray-init）。`scan_s`←`db_fetch_s`、`adapter_wall_s`←`operator_wall_s`（最紧的 adapter 等价段）、
`sink_s`←`writeback_s`；`construct_s` 由 Arrow build + organizer 合成；`wrapper_wall_s` 是完整子进程墙。
跨臂绝对 wall 比较需先实现统一边界；因此该臂当前 `comparison_admission=blocked_unified_timing_boundary`，只能做
可运行性/正确性门禁。`report.json` 的 `identity` 记录 `effective_k`（= `min(max-inflight,
actor_workers × concurrency)`，因 config 强制 `>= max-inflight` 故 == 声明 K）、`declared_max_inflight`、
actor 拓扑、active-work、`http_transport`、`temperature`。`_profiler_work/` 子目录存 profiler 的 request-trace +
completion-evidence + source-scan evidence + summary CSV。修改 `project_static.py` 的 argv 或 CSV 合并后运行
`python -m unittest tests.baselines.text.test_project_static tests.baselines.text.test_baseline_provenance tests.observability.test_completion_evidence_trace`。
