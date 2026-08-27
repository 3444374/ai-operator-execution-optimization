# 全网格扫掠计划：4 臂 × 规模 × 并发（full_grid_sweep_plan）

> **状态（2026-08-27）**：`paused / no-run-authorization`。十字切片和已有容量证据足以支撑
> 当前材料，完整矩形只是可选扩展；PostgreSQL 中立语义算子、provider 与 query lifecycle 资格项完成前不得启动。
> 本文件是历史 DESIGN/PLAN 文档，不包含实验结果。

---

## 0. 目标与非目标

**目标（FACT，来自任务上下文 + `experiments/AGENTS.md`“职责与边界”）**

构造并排序一个覆盖 **4 臂 × 9 规模 × 多级并发 × reps=3** 的扫掠网格，使以下问题在统一校准合同下可被归因回答：

- 在固定 vLLM 服务（同 model/protocol/cap/三 flag）下，4 条执行路径（bounded_http / duckdb_ai / lb_rr / project_static）的吞吐/延迟/stability 如何随**规模**与**并发**两个独立轴变化？
- 各臂的饱和点（吞吐见顶）与塌陷点（过载）分别落在哪个 (scale, C_total)？
- 在同 offered-load（per-backend ≈32）下，project 调度方法（actor pool + token-budget organizer + admission credit）相对 baseline 轨的稳态 ordering 是否稳定？

**非目标（`experiments/AGENTS.md`“报告与结论（结果边界）”）**

- **在 project_static 2-endpoint 挂死修复并通过 256 行验证门之前，任何含 project_static 的 cell 不计入主排名**（INFERENCE：当前 ramp 已据此把 project_static 排除；FACT，见 §1）。在此之前**不能声称"项目方法优于/劣于 baseline"**——这是 §1 的硬前置。
- **不能把两个维度同时上涨后的差异归因到任一策略**（本计划的归因纪律）。网格的物理笛卡尔积允许存在，但归因必须沿正交单变量切片读取。
- **不能把 lb_rr（gateway_system_diagnostic）与 bounded/duckdb 并入同一 "concurrency=64" 柱组排名**（INFERENCE，见 §3 可比性裁定）。lb_rr 是网关系统轨，formal_baseline_eligible=false，须分轨展示。
- **不能把 phase2_2048_tb 的 1-rep diagnostic 当 formal 证据**（FACT，见 §2 + §3 reps 纪律）。它只能 seed 候选 operating point。
- 正式材料不使用 RC1/RC2/BL1 等内部代号（根 `AGENTS.md`“文档受众与对外表达”）。

---

## 1. project_static 2-endpoint 挂死：根因、机制、修复、验证门

### 1.1 已验证的代码事实（FACT）

| 编号 | 事实 | 证据 |
|---|---|---|
| F1 | `wait_until_ready` 对**所有 endpoint pool 的全部 actor** 发射 `ready.remote()`，并在一个无 timeout 的 `ray.get(ready_refs)` 上阻塞 | `code/src/scheduling/runtime/ray_adapter.py:268-273`（已 Read 确认：`evidence = tuple(ray_module.get(ready_refs))`，无 `timeout=`） |
| F2 | `run_project_static` 的 `subprocess.run(cmd, capture_output=True, text=True)` **无 timeout** | `code/src/baselines/text/products/project_static.py:411`（已 Read 确认） |
| F3 | 对照：lb_rr 的 cell 执行 `subprocess.run(..., timeout=900)`——**有** timeout | `code/scripts/baselines/multicard_scale_ramp.py:413`（已 Read 确认） |
| F4 | `_ensure_ray_head`（commit 140eefd）已实现：清 stale `/tmp/ray/ray_current_cluster` 指针 + 起/复用干净 head + 校验无 leftover named actor / held resource | `code/scripts/baselines/multicard_scale_ramp.py:575-651`（已 Read 确认；docstring:583-584 明确记载「rm 指针后 2-endpoint project isolation rc=0 in 15s」） |

### 1.2 两层互补的根因叙事（ Investigators 之间的表面分歧实为分层 ）

两位 investigator 对"根因"的定位不同，但经验证是**互补的两层**而非矛盾：

- **触发层（TRIGGER，FACT that it is documented + addressed in code）**：ramp 共享 Ray head 上残留的 stale `/tmp/ray/ray_current_cluster` 指针指向死 GCS，使 project_static 的 `ray.init()`（无显式 address）读它后卡约 14 分钟。gate 臂不经 Ray 故不受影响——这正是"project 挂、gate 臂过"的已知症状。`_ensure_ray_head`（F4）专门修此层。（证据：`multicard_scale_ramp.py:580-584` docstring；`experiments/results/multicard_saturated_2048_20260806/README.md:64`）
- **症状层（SYMPTOM，FACT that the bounds are absent）**：无论触发是 stale 指针、runtime_env install stall、还是集群 CPU 不足以调度 16 个 actor，只要任一 actor 的 `ready()` 不返回，F1 的无界 `ray.get` + F2 的无界 `subprocess.run` 就会把该触发放大成**无限挂死**而非 fail-fast。lb_rr 因 F3 有 timeout 而能 fail-fast，故同一 ramp 上 lb_rr done 27/27、project 挂。

**综合置信度：medium。**（依据 HANG investigator 的 confidence=medium + 我对 F1/F2/F3/F4 的直接 Read 验证。）

**仍 UNCONFIRMED 的两点：**

1. commit 140eefd 的 `_ensure_ray_head` 是否**单独足以**修好 ramp 上下文里的 2-endpoint 挂死（COST investigator 视为"ALREADY IN CODE"；HANG investigator 认为还需 per-cell 重跑头部卫生检查 + 两层 timeout bound）。这是 **256 验证门要经验性回答的问题**，不能纸上裁定。
2. HANG investigator 主张的更深层机制——即"共享 head 上至少 1/16 actor 因 CPU 不足无法被调度（num_cpus 默认 0.25 → 16 actor = 4.0 CPU，cgroup cap 可能不足）"——**未经验证**。若 256 门挂死，F1 的 bounded `ray.wait` 诊断输出（含 `ray.cluster_resources()` / `ray.available_resources()`）将直接证实或证伪此假设，关闭 task #119。

### 1.3 提议修复（三层，按必要性排序）

**层 1（必要，与触发无关的防御性修复）**：把 `ray_adapter.py:273` 的无界 `ray.get(ready_refs)` 改为有界 `ray.wait(ready_refs, num_returns=len(ready_refs), timeout=READY_TIMEOUT_S)` 循环，超时（建议 90s）fail-closed，并在超时时输出未 ready 的 actor 数量+身份 + `ray.cluster_resources()` / `ray.available_resources()`。直接关闭 task #119（"capture profiler stderr to locate the deadlock"）。

**层 2（必要，达到与 lb_rr 的执行对等）**：给 `project_static.py:411` 的 `subprocess.run` 加 `timeout=`（复用 `idle_timeout_s` 或新增 `profiler_timeout_s`），在 `subprocess.TimeoutExpired` 时 kill 子进程并返回 `stderr_tail`，使该 cell 被记为 failed 而非挂死整个 ramp——与 lb_rr cell（F3）对等。

> 层 1+层 2 **无论触发是哪一种都必须做**（FACT，F1+F3 的不对称是结构性的）。它们把"挂死"降级为"可观测的失败"，是后续诊断的前提，不是可选优化。

**层 3（视 256 门结果决定）**：若 256 门仍挂死，修共享 head 的调用时机——在 `multicard_scale_ramp.py` 里把 `_ensure_ray_head` 的干净头部校验**移到每个 project_static cell 之前**（而非仅在 ramp 启动时调一次），或让 profiler 子进程像可工作的 saturated run 那样每次自起本地 cluster；并日志 `ray.cluster_resources()` 使低 CPU 计数可见。

### 1.4 256 行验证门（go/no-go）

**配置**：2×4090，两 endpoint 健康，干净 Ray head，scale=256，K=32（8 actor × 4 ray_concurrency），2 endpoint，reps=3，**层 1 + 层 2 已落地**（或先证 140eefd 单独够用，再决定是否必须层 1+2）。

**PASS 须全部满足**（HANG investigator validation_plan）：

- (a) 3/3 reps `status=passed`、`exit_code=0`；
- (b) profiler summary 含 `status==ok` 的 formal 行；
- (c) **GPU util 在 cell 内高于 0**（`project_resource.csv` per-sample）——直接证明 `wait_until_ready` 完成且请求到达 vLLM；
- (d) exactly-once 256 行、0 failed rows；
- (e) 两 endpoint 均被使用（各 backend `vllm_request_success_delta > 0`，round-robin）；
- (f) cell wall 显著 < 60s。

**附加隔离验证**：1 个 1-endpoint parity cell（同一 ramp driver，单 `endpoint_url`）也须 pass——隔离"共享 head wiring 是否真修好"，避免 2-endpoint 症状被掩盖。

**诊断附加**：任一 rep 触达 readiness barrier 时，bounded `wait_until_ready` 的输出（un-ready actor 数 + cluster/available resources）必须追加到 cell record，关闭 task #119。

**回归**：重跑已知良好的 saturated-2048 2-endpoint 配置，确认有界 barrier 未拖慢/破坏此前工作的 fresh-head 路径。

> 门通过前：project_static 整臂 BLOCKED（FACT，§0 非目标 + §2 missing_cells）。

---

## 2. 全网格矩阵

### 2.1 公共坐标轴

- **规模轴**（9 级）：{64, 128, 256, 512, 1024, 2048, 4096, 8192, 10570}（FACT，任务上下文）。
- **并发轴**：4 臂 JSON `concurrency` 字段语义**互不兼容**（FACT，`multicard_scale_ramp.py:74`），必须归一到 **C_total**（总在飞行）才能横比：

| 臂 | 字段语义 | C_total 映射 | 可扫 C_total 级别 | 硬约束 |
|---|---|---|---|---|
| bounded_http | per-endpoint c（2 shard 各打 1 endpoint） | C_total = 2c | {2,4,8,16,32,64,128} | c=64 (C_total=128) 已知塌陷（FACT，`phase2_2048_tb/ramp_run.json:106-116` shard rc=2；saturated ADDENDUM c≥64 曲线 100%→41%→28%） |
| duckdb_ai 〔harness_pre_split_diagnostic〕 | per-endpoint c | C_total = 2c | {2,4,8,16,32,64,128} | **UNCONFIRMED**：c64 cell summary 报 `duckdb_ai_max_concurrent_requests=32` 而非 64，疑似 adapter 钳到 32（必须 grid 前 fail-closed 核 `==c`）；scale≥8192 cap=64 失败（FACT，`ramp_gate/ramp_run.json:55-65,83-93` shard exit 2） |
| lb_rr 〔gateway_system_diagnostic〕 | 单进程 TOTAL（nginx 对半到 2 backend） | C_total = c (≈c/2 per backend) | {2,4,8,16,32,64,128} | 128→64/backend 塌陷风险；backend-skew ≤10% 硬门禁（`multicard_scale_ramp.py:449-456`） |
| project_static 〔project_scheduled_method，非 baseline〕 | per-endpoint K=max_inflight | C_total = 2K | {2,4,8,16,32,64}（**无 128**） | K ≤ actor_workers × ray_concurrency = 8×4 = 32 硬上限（`project_static.py:134-141,152-158`，slots<K 则 raise）；2-endpoint 路径挂死（§1） |

**INFERENCE（可比性裁定）**：bounded c=32×2 与 duckdb c=32×2 是 grid 里最干净可比的一对（同 gate 路径、同 per-shard 语义、同 manifest 预分）。lb_rr c=64 与 project K=32×2 只在 **offered load per-backend ≈32** 这一意义上匹配前两者，但调度机制根本不同，**不可并入同一柱组排名**，须按 `comparison_role` 分轨（§3）。

### 2.2 覆盖现状（FACT，据 GRID investigator）

| 来源 | 覆盖 | reps | 备注 |
|---|---|---|---|
| `phase2_2048_tb` | **scale=2048 这一行**：bounded c∈{1..64}(C_total 2..128) 7 格；duckdb c∈{1..64} 7 格全 pass；project K∈{1..32}(C_total 2..64) 6 格全 pass | 1（diagnostic） | = 20 cells (19 pass/1 fail)，无 lb_rr，无 TOST/CV（`phase2_2048_tb/README.md:1,36-44`） |
| 当前在跑的 3-path ramp | **C_total=64 这一列**：bounded/duckdb c=32×2 + lb_rr c=64，9 scales | 3 | lb_rr done 27/27；bounded/duckdb ~74%；project_static 排除（`multicard_lbrr_scale_ramp/actual_run_config.json:24` + 任务上下文） |
| 交叉格 (2048, C_total=64) | 两边都覆盖：bounded/duckdb/lb_rr reps=3（现 ramp）；project 仅 1 rep（phase2） | — | project 该格需 1rep→3rep 升级 |

**注意（FACT）**：已 commit 的 `lb_rr actual_run_config.json` 是更早的 reps=1 screening 跑（warmup_per_cell=false、无 vllm_config_strict）；reps=3 的正式 ramp 在跑但**尚未 commit/审计**。

### 2.3 真正缺失的格（对完整 2-D 网格）

1. **lb_rr 的整条并发曲线**（全 9 scales）：phase2 明确排除 lb_rr；现仅有 C_total=64 单点。lb_rr × {C_total=2,4,8,16,32,128} × 9 scales ≈ 54 cells 零数据。
2. **project_static 整臂（挂死后）**：仅 phase2 的 2048/1rep 曲线；非 2048 scales 全缺；全部 reps=3 formal 升级缺。**全部 BLOCKED 在 §1 修复**。
3. **bounded/duckdb 的离轴 (scale, concurrency) 大块矩形**：约 196–216 cells genuinely unmeasured。
4. **phase2 的 2048 并发切片的 reps=3 formal 升级**：当前仅 1-rep diagnostic，无 TOST/CV。
5. **逐格阻断**：duckdb × {8192,10570} cap-64 失败；bounded × C_total=128 塌陷；project × C_total=128 结构性不可达。

### 2.4 推荐范围（十字切片，非完整矩形）

**推荐**：两条过峰点的正交 1-D 切片（"十字"），**不要**完整 2-D 矩形。理由（INFERENCE）：

1. 饱和点是 vLLM 服务端常量（KV/调度容量），基本不随排队 workload 大小平移 → **交互项预期弱**，完整矩形 largely 冗余；
2. 两个主效应已被独立验证（phase2 并发曲线 + 现 ramp 规模平台），十字只需把它们补到 reps=3 + 补齐缺失臂；
3. duckdb 是 c 无关的（FACT，phase2 下 c1≈c64≈370 rows/s），扫 duckdb 全并发×规模 mostly 冗余，scale 才是它的有效轴。

| 切片 | 内容 | cells（reps=3） |
|---|---|---|
| **A：并发 @ 峰值 scale 2048**（全 4 臂） | bounded/duckdb/lb_rr ×7 C_total + project ×6 | 27 |
| **B：规模 @ 峰值并发 C_total=64**（全 4 臂） | 4×9 | 36 |
| 重叠 (2048, C64) | — | 4 |
| **并集（十字）** | | **59** |
| 扣除现 ramp 已在跑/已完成 27 cells | 真正新增 | ~33（切片 A 的 24 + 切片 B 的 9，后者全阻塞于 §1） |
| 可选切片 C（交互抽查） | bounded+project @ scale 8192 × 6 并发 | +12 → 合计 71 |

**并发分辨率裁定**：取**粗 7 级 {C_total=2,4,8,16,32,64,128}**（不要 64 个整数级——phase2 已证曲线对数平滑）；进一步建议落 **6 级 {2..64}**（去掉塌陷/不可达的 128）成干净矩形。

**总 cell 计数汇总**：
- 完整矩形 A（7 级，含 128）：243 cells × reps=3 = **729 formal + 243 warmup ≈ 972 executions**（FACT，COST: ~17–21h）
- 完整矩形 B（6 级，干净）：216 × 3 = **648 formal + 216 warmup ≈ 864 executions**
- **十字推荐**：**59 cells × reps=3 ≈ 236 executions**（~6× phase2）

---

## 3. 校准合同（frozen / swept / 可比性）

### 3.1 全 grid 冻结变量（FACT，据 CONTRACT investigator + code）

| 冻结项 | 值 | 证据 |
|---|---|---|
| MODEL+PROTOCOL+CAP | qwen2.5-7b / chat_completions / temperature=0 / max_tokens=cap=64 / fixed_output_cap / raw chat prompts | `duckdb_ai_c64 shard_0/summary.json:38`；同 model/protocol/service_config 三硬门禁 `multicard_scale_ramp.py:209-215` |
| vLLM 三 flag | `--max-num-seqs 256`、`--max-num-batched-tokens 8192`、`--enable-prefix-caching` (ON)，且 `vllm_config_strict=true`（缺 flag / prefix-cache 不可验证即 fail-closed，非 WARN） | `multicard_scale_ramp.py:543-572 _verify_vllm_config`；`lbrr config:13-15` |
| CACHE CONTROL | `warmup_per_cell=true`（每 measured cell 前跑 bounded_http @ warmup_c=32 暖该 cell manifest 的两 endpoint，进入同一 cache-hot 态） | `multicard_scale_ramp.py:681-738,766-769`；deploy §9.1 #4 |
| MANIFESTS | gate/project 臂用 2-shard `squad_dev_<N>.jsonl`（manifest 预分）；lb_rr 用 `endpoint_count=1 lbrr_dev_<N>.jsonl`（全行→LB→nginx 分）；同 scale 源行 SHA 一致；`endpoint_predicted_work_skew_max=0.02` | config |
| TOPOLOGY | 2 endpoint（8000/8001）+ Ray head 127.0.0.1:6380；project 走 `_ensure_ray_head` | `multicard_scale_ramp.py:575-651` |
| project actor 拓扑 | 8 actor_workers × 4 ray_concurrency = 32 slot（**禁止在 grid 内改**，否则 K 与 actor 同时变=违例第二变量） | `project_static.py:134-141,152-158` |
| REP STRUCTURE | 1 warmup + 3 formal interleaved，按 (scale,arm,concurrency,rep) key 独立归并；overall_status=passed 仅当 3/3 passed | `multicard_ramp_aggregate.py:339-370,379-388` |
| HARD GATES | provenance_fields_present、native_arms_have_no_project_scheduler、exactly_once、failed_rows=0、worker_failures=0、both_endpoints_used、service_counter_consistency、vllm_running_final=0、vllm_waiting_final=0、same_model/protocol/service_config；lb_rr 另有 backend request-skew 与 token-work-skew ≤10% | `multicard_scale_ramp.py:201-215,449-456` |

### 3.2 每臂 C_total=64 的并发语义 + 扫描映射（FACT，code 确认）

- **bounded_http**：`concurrency` = per-endpoint c；C_total=2c；C_total=64 → c=32/shard（`multicard_scale_ramp.py:199 _gate_config_for_cell`）。扫 c∈{1,2,4,8,16,32,64} → C_total∈{2..128}。
- **duckdb_ai**：同 gate 路径，per-endpoint c；C_total=2c。**必须核实** `duckdb_ai_max_concurrent_requests==c`（c64 summary 报 32 的疑点），否则 c>32 静默退化。
- **lb_rr**：`concurrency` = 单进程 TOTAL；C_total=c；C_total=64 → c=64 total ≈32/backend（`multicard_scale_ramp.py:372-374,404-407`）。扫 c∈{1,2,4,8,16,32,64} → 0.5..32/backend（要匹配 bounded c=32 须跑 lb_rr c=64）。
- **project_static**：`concurrency` = per-endpoint K=max_inflight；C_total=2K；C_total=64 → K=32；EFFECTIVE K = min(K, 8×4)=32（`project_static.py:134-141`）。扫 K∈{1,2,4,8,16,32} → C_total∈{2..64}；**K>32 需改 actor 拓扑=违例**，grid 禁止。

### 3.3 reps=3 + sample CV 纪律（FACT）

- 每 cell 1 warmup + 3 formal reps；mean 与 sample CV（statistics.stdev，n-1）跨 **passed** reps 计算（`multicard_ramp_aggregate.py:330-336 _mean_cv`）。
- n_passed<3 → overall_status=partial/failed，`failed_rep_errors` 入报告；仅 1 passed rep 时 CV 退化为 0（无信息，**须显式标注不参与排名**）。
- phase2 的 1-rep diagnostic：CV 未定义（n=1），只 seed，**不构成 formal 证据**；本 grid 的 reps=3 才是首次让 attribution 成立。
- 等价门禁（INFERENCE，项目 5% 门槛惯例 + deploy README §9.1）：跨 cell 比较要求两臂 repeat-mean 吞吐/JCT 在 5% 内且 ≥2/3 reps 同向方可称等价；CV>~5% 的 cell 标 unstable 不入主排名。每 cell 必报 mean + CV + n_passed/n + failed_rep_errors 四件套。

### 3.4 扫描 vs 固定配置（本计划归因纪律）

网格是 scale×concurrency 笛卡尔积，但**归因必须拆成两条正交单变量 sweep**：

- **(A) SCALE RAMP @ FROZEN concurrency C_total=64**（即现 ramp）：concurrency 冻结、scale 唯一移动 → 判 tokens/s/rows_s 是否 plateau、TTFT 是否随 scale 退化、3-arm ordering 是否稳定。按 §7.5「≥60 秒稳态」确认 plateau scale（预期落 2048+，因 64/128/256/512 operator wall 太短，2048 行 screening 才 ~5s）。
- **(B) CONCURRENCY SWEEP @ FROZEN scale=plateau**（phase2 扩展版）：scale 冻结、concurrency 唯一移动。

**对角 cell（两维都动）不可独立归因**，只当 consistency check。不得在两维都上涨的 cell 上声称「strategy X 在规模 Y 并发 Z 下更优」。

### 3.5 可比性警告（必须入报告）

1. **lb_rr 是 gateway 系统轨**（protocol §2.6；`comparison_role=gateway_system_diagnostic`，`formal_baseline_eligible=false`）：多一跳 nginx，单进程 C_total=64 调度动力学 ≠ 两独立进程各填 32；`scheduler_owner` 含 nginx_round_robin。**系统级结论 only，不得与 bounded/duckdb 同 bar 排名**。
2. **project_static 是 project_scheduled_method（非 baseline）**：K=32 是 actor-slot + token-budget organizer + per-endpoint active-work 65536 admission credit + request-level replenishment；有效在飞行取决于 flush/credit 而非裸 K。与 bounded/duckdb 的 semaphore cap 在数值相等时**机制不同**，offered-load 匹配但 scheduling 不可直接归因对比，**须分轨展示**。
3. **timing_granularity 不可混比（aggregator 硬强制）**：bounded/project=request（有 per-row E2E+TTFT）；duckdb/lb_rr=query_barrier（只有整条 SQL JCT，无 per-row 延迟/TTFT）。跨这两类的 request-E2E/TTFT 横比被 aggregator 禁止（query_barrier 臂 `request_e2e` 置 None，`multicard_ramp_aggregate.py:216-222,244-256`）。**只有来自 vLLM counter delta 的 tokens/s 与 rows/s 是四臂同口径可比**。
4. **bounded client 连接上限**：c=64 (C_total=128) FACT 已知坏（httpx 0.28.1 AsyncClient 默认连接上限 100）。grid 前必须确认 bounded client 显式设 `Limits.max_connections=max_keepalive_connections=c×endpoint_count`（deploy README §9.1 表），否则 c≈50 以上即崩。
5. **duckdb 有效并发钳制疑点**：必须强制每臂 `effective==configured`（project 已记 `effective_k`；duckdb 须核 `duckdb_ai_max_concurrent_requests==c`），不一致 fail-closed。
6. **manifest 不同分片**：lb_rr 用 endpoint_count=1 lbrr_dev（全行→LB→nginx 分），gate/project 用 2-endpoint squad_dev（manifest 预分）。源行 SHA 一致，但 per-backend prompt 分布对 lb_rr 是 nginx 控制非 manifest 控制——属臂语义固有（protocol §2.6），须记入 manifest 字段不假装等价。warmup 对 lb_rr 单 endpoint manifest 须用 endpoint_index=0 把全集暖两 backend（两 vLLM prefix cache 独立不共享，`multicard_scale_ramp.py:654-678`）。
7. **60 秒稳态门禁**：小规模（64/128/256/512）operator wall 太短只作 screening；formal plateau 归因必须用 `model_serving_wall≥60s` 的 scale。plateau scale 未确认前，concurrency sweep 的 scale 选择**不锁定**。
8. **GPU/拓扑须全 grid 冻结**：同一对 backend（8000/8001）、同 model、同 service config（三硬门禁已生效）、非 Blackwell 同型 GPU（deploy §11）。4-endpoint 变体（$PORTS=4）与本 grid 无关。每 CSV 记 server_version/pgvector_version。

---

## 4. 成本与排序

### 4.1 总量与 wall-clock（FACT + INFERENCE，COST investigator）

- **完整矩形 A（729 formal executions / 972 含 warmup）**：~17–21h（warmup on）；~17h（warmup off）。每臂（warmup on）：bounded 6.5h / duckdb 2.2h / lb_rr 7.7h（单 endpoint warmup = 2× bounded c32 wall）/ project 4.6h。
- **十字推荐（59 cells）**：~236 executions ≈ ~6× phase2 总量。
- **模型校准**：同模型复现现 ramp（lb_rr c64×9 + bounded/duckdb c32×9，reps=3，warmup）≈ 1.06h，与任务状态（lb_rr done 27/27、~74%）一致 → 模型可信（FACT 模型自校验）。

**每格 wall 基**（FACT）：per-cell wall = scale / rows_per_s（稳态），floor 0.8s，+ warmup + 10s inter-cell 开销，×3 reps。
- lb_rr per-scale wall：256=1.13s / 2048=5.94s / 4096=17.9s / 8192=49.5s / 10570=66.96s（`multicard_lbrr_scale_ramp/ramp_aggregate.md`；与任务提示 256~1.2s/10570~66s 吻合）。
- bounded/duckdb c32 wall：bounded 4096=20.4s/8192=46.1s/10570=60.8s，duckdb 4096=21.3s（`multicard_scale_ramp_20260806/raw/ramp_gate/ramp_aggregate.md`）。
- 并发 scaling：bounded rows/s c1=20.21..c32=413.04；duckdb flat ~370（c 无关）；project K1=19.38..K32=246.35（`phase2_2048_tb/ramp_aggregate.md`）。
- **INFERENCE**：小规模（≤1024）c32 吞吐、project 大规模吞吐、10s/cell 开销（project Ray restart 可能更高）。

### 4.2 并行性约束（FACT）

**无跨臂/跨 cell 并行**。两 GPU 被固定 vLLM 部署独占（8000@GPU0、8001@GPU1）；任一第二 client 流争抢同一 GPU 会污染 feeding-saturation 门禁（≥95% of bounded）和所有吞吐数（AGENTS 7.5C 硬违例）。lb_rr 是单进程穿一个 nginx LB（8500），project 需独占 Ray head → 臂间也不能共享服务。唯一并行是 **intra-cell by design**：gate cell 跑 2 shard（每 backend 一个），warmup 起并行 bounded_http shard（`multicard_scale_ramp.py:702-715`）。**其余全串行**。

### 4.3 排序（硬前置）

1. **修 project_static 挂死**（§1 层 1+2 必须；层 3 视 256 门）。注：`_ensure_ray_head`（140eefd）已在代码中（F4），但 F1/F2 的有界化**尚未在代码中**——这是新工作，非已存在。
2. **256 验证 project_static**（§1.4）：1 个 K32 @ scale-256 cell（reps=3）端到端证明修复，~1 min wall。**go/no-go 门**。
3. **补全 4 臂峰值并发规模 ramp**：现 ramp 已覆盖 subset-C 的 81/108（lb_rr c64×9 + bounded/duckdb c32×9，reps=3）；256 门过后**只缺 project_static K32×9×3 ≈ 27 cells (~0.32h)**。**这是 256 门后最廉价的真正下一步**，应在启动任何宽并发扫掠**之前**完成——产出一个干净 4 臂峰值并发规模 ramp，回答核心对比。
4. **十字切片 A（并发 @ 2048）+ B（规模 @ C64，project 部分）**：即 §2.4 的 ~33 个真正新增 cell。
5. 可选切片 C（交互抽查）。
6. 剩余全并发扫掠（若十字结论显示交互项强才启动，INFERENCE 预期弱）。

**cell 内顺序**：小规模先（廉价，暴露 per-arm 失败如 bounded c64 / duckdb 8192+）；同 scale 内各臂 back-to-back 跑，使 warmup 态一致。

### 4.4 最小可行子集（tiered，cheapest-first）

| Tier | cells | wall | 内容 |
|---|---|---|---|
| A | 24 | ~0.6h | 4 臂 × {2048 峰值, 10570 满规模} × peak-c × reps=3 → 回答"4 臂在 operating point 与满规模的排名" |
| B | 36 | ~1.0h | 加 8192 塌陷点 |
| C | 108 | ~1.4h | 4 臂 × 9 scales × peak-c（完整规模轴，无并发扫）；现 ramp 已覆盖 81/108，**仅缺 project K32×9×3 (27 cells, ~0.32h)** |

**推荐路径**（COST）：跑完现 ramp + 256 验证 + 加 project K32×9×3 (~0.3h) → 以 ~1.4h 总成本（多数已花）产出干净 4 臂峰值并发规模 ramp，回答核心对比。宽并发扫掠留给 1-2 个代表 scale（2048 峰值 + 8192 饱和）而非全 9。

---

## 5. 前置与风险

### 5.1 前置（按依赖序）

1. **挂死修复优先**（§1）。修复前任何含 project 的 cell 跑不了，也不计权重。
2. **vLLM config strict = ON**（formal 必备）：screening 跑曾声明 max_num_seqs/8192 但 cmdline 漏 flag → effective=vLLM defaults（`phase2_2048_tb/README.md §2`）；formal grid 不开 strict 则结果**不可比**。
3. **bounded client 连接上限显式设**（§3.5 #4）：否则扫到 c≈50+ 即崩。
4. **duckdb effective 并发核验**（§3.5 #5）：每 cell 核 `duckdb_ai_max_concurrent_requests==c`，否则 c>32 静默退化。
5. **manifest 生成 + SHA 固定**（§3.1）：不可中途换 manifest SHA/行数/model/protocol/cap。
6. **机器时间预算**：十字 ~6× phase2；完整矩形 729 executions ~17–21h 无人值守。

### 5.2 风险（COST investigator + 综合）

- **bounded_http c64 (C_total=128)** 在 phase2 FAILED（vLLM overload 塌陷，`phase2_2048_tb/ramp_run.json:106-116`）→ 各 scale 的 c64 cell 多半失败；预算为 fail 或直接去掉 bounded c64。
- **duckdb_ai 8192/10570** 在 gate ramp FAILED（shard exit 2，`ramp_gate/ramp_run.json:55-65,83-93`）→ 大规模 duckdb cell 在 root-cause 前不可靠。
- **duckdb 是 c 无关的** → 扫全 7 c 级 mostly 冗余；收 c32-only 省 ~1.5h。
- **lb_rr 单 endpoint warmup = 2× bounded c32 wall** 使 lb_rr 最贵（7.7h）；可对 lb_rr 设 `warmup_per_cell=false`（cell 通过 nginx round-robin 自暖两 backend）省 ~30%（注：与 §3.1 冻结 `warmup_per_cell=true` 冲突——**若采用须作为显式 documented 例外并重审 prefix-cache 等价性**，INFERENCE）。
- **低 c × 大规模 cell 病理慢**：bounded/lb_rr c1 @ 10570 ≈ 20 min/rep（1240s），dominate 全 grid 成本；cap 低 c 到小 scale 或 drop c1/c2 at 4096+。
- **project actor-slot cap=32 → 无 K64**；Ray head teardown/restart 在 project cell 间可能 >10s（或给 project 臂 +1–2h vs 10s 假设）。
- **21h 无人值守**：`ramp_run.json` 每 cell 原子写（`multicard_scale_ramp.py:741-754`）使部分结果在中止后存活，但 vLLM crash 会污染后续 cell；需周期监控（COST 提示 CronCreate watcher，对应 task #16）。
- **小规模（≤1024）吞吐是 INFERENCE**（未直接测）；sub-second wall 下 reps=3 CV 不可靠。
- **修复不确定性**：§1.2 的 UNCONFIRMED 两点——若 256 门揭示 140eefd 不足、或揭示 CPU 不足调度机制，则层 3 + 可能 actor num_cpus 调参是额外前置，会推迟网格启动。

---

## 6. 能回答 vs 不能回答

### 6.1 本网格（十字 + 后续可选宽扫）能回答

1. **4 路径在峰值 operating point（scale 2048 × C_total=64）与满规模（10570）的稳态排名**（tokens/s、rows/s、TTFT 分位 where granularity 允许）——Tier A ~0.6h 即得。
2. **规模轴上 4 臂的 plateau 形状与塌陷点**（bounded/lb_rr 大规模塌陷 scale；duckdb cap-64 失败 scale；project 是否随 scale 退化）——现 ramp + project K32×9×3 补全后即得。
3. **并发轴上（@ plateau scale 2048）4 臂的饱和点与过载塌陷**（bounded 见顶 C_total≈64；duckdb c 无关 plateau；project K 见顶；lb_rr 网关塌陷点）——切片 A。
4. **feeding-saturation 门禁**（每 cell `tokens_per_s ≥ 95% of bounded`）在各 (scale, concurrency) 是否成立——全 cell。
5. **交互项是否弱**（切片 C 抽查 scale 8192 的并发曲线 vs scale 2048 是否平移）——若平移则证明饱和点是服务端常量、完整矩形冗余（INFERENCE 预期）。
6. **project_static 相对 baseline 在同 offered-load 下的 ordering 是否稳定**——**仅在 §1 修复 + 256 门 PASS 后**；在此之前 project cell 不计主排名（§0 非目标）。

### 6.2 本网格**不能**回答（`experiments/AGENTS.md`“报告与结论（结果边界）”）

1. **"项目调度方法优于 baseline"的系统性主张**——project_static 是 project_scheduled_method（非 baseline，§3.5 #2），与 bounded/duckdb 机制不同；grid 给的是同 offered-load 下的 ordering，**不是 baseline-beats 声明**。须分轨展示。
2. **lb_rr vs bounded/duckdb 的同柱排名**——lb_rr 是 gateway 系统轨（§3.5 #1），不可并入同一 concurrency 柱组；只能给系统级容量/稳定性结论。
3. **per-row request-E2E / TTFT 的四臂横比**——timing_granularity 不兼容（§3.5 #3）；duckdb/lb_rr 只有 query_barrier JCT，跨类被 aggregator 禁止。**只有 tokens/s 与 rows/s 四臂同口径**。
4. **对角 cell（scale 与 concurrency 都动）的独立策略归因**——本计划禁止；对角 cell 只当 consistency check。
5. **>C_total=64 的 project 行为**——actor 拓扑硬上限 32/endpoint（§2.1），C_total=128 结构性不可达，不可外推。
6. ** plateau scale < 2048 的 formal 归因**——小规模 operator wall <60s（§3.5 #7），只作 screening。
7. **screening 跑（reps=1、无 warmup_per_cell、无 strict）与 formal 跑的合并结论**——合同不同不可合并；已 commit 的 lb_rr reps=1 config 与在跑 reps=3 ramp 须分别报告。
8. **修复前 project_static 的任何性能结论**——挂死下无数据（§1）。

---

*本计划是 DESIGN/PLAN 文档。执行前须确认：§1 修复落地、256 行验证满足预先条件、§3.1 的配置在运行期间保持不变、§5 前置条件满足。结论性声称须遵循 §6 与 `experiments/AGENTS.md`“报告与结论（结果边界）”。*
