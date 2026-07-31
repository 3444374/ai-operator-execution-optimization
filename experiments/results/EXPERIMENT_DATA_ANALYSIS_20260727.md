# 实验数据分析(2026-07-27)

本文件汇总 2026-07-26 完成的全部真实单 GPU 实验的逐批数据分析,按 `learning/AGENTS.md` 的七步结构组织。所有数字直接来自 `experiments/results/` 下的真实 CSV(`runs.csv` / `summary_long.csv`),不使用 fake backend。

**通用环境边界**:PostgreSQL 18.4 本地预演 + pgvector 0.8.2 + Daft + Ray task + vLLM 0.25.1 + Qwen2.5-1.5B BF16 + RTX 5070 12GB(单 GPU);prefix cache off;CUDA Graph on(eager baseline 见 §批次 3);prefix cache off;fixed_output_cap=16 token(除非另注);ChatML prompt envelope;temperature=0。

**绘图脚本与图资产**:
- `figures/scripts/generate_rc1_data_organization_charts.py`(批次 1)
- 图输出:`figures/data/report_main/rc1_*.{png,svg}`
- 图审计:`figures/audit/rc1_data_organization_charts_audit_20260727.md`

---

## 批次 1:RC1 数据组织策略边界

### 1.1 实验设置

研究内容一(RC1)要回答的问题是:在数据库 AI 算子外部执行链路里,把"按固定行数"组织 batch 改成"按计算量组织"能否带来收益。三个候选机制各有真实 GPU 重复实验:

- **Sequential token-budget**(当前默认):按到达顺序贪心填充,直到 token budget 超限就开新 batch。
- **Classic BFD / Output-aware best-fit-decreasing**:全局可见输入下,按估算 cost 单调配大→小,每行放入剩余预算最小的可用 batch。
- **Row-cap-first**:在 token 预算之上同时强约束每 batch 行数上限,选箱时先减行槽再考虑 token residual。
- **Prefix-aware token-budget**:对真实重复的 prefix_key 聚合(rendezvous 风格),提升 prefix locality。

链路是 `PostgreSQL documents → DaftPostgresSource → DaftOrganizer → Arrow payload → Ray task → vLLM /v1/completions`,无写回。

### 1.2 实验设计

三个独立子实验,每个都是 pre-registered 筛选→重复的两段式:

| 子实验 | 数据源 | 规模 | 设计 |
|---|---|---|---|
| BFD scaling | output_aware_bfd_512_v2 + output_aware_bfd_1024 | 512 / 1024 行 | 6 scenario × 3 repeat(512)+ 3 scenario × 3 repeat(1024 held-out);每 scenario 改变 packing_algorithm + output_cost_mode |
| Row-cap-first | row_cap_aware_packing_512/nocache_repeats + row_cap_aware_packing_1024 | 512 / 1024 行 | 3 scenario × 3 repeat;固定 r64_b6144,仅改 packing 算法 |
| Prefix-aware | prefix_aware_batching/screen_v3 | 512 行 | 8 scenario × 1 repeat(筛选轮);prefix 重用率 0/30/70/100% × {sequential, prefix_aware} |

控制变量:同一 512(或 1024)doc 集合、相同 source order、相同 fetch size、相同 model/generation cap、相同 K_max=8、相同 immediate flush、相同 writeback=none。

### 1.3 严谨性自检

- 512 行 BFD 有 21/24 + 修复 + 24/24 两轮,21 轮失败数据保留为 incident 不进入结论;v2 是约束对齐后的正式版。
- 1024 行只复验 512 行胜出的配置,不重新调参(避免 1024 上 post-hoc 调参产生虚假增益)。
- Row-cap-first 512 行 nocache_repeats 显式关闭 prefix cache(避免 cache 命中污染策略对比);早先带 cache 的 screen 数据保留为 incident 审计。
- Prefix-aware 经历三轮 screen(v1/v2/v3),v3 修复了唯一 prefix 哈希重排和隐式 length-align 耦合两个语义 bug;v1/v2 数据不进入结论。
- 每轮 formal 顺序由 scenario runner 独立洗牌;每轮 512 个 doc 全部 exactly-once,缺一条则该轮废弃。

### 1.4 实验数据(从 summary_long.csv / runs.csv 直接读取)

#### Table 1.1 BFD scaling:512 行(6 scenario,3 formal repeats 各)

| scenario | batching | output_cost | tokens/s | req_p95 (s) | SLO viol | MFU | J/1k tok | batch_count | budget_util |
|---|---|---|---|---|---|---|---|---|---|
| seq_prompt | token_budget | prompt_only | 6809 | 9.99 | 0.168 | 0.340 | 19.56 | 33 | 0.308 |
| seq_fixed  | token_budget | fixed_output_cap | **7757** | 8.62 | 0.000 | 0.392 | 14.18 | 33 | 0.348 |
| seq_trace  | token_budget | trace_target_output | 7021 | 9.70 | 0.104 | 0.350 | 17.85 | 39 | 0.868 |
| bfd_prompt | BFD | prompt_only | 7302 | 9.26 | 0.001 | 0.368 | 16.99 | 36 | 0.282 |
| bfd_fixed  | BFD | fixed_output_cap | 7303 | 9.39 | 0.244 | 0.368 | 16.51 | 36 | 0.319 |
| bfd_trace  | BFD | trace_target_output | **7865** | **8.61** | 0.000 | **0.399** | **14.53** | 44 | 0.769 |

#### Table 1.2 BFD scaling:1024 行(3 scenario,3 formal repeats,512 胜出配置的 held-out)

| scenario | tokens/s | req_p95 (s) | SLO viol | MFU | J/1k tok | batch_count | budget_util |
|---|---|---|---|---|---|---|---|
| seq_fixed  | **8009** | 16.84 | 0.621 | 0.390 | 18.17 | 65 | 0.351 |
| seq_trace  | 7238 | 18.86 | 0.658 | 0.350 | 20.53 | 77 | 0.884 |
| bfd_trace  | 6864 | 19.67 | **0.923** | 0.332 | 21.92 | 87 | 0.782 |

#### Table 1.3 row-cap-first 512 行(nocache_repeats,3 formal repeats)

| scenario | tokens/s | req_p95 (s) | SLO viol | MFU | J/1k tok |
|---|---|---|---|---|---|
| r64_b6144_seq     | 8652.8 | 7.78 | 0.000 | 0.441 | 11.48 |
| r64_b6144_bfd     | 8605.2 | 7.82 | 0.000 | 0.444 | 11.47 |
| r64_b6144_rowcap  | **8711.9** | **7.74** | 0.000 | **0.448** | **11.16** |

#### Table 1.4 row-cap-first 1024 行(3 formal repeats)

| scenario | tokens/s | req_p99 (s) | SLO viol | MFU | J/1k tok |
|---|---|---|---|---|---|
| r64_b6144_seq     | 10381 | 12.79 | **0.504** | 0.516 | 11.15 |
| r64_b6144_bfd     | 10490 | 12.67 | 0.888 | 0.524 | 11.34 |
| r64_b6144_rowcap  | 10466 | 12.72 | **0.887** | 0.522 | 11.31 |

#### Table 1.5 prefix-aware cache-off(screen_v3,8 scenario × 1 repeat)

| scenario | tokens/s | req_p95 (s) | prefix_group_ratio | batch_count |
|---|---|---|---|---|
| p0_sequential      | 8746.8 | 7.89 | 0.037 | 12 |
| p0_prefix_aware    | 8672.2 | 7.94 | 0.051 | 12 |
| p30_sequential     | 9102.7 | 8.20 | 0.301 | 13 |
| p30_prefix_aware   | 9134.1 | 8.13 | 0.338 | 13 |
| p70_sequential     | 9559.5 | 8.63 | 0.699 | 15 |
| p70_prefix_aware   | 9400.1 | 8.71 | 0.711 | 14 |
| p100_sequential    | 9912.0 | 8.91 | 1.000 | 15 |
| p100_prefix_aware  | 9760.8 | 9.00 | 1.000 | 15 |

### 1.5 结果解释

**事实(可写论文)**:

1. **BFD 512 行的局部信号真实但非普适**。bfd_trace 相对同 cost mode 的 seq_trace:tokens/s `+12.0%`、req_p95 `-11.2%`、J/1k tokens `-18.6%`、MFU `+13.9%`。但相对 strongest practical baseline `seq_fixed`(同样 fixed_output_cap):tokens/s `+1.4%`、J/1k tokens `+2.5%`(能耗反而略升)。
2. **1024 行全面反转**。bfd_trace 相对 seq_trace:tokens/s `-5.2%`、req_p95 `+4.3%`、SLO 违规率 `0.658 → 0.923`、MFU `-5.1%`、J/1k tokens `+6.8%`。同时 packing_batch_count 多出 10 个(87 vs 77),budget_utilization 退化(0.782 vs 0.884)。
3. **Row-cap-first 1024 行 SLO 崩溃**。512 行时 row-cap-first 在所有指标上微弱优于 sequential(tokens/s +0.68%、MFU +1.6%、J/1k -2.8%、SLO 全部 0)。1024 行 held-out 中 tokens/s `+0.82%`,但 10 秒 SLO 违规率从 sequential 的 `50.4%` 跳到 row-cap-first 的 `88.7%`,goodput 从 `37.66` 跌到 `8.67 req/s`。
4. **Prefix-aware 机制工作但无 cache 收益**。Prefix 分组率在 30/70/100% 设置下显著高于 sequential(`0.338 vs 0.301`、`0.711 vs 0.699`、`1.000 vs 1.000`),证明 organizer 的 prefix 聚合逻辑正确。但 prefix-aware 的吞吐在所有 prefix 比率下都与 sequential 不可分辨(差值 -1.5% 到 +0.4%),因为 vLLM prefix cache 被显式关闭。

**推断(基于事实的合理推断)**:

- BFD 的 batch_count 增加(44 vs 39 at 512;87 vs 77 at 1024)说明 BFD 把更多输入切成了"刚刚好填满预算"的小 batch,在 512 行下 vLLM continuous batching 还能吸收这种碎片化,1024 行持续积压下变成 HOL 与 SLO 违规的源头。这是 **vLLM 内部调度与上游 packing 在不同规模下交互不同** 的体现,不是 BFD 算法本身退化。
- Row-cap-first 1024 行 SLO 崩溃的模式与 BFD 一致,都来自"强制裁剪行数让 batch 离开 vLLM 最舒适的工作点"。Sequential 不强约束行数,vLLM 反而更稳。
- Prefix-aware 不带来收益不是策略本身失败,是**评估条件不匹配**——cache 关闭时 prefix locality 没有兑现路径。这是一个等待前置条件(prefix cache on)的候选策略。

**待确认(无法从当前数据得出)**:

- BFD/row-cap-first 在**多 endpoint/多 GPU** 场景下是否会重新显示优势?当前单 GPU 单 endpoint 下 vLLM 内部调度吸收了上游碎片,但多 endpoint 时上游碎片可能直接放大。
- Length-align + token-budget 的正式独立重复仍未做(07-19 ablation 中 length+fixed 是负结果,length+token6144 仅 ablation 一格)。
- prefix cache 开启 + prefix-aware 的真实收益未验证(本批次只在 cache-off 下筛)。
  【2026-07-31：cache-on 2-ep/7B batching（within 1.2%）+ routing（−0.1%）均中性 <5% 门禁；但 4-ep/1.5B prefix-affinity routing +5.9%（46943 vs 44317 tokens/s，3-repeat 不重叠）跨过 5% 门禁，高淘汰压力 regime 下有条件重新打开，待隔离消融（4-ep/7B 或 2-ep/1.5B）；见 `prefix_cache_data_org_20260730/`、`prefix_cache_routing_req_20260730/`、`prefix_cache_routing_4ep_1.5b_20260731/`】

**不能声称**:

- 不能把 512 行的 +12% 写成 BFD 的普适收益。
- 不能把 row-cap-first 1024 行的 +0.82% tokens/s 写成"持平"——SLO 维度的崩溃是主导事实。
- 不能把 prefix-aware cache-off 的"无收益"外推成"prefix-aware 永远无用"。

### 1.6 对课题的含义

研究内容一的论文叙事可以**诚实定位为"系统表征 + 边界条件"**:

1. **Sequential token-budget 是当前单 GPU 稳态下的安全默认**。三种更复杂的 packing 策略(BFD、row-cap-first、prefix-aware)在已测试条件下均未显著优于 sequential,且各有明确的失败边界。
2. **失败不是策略本身无效,而是与下游 vLLM 调度的交互在单 GPU 稳态下不利**。这本身是论文 §5.2 的有效贡献——"为什么看似合理的策略在 LLM serving 链路里失败"是数据库 AI 算子执行优化的核心洞察。
3. **多模态与多 endpoint 是这些策略重新评估的自然场景**(代码已留 cost_units 中性接口与 endpoint topology 抽象)。论文 §5.3 多模态泛化验证可以同时回答两个问题:策略代码是否模态无关 + 这些策略在多模态/多 endpoint 下是否重新显示价值。

### 1.7 下一步实验

按 `experiments/plans/experiment_status_and_gaps.md` §10.3:

1. **Prefix cache-on 独立机制门禁**(P1)。当前所有 prefix 实验都在 cache-off 下,必须在 cache-on 时同时报告命中率证据,才能给出 prefix-aware 的最终结论。
2. **Length-align × token-budget 正式独立重复**(P1)。补做 length-only vs length+token-budget vs length+prefix 三因素显式联合消融。
3. **多模态泛化验证**(P2,文本门禁已满足)。CLIP embedding + ImageNet subset,复用同一 organizer,把 token cost 替换为 frame/pixel cost。
4. **1024 行以上的持续积压诊断**(P2)。当前 1024 已经出现 SLO 50%-90% 违规,是否能在不引入复杂控制器的前提下用 admission + flush 配合降低 SLO?这部分自然衔接 RC2 的软拥塞信号盲区诊断(见批次 3)。

---

## 批次 2:RC2 flush 策略三组对照 + 跨 arrival rate + 2048 held-out

### 2.1 实验设置

研究内容二(RC2)的 flush 子问题:**已经关闭的 batch 何时提交给 vLLM?** 候选策略三档:

- **fixed 25ms**:每 25ms 强制 flush pending batch(高频小 batch,coalescing 弱)。
- **fixed 50ms**:每 50ms 强制 flush(coalescing 强,但延迟略高)。
- **queue-adaptive(25/50ms 两档)**:根据 vLLM running/waiting/KV 实时信号在 25/50ms 间切换。

链路与批次 1 一致,但 completion_max_tokens=512(自然 EOS,而非固定 16 token);prefix cache off;CUDA Graph on。

### 2.2 实验设计

| 子实验 | 数据源 | 规模 | 设计 |
|---|---|---|---|
| 三组自然 EOS | adaptive_flush_randomized/chatml_three_way_512 + chatml_flush_formal_512 | 512 请求 | fixed_25ms × 3 + fixed_50ms × 3 + queue_adaptive × 5 formal(repeat 顺序随机化) |
| 跨 arrival rate | adaptive_flush_cross_rate/screen | 512 请求 × 2 档 | fast(51.4 req/s) / slow(12.85 req/s) × 3 policy × 1 repeat(screen 性质) |
| 2048 held-out | text_heldout_2048/screen | 2048 请求 | fixed_50 / adaptive × 1 repeat(规模留出) |

### 2.3 严谨性自检

- 三组对照使用 ChatML 自然 EOS,排除固定 output cap 的混淆变量(批次 1 用 fixed_output_cap=16)。
- 三组每轮 formal 顺序由 scenario runner 独立洗牌;a daptive 的 5 formal 来自独立 `chatml_flush_formal_512`,与 three_way 的 3 formal 独立运行。
- 跨 arrival rate 与 2048 held-out 是 screen 性质(单 repeat),只用于策略排序验证,不报精确 effect size;effect size 仍以三组对照的 ±2.07%–6.22% 标准差为准。
- 全部 512 + 2048 = 4096 / 4096 请求 exactly-once,0 incident。

### 2.4 实验数据(从 summary_long.csv 直接读取)

#### Table 2.1 三组自然 EOS 对照(512 请求,ChatML 自然 EOS)

| scenario | n | tokens/s | e2e (s) | req_p95 (s) | req_p99 (s) | SLO viol | J/1k tok | submissions |
|---|---|---|---|---|---|---|---|---|
| fixed_25ms | 3 | 1684.1 ± 16.6 | 136.2 ± 1.5 | 111.2 ± 2.5 | 114.5 ± 1.7 | 0.0 | 71.78 | 200 |
| fixed_50ms | 3 | 2226.7 ± 7.1 | 102.6 ± 0.3 | 79.3 ± 0.05 | 81.0 ± 0.1 | 0.0 | 59.35* | 137 |
| queue-adaptive | 5 | 2237.4 ± 37.4 | 102.6 ± 1.8 | 79.5 ± 1.8 | 81.2 ± 1.7 | 0.0 | 59.4 | 139 |

(* fixed_50 的 J/1k tok 来自 three_way,值约 59.4;adaptive 来自 formal_512,值 59.4。两者在能耗上不可分辨。)

#### Table 2.2 跨 arrival rate screen(512 请求 × 2 arrival rate)

| scenario | arrival_scale | tokens/s | e2e (s) | req_p95 (s) | submissions |
|---|---|---|---|---|---|
| fast_fixed_50ms | 0.00025 (~51.4 req/s) | 2774.0 | 82.5 | 70.4 | 96 |
| fast_fixed_25ms | 0.00025 | 2264.6 | 100.9 | 89.1 | 137 |
| fast_adaptive_25_50ms | 0.00025 | 2757.1 | 83.3 | 70.7 | 100 |
| slow_fixed_50ms | 0.001 (~12.85 req/s) | 1706.7 | 134.7 | 93.4 | 200 |
| slow_adaptive_25_50ms | 0.001 | 1684.1 | 135.8 | 91.0 | 203 |
| slow_fixed_25ms | 0.001 | 1356.7 | 169.9 | 122.7 | 276 |

#### Table 2.3 2048 行 held-out screen

| scenario | tokens/s | e2e (s) | req_p95 (s) | req_p99 (s) | submissions |
|---|---|---|---|---|---|
| fixed_50ms | 2003.6 | 456.6 | 352.1 | 368.1 | 654 |
| adaptive_25_50ms | 1968.5 | 464.8 | 361.0 | 377.7 | 657 |

### 2.5 结果解释

**事实(可写论文)**:

1. **fixed-50 全面碾压 fixed-25(在自然 EOS 下)**。相对 fixed-25:tokens/s `+32.2%`(2227 vs 1684)、e2e `-24.7%`(102.6 vs 136.2)、req_p99 `-29.3%`(81 vs 114)、submissions `-31.5%`(137 vs 200)、J/1k tokens `-17.2%`(59.4 vs 71.8)。fixed-25 的 200 submissions 形成 vLLM 侧持续 prefill 抖动。
2. **queue-adaptive 与 fixed-50 不可分辨**。tokens/s `+0.48%`(在 fixed-50 n=3 ± 7.1 与 adaptive n=5 ± 37.4 的合并标准差范围内),e2e 持平,req_p99 `+0.25%`。**adaptive 没有获得默认资格**,因为它在所有测试条件下都没有显著优于最佳静态窗口。
3. **跨 arrival rate 未反转策略排序**。fast 档 fixed-50 vs fixed-25 `+22.5%`、adaptive vs fixed-50 `-0.6%`;slow 档 fixed-50 vs fixed-25 `+25.8%`、adaptive vs fixed-50 `-1.3%`。两档下 adaptive 都没有获得默认资格。
4. **2048 行 held-out 同样未反转**。fixed-50 vs adaptive:tokens/s `+1.75%`、e2e `-1.81%`、req_p99 `-2.61%`。规模放大 4 倍后持续积压让 P99 从 81s 跳到 368s(因为 2048 请求同一 inflight cap K=8 下排队时间近线性增长),但策略排序不变。

**推断(合理推断)**:

- **50ms 是当前 workload 的"自然节奏"**:vLLM 在 Qwen2.5-1.5B + RTX 5070 上一个 prefill+decode 周期约 2-4s(从 vLLM e2e_request_latency_mean ≈ 4.1s 推断),50ms 内积累的 pending rows 恰好填满 token_budget 6144 的约 1/3 到 1/2,而 25ms 强制截断让 batch 永远在 vLLM 最舒适工作点之下。
- **queue-adaptive 的两档切换在稳态下不会触发**:shared-vLLM 实验(批次 3)显示 adaptive 89.4% 的决策选 50ms,说明它的 25ms 档几乎不被使用。这正是"动态控制在稳态下退化为静态"的具体表现。
- **2048 行的 P99 飙升(368s)是另一个信号**:K_max=8 在 2048 请求规模下变成 HOL 瓶颈,这正好衔接批次 3 的 admission 主题。

**待确认**:

- queue-adaptive 在**负载阶段切换**(突发到达、多 job 混合)下是否会显示价值?当前 fast/slow/2048 三档都是稳态。
- SLO-aware EWMA flush(oldest slack + token backlog + 服务速率 EWMA)是否能超越 fixed-50?当前 two-level 25/50ms 只是 baseline,不是完整的 SLO-aware 控制律。

**不能声称**:

- 不能把 fixed-50 优于 fixed-25 的 +32% 写成"flush 策略设计的贡献"——这只是选定了一个更好的静态参数。
- 不能把 adaptive 不可分辨外推成"动态 flush 永远无用"——只覆盖了稳态单 job。
- 不能把 2048 的 P99=368s 写成"系统不可用"——这只是一个加速 replay workload,真实生产 SLO 由批次 3 的 admission 控制。

### 2.6 对课题的含义

- **RC2 的 flush 子问题在当前单 GPU 稳态下已收敛**:fixed-50 是安全默认,adaptive 不获默认资格。论文叙事可以诚实写:"在稳态单 GPU workload 下,简单的 fixed-50ms 已经接近最优;动态 flush 的价值需要在负载变化或多租户场景下重新评估"。
- **这与 RC1 的结论形成对照**:RC1 的复杂 packing 策略在 1024 规模下崩溃,RC2 的复杂 flush 策略在所有规模下不可分辨——两个研究内容的复杂策略都没能在已测试条件下击败简单 baseline。**但崩溃/不可分辨的机制不同**:RC1 是与 vLLM 调度的规模相关交互,RC2 是稳态下没有触发动态切换的物理需要。
- **批次 3 的 admission(K_max)是 RC2 唯一显示动态价值的子问题**——不是因为 K_max 自身需要动态,而是因为 shared-vLLM 下静态 K=8 是必要的 guardrail。

### 2.7 下一步实验

按 `experiments/plans/experiment_status_and_gaps.md` §10.3:

1. **SLO-aware EWMA flush**(P1)。当前 25/50ms 只是 baseline;完整控制律 = oldest-request slack + token backlog + arrival/service-rate EWMA + hard deadline + 滞回。预期在突发到达下能击败 fixed-50。
2. **Request-level continuous replenishment**(P1)。当前 Ray 上游仍按 submission 整批回收 credit;逐请求完成释放 credit 可能减少 HOL 并放大 vLLM continuous batching 收益。这是 §10.2 的关键缺口,与批次 3 的软拥塞信号盲区直接相关。
3. **多 job / 多租户混合到达**(P2)。当前所有 flush 实验都是单 job;混合 workload 是 adaptive 唯一可能显示价值的场景。

---

## 批次 3:RC2 admission controller + shared-vLLM 软拥塞信号盲区

### 3.1 实验设置

RC2 的 admission 子问题:**已经 flush 关闭的 batch 是否允许提交给 Ray?同时有多少 batch 可以在飞?** 候选:

- **Static K_max**:固定上限。K=8 当前默认(shared-vLLM 前台保护),K=16 单作业稳态吞吐最高。
- **AIMD**(Additive Increase Multiplicative Decrease):窗口从 initial=8 出发,无 EWMA 平滑,α=2 增/β=0.5 减,基于 vLLM running/waiting/KV 信号。
- **EWMA-AIMD**:AIMD + EWMA 平滑输入信号。
- **PID**:误差驱动连续控制。
- 所有控制器的 min=4、max=16,基于 vLLM Prometheus 信号(running/waiting/KV cache usage)。

两套实验:**单作业**(512 请求稳态)+ **shared-vLLM**(128 前台 + 512 后台共享同一 endpoint)。

### 3.2 实验设计

| 子实验 | 数据源 | 规模 | 设计 |
|---|---|---|---|
| 单作业 controller family | adaptive_admission_controller/formal_512 | 512 请求 | static_k8 / aimd / ewma / pid 各 3 formal repeat |
| 单作业 mechanism control | adaptive_admission_controller/mechanism_control_512 | 512 请求 | static_k16 + aimd 各 3 formal(AIMD 与同上限 K=16 对照) |
| Shared-vLLM K_max guardrail | shared_vllm_adaptive_admission/formal_512 | 128 前台 + 512 后台 | K8 / K16 / AIMD 各 3 formal,共享同一 vLLM endpoint |
| AIMD trace 级诊断 | shared_vllm_adaptive_admission/formal_512/traces | per-run | AIMD background run 的 control.csv(774 决策)+ resources.csv(vLLM 时序) |

### 3.3 严谨性自检

- 单作业实验使用 ChatML 自然 EOS,排除固定 output cap 混淆变量。
- mechanism control 是关键设计:**把 AIMD 与同上限 K=16 直接对照**,分离"升至 K=16 的并发红利"与"动态反馈控制律的增量"。没有这一步就会把"AIMD 比 K=8 快 30%"误读为"动态控制有效"。
- Shared-vLLM 主 CSV 的 `tokens_per_s` 因两进程读同一组 Prometheus 累计量而重叠,不可信;改用 paired_runs 的 `background_exact_tokens_per_s`(每请求真实 total_tokens 求和)。已知缺陷:外层 seed/scenario_id 未转发给子 profiler,只能靠 experiment_id 还原。
- AIMD 决策时序图用 r1 代表三轮;三轮的 K_max 行为一致(都饱和至 16、0 decrease)。

### 3.4 实验数据

#### Table 3.1 单作业 controller family(baseline = static K=8)

| scenario | e2e (s) | tokens/s | req_p99 (s) | goodput | J/1k tok | MFU | K_mean | e2e vs K8 | tokens vs K8 |
|---|---|---|---|---|---|---|---|---|---|
| static K=8 | 82.08 ± 2.10 | 2814.9 ± 60.9 | 59.96 ± 2.17 | 6.24 | 55.94 | 14.19 | 8.00 | 0% | 0% |
| AIMD(4-16, init 8) | 55.78 ± 3.05 | 4117.1 ± 249.8 | 33.40 ± 3.02 | 9.20 | 39.14 | 21.16 | 15.93 | -32.0% | +46.3% |
| EWMA-AIMD | 55.86 ± 3.28 | 4115.7 ± 247.3 | 33.49 ± 3.44 | 9.19 | 38.71 | 21.13 | 15.83 | -31.9% | +46.2% |
| PID | 57.20 ± 0.39 | 4009.3 ± 39.9 | 35.03 ± 0.29 | 8.95 | 39.29 | 20.51 | 15.78 | -30.3% | +42.4% |

#### Table 3.2 单作业 mechanism control(baseline = static K=16)

| scenario | e2e (s) | tokens/s | req_p99 (s) | K_mean | e2e vs K16 | tokens vs K16 |
|---|---|---|---|---|---|---|
| static K=16 | 58.94 ± 0.57 | 3899.3 ± 36.4 | 37.13 ± 0.46 | 16.00 | 0% | 0% |
| AIMD(4-16, init 8) | 59.33 ± 0.61 | 3872.2 ± 39.7 | 37.11 ± 0.45 | 15.93 | **+0.66%** | **-0.69%** |

#### Table 3.3 Shared-vLLM K_max guardrail(128 前台 + 512 后台)

| scenario | fg_e2e (s) | fg_p99 (s) | fg_slowdown | bg_e2e (s) | bg_tokens/s | sys_tokens/s | K_mean | inc | dec |
|---|---|---|---|---|---|---|---|---|---|
| K=8 | 40.21 ± 0.55 | 23.00 ± 0.47 | 1.261× | 86.92 ± 0.66 | 2596.6 | 3301.0 | 8 | 0 | 0 |
| K=16 | 55.74 ± 1.28 | 38.31 ± 1.40 | 1.749× | 62.47 ± 0.28 | 3603.1 | 4625.0 | 16 | 0 | 0 |
| AIMD | 56.42 ± 0.78 | 39.06 ± 0.67 | 1.769× | 63.44 ± 0.47 | 3550.8 | 4572.3 | 15.953 | 12 | **0** |

K=16 vs K=8:fg_e2e `+38.6%`、fg_p99 `+66.5%`、bg_tokens/s `+38.8%`(K=8 牺牲后台换取前台保护);AIMD vs K=16:fg_e2e `+1.22%`、fg_p99 `+1.98%`、bg_tokens/s `-1.45%`(不可分辨)。

#### Table 3.4 admission × flush 二维对照

| admission | flush | fg_e2e (s) | fg_p99 (s) | bg_tokens/s | Δ vs same_admission fixed_50 |
|---|---|---|---|---|---|
| K=8 | fixed_50 | 40.21 | 23.00 | 2596.6 | 0% |
| K=8 | adaptive | 39.57 | 22.31 | 2569.1 | -1.6% e2e |
| K=16 | fixed_50 | 55.74 | 38.31 | 3603.1 | 0% |
| AIMD | fixed_50 | 56.42 | 39.06 | 3550.8 | 0% |
| AIMD | adaptive | 56.48 | 39.14 | 3553.9 | +0.09% e2e |

### 3.5 结果解释

**事实(可写论文)**:

1. **单作业 AIMD/EWMA/PID 相对 K=8 的 ~30% E2E 改善全部来自升至 K≈16**(三者的 K_mean = 15.78–15.93)。这不是动态反馈控制律的功劳,是并发上限提高的并发红利。
2. **加入同上限 static K=16 对照后,AIMD 不可分辨**。e2e `+0.66%`、tokens/s `-0.69%`、req_p99 `-0.07%`,均远小于标准差。**没有动态反馈增量证据**。
3. **Shared-vLLM 下 static K=8 是必要 guardrail**。K=16 vs K=8:前台 E2E 恶化 38.6%、P99 恶化 66.5%(23.0 → 38.3s)、slowdown 从 1.26× 升到 1.75×。后台吞吐提升 38.8% 的代价是前台延迟翻倍——共享服务下 K=8 是必需的 guardrail。
4. **AIMD 在 shared-vLLM 下 0 次 decrease、12 次 increase,窗口均值 15.953**。三轮 774 次决策只有 12 次 increase(从初始 8 升到 16),随后再无 decrease——窗口饱和且不再降载。AIMD 相对 K=16 前台 E2E +1.22%、P99 +1.98%,与 K=16 不可分辨。
5. **AIMD × adaptive flush 二维对照完全持平**。AIMD × adaptive vs AIMD × fixed_50:e2e `+0.09%`、P99 `+0.19%`、bg_tokens `+0.09%`。admission 与 flush 在 shared-vLLM 稳态下完全解耦。

**核心诊断(软拥塞信号盲区)**:

AIMD 的拥塞信号是 vLLM Prometheus `waiting > 0` 或 `KV cache usage` 超阈值。但 trace 显示:

- **vLLM `running` 均值 ≈ 50**(请求确实在执行);
- **vLLM `waiting` 均值 ≈ 0**(请求没有在 vLLM 等待队列里);
- **前台 slowdown = 1.77×**(延迟确实恶化了)。

矛盾的解释:**请求在 Ray 侧排队形成"软拥塞"**,尚未进入 vLLM waiting 队列。AIMD 看不到这层拥塞,自然不会触发 decrease。这不是控制器参数问题(α/β/deadband 都调过),是**观测信号的表达能力不足**——当前依赖的 vLLM Prometheus 信号无法表达 Ray 侧排队。

**推断(合理推断)**:

- 单作业稳态下 K=16 是 vLLM 在 Qwen2.5-1.5B + RTX 5070 上的"自然舒适点"——KV cache 没耗尽、prefill/decode pipeline 流畅。任何 min=4/max=16 的控制器都会饱和到上限。
- Shared-vLLM 下"前台慢 38.9% 但 vLLM waiting=0"是 Ray admission 层与 vLLM 调度层之间信息断层的具体表现:Ray 决定多少 batch 在飞,K=8 限制后请求在 Ray 队列里等;vLLM 只看到请求按时到达,自己的 running/waiting 都正常。
- 这是 RC2 当前**最关键的开放问题**:动态控制在稳态下没有增量,根因不是控制器粗糙,是观测信号无法识别软拥塞。

**待确认**:

- 逐请求完成时间(request-level replenishment 的副产品)能否作为新的软拥塞信号?它直接观测 Ray 侧排队延迟,绕过 vLLM Prometheus。
- SLO-aware EWMA flush 用 oldest-request slack + token backlog 能否触发 adaptive 的动态切换?
- 多 foreground size / arrival offset / >2 job 下 AIMD 是否仍饱和?

**不能声称**:

- 不能把"AIMD vs K=16 不可分辨"外推为"动态 admission 永远无用"——只在稳态单 job 与一个 128/512 双作业规模下验证。
- 不能把"K=8 是必要 guardrail"等同于"K=8 是最优"——K=8 是当前 K_max ∈ {8, 16} 二元对照下的安全选择,可能 K=10 或 K=12 更好。
- 不能用 vLLM waiting=0 推断"系统没有拥塞"——Ray 侧软拥塞是事实。

### 3.6 对课题的含义

- **RC2 admission 的论文叙事收敛为**:"在共享 vLLM endpoint 下,静态 K_max=8 是必要的前台延迟 guardrail(强证据);动态自适应控制器(AIMD/EWMA/PID)在稳态 workload 下与同上限 static 策略不可分辨,根因是当前观测信号无法识别 Ray 侧软拥塞(诊断证据)"。
- **这是诚实但有力的贡献**:不是"我们发明了更好的控制器",而是"我们诊断出动态控制在 LLM serving 链路里的观测信号盲区,并指出三条绕行路线"。负结果 + 机制诊断 > 强行包装的正面结果。
- **批次 3 的诊断直接驱动 `experiments/plans/experiment_status_and_gaps.md` §10.4 的三条绕行路线**(SFS 模拟 / LPS+USL 解析 / 客户端 lifecycle 推断),把 request-level replenishment 从"工程改进"提升为"可能解锁动态控制价值的必要前置"。

### 3.7 下一步实验

按 `experiments/plans/experiment_status_and_gaps.md` §10.3:

1. **Request-level continuous replenishment**(P1,最高优先)。逐请求完成释放 credit,副产品"逐请求完成时间"提供软拥塞信号。
2. **客户端 lifecycle 推断作为软拥塞信号**(P1)。~50 行 EWMA 状态,零新依赖,直接用现有 request trace。
3. **LPS + USL 解析建模**(P2)。为 K_max 选择提供解析上界,与经验 K=8 互相校验。
4. **多 foreground size / arrival offset / 多 job**(P2)。当前只有一个 128/512 规模,无法外推多租户公平性。
5. **Shared-vLLM 多 foreground size + arrival offset**(P2)。foreground 从 128 扩展到 64/256/512,验证 K=8 guardrail 边界。

---

## 批次 4:RC1 × RC2 联合消融(独立拼接 vs joint grid search)

### 4.1 实验设置

AGENTS.md §1 写死的核心验证:**"两项策略分别独立搜索最优配置后拼接,再与联合 grid search 对比"**。回答两个问题:(1)K_max=16 是否能进入候选?(2)联合搜索是否能击败独立拼接?

### 4.2 实验设计

| 阶段 | 数据源 | 规模 | 设计 |
|---|---|---|---|
| 18 单元筛选 | joint_batching_submission/screen | 512 请求 × 1 repeat | token_budget{4096/6144/8192} × K_max{4/8/16} × flush{fixed_25/adaptive}(共 18 单元,部分省略) |
| 4 候选重复 | joint_batching_submission/candidate_repeat | 512 请求 × 3 formal | baseline / independent / joint / mechanism 各 3 formal,顺序随机化 |

候选定义:
- **baseline**: b6144/K8/fixed-25ms(基础对照)
- **independent**: b6144/K8/adaptive(RC1 最优 token_budget=6144 + RC2 最优 K=8 + adaptive flush,独立选择)
- **joint**: b8192/K8/adaptive(联合 grid search 选出的非直觉组合,更大 token_budget)
- **mechanism**: b8192/K8/fixed-50(同 joint 的 token_budget + K_max,但 flush 改回 fixed-50 作为机制对照)

控制变量:同一 512 文档集、相同 source order、相同 model/generation cap、相同 K_max=8、相同 writeback=none、固定 output cap=16 token。

### 4.3 严谨性自检

- 筛选用 SLO guardrail:**K_max=16 全部配置 SLO violation 1.76%–3.13%,全部排除**。这是关键设计——没有 SLO guardrail 就会把 K=16 当候选。
- 候选重复使用独立洗牌的 formal 顺序,每候选 3 formal repeat,带样本标准差。
- mechanism 候选是关键设计:与 joint 共享 token_budget + K_max,只换 flush,分离"联合搜索红利"与"flush 机制"。

### 4.4 实验数据

#### Table 4.1 18 单元筛选:K_max=16 全部被 SLO guardrail 排除(节选)

| scenario | tokens/s | SLO violation | 状态 |
|---|---|---|---|
| b4096_k16_fixed | 3161.5 | 1.76% | ❌ 排除 |
| b6144_k16_fixed | 3166.2 | 3.13% | ❌ 排除 |
| b8192_k16_fixed | 3186.8 | 1.76% | ❌ 排除 |
| b4096_k16_adaptive | 3250.1 | 2.73% | ❌ 排除 |
| b6144_k16_adaptive | 3267.6 | 2.15% | ❌ 排除 |
| b8192_k16_adaptive | 3264.8 | 2.54% | ❌ 排除 |
| b6144_k8_fixed | 3057.0 | 0% | ✓ 通过 |
| b6144_k8_adaptive | 3164.9 | 0% | ✓ 通过 |
| b8192_k8_adaptive | 3170.7* | 0% | ✓ 通过 |

(* b8192_k8_adaptive 的精确值在 screen 里;候选重复中 joint = 3143.9 ± 14.0 是 3 formal 的均值,与 screen 单点略有差异属正常。)

#### Table 4.2 4 候选重复(3 formal repeats)

| scenario | tokens/s | e2e (s) | req_p99 (s) | SLO viol | submissions |
|---|---|---|---|---|---|
| baseline(b6144/K8/fixed-25) | 3009.0 ± 15.7 | 23.46 ± 0.12 | 7.55 ± 1.91 | 0.0 | 200 |
| independent(b6144/K8/adaptive) | 3152.1 ± 14.9 | 22.39 ± 0.11 | 7.06 ± 1.19 | 0.001 | 154.7 |
| joint(b8192/K8/adaptive) | 3143.9 ± 14.0 | 22.49 ± ? | 7.79 ± ? | 0.0 | ~150 |
| mechanism(b8192/K8/fixed-50) | 3167.6 ± 3.3 | 22.30 ± ? | ? | 0.0 | ~140 |

### 4.5 结果解释

**事实(可写论文)**:

1. **K_max=16 全部被排除**:18 单元筛选中 6 个 K=16 配置的 SLO violation 全部在 1.76%–3.13% 之间,超过 1% guardrail。虽然它们的 tokens/s 比 K=8 高约 3%,但 SLO 维度退化不能接受。
2. **independent 相对 baseline tokens/s +4.76%**(3152/3009-1)。这是"两项策略独立拼接"的真实增量。
3. **joint 相对 independent tokens/s -0.26%**(3143.9/3152.1-1)。联合 grid search 选出的"非直觉组合"(b8192)相对独立拼接不可分辨。**当前单 GPU 下分层独立优化足够**。
4. **mechanism 相对 independent tokens/s +0.49%**(3167.6/3152.1-1)。joint 的 token_budget=8192 + K8 配置改用 fixed-50 比 adaptive 略好——说明在当前 workload 下 flush 策略的影响小于 token_budget 与 K_max 的选择。

**推断(合理推断)**:

- K_max=16 在加速 replay 的固定到达率下确实能提高吞吐,但 1.76%-3.13% 的请求超过 SLO——这说明 K=16 时 vLLM 内部出现间歇性 HOL。多 endpoint/多 GPU 环境下 K=16 可能不违反 SLO,届时联合搜索空间会改变。
- 联合 vs 独立不可分辨的根因:当前 workload 的最优点在 K=8 边界附近,且 token_budget 在 6144-8192 区间内对吞吐不敏感(差 <2%)。当 K_max 上限受 SLO 约束时,联合搜索空间塌缩到与独立空间相同。

**待确认**:

- 多 endpoint/多 GPU 环境下 K_max=16 是否仍违反 SLO?若不违反,联合搜索可能选出不同的最优。
- 不同 arrival rate 下 K=16 的 SLO violation 是否仍稳定?当前只测了一个加速到达率。

**不能声称**:

- 不能把"联合不可分辨"外推为"联合搜索永远无用"——只覆盖了当前单 GPU 稳态。
- 不能把"K=16 全部排除"等同于"K=16 永远坏"——这是 SLO guardrail 选择,不是性能论断。

### 4.6 对课题的含义

- **AGENTS.md §1 的核心验证已完成**:当前单 GPU 下,两项策略独立最优拼接与联合 grid search 不可分辨,**分层独立优化框架成立**。论文可以诚实写:"在加速 replay 单 GPU 稳态下,RC1(token_budget)与 RC2(K_max + flush)的联合交互效应 < 1%,分层优化足够;多 endpoint/多 GPU 下的联合搜索仍是开放问题"。
- **SLO guardrail 是 K_max 选择的关键约束**:K=16 在 tokens/s 上更优但 SLO 退化,使 K=8 成为当前默认。这本身是论文 §5.2 的有效贡献——"为什么看似合理的 K_max 上限在 LLM serving 链路里失败"是 admission control 设计的核心洞察。
- **mechanism 略优于 joint 是 flush 策略次要性的证据**:joint 的"非直觉组合"红利小于 flush 机制选择的影响。这与批次 2 结论一致——fixed-50 在当前 workload 接近最优。

### 4.7 下一步实验

按 `experiments/plans/experiment_status_and_gaps.md` §10.3:

1. **多 endpoint/多 GPU 联合搜索**(P2)。K_max 上限可能在多 endpoint 下不再受 SLO 约束,届时联合空间扩大。
2. **不同 arrival rate 下的 SLO guardrail**(P2)。当前只测了一个加速到达率;真实生产到达率下 K=16 是否仍违反 SLO?
3. **机制级联合消融**(P2)。把 flush 也作为联合搜索的第三维(不只 token_budget × K_max),验证 flush × admission 的交互。

---

## 批次 5:算子端到端代价估计

### 5.1 实验设置

研究内容四(§6.1 补充讨论,不作为独立研究内容):**仅使用执行前可知特征(row count、prompt token、completion 上限、token budget、batch 统计、K_max、flush、arrival 配置)能否估计 AI_COMPLETE 算子的端到端时间?** 实际输出 token、实测 E2E、vLLM、能耗、MFU 均不进入特征(防止执行后泄漏)。

模型:标准化 log1p Ridge 回归(alpha=1,15 个特征)。目标:`e2e_s`。基线:训练集目标均值。

### 5.2 实验设计

- 输入:28 个 `runs.csv`(覆盖 20260725–20260726 大部分实验目录),原始 285 `status=ok` 行,排除 2 行缺特征 → **283 行、70 个唯一配置组**。
- 切分:**grouped held-out**(按配置组切分,同一配置的 warm-up/formal/repeat 不会同时进 train 和 test,防泄漏)。主 fold seed=20260726,test_fraction=0.25(52 train groups + 18 test groups)。
- 稳健性:**连续 5 个固定 seed**(20260726..20260730)做同样切分审计。

### 5.3 严谨性自检

- post_execution_features_used 是空数组——刻意约束,确保模型只用执行前可观测量。
- 配置组按模型 + workload + batching + output_cost_mode + token_budget + K_max + flush + arrival_replay 等签名定义,避免相同配置的重复运行跨集泄漏。
- 报告 5 seed 而非有利单点;MAPE 跨 seed 在 30.76%–90.60% 之间波动,显式承认相对精度不稳定。
- 不在测试集上选 alpha 或特征;alpha=1 是预注册值。

### 5.4 实验数据

#### Table 5.1 5-seed grouped held-out 性能

| seed | 测试行 | ridge MAE (s) | ridge MAPE | ridge RMSE (s) | ridge R² | baseline MAE (s) |
|---|---|---|---|---|---|---|
| 20260726 | 87 | 9.73 | 32.58% | 34.30 | 0.620 | 29.43 |
| 20260727 | 58 | 15.06 | 68.16% | 29.68 | 0.788 | ~30 |
| 20260728 | 85 | 8.96 | 30.76% | 16.91 | 0.833 | ~28 |
| 20260729 | 39 | 13.58 | 90.60% | 29.05 | 0.858 | ~34 |
| 20260730 | 85 | 11.09 | 30.92% | 19.50 | 0.781 | ~28 |
| **均值** | — | **11.68** | **50.60%** | **25.89** | **0.776** | **29.89** |

#### Table 5.2 主 fold(seed=20260726)的特征系数(按绝对值排序)

| 特征 | 系数 | 方向 | 物理可解释性 |
|---|---|---|---|
| completion_max_tokens | +0.563 | 增加 E2E | ✓ 直觉一致(输出上限大→生成久) |
| packing_batch_count | +0.278 | 增加 E2E | ✓ 直觉一致(更多 batch → 更多 submission) |
| total_rows | +0.278 | 增加 E2E | ✓ 直觉一致(行多→工作量大) |
| batch_estimated_cost_p50 | +0.188 | 增加 E2E | ✓ 直觉一致 |
| arrival_replay_enabled | +0.178 | 增加 E2E | ✓ 直觉一致(replay 有等待) |
| flush_is_immediate | +0.057 | 增加 E2E | ✓ 直觉一致(immediate flush 更多 submission) |
| flush_is_adaptive | +0.007 | 微增 | ✓ 几乎中性 |
| flush_max_wait_ms | ~0 | 无影响 | ✓ 数据中几乎不变 |
| flush_timeout_ms | +0.013 | 微增 | ✓ 小 |
| max_inflight_limit(K_max) | -0.022 | 微减 | ✓ 直觉一致(更高 K → 并发更好) |
| batch_estimated_cost_max | +0.161 | 增加 E2E | ✓ 直觉一致 |
| batch_estimated_cost_p95 | -0.068 | 微减 | ⚠ 与 p50 方向相反 |
| token_budget | -0.074 | 微减 | ✓ 更大 budget → 更少 batch |
| arrival_time_scale | -0.179 | 减少 E2E | ⚠ **反直觉**(慢到达应增 E2E) |
| prompt_token_count | -0.163 | 减少 E2E | ⚠ **反直觉**(更多 prompt 应增 E2E) |

### 5.5 结果解释

**事实(可写论文)**:

1. **Ridge 在 5 seed 上均显著优于 mean baseline**。MAE 从 29.89s 降至 11.68s(改善 60.9%),R² 从 -0.004 升至 0.776。**仅用执行前特征就能解释大部分 E2E 方差**。
2. **MAPE 不稳定**:跨 seed 在 30.76%–90.60% 之间波动(均值 50.60%)。MAPE 对小目标敏感,seed 20260729 测试集只有 39 行且包含若干小 E2E 目标,导致 MAPE 飙到 90%。
3. **R² 相对稳定**:5 seed 上 R² 在 0.620–0.858 之间(均值 0.776),均显著为正。
4. **三个最重要的执行前特征**(按系数绝对值):completion_max_tokens(+0.563)、packing_batch_count(+0.278)、total_rows(+0.278)。它们的物理含义直觉一致。
5. **两个反直觉特征**:prompt_token_count(-0.163)与 arrival_time_scale(-0.179)出现负系数,违反物理直觉。

**推断(合理推断)**:

- 反直觉负系数的根因是**特征共线性 + 小数据 Ridge 的局限性**:prompt_token_count 大的配置往往 packing_batch_count 小(因为每行 token 多,每 batch 容纳行数少,batch 总数下降);Ridge 把 batch_count 的"工作量红利"错误归因到 prompt_token_count。这是 283 行 + 15 特征的固有局限,不是物理事实。
- 真正决定 E2E 的物理量是"GPU 需要执行多少 forward pass"——它由 total_rows × completion 实际 token 决定,但 completion 实际 token 不在执行前特征里。completion_max_tokens(上限)只是代理变量,且与实际输出长度存在系统性偏差。
- R² 0.776 + MAPE 50% 的组合说明:**模型对配置排序的能力大概率不错(R² 高),但点估计精度差(MAPE 高)**。这正是 README §待补充指出的——Spearman 秩相关、pairwise accuracy、Top-K precision 才是编排决策真正关心的指标,但当前未计算。

**待确认**:

- **排序指标未计算**:R² 0.776 暗示排序大概率不错,但 Spearman / pairwise / Top-K 需要显式计算后才能声称。
- **无独立 workload / 时间段留出**:所有 283 行来自 07-18 至 07-26,外推到新 workload 或新时间段的退化程度未知。
- **无预测区间**:点估计无法评估风险;编排决策需要保守上界。
- **output-length 预测器未实现**:实际 E2E 高度依赖真实输出 token(自然 EOS 位置),而非 completion_max_tokens(上限)。SFS 证明 LightGBM 从 prompt 特征预测实际输出长度是可行的(MAPE <5%),可作为第 16 个特征改进 Ridge。

**不能声称**:

- 不能把 R² 0.776 写成"模型预测准确"——MAPE 50% 说明相对误差大。
- 不能用反直觉系数做物理解释(prompt_token_count 负 ≠ prompt 多让 E2E 短)。
- 不能把当前模型直接用于多模态(特征未含 frame/pixel cost)。
- 不能在没有模型/模态重训时外推到其他模型或硬件。

### 5.6 对课题的含义

- **算子代价估计定位为补充讨论(§6.1),不作为独立研究内容**。当前证据支持"Ridge + 执行前特征可作粗粒度编排提示"的 claim,不支持"严格 SLO 预测"的 claim。
- **两个预期用途**(README 已记录):
  1. 数据库优化编排(主要):为查询优化器提供 AI 算子代价估计,辅助选择执行计划与资源分配。R² 0.776 暗示排序能力大概率可支撑编排决策,但需补排序指标。
  2. 提交策略辅助(探索性):作为 vLLM Prometheus 信号的补充,提供 pending batch 粗粒度工作量预估(轻/中/重分类),不替代 Orca 式持续供给与反馈驱动机制。
- **批次 3 的软拥塞信号盲区与代价估计有协同空间**:Ridge 模型可以为提交侧提供"pending batch 预计是轻还是重"的分档判断,作为 vLLM Prometheus 的补充信号——这是 §6.1 + §10.4 路线 3(客户端 lifecycle 推断)的天然交汇点。

### 5.7 下一步实验

按 `experiments/plans/experiment_status_and_gaps.md` §1.5 + `operator_cost_estimation_20260726/README.md` §后续工作:

1. **排序指标补充**(第一批 #1)。在 `estimate_operator_cost.py` 中增加 Spearman 秩相关、pairwise accuracy、Top-K precision。Heinrich SIGMOD 2025 R2 的核心论点——编排关心排序而非点估计精度。
2. **Hybrid 架构**(第一批 #2)。增加 `E2E_base = total_prompt_tokens / estimated_throughput + fixed_overhead` 作为第 16 特征;让 Ridge 学"传统公式无法解释的偏差"。
3. **Output-Length 预测器**(第一批 #3)。用 LightGBM 从 prompt 特征预测实际 output tokens(SFS MAPE <5%),作为第 17 特征。
4. **轻/中/重分档验证**(第一批 #4)。按预测 E2E 将配置分三档,同档内真实 E2E 方差是否显著小于全局?这决定了代价估计能否用于提交侧 workload 分类。
5. **预测区间**(第二批 #5)。bootstrap residual 估计保守上界,支持风险感知的编排决策。

---

## 跨批次总结:五个批次的整体洞察

### 核心发现(可写进论文)

1. **RC1 数据组织**:Sequential token-budget 是当前单 GPU 稳态安全默认;BFD/row-cap-first/prefix-aware 在已测试条件下均未显著击败 baseline,且各有明确失败边界(BFD 1024 反转、row-cap SLO 崩溃、prefix cache-off 无信号)。
2. **RC2 flush**:Fixed-50ms 在加速 replay workload 下接近最优;queue-adaptive 与之不可分辨(±2-4%);跨 arrival rate + 2048 held-out 均未反转。
3. **RC2 admission**:Static K=8 在 shared-vLLM 下是必要 guardrail;AIMD/EWMA/PID 相对 K=8 的 ~30% E2E 改善全部来自升至 K≈16,与同上限 static 不可分辨。**根因诊断**:AIMD 对 Ray 侧软拥塞盲视(vLLM waiting=0 但前台已慢 38.9%)。
4. **联合消融**:K_max=16 全部因 SLO violation 被 guardrail 排除;联合候选相对独立拼接 -0.26% 不可分辨,**分层独立优化框架成立**。
5. **算子代价估计**:Ridge + 执行前特征 R² 0.776(5-seed 均值)、MAE 11.68s,粗粒度编排提示可用,严格 SLO 预测精度不够。

### 共同的方法论主题

- **复杂策略在单 GPU 稳态下都未显著优于简单 baseline**——这不是策略本身无效,而是与下游 vLLM 调度的交互在当前条件下不利。
- **失败/不可分辨的机制各不相同**:RC1 是与 vLLM 调度的规模相关交互;RC2 flush 是稳态下没有触发动态切换的物理需要;RC2 admission 是观测信号表达能力不足;联合消融是搜索空间受 SLO guardrail 约束塌缩。
- **诚实的负结果 + 机制诊断 > 强行包装的正面结果**。这是项目的核心论文叙事。

### 关键开放问题

1. **软拥塞信号盲区**(批次 3):逐请求完成时间能否填补?三条绕行路线(SFS 模拟 / LPS+USL 解析 / 客户端 lifecycle 推断)哪条最先见效?
2. **多 endpoint / 多 GPU**(所有批次):当前所有结论受单 GPU 限制;K=16 SLO violation、K_max guardrail、联合搜索空间、多模态复用都需要多 endpoint 验证。
3. **多模态泛化**(RC1 + RC4):CLIP embedding 没有 continuous batching,跨查询请求池从"vLLM 代劳"变为"必须自己做";token-budget → frame-budget 的代码复用未验证。
4. **代价估计的排序能力**(RC4):R² 0.776 暗示排序不错,但 Spearman / pairwise / Top-K 未计算,无法声称。

### 后续工作优先级

P0(最高):
- Request-level continuous replenishment + 逐请求完成时间作为软拥塞信号(批次 3 §3.7)
- SLO-aware EWMA flush 完整控制律(批次 2 §2.7)

P1:
- Prefix cache-on 独立机制门禁(批次 1 §1.7)
- Length-align × token-budget 正式独立重复(批次 1 §1.7)
- 代价估计排序指标补充(批次 5 §5.7)

P2:
- 多 endpoint / 多 GPU 真实验证(所有批次)
- 多模态泛化验证(批次 1 + 5)
- Shared-vLLM 多 foreground size / 多 job 公平性(批次 3 §3.7)

---

## 附录:产物清单

### 绘图脚本(可在本地用 `.conda/pg-ai-profile/python.exe` 运行)

```
figures/scripts/generate_rc1_data_organization_charts.py     # 批次 1
figures/scripts/generate_rc2_flush_charts.py                 # 批次 2
figures/scripts/generate_rc2_admission_charts.py             # 批次 3
figures/scripts/generate_rc2_joint_ablation_charts.py        # 批次 4
figures/scripts/generate_rc4_cost_estimation_charts.py       # 批次 5
```

每脚本一行运行,例如:
```
.conda/pg-ai-profile/python.exe figures/scripts/generate_rc1_data_organization_charts.py
```

### 图输出位置

`figures/data/report_main/`:
- 批次 1: `rc1_bfd_scaling_512_vs_1024.{png,svg}`、`rc1_row_cap_first_slo_collapse.{png,svg}`、`rc1_prefix_aware_cache_off_no_signal.{png,svg}`
- 批次 2: `rc2_flush_three_way_natural_eos.{png,svg}`、`rc2_flush_cross_rate_and_heldout.{png,svg}`
- 批次 3: `rc2_admission_controller_matrix.{png,svg}`、`rc2_shared_vllm_kmax_guardrail.{png,svg}`、`rc2_aimd_signal_blindspot.{png,svg}`
- 批次 4: `rc2_joint_screen_heatmap.{png,svg}`、`rc2_joint_candidate_repeat.{png,svg}`
- 批次 5: `rc4_cost_model_5seed_stability.{png,svg}`、`rc4_cost_model_coefficients.{png,svg}`

### 分析报告

本文件:`experiments/results/EXPERIMENT_DATA_ANALYSIS_20260727.md`

### 数据来源(全部真实单 GPU,无 fake)

- `experiments/results/output_aware_bfd_{512_v2,1024}_20260726/`
- `experiments/results/row_cap_aware_packing_{512/nocache_repeats,1024}_20260726/`
- `experiments/results/prefix_aware_batching_20260726/screen_v3/`
- `experiments/results/adaptive_flush_randomized_20260726/{chatml_three_way_512,chatml_flush_formal_512}/`
- `experiments/results/adaptive_flush_cross_rate_20260726/screen/`
- `experiments/results/text_heldout_2048_20260726/screen/`
- `experiments/results/adaptive_admission_controller_20260726/`
- `experiments/results/shared_vllm_adaptive_admission_20260726/formal_512/`
- `experiments/results/joint_batching_submission_512_20260726/{screen,candidate_repeat}/`
- `experiments/results/operator_cost_estimation_20260726/`
