# SAOR 数学模型、控制分层与适用场景审计

> 状态：`saor-v0.4-design-revision`。本文依据 2026-08-11 capacity-only 负结果和既有两/四 Job
> 干扰结果，对 SAOR 的控制对象、
> 可证明部分、经验控制部分和 benchmark 重新分层。它不把一次 development run 写成算法结论，
> 也不宣称实际实现已经获得 MaxWeight/VTC 的理论保证。

## 1. 审计结论

当前 SAOR 思路中值得保留的是 **Stage-Aware Ordered Release**，而不是“两档 K 动态切换”本身。
`saor-v0.4` 进一步冻结：**总 request/work envelope 在主实验中固定；动态的对象是活跃 Job
集合内的份额、借用、回收和释放顺序。** 后续实现分成一个主控制器和一个暂停分支：

1. **SAOR-Release（主方法候选）**：在冻结的安全总容量内，对数据库 Job 的 head request 做
   completion-driven ordered release，维护物理 backlog、公平债务和 SLO 债务。它是最接近
   constrained queueing / MaxWeight 模型、也最适合多 Job 数据库 AI 场景的部分。
2. **Safe-Capacity Governor（`parked-conditional`）**：慢速选择 K/work 档位。现有数据没有
   证明它相对强静态点有必要性；只有离线 oracle 先证明可利用的 Pareto 空间，才允许恢复。
   恢复后仍必须使用反事实 response model、风险上界、hysteresis 和 drain debt，且不继承
   SAOR-Release 的 throughput-optimality 结论。

现有 capacity-only arm 多数时间停留在 K160，并在末段切换。它与 K160 的吞吐差仅约 0.5%，
低于 threshold 约 1.5%，且没有改善 Jain 或 Job B tail。这说明该 workload 的主要机会不是
“在线猜 K”，也说明公平债务从未进入实际动作。继续在同一 trace 调 `V` 或 risk weight 会
成为事后参数搜索，不再推进研究问题。

### 1.1 现有证据究竟支持什么

现有实验支持的是“固定总容量内的多 Job 动态分配问题”，不是“总 K 必须动态变化”：

- 该机器/模型签名下，K160 是强静态效率点；capacity-only SAOR 相对它只有约 0.5% 吞吐差，
  且未改善 Job B tail 或 Jain。因此动态 K 当前应淘汰出主线。
- eager 两 Job 中，固定分区使 short 的 quota-only JCT 增加 59.00%；相同总上限下 shared
  相对 static 使 short JCT 降低 48.94%、总吞吐提高 31.85%、long JCT 降低 25.75%。四 Job
  中 shared 相对 static 总吞吐提高 8.68%、short JCT 降低 72.23%，但 Jain 从 0.960 降到
  0.923。这证明静态分区会浪费或错配份额，也证明无约束共享会产生公平/稳定性代价。
- online replay 中结论方向不同：shared 提高总吞吐却伤害 short/Jain。因此 arrival、active、
  drain 状态确实影响正确的份额分配，但还不能推出 SAOR 已解决该问题。

上述 static/shared 对照仍缺少一个决定性反事实：**同一固定 K 下，不设置项目级 Job 配额和
公平队列，只把所有请求按到达顺序交给 vLLM FCFS。** shared 相对 static 的收益可能只是消除
静态分区浪费；若 global FIFO/no-op 已同时达到相同效率、tail 和公平，则 SAOR 没有必要。
因此现阶段的严谨判定是：dynamic-K 版本 `reject and pivot`；fixed-envelope active-set
release 版本 `accept with revisions`，等待 no-op killer baseline。

“no project K”不等于系统真的没有边界：vLLM 仍受 `max_num_seqs`、每 iteration 的
`max_num_batched_tokens` 和 KV 容量约束，只是把外部队列移入 HTTP/vLLM。现有 ShareGPT
bounded 校准中，C128 已达到 C256 吞吐的 98.22%；C256 只再增 1.82%，却使 TTFT mean 从
0.829 s 增至 6.181 s、waiting mean 从 3.7 增至 116.8、KV max 接近 1。因此“无项目级 Job
调度”必须作为强 baseline 跑，但不能预设无限提前提交没有 tail 代价。

## 2. 现有逻辑为什么不闭合

### 2.1 把两个问题塞进同一个 score

容量选择回答“总共允许多少 unfinished work”，Job release 回答“这部分容量先给谁”。前者
主要受 tail、安全、不可撤销 active work 和慢响应影响；后者主要受 backlog、entitlement 和
SLO debt 影响。把两者同时写入一个未归一化的 DPP score 后，队列项随 backlog 线性增长，
而 tail/KV proxy 是小范围归一化数，导致高服务率档位几乎总占优。

这不是简单把 `V` 调大就能解决：不同量纲下任何一个 `V` 都只适用于某一 backlog 范围，且
改变 `V` 会同时改变 delay--penalty trade-off。正确修正是把已知安全条件放入 feasible set，
再在安全集合内做 Job release；不是用一个软权重让安全和队列互相抵消。

### 2.2 当前臂 EWMA 不能比较未运行臂

capacity adapter 只更新当前 K 的 goodput/tail/energy EWMA，却同时给 K128/K160 打分。对未运行
档位的分数，本质上来自旧状态或默认值，不是同一系统状态下的反事实估计。unknown service
rate 的 queueing scheduler 通常需要显式学习/探索或离线模型；例如 Discounted-UCB MaxWeight
把学习和调度共同建模，而不是把 current-arm EWMA 当作所有臂都已知。

### 2.3 降档不是即时动作

外部 window 只能停止发新 work，不能撤销已经进入 vLLM continuous batch 的请求。若当前
active work 高于新 K，真正状态是“目标已降、pipeline debt 尚未排空”，而不是容量立即变小。
现有 14 次切换集中在末段，正好落入长请求/KV 排空延迟区；普通逐 slot MaxWeight 的即时服务
假设因而不成立。带 reconfiguration delay 的 MaxWeight 工作使用 hysteresis：只有候选 schedule
显著优于当前 schedule 才切换。SAOR 也必须显式建模这个延迟，而不是事后加一个任意 dwell
samples。

### 2.4 aggregate engine state 不等于 Job 风险

waiting/KV 可以表达共享 engine 压力，却不能识别哪个 Job 正在积累 attained-service lag、
deadline miss 或 P99。capacity-only run 改善 Job A tail 同时恶化 Job B P99 和 Jain，正说明
aggregate proxy 与目标约束不一致。公平/SLO 必须通过 per-Job completion ledger 更新，不能
从总 KV 或 GPU utilization 推断。

## 3. 修正版控制架构

```text
slow/offline calibration
  ├─ one minimum-saturation safe envelope
  └─ cost/work calibration signature
                │
                ▼
SAOR-Release (fast, primary)
  ├─ per-Job ready/active work
  ├─ active-set entitlement + idle borrowing/reclaim
  ├─ fairness and SLO virtual debt
  ├─ Job-head ordered release
  └─ completion-driven replenishment
                │
                ▼
unmodified vLLM FCFS
  └─ continuous batching / chunked prefill / KV management
```

### 3.1 主方法默认固定总容量

第一组 formal 应冻结同一最大 `request/work` envelope，让所有策略共享相同 vLLM FCFS、
continuous batching 和 credit 上限。SAOR-Release 只决定下一个 eligible Job-head；不切 K、不改
endpoint scheduler、不在线扩缩 Ray actor。这样才能把多 Job 效果归因于 release order、idle
borrowing 和 virtual debt。

K160 在 development run 中没有 OOM/failure/credit leak，且相对 K128 有吞吐/JCT收益，因此
不能预设它“不安全”。若用 K160 作为总 envelope，必须把其 Job B tail/fairness 风险作为强
静态对手；SAOR-Release 的任务是在不降低总上限的条件下改善 lag/slowdown/SLO，而不是靠少喂
work 获得表面公平。若正式 safety gate 认为 K160 不满足预注册 SLO，则改用达到条件的最小
饱和安全点，选择规则仍相同。

这里的“动态”定义为：当 backlogged/eligible Job 集合变化时，权重份额随之重新归一化；空闲
Job 的未用份额立即可借，Job 返回或新 Job 到达后，只在后续 completion 释放 credit 时回收，
不抢占已经进入 vLLM 的请求。总 envelope 始终不变。

### 3.2 暂停分支：governor 选择最高可认证安全档

令 $z_t$ 为包含 ready work、active-work age histogram、completion rate、TTFT/TPOT、SLO
miss、waiting/KV 和 freshness 的状态。对每个离线校准档位 $k$，预测未来 $H$ 个周期的
goodput 与风险区间：

$$
(\widehat G_{k,H}(z_t),\; U^{tail}_{k,H}(z_t),\; U^{fail}_{k,H}(z_t)).
$$

安全集合定义为硬约束：

$$
\mathcal K_{safe}(z_t)=\{k:
U^{tail}_{k,H}\le \tau_{tail},\;
U^{fail}_{k,H}\le \epsilon_{fail},\;
M_k\le M_{hard}\}.
$$

governor 在安全集合内选预测 goodput 最高的档位；若观测 stale、signature 变化或集合为空，
回退冻结安全点。waiting/KV 是预测特征和诊断证据，不是硬编码安全真值。

从 $k$ 降到 $k'$ 时定义不可撤销 pipeline debt：

$$
D_e(t)=[R_e(t)-K^{work}_{e,k'}]^+.
$$

$D_e>0$ 时不再给该 endpoint 新 lease，直至 completion 使 debt 清零。只有候选相对当前档位的
保守收益超过 hysteresis $h(W_t)$，并满足按实测 drain/recovery envelope 定义的最短驻留条件，
才允许切换。该思路来自带 reconfiguration delay 的 adaptive MaxWeight，但这里仍需重新证明
其对不可撤销 LLM request 的适用性。

以上只保留为恢复条件与理论备忘，不进入 `saor-v0.4` 主 benchmark、主算法图或贡献表述。

## 4. SAOR-Release 的数学模型

### 4.1 状态与队列

以固定控制周期 $t=0,1,\ldots$ 建模。对 Job $j$：

- $U_j(t)$：尚未完成的总 normalized model-work，包括 ready 与 active work；
- $A_j(t)$：本周期新变为 model-ready 的 work；
- $C_j(t)$：本周期由 vLLM 实际完成、并经 actual token/work 修正的 work；
- $F_j(t)$：共同积压期间的 weighted-service fairness debt；
- $Z_j(t)$：SLO miss-rate debt。

只用 ready queue 会把 work 释放给 vLLM 后误当作系统完成，因此稳定性分析使用总 unfinished
work：

$$
U_j(t+1)=[U_j(t)-C_j(t)]^+ + A_j(t).
$$

对模型阶段定义 eligible/backlogged 集合：

$$
B_M(t)=\{j:Q^{ready}_{j,M}(t)+R_{j,M}(t)>0\}.
$$

尚在数据库读取或 CPU prepare、没有 model-ready work 的 Job 不在 GPU 服务份额上积累债务；
否则会把上游瓶颈错误地转换成 GPU 优先级。设 $j\in B_M(t)$ 的权重为 $\phi_j>0$，目标份额：

$$
\rho_j(t)=\frac{\phi_j}{\sum_{i\in B_M(t)}\phi_i}.
$$

公平和 SLO debt 用实际 completion 更新：

$$
F_j(t+1)=
[F_j(t)+\rho_j(t)C_{tot}(t)-C_j(t)]^+,
$$

$$
Z_j(t+1)=
[Z_j(t)+M_j(t)-\epsilon_jN_j^{done}(t)]^+.
$$

现有“prompt/output token 组织”不需要取消：token-budget 仍用于把多条完整请求组织成 cohort；
单行 prompt 不按 token 切成多个语义隔离请求。公平以实际 weighted prompt/output token 记账，
resource/admission 以校准 work 记账，两者保持独立。

### 4.2 动作与 capacity region

动作 $a\in\mathcal A_K(H_t)$ 是冻结总 envelope $K$ 下的 Job-head release/routing 组合。其
条件平均 completion service vector 为：

$$
\mu(a,H_t)=\mathbb E[C(t)\mid \Theta(t),H_t,a(t)=a].
$$

capacity region $\Lambda_K$ 定义为在该固定 envelope、明确 FCFS/continuous-batching 服务合同
与允许的 randomized release policies 下可稳定支持的 arrival vectors 的闭包。该定义只相对
外部可控系统，不声称覆盖所有可能的 vLLM 内部 scheduler。

### 4.3 Oracle DPP 动作

定义：

$$
L(t)=\frac12\sum_j\left[U_j(t)^2+\eta_FF_j(t)^2+\eta_ZZ_j(t)^2\right].
$$

在所有满足 exact credit、ordered-head 和硬安全条件的动作中，oracle SAOR 选择最小化：

$$
\begin{aligned}
\Psi(a)=
&-\sum_j U_j\widehat\mu_j(a)
+\eta_F\sum_jF_j
\left(\rho_j\widehat\mu_{tot}(a)-\widehat\mu_j(a)\right)\\
&+\eta_Z\sum_jZ_j
\left(\widehat m_j(a)-\epsilon_j\widehat n_j(a)\right)
+Vg(a).
\end{aligned}
$$

安全不进入 $g$ 软加权；$g$ 只保留在安全动作之间有明确单位和界的运行 penalty，例如能耗或
切换成本。第一版可以设 $V=0$，先验证稳定、公平与 SLO debt，再单独加入 penalty。

## 5. 可证明结论与完整证明条件

### 5.1 Oracle theorem（目标陈述）

若满足：

1. $A_j,C_j,M_j,N_j^{done}$ 有界且具有有限二阶矩；
2. exogenous state 在 calibration signature 内平稳遍历；
3. $\mathcal A_K$ 有限，oracle conditional mean service 已知；
4. arrival vector 严格位于 $\Lambda_K$ 内，且 fairness/SLO constraints 联合可行并存在
   $\varepsilon>0$ 的 Slater slack；
5. completion feedback、request work 和 dispatcher inversion 有界，并已纳入状态/常数；

则 exact oracle DPP 可争取证明所有 $U,F,Z$ 强稳定，长期 weighted-service deficit 与 SLO
miss-rate 不为正；若 penalty 有界，则：

$$
\limsup_{T\to\infty}\frac1T\sum_{t<T}\mathbb E[g(t)]
\le g^*+\frac{B}{V},
$$

且平均 backlog 为 $O(V)$。

### 5.2 证明骨架

对三个 queue recursion 使用：

$$
([Q-b]^++a)^2\le Q^2+a^2+b^2+2Q(a-b),
$$

把所有有界二次项收进有限常数 $B$，得到 conditional drift 上界。oracle 动作逐周期最小化
动作相关线性项；与具有 $\varepsilon$ slack 的 stationary randomized policy 比较，可得：

$$
\Delta(t)+V\mathbb E[g(t)\mid\Theta(t)]
\le B+Vg^*-\varepsilon\sum_j(U_j+F_j+Z_j).
$$

从 $t=0$ 到 $T-1$ 求和、望远镜消去 Lyapunov 项并令 $T\to\infty$，分别得到 penalty gap
和平均 backlog 界。又因为：

$$
F_j(T)\ge\sum_{t<T}[\rho_j(t)C_{tot}(t)-C_j(t)],
$$

若 $F_j(T)/T\to0$，即可推出长期 weighted-service deficit 约束；$Z_j$ 同理推出 SLO miss-rate
约束。完整 proof appendix 仍需写出 $B$、capacity region、active-set/counter-lift 和所有
条件期望，以上只是可审计骨架，不标记为“定理证明完成”。

### 5.3 实现不能自动继承 oracle theorem

真实系统的 $\widehat\mu$ 来自估计。因为 MaxWeight score 含 $U\widehat\mu$，即使
$|\widehat\mu-\mu|\le\delta$，误差也可随 $U\delta$ 无界增长，不能随意改写成常数
$C$-additive error。实现要获得理论桥接，只能选择以下之一：

- 证明真实 weighted service 至少达到 oracle 的 $\alpha$ 比例，并只对收缩后的 capacity
  region 声明稳定；
- 对未知 service rate 引入有理论分析的学习机制，并记录探索成本；
- 只保留 oracle theorem，把线上控制器称 empirical，不宣称 throughput-optimal。

当前项目应选择第三条作为诚实默认，以 held-out ranking 和 offline oracle 判断以后能否升级
到第一条。capacity governor 由于反事实、delay 和 non-preemption 更复杂，必须独立论证。

### 5.4 可以先完成的两个较弱性质

在完整随机稳定性定理之前，可以先对实现合同证明两个不依赖 service-rate 预测的性质：

1. **固定 envelope 不越界**：若每次 release 前原子检查 request/work credit，且只由唯一
   completion ledger 释放，则所有时刻都有
   $\sum_jR_{j,e}^{req}\le K_e^{req}$ 和
   $\sum_jR_{j,e}^{work}\le K_e^{work}$。
2. **工作守恒**：若 endpoint 有可用 credit，且至少一个 Job-head eligible 并能装入剩余
   work credit，则动作集合不得让 hold 成为可发布动作，策略必须释放至少一个 head。该性质
   保证 idle borrowing，但不自动保证公平或吞吐最优。

公平还需要 active-set counter lift：Job 从 idle 返回时，其债务/虚拟服务基线最多抬到当前
active 集合的最低合法基线，不能携带空闲期未使用份额形成无限 credit。实际请求大小未知时，
release 用 estimated work 预扣，completion 用 actual work 修正；任何 service-lag 上界都必须
显式包含最大单请求 work、估计误差和 non-preemptive feedback delay，不能照搬 VTC 的 token
级上界。

## 6. 更合适的 benchmark

### 6.1 主 benchmark：固定容量的多 Job ordered release

这是最符合算法名、数学模型和数据库背景的场景。

建议冻结一个通过 safety/feeding gate 的强总 envelope，显式 vLLM FCFS。第一项不是扩大
workload 矩阵，而是跑一个能直接证伪 SAOR 必要性的 active-set 场景：

1. `bulk-only`：long/batch Job B 先到并可借用全部总 envelope；
2. `foreground-arrival`：B 仍积压时 latency-sensitive Job A 到达；已有 B request 不抢占，
   只在 completion 后逐步把新 credit 回收给 A；
3. `foreground-drain`：A 完成后，B 重新借用全部空闲份额。

这三个阶段使用同一 immutable manifest，不通过改变总 K 制造收益。它精确区分：

- static partition 在第一、三阶段是否浪费 A 的预留份额；
- global FIFO/no-op 在第二阶段是否因 B 已形成到达序列而伤害 A tail/SLO；
- shared DRR 是否已足以解决问题；
- SAOR 的 active-set、SLO debt 和回收顺序是否提供额外 Pareto 收益。

通过这项 killer baseline 后，才扩展到：

- 4 个数据库 Job，至少一个 short/latency-sensitive Job 与多个 long/throughput Job；
- equal-weight 与 3:1 weighted 两种 entitlement；
- 所有 Job 持续共同积压窗口 + staggered arrival/departure/idle borrowing 窗口；
- 每条数据库记录仍为完整 request，prompt/output shape 从冻结 manifest 读取；
- 先用受控 short/long shape，再用真实 trace 做外部有效性。

主比较固定为：global FIFO/no project Job scheduler、static partition、shared DRR、
external VTC-style、SAOR-Release。global FIFO 与所有项目策略使用相同 K/window、source/sink、
manifest、endpoint 和 vLLM flags。headline 不只看 tokens/s，而是：

- weighted service Jain、GPS service lag max/P95/偿还时间；
- Job slowdown 对 matched solo、JCT/TTFT P95/P99、SLO miss；
- starvation/max age、avoidable idle、correct goodput、energy；
- throughput--fairness--tail Pareto。

晋级规则按正式重复的噪声和业务最小效应预注册，不把算法参数硬编码成结论阈值。若
SAOR-Release 不能在 correctness/failure 不退化、总吞吐基本不损失的条件下，相对 global
FIFO 和 shared DRR 至少改善一项预注册的 worst-Job tail/SLO/service-lag 指标，则淘汰 SAOR；
若 DRR 已达到相同 Pareto 前沿，则保留 DRR，不再包装新算法。

VTC artifact 已公开 overload、proportional、on/off、Poisson short/long、increase 和 distribution
shift suites，可借其 **workload shape 与指标定义**，但实现仍是 S-LoRA artifact，不能和本项目
upstream vLLM 做绝对性能排名。

### 6.2 独立 benchmark：dynamic capacity 是否真的存在机会

该分支当前暂停，不与主 benchmark 混跑。若未来恢复，先做 offline oracle gate，再决定是否
实现 governor：

1. 使用 recovery-gated square wave 或有限 burst，不继续抬高同一平均 B rate；
2. 每个独立周期开始前要求 active work/waiting 清零，KV 与 completion rate 回到预注册基线带；
3. phase 长度至少覆盖对应档位的实测 P99 drain/recovery time；
4. 低压 phase 必须重复证明 upper 相对 lower 的收益；高压 phase 必须重复证明 upper 违反
   预注册 tail/SLO 而 lower 保持可接受；
5. 比较 lower、upper、简单 threshold、governor、clairvoyant/offline oracle；
6. oracle 若不能相对最佳 static 形成超过重复噪声和预注册最小效应的 Pareto 增量，直接淘汰
   dynamic capacity。

vLLM 官方 benchmark serving 可用 Gamma inter-arrival：`burstiness<1` 产生高变异 arrival；
BurstGPT 提供真实 timestamp、request/response token 和 burst pattern；两者适合外部有效性，
但 controlled mechanism run 仍应使用冻结 manifest 和明确 recovery gate。

### 6.3 trace 与模拟器的角色

- **BurstGPT**：用于 arrival burst、token shape 和 distribution shift；允许按硬件缩放 RPS。
- **ServeGen**：用于从生产特征生成可控 workload；若引入，冻结上游 commit/config。
- **Vidur**：可做容量/action-set 初筛和反事实 oracle 原型，但必须用当前 4090/Qwen/vLLM profile
  校准，模拟结果不能替代真实 formal。
- **VTC artifact**：借 on/off、short/long、公平指标；不复用其 S-LoRA 吞吐数字。

### 6.4 图像/CPU--GPU benchmark 与 SAOR 的关系

图像验证的是 staged backpressure，不是 token fairness。先完成 static HSE：真实
`pending-prepare → ready-block → pending-model → result` 队列、byte-bounded buffer 和 typed
block。只有 static HSE 不输 current project static 后，才设计 cold encoded image 与 cached/
prepared image 混合、CPU-heavy 与 GPU-heavy phase 变化的多 Job benchmark。此时比较 Ray Data
native、current static、static HSE、简单 differential controller 和 SAOR-HSE；不得用低 GPU
utilization 本身作为成功指标。

## 7. 公平怎样判定

Jain 只是一个聚合统计，不是公平定义。正式报告同时使用：

1. **资源/服务公平**：共同积压窗口内 $x_j=S_j/\phi_j$ 的 Jain；
2. **有界滞后**：相对理想 GPS 的 max/P95 service lag 与债务偿还时间；
3. **体验隔离**：每个 Job 相对 matched-solo 的 normalized slowdown、tail 和 SLO miss；
4. **无饥饿**：maximum wait age、无限/超时请求数；
5. **工作守恒**：有 eligible ready work 且安全 credit 可用时的 avoidable-idle ratio；
6. **效率代价**：correct goodput、能耗和 tail 与公平指标画 Pareto，不压成单一综合分。

不能对原始 TTFT 直接算 Jain 后称“公平”，因为不同 request shape 的理想 latency 本就不同；
也不能只看 service Jain，因为所有 Job 同样慢仍可得到高 Jain。

## 8. 实施与停止顺序

1. 冻结 capacity-only 为 `not-promoted`，Safe-Capacity Governor 标记
   `parked-conditional`，不再在旧 A20/B4.5 trace 调权重；
2. 把现有 ordered release 接入 per-Job completion ledger，固定总 K；先补 global FIFO/no-op
   与 DRR killer baseline，再运行 SAOR；
3. 先跑上述 `bulk-only → foreground-arrival → foreground-drain` 决定性场景；通过后才跑
   four-Job、3:1 weighted 和异构 held-out；
4. 只有用户重新激活 dynamic capacity 时才构建 finite-horizon oracle；oracle 不过门即永久
   停止 governor；
5. HSE 先接 static real-ready broker 和 byte ledger，再逐项做 packed uint8/pinned/DALI/cache；
6. 只有 release core、governor、HSE 各自过独立门后才做组合，不运行“一次打开所有机制”的
   联合实验。

## 9. 一手依据

- Tassiulas--Ephremides constrained queueing / MaxWeight：
  <https://drum.lib.umd.edu/items/571fda52-aefb-4497-9a2d-69d8c7c907b9>
- Neely drift-plus-penalty 收敛分析：<https://arxiv.org/abs/1412.0791>
- 带 reconfiguration delay 的 adaptive MaxWeight：<https://arxiv.org/abs/1511.03417>
- unknown service rate 的 Discounted-UCB MaxWeight：
  <https://proceedings.mlr.press/v206/yang23d.html>
- VTC 论文与 artifact：<https://www.usenix.org/conference/osdi24/presentation/sheng>、
  <https://github.com/Ying1123/VTC-artifact/tree/main/fair_bench>
- vLLM scheduler capacity fields（`max_num_seqs`/`max_num_batched_tokens`）：
  <https://docs.vllm.ai/en/stable/api/vllm/config/scheduler/>
- vLLM benchmark load pattern：<https://docs.vllm.ai/en/latest/benchmarking/cli/>
- BurstGPT trace：<https://github.com/HPMLL/BurstGPT>
- Vidur simulator：<https://github.com/microsoft/vidur>
- ServeGen：<https://github.com/alibaba/ServeGen>

## 10. Reviewer-style idea verdict

### 10.1 定位与 fatal flaws

- **paper type**：New Setting + mature-method transplantation，不是全新的 MaxWeight 或公平算法。
- **一句话故事**：在不修改 vLLM 的数据库 AI 执行链路中，用分阶段真实 work 与 Job debt 控制
  FCFS 前的 ordered release，并把异构 data path 与模型服务 continuous batching 连接起来。

| flaw | severity | defense |
|---|---|---|
| 与 Ray Data streaming/backpressure、VTC/MaxWeight 的增量若说不清，会被认为只是组合已有机制 | MAJOR | 明确 Ray/Daft 拥有执行，VTC 拥有 in-engine token fairness；项目只 claim DB Job/stage state 到 fixed-envelope ordered release 的映射，并使用 native/VTC-style 强 baseline |
| 同时承诺新执行模型、动态 K、公平算法、定理、GPU 数据通路和多模态，scope 会跨越多个 paper type | MAJOR | 主贡献只保留 work-unit/typed stage contract 与 SAOR-Release；HSE 是底座，governor/DALI/cache 是 parked 或后续工作 |
| shared 相对 static 的收益可能完全来自去掉静态分区；缺 global FIFO/no-op 时仍属 solution-seeking | MAJOR | 把 fixed-K global FIFO 和 shared DRR 设为 killer baseline；任一简单策略落在同一 Pareto 前沿即淘汰 SAOR |

没有不可修复的 CRITICAL flaw，但三项 MAJOR 必须在 formal 前持续收窄。

### 10.2 五维审计

| dimension | score | evidence | lift |
|---|---:|---|---|
| Higher | 6/10 | 目标是改善多 Job service lag/SLO/JCT；capacity-only 尚未胜出 | 用 fixed-envelope 4-Job decisive experiment 给出明确 Pareto 增量 |
| Faster | 6/10 | K160/K128 已证明容量差，HSE 有明确 CPU 木桶；SAOR 本身目前只到约 0.5% vs K160 | 将 flow gain 与 work-reduction gain 分开，只有正确完成吞吐/JCT ≥5% 才 claim |
| Stronger | 7/10 | fail-closed signature、bounded work/bytes、text/image stage abstraction 与 failure boundary 已显式化 | 补 held-out arrival/mix 和 counterexample，不只跑一个 phase trace |
| Cheaper | 5/10 | 同硬件、能耗/内存将记录，但尚无显著 $/work 结果 | 报 CPU-core-s、GPU-s、J/1K work 与 cache storage/refresh 成本 |
| Broader | 8/10 | 将 queueing control 迁移到 DB Job + vLLM/typed GPU actor，并统一 text/image descriptor | 保持同一 policy interface 的跨模态验证，但不要把两模态中不同 work 强行说成同一物理量 |

最高潜力是 **Broader + Stronger**，Faster 必须由实验而不是架构图获得。

### 10.3 范式与可行性

| probe | result | rationale |
|---|---|---|
| First Principles | Partial | 挑战“GPU utilization/waiting 足以表示健康”和“一个 active-work 标量能覆盖多阶段”的隐含假设 |
| Elephant in the Room | Partial | DB/Daft/Ray ready state 与 serving state 的脱节真实存在，但相邻系统已覆盖一般 backpressure |
| Technology Cycle | Yes | 数据库 AI 算子、LLM serving continuous batching 与多模态 data engine 的成熟使统一外部控制现在可实验 |
| Hamming's Rule | No | 成功会改善一个重要系统切片，但不会单独重排整个 LLM serving/数据库领域优先级 |

结论是 incremental with disruptive seeds，适合系统论文的严谨 new-setting framing，不适合包装成
范式革命。

| risk | level | mitigation |
|---|---|---|
| Compute | Medium | 当前 2×4090 足以完成 7B/CLIP controlled formal；不扩张到大集群通用性 |
| Data | Low | 已有冻结 DB manifests，BurstGPT/VTC suites/ServeGen 可公开获得 |
| Engineering | Medium--High | 先接 static broker/ledger，再接 release；禁止一次重构全 pipeline |
| Theory | High | 先完成 oracle appendix；unknown-service/governor 不获证明就保持 empirical，并安排独立数学复核 |
| Timeline | High if combined | 先做 HSE static 与 SAOR-Release killer baseline；governor 保持 parked，不进入当前串行路径 |

**Verdict：分版本判定。** dynamic-K SAOR 为 **Reject and Pivot**；fixed-envelope active-set
SAOR-Release 为 **Accept with Revisions**。后者只有通过 global FIFO/no-op 与 shared DRR 两个
killer baseline 才能晋级；HSE 作为执行底座，capacity governor 保持 `parked-conditional`。
