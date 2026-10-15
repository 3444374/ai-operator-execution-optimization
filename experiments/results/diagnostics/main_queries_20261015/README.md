# 固定 main 的原 Movie／Animals 实际模型检查

本页分列main464的单次资格与[main eb697的连续运行补充](#long-run)，各自保留实际源码和运行身份。

main `464de926cb85c2bc526a9402442776b8d60db966` 的四臂完成16个原版查询，9216次实际模型POST全部返回，未知请求和HTTP错误为0。原SDK解析、发生项／像素关联、官方评价与请求资源检查通过；**模型质量存在下述不足，单次耗时不支持稳定性能排名**。

## 来源与执行范围

本轮运行 `main-model-r1`，UTC 2026-10-15T11:00:32.054819+00:00 至 2026-10-15T11:14:47.264905+00:00。
在已发布的[文本闭包](../main_lotus_text_20261015/README.md)与[Animals闭包](../main_lotus_animals_20261015/README.md)上固定同一个main，运行前逐文件核对全部776个Python／运行资产，没有导入集成分支的新代码。
SemBench固定 `c814e3807e72d4cf876b852b17e77f3cc94575c2`；Movie使用原SF2000及原Q2/Q3的120个合格发生项，Animals为原64图。原方法拥有提示、ImageArray、Filter后处理、LIMIT／COUNT和evaluator；四臂只替换执行owner。

一张RTX4090，vLLM0.25.1；driver为LOTUS1.2.4、LiteLLM1.95.0、OpenAI2.50.0、Daft0.7.21、Ray2.56.1和Arrow24。
Movie为Qwen2.5-7B-Instruct，BF16、maxlen4096、max-num-seqs4、batched-tokens2048、显存比例0.80、生成上限8、请求超时30秒。
Animals为Qwen2-VL-2B-Instruct，BF16、maxlen12288、seqs4、batched-tokens8192、显存比例0.75、生成上限8192、请求超时120秒；processor固定max_pixels1003520／min_pixels3136、image1／video0，MM缓存0。两服务均TP1／PP1、eager、prefix cache关闭，完整模型／tokenizer文件指纹见[来源摘要](provenance.json)。

每格1个完整查询，warm-up0、自动重试0、结果缓存关闭；顺序均为LOTUS→Daft→Ray Data→SemLoom，Movie在各owner内依次Q1/Q2/Q3，然后换Animals服务。请求上限4；native threads8，Ray actors2×async2、batch1／source blocks4。SemLoom workers4、held4、batch1、Arrow/full response、window2MiB／object8MiB；Animals另显式4MiB请求、8MiB总input、4MiB+24 Ray窗口、8MiB对象额度。

## 全部查询与请求观察

| 查询 | 执行 owner | 实际请求 | 完整查询 s | HTTP P50 ms | P95 ms | P99 ms | 实际峰值 |
|---|---|---:|---:|---:|---:|---:|---:|
| Movie Q1 | LOTUS / LiteLLM | 2000 | 75.086481 | 125.023 | 139.133 | 144.671 | 4 |
| Movie Q2 | LOTUS / LiteLLM | 120 | 4.992059 | 122.070 | 135.224 | unavailable | 4 |
| Movie Q3 | LOTUS / LiteLLM | 120 | 4.431637 | 117.907 | 129.431 | unavailable | 4 |
| Movie Q1 | Daft Native | 2000 | 75.978512 | 123.869 | 135.754 | 148.353 | 4 |
| Movie Q2 | Daft Native | 120 | 4.612666 | 122.862 | 136.438 | unavailable | 4 |
| Movie Q3 | Daft Native | 120 | 4.421196 | 120.522 | 128.257 | unavailable | 4 |
| Movie Q1 | Ray Data | 2000 | 94.880947 | 127.506 | 142.768 | 147.879 | 4 |
| Movie Q2 | Ray Data | 120 | 9.937847 | 117.106 | 136.130 | unavailable | 4 |
| Movie Q3 | Ray Data | 120 | 9.651480 | 112.804 | 133.998 | unavailable | 4 |
| Movie Q1 | SemLoom | 2000 | 75.738825 | 119.028 | 134.689 | 138.499 | 4 |
| Movie Q2 | SemLoom | 120 | 4.544440 | 117.080 | 130.342 | unavailable | 4 |
| Movie Q3 | SemLoom | 120 | 4.617693 | 114.595 | 128.920 | unavailable | 4 |
| Animals Q1 | LOTUS / LiteLLM | 64 | 36.856632 | 359.067 | 703.565 | unavailable | 4 |
| Animals Q1 | Daft Native | 64 | 43.368209 | 350.718 | 488.106 | unavailable | 4 |
| Animals Q1 | Ray Data | 64 | 63.207310 | 282.314 | 330.570 | unavailable | 2 |
| Animals Q1 | SemLoom | 64 | 40.961455 | 286.582 | 474.538 | unavailable | 4 |

完整查询时间按各前端的已记录边界解释：Movie从查询准备开始至query terminal，并列的ready-query时间保留在[完整机器结果](results.json)；各owner的共享初始化及最终退出仍在原件中。Animals为原ImageArray→sem_filter→COUNT调用的进入至完整结果返回，外部LM／执行器准备、查询后评价及物理退出不在该窗口。它们不是包含所有进程启动的冷应用时间，不能跨工作负载相除排名。Movie服务启动35.232444s、Animals启动60.094464s单列；两者退出码0、剩余自有进程均为空。

Animals应用runner的较宽计时也保留：LOTUS 38.753168s、Daft 45.647983s、Ray Data 77.221304s、SemLoom 58.412485s。该窗口从run_query内部开始至finally完成，包含校验／准备、查询、评价／归档与资源关闭，排除外部模型服务启动和Python进程导入；不能将它与内部查询窗口相减称作某个阶段的纯成本。Movie每owner一次初始化／关闭覆盖三个查询，未另采一条与Animals应用窗口同义的墙钟，标为unavailable；不据查询表构造冷应用排名。

HTTP样本只取父进程共同网关的实际上游dispatch至完整上游body读取，包含服务队列、模型和HTTP传输，不称纯推理时间。分位为经验nearest rank，查询中的请求不充作独立查询重复。P99仅在每格2000请求的Movie Q1列出；120／64请求的P99、全部P99.99与查询级尾分位均为unavailable。控制器保留的旧通用summary若包含小样本高分位，本报告不采用。

## 质量及可支持的判断

官方Movie Q1/Q2 LIMIT结果的precision／recall／F1均为1.0，但只评价返回的5行，不能据此宣称全量Filter正确。Q1的完整发生项审计为：

| Movie Q1 执行 owner | TP | FP | TN | FN |
|---|---:|---:|---:|---:|
| LOTUS / LiteLLM | 1049 | 11 | 504 | 436 |
| Daft Native | 1048 | 11 | 504 | 437 |
| Ray Data | 1049 | 11 | 504 | 436 |
| SemLoom | 1048 | 11 | 504 | 437 |

Q2四臂均TP7、FP0、TN106、FN7；Q3均预测COUNT7、真值14，绝对误差7、相对误差50%。没有无效或缺失发生项。四臂实际请求值的多重集合相同，Q2/Q3逐行判定一致；Q1逐行判定并非全部相同，尚未核定差异成因。现有结果不证明Movie模型质量等价或exact Filter质量，不把少量查询时差称为收益。

Animals四臂COUNT均3／真值3，原评价误差0；逐图TP3、FP0、TN61、FN0，64个判定相同。该结论仅覆盖本次固定64图与模型，不外推更大数据或其他模态／查询。

复算从实际完整HTTP响应经固定OpenAI／LiteLLM转换、原Filter parser重放，再核每个row_id恰好消费一次响应。Animals从实际PNG恢复RGB像素身份关联原发生项。9216份关联全部通过；外层模型trace与各查询的实际请求hash多重集合相等，vLLM最终成功计数为Movie8960／Animals256。有效请求相同不保证模型数值输出逐行相同。

## 资源、故障与原件恢复

正常共享lease，`recover_stale=False`；lease持有覆盖服务启动至物理退出。所有实际HTTP区间峰值≤4；Animals Ray本轮实际峰值2，按实值保留。CPU driver8核、service8核分开；controller+client的8GiB容量由运行检查监督，采样不是硬RSS／物理独占内存上限，0.5秒采样可能漏过短峰。服务显存与逻辑input／object额度分别解释。

健康查询没有累计wall截止。工作无活动120秒、启动无活动180秒、清理120秒及有限请求超时用于故障；停止先关闭后续分配，不退款／重开预算。最终unknown0、自有进程inventory已确认、observer停止，无强制清理动作／survivor／inventory error；正常释放后现场GPUcompute为空。

原模型轮没有重跑或失败cell。离线复算首版把底层adapter响应转换当成native SDK转换，哈希不匹配后停止；修正为原生OpenAI默认字段及LiteLLM转换后通过，初次脚本／失败日志保留，新增模型请求0。独审另补响应逐份消费及row_id覆盖，4项定向用例通过；后续等价性按row_id映射比较，全部旧分析保留。首次封存工具使用系统Python缺少file_digest，未生成归档；换已有固定driver后封存，未改变模型观测。

完整原件以run id `main-model-r1`、归档342093068bytes、SHA256 `a71ae517cf7c8e3d199a603d0104f05ebc6bc38c737f28c3b8fbc6b47ec20286`定位，364份证据逐一列于内部evidence-manifest，加清单自身共365文件；独立Mac副本逐文件字节数／SHA已核同。原件包含实际请求／响应、逐行结果、服务metrics／启动退出、源码和模型身份、预算与全部失败；由项目维护者按run id和SHA提供，仓库不复制大量原数据或私有机器路径。[复算入口](recompute.py)接受恢复目录及原SemBench／数据manifest路径，不发HTTP或启动模型；输出与[results.json](results.json)相同。

后续auto报告复用本入口的Animals响应／像素关联函数，原算法抽取后再次用全部保留响应做CPU复算，完整结果JSON与本页发布值相同，额外模型请求0。当前脚本指纹和更正原因见[provenance](provenance.json)；[初次不可变报告](https://github.com/3444374/ai-operator-execution-optimization/tree/90013a5576eab77e6763c94c784d333de557d657/experiments/results/diagnostics/main_queries_20261015)及其原指纹保留。本页模型来源仍为main464，未转给新源码。

恢复控制器时将`controls/`中的controller／worker放至运行根，将`controls/helpers/`恢复为同根`main-model-helpers/`；控制器计划中的6份文件哈希必须先一致。只复算时使用固定main464及上述SDK环境：

```bash
CUDA_VISIBLE_DEVICES="" PYTHONPATH=code python /path/to/recompute.py --root /path/to/main-model-r1 --checkout /path/to/sembench --movie-manifest /path/to/movie-manifest.json --movie-csv /path/to/Reviews.csv --animals-manifest /path/to/animals-manifest.json --output /path/to/results.json
```

本次不新增PG、PZ、IMLane、CLIP／HSE／RM/index或auto／多GPU资格；合并文档不改变这些边界。auto必须绑定其实际新源码、正常lease和独立受影响查询复验。

<a id="long-run"></a>

## 固定 main eb697 的四臂连续运行补充

本节是独立运行 `sembench-long-20261015-r2`，实际源码 `eb697477e7f9173228a87efe24331bd34bd66066`，
SemBench仍为 `c814e3807e72d4cf876b852b17e77f3cc94575c2`。UTC 2026-10-15 17:15:34.654214至19:25:12.749001，
控制器墙钟7778.095秒。Movie原Q1的2000发生项每臂19次，Animals原Q1的64图每臂8次；
108个完整查询、154048次实际请求全部返回，HTTP错误／重试0。
先前r1在首个2000请求查询完成后因外层聚合`round`重复键失败，独立保留、排除于本节聚合；累计实际调用156048，未重置预算或补跑。

[配置](long_run_config.json)、[全部重复与机器汇总](long_run_summary.json)、[离线复算](long_run_recompute.py)提供本节查询、HTTP、资源与MFU的统一复算来源。
硬件／SDK版本、原提示／解析／LIMIT／COUNT职责沿用上文角色，实际服务参数以本节配置为准。
四个隔离Python owner按轮轮换位置，每四轮反转基序；所有完整query服务窗互不重叠。
Movie每臂19次owner身份相同，保留原生逐查询图／actor生命周期，不私造持久池；
Animals Python进程常驻，但每次`run_query`仍创建／关闭LM、执行器及所需Ray运行时。
请求额度4，Animals原生Ray实际HTTP峰值2，其余三臂4；Movie四臂均4。

首轮是设施／owner就绪后的首次测量，不能称机器冷启动；后续也不保证所有缓存已暖。
模型服务启动单列Movie35.504548秒、Animals59.016059秒，不计入查询表。
Movie共享owner初始化／退出没有单独同义应用墙钟，标为unavailable，不构造冷应用排名。
Animals完整查询从原ImageArray／Filter／COUNT进入至完整结果返回；较宽应用窗口从`run_query`内部进入至finally关闭，
包含准备、评价／归档及清理，排除Python导入与外部服务启动。两窗口不相减充作纯阶段成本。

### 完整查询、应用与顺序

表内单位秒；后续和全部均为「均值／中位数 [最小, 最大]」，首轮n=1时四量相同。
Movie后续n=18／全部n=19，Animals后续n=7／全部n=8。每轮是固定数据的重复，不能当作新增独立行样本。
| 工作负载／owner | 首轮 | 后续完整查询 | 全部完整查询 |
|---|---|---|---|
| Movie LOTUS | 74.522 | 73.737／73.816 [73.186, 74.100] | 73.778／73.822 [73.186, 74.522] |
| Movie Daft | 75.094 | 74.045／74.100 [73.261, 74.959] | 74.101／74.124 [73.261, 75.094] |
| Movie Ray Data | 87.764 | 77.436／77.436 [75.283, 79.456] | 77.979／77.455 [75.283, 87.764] |
| Movie SemLoom | 74.350 | 73.921／73.812 [73.106, 75.320] | 73.943／73.818 [73.106, 75.320] |
| Animals LOTUS | 36.908 | 36.104／36.106 [35.880, 36.263] | 36.204／36.130 [35.880, 36.908] |
| Animals Daft | 42.085 | 40.391／40.404 [40.007, 40.627] | 40.603／40.452 [40.007, 42.085] |
| Animals Ray Data | 61.342 | 60.654／60.775 [59.418, 61.805] | 60.740／60.781 [59.418, 61.805] |
| Animals SemLoom | 39.789 | 39.127／39.008 [38.617, 39.976] | 39.210／39.118 [38.617, 39.976] |
Animals较宽应用窗口另列，同样单位与统计格式：
| owner | 首轮应用 | 后7次应用 | 全部8次应用 |
|---|---|---|---|
| LOTUS | 38.786 | 37.561／37.591 [37.300, 37.730] | 37.714／37.603 [37.300, 38.786] |
| Daft | 44.123 | 41.886／41.899 [41.474, 42.105] | 42.166／41.936 [41.474, 44.123] |
| Ray Data | 75.042 | 72.381／72.236 [70.661, 74.486] | 72.714／72.277 [70.661, 75.042] |
| SemLoom | 57.695 | 54.574／54.192 [54.074, 55.549] | 54.964／54.476 [54.074, 57.695] |
顺序趋势只作描述：Movie前／后段为round1–9／10–18各9次；Animals为round1–3／5–7各3次，
中间round4仍保留在总统计及斜率中。斜率对全部后续轮作普通线性拟合，单位秒／轮，无因果调整或显著性声明。
位置条件统计及逐轮顺序在JSON；排除首轮后Movie每位置4–5次、Animals1–2次，不是完全平衡设计。
| 工作负载／owner | 前段中位 | 后段中位 | 斜率 s/轮 |
|---|---|---|---|
| Movie LOTUS | 73.922 | 73.705 | -0.0126 |
| Movie Daft | 74.199 | 74.076 | +0.0074 |
| Movie Ray Data | 78.512 | 76.748 | -0.1740 |
| Movie SemLoom | 73.512 | 74.223 | +0.0123 |
| Animals LOTUS | 36.154 | 36.093 | +0.0104 |
| Animals Daft | 40.500 | 40.404 | +0.0077 |
| Animals Ray Data | 60.788 | 60.746 | +0.0278 |
| Animals SemLoom | 38.822 | 39.308 | +0.1724 |
Movie Ray后半段中位较前半段低，SemLoom后半段较高；原因未隔离。
SemLoom与LOTUS完整查询时间接近，现有质量和顺序条件不支持稳定收益、等价或总体尾延迟结论。

### 完整HTTP请求分布

单位毫秒，全部实际dispatch至上游完整响应body读取，包含服务队列、模型及HTTP；不含dispatch前客户端排队／序列化，
不称纯模型时长。按所选查询的全部请求合池后取nearest rank，未平均每查询分位。
请求按查询聚类，n不是独立查询数；Animals首轮64条的P99†等于max，后448／全512条的P99也仅是描述量，
没有稳定总体尾部资格。旧在线首轮P99置null为保守报告策略，完整事件没有遗漏；全部P99.99和查询级尾分位unavailable。
| 工作负载／owner | 分组 | n | P50 | P95 | P99 | max |
|---|---|---|---|---|---|---|
| Movie LOTUS | 首轮 | 2000 | 127.837 | 137.872 | 140.909 | 166.813 |
| Movie LOTUS | 后续 | 36000 | 126.984 | 138.478 | 142.360 | 165.534 |
| Movie LOTUS | 全部 | 38000 | 127.035 | 138.445 | 142.349 | 166.813 |
| Movie Daft | 首轮 | 2000 | 127.913 | 138.289 | 143.198 | 150.400 |
| Movie Daft | 后续 | 36000 | 126.970 | 138.235 | 141.739 | 158.929 |
| Movie Daft | 全部 | 38000 | 127.040 | 138.241 | 141.780 | 158.929 |
| Movie Ray Data | 首轮 | 2000 | 118.986 | 134.837 | 141.374 | 150.697 |
| Movie Ray Data | 后续 | 36000 | 119.380 | 136.275 | 142.963 | 158.190 |
| Movie Ray Data | 全部 | 38000 | 119.362 | 136.202 | 142.913 | 158.190 |
| Movie SemLoom | 首轮 | 2000 | 118.994 | 134.542 | 140.423 | 150.364 |
| Movie SemLoom | 后续 | 36000 | 119.247 | 133.974 | 139.648 | 150.785 |
| Movie SemLoom | 全部 | 38000 | 119.235 | 134.013 | 139.730 | 150.785 |
| Animals LOTUS | 首轮 | 64 | 351.359 | 518.664 | 651.287† | 651.287 |
| Animals LOTUS | 后续 | 448 | 351.302 | 510.859 | 572.437 | 587.730 |
| Animals LOTUS | 全部 | 512 | 351.359 | 517.972 | 572.437 | 651.287 |
| Animals Daft | 首轮 | 64 | 341.701 | 505.296 | 585.852† | 585.852 |
| Animals Daft | 后续 | 448 | 344.168 | 463.019 | 495.859 | 588.704 |
| Animals Daft | 全部 | 512 | 344.168 | 466.235 | 511.851 | 588.704 |
| Animals Ray Data | 首轮 | 64 | 244.681 | 313.769 | 357.908† | 357.908 |
| Animals Ray Data | 后续 | 448 | 253.712 | 322.774 | 336.045 | 357.893 |
| Animals Ray Data | 全部 | 512 | 252.195 | 321.611 | 336.045 | 357.908 |
| Animals SemLoom | 首轮 | 64 | 327.052 | 437.367 | 500.966† | 500.966 |
| Animals SemLoom | 后续 | 448 | 318.792 | 447.059 | 495.247 | 523.170 |
| Animals SemLoom | 全部 | 512 | 322.070 | 447.059 | 495.394 | 523.170 |
### 质量、资源及观测覆盖

Movie每次官方LIMIT五行precision／recall／F1均1.0，只评价返回行。
全2000发生项的Filter审计没有invalid／missing，每次FP11、TN504；下表保留每臂F1与FN范围，质量等价未证明。
| Movie owner | 全量Filter F1范围 | 每次FN范围 |
|---|---|---|
| LOTUS | 0.823899–0.824361 | 436–437 |
| Daft | 0.823437–0.824361 | 436–438 |
| Ray Data | 0.823437–0.824823 | 435–438 |
| SemLoom | 0.823437–0.824361 | 436–438 |
Animals四臂全部8次均COUNT3／真值3、TP3／FP0／TN61／FN0，原评价误差0、F1=1；
结论限于反复使用的同64图，不称2048独立图。

资源按每次外层`entered_monotonic_ns`至`ended_monotonic_ns`内的采样点归臂，包含查询、观察与审计，
比上表内部完整query／应用窗口更宽；不用可能滞留到空档的phase／unit标签分组。
下表为该臂全部查询外层窗口的并集，n／Σ秒是采样数／这些互不重叠窗口的总秒数；
全程7774点，平均间隔1.000573秒、最大1.033318秒。各外层窗口最大未采样间隙≤1.012031秒；
所有108条HTTP服务窗均有采样（每条7–74点），1秒采样可能遗漏短峰。
| 工作负载／owner | n／Σ秒 | 活跃owner／全部client RSS峰 GiB | CPU活跃／闲置／服务 核 | GPU busy均值 % | 显存峰 MiB | 功率均／峰 W |
|---|---|---|---|---|---|---|
| Movie LOTUS | 1426／1426.807 | 1.179／9.264 | 0.270／0.216／0.764 | 96.79 | 19752 | 398.86／422.17 |
| Movie Daft | 1433／1431.474 | 1.233／9.269 | 0.285／0.215／0.762 | 95.93 | 19752 | 395.80／423.92 |
| Movie Ray Data | 1505／1504.444 | 4.178／9.717 | 0.616／0.097／0.697 | 86.93 | 19752 | 374.46／442.31 |
| Movie SemLoom | 1432／1432.584 | 3.160／9.259 | 0.718／0.121／0.791 | 95.51 | 19752 | 400.94／432.42 |
| Animals LOTUS | 326／326.094 | 2.135／7.779 | 0.920／0.010／0.422 | 17.79 | 19000 | 118.83／437.05 |
| Animals Daft | 362／362.513 | 2.748／8.532 | 0.956／0.010／0.387 | 16.22 | 19000 | 112.01／443.58 |
| Animals Ray Data | 608／606.465 | 6.032／11.633 | 1.314／0.009／0.234 | 8.59 | 19000 | 86.47／377.52 |
| Animals SemLoom | 464／464.566 | 3.855／9.684 | 1.398／0.009／0.298 | 12.54 | 19000 | 96.87／433.11 |
CPU是同一query外层窗内相邻采样的匹配PID／出生身份CPU秒差除墙钟秒，再取样本均值；
漏采的短生存进程／出生退出间隔未计，不是完整CPU计费。全部client含其他闲置owner，RSS求和可重复计共享页，
24GiB只是观察停止阈值，非内核硬内存限；CPU分配service0–7／clients8–15已核实际affinity。
共驻原生Ray逻辑资源不等于物理独占核，表中闲置CPU开销真实存在，不能宣称无干扰。
GPU busy是滚动采样活动百分比，不是MFU；最大温度66°C。服务RSS、各分组首／后续资源、每查询覆盖在JSON。
连续Ray object-store实际已用字节、逐对象存活及纯prefill／decode／kernel／传输拆分unavailable；
逻辑Core计数／配置额度／服务累计直方图不能补作这些未采指标。

首轮后625个恰好四个固定PID／出生身份、无client后代的采样中，总RSS从5535883264至8456990720字节，
增加2921107456字节（2.720GiB）；LOTUS／Daft／Ray／SemLoom分别增加757518336／854904832／756121600／552562688字节。
新增worker不能解释这组增长；分配器、缓存、活动准备或仍持有数据的具体原因尚未追踪，不能证明或排除泄漏。
16次Animals Ray／SemLoom调用分别采到22／19个临时后代，跨轮身份不复用，各外层结束后首采样均已消失。
SemLoom在Movie19次／Animals8次已记录的Core held tasks／input／result／inflight／work及open jobs均零；其他臂没有同类Core观测。
最终client数0、自有survivors空、正常lease释放，
独立终态检查GPU compute apps空。“归零”指进程退出，不表示存活期间RSS回落或整机显存／滚动busy为零。

### Movie useful-forward MFU估算

这是一项架构特定的有效前向FLOPs估算，不是实测硬件FLOPs。用实际每请求API的prompt tokens P及completion tokens O，
首个输出的logits来自prefill，随后O−1次decode；服务计入的终止token沿API usage计数，未直接留存逐请求EOS ID。
实际28层、hidden3584、FFN18944、Q heads28／KV heads4、vocab152064；FMA按2 FLOPs、因果注意力按有效下三角计：

`T=P+O−1; F=13050576896*T+200704*T*(T+1)+1089994752*O`。

普通decode KV保留；prefix cache关闭，speculative=None，实际初始化日志核同BF16、未量化、eager、TP1／PP1，
Movie抢占／prefix命中计数0。忽略padding、重计算、norm／softmax／rotary／sampling等，无法还原实际kernel执行量。
分母峰值165.2×10¹² FLOP/s来自[NVIDIA Ada白皮书v2.02附录A第30页](https://images.nvidia.com/aem-dam/Solutions/geforce/ada/nvidia-ada-gpu-architecture.pdf)
的4090 dense BF16／FP32 accumulation额定boost峰值；未用330.4稀疏值，未作实测持续频率校准。

每臂聚合严格为`Σ有效F/(峰值×Σ所选不重叠窗口秒)`，不是平均逐查询比率；完整query窗口含准备与原生执行，
服务窗口为每查询首dispatch至末body，含内部空洞、队列与传输。下表排除查询间隙，不能把某臂首末span中的其他臂工作计入它的分母。
| Movie owner | 首轮完整query % | 后18完整query %／分母s | 后18服务窗 %／分母s | 全19完整query %／分母s |
|---|---|---|---|---|
| LOTUS | 28.0710 | 28.3701／1327.263300 | 28.6390／1314.799789 | 28.3542／1401.785791 |
| Daft | 27.8574 | 28.2518／1332.817392 | 28.7580／1309.358998 | 28.2308／1407.911109 |
| Ray Data | 23.8358 | 27.0149／1393.841157 | 30.1873／1247.364745 | 26.8266／1481.604896 |
| SemLoom | 28.1360 | 28.2995／1330.573176 | 28.6973／1312.130219 | 28.2908／1404.923344 |
四臂顺序campaign全部152000请求的分子262644566431629312 FLOPs，第一dispatch至最后body墙钟5831.336892760秒，
比值27.2640421836%。76条服务窗总计5469.585540001秒，另含361.751352759秒查询间隙；
不含服务／owner启动和末尾退出。这是四臂整段campaign占额定峰值的估算，不是单臂排名，也不是跨臂利用率的算术平均。
没有加总并发请求的18681.460403775秒；那样会得到错误的8.51035%。实际API token总数与服务prompt19576916／generation608000核同。
Animals整体MFU为unavailable：视觉塔／processor-grid FLOPs尚未实现，不能以文本2N或GPU busy补齐。

### 原件、审计与离线恢复

原件完整保留在r1／r2归档，压缩3050521126字节（3.051GB／2.841GiB），SHA256
`1bd416479de5604dddfcd7c8a5c6dbe47532f11d70cf2973a90968a5f4a5e54a`；
1902个原件共6691489218字节，服务器与独立Mac副本的归档SHA及全部成员字节／SHA已核同。互斥原始体积构成为：
| 原件类别 | 文件数 | 未压缩字节 |
|---|---|---|
| r2 Animals | 320 | 3476531324 |
| r2 Movie | 1424 | 2777001322 |
| r2 service-metrics＋resources | 2 | 342090386 |
| r2其余控制／账本／源码等 | 53 | 45113519 |
| r1全部失败原件 | 103 | 50752667 |
其中32份Animals原生`requests.jsonl`共3458515136字节，包含原有图像请求体；外层未另复制图像，但原驱动本身并不轻量。
Movie也保留原请求／协议／展开summary等审计原件，连续服务与资源观测共342MB。轻量预估漏计了这些原驱动产物；
不据manifest推算各类别压缩后大小，不删除或重写原件。权重、独立数据集、凭证／runtime env及SSH材料未纳入。

两项独立离线审计分别从逐请求P/O与矩阵形状、分组／时间边界／PID出生身份／文件manifest复算；
76条MFU、12个Movie分组及8臂首／后续／全部查询和请求分位与发布值一致，无未解决必修项。
独审未启动模型、SSH或改动运行数据；归档全成员核验复用已完成的备份收据，并非独审再次解压核验。
首次本地汇总的全局gateway ID唯一性断言失败后，按实际服务重建边界修为`(kind, gateway_request_id)`；原失败保留，新增模型请求0。

维护者按上述run id及SHA提供原件、`final-export.jsonl.gz`（SHA256
`52cd2ac40e20dd2c524b16eed8a16c52c54e419780717f17a7fb63a1bc8632a6`）及原始`EVIDENCE_MANIFEST.json`
（SHA256 `9c361209534ad20e93511bb331cb57acbfa02bd060f786a2d6b473b78ec701f6`）。
compact export保留所有请求的P/O、完成／重试状态和dispatch／body时刻、全部108查询、7774资源点及终态；
逐发生项／完整响应／完整错误及其余阶段证据仍从原归档恢复，export不取代它们。
复算需要Python3.11+和标准库；命令只读已有文件，输出应与JSON完全相同：

```bash
python long_run_recompute.py --export /path/to/final-export.jsonl.gz --manifest /path/to/EVIDENCE_MANIFEST.json --output /path/to/long_run_summary.json
```

本节不新增Q2／Q3、PG、PZ、CLIP／HSE、多GPU或其他模型资格；不从本次重复次数升级质量等价或方法收益结论。
