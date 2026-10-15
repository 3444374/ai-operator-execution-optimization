# 固定 main 的原 Movie／Animals 实际模型检查

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
