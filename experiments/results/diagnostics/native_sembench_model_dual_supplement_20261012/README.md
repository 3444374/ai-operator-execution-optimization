# 原Q2双卡缺项的独立补测

当前为补测前的启动诊断与计划，尚未建立新增模型服务。[原轮](../native_sembench_model_20261012/README.md)固定3bd53587执行源码，单卡18槽与双卡11槽已发布；原85分钟窗口结束，原首次时钟、发送账本和两次退出1保持。

## 缺项与新增预算

原双卡已完成第一轮J I S R D N，以及第二轮N D R S I；只补第二轮J、第三轮D N J I S R。字母沿原计划的六路径。

每缺项仍用原公开CLI、同路径预热1／测量1／max_attempts240，共7个预热、7个测量查询，计划1680逻辑模型调用／5040三跳POST。新独立上限1800秒、2048逻辑／6144POST，时钟从本次外层controller启动开始，含服务建立、查询和清理；最后120秒留给清理，服务建立累计不超过900秒。失败与未知均扣记，不扩大额度。

只用已有两卡、缓存Qwen2.5-7B-Instruct、原vLLM及依赖。执行源码、数据／原gold、提示／生成／parser、119完整值摘要的120出现60/60路由、服务参数和原生控制保持。driver／作者CPU72–81，API为82–89和90–97。两个服务重新建立，历史请求从0开始；原单卡及双卡已完成槽位不重复。

新服务状态与分轮有混杂，新增样本按独立窗口报告，不写成原轮连续n3。不增加匹配对照或据此归因双卡加速；若需匹配对照，须另有具体有限方案。完整查询与ready查询、request分别报告n／P50／P95／P99／max及全部值，预热单列。

[原矩阵与预算摘要](ORIGINAL-SUMMARY-AND-BUDGET.json)保留单卡六路径全部值。原轮实际7680逻辑／23040POST、2964.56秒，启动累计140.11秒；剩余旧额度不用于新窗口。

## 启动JSON诊断

原失败CLI PID297104已安装发送观察并等待release，preflight退出0，最终marker有效，release与execute完成记录不存在，该槽0模型调用。原控制程序保存了JSONDecodeError，但未保存当次读取字节和逐帧traceback。

本地在实际`await_cli_release`写标记和`release_cli`读标记的函数接点，暂停覆盖写后的空文件或部分文本，各重复3次。旧代码6/6触发JSONDecodeError；空文件3次都为`Expecting value: line 1 column 1 (char 0)`，与原错误一致。该复现为零HTTP／模型／GPU的真实文件操作。

最小改动是首次marker与cli_waiting更新先写完整同目录临时文件，再原子替换；父程序继续检查PID／出生身份、发送hook与helper摘要。相同受控暂停下6/6正常完成；子进程退出、helper错误、出生身份错误和持久坏JSON仍拒绝。发送入账／HTTP hook／选择及返回函数的AST保持，六个其他helper逐字节保持；比较的code/src和作者实现未改。

机制已在本地复现，原现场的瞬时空读仍是受源码与最终有效marker支持的推断。原失败不是模型答案JSON解析；持久损坏与进程提前退出不符合现存marker／等待状态。断连不参与本地复现，原远端异常已经写入控制结果；现有证据不支持将其写成模型服务失败。

- [旧代码复现](bootstrap-old-reproduced.json)、[原子发布验证](bootstrap-fixed-verified.json)、[函数与摘要核对](bootstrap-repair-provenance.json)、[最小差分](bootstrap-marker-repair.diff)。
- [离线复现脚本](reproduce_bootstrap_race.py)、[原reader](bootstrap_reader.py)、[旧marker实现](pre_send_ledger.original.py)、[修补marker实现](pre_send_ledger.atomic.py)。脚本用AST取原函数接点，不导入SDK或模型包。

```bash
python3 reproduce_bootstrap_race.py --ledger pre_send_ledger.original.py --support bootstrap_reader.py --expected old-fails --output old-replay.json
python3 reproduce_bootstrap_race.py --ledger pre_send_ledger.atomic.py --support bootstrap_reader.py --expected fixed-passes --output fixed-replay.json
```

启动前有限Spec／Standards审阅必修均0。[旧结果路径回放](controller-prefix-verified.json)保存生成器误改旧目录所产生的启动前FileNotFoundError及修正后的七槽核验；[额度函数回放](budget-reserve-verified.json)用纯SQLite合成记录核2048逻辑、6144POST及两类清理时间拒绝，不产生真实HTTP。

[载荷检查](payload-privacy-proof.json)逐项核对15文件／22954字节，无凭据、SSH／ENV内容、模型、CSV或请求文本；state仅保留同一服务器既有资产路径引用。准备上传两次被自动审批拒绝，认为既有授权不足以涵盖具体文件与目的地；两个操作均未执行。新增服务／模型调用／POST均0，新30分钟时钟尚未写入，等待本人确认具体范围。

实际补测记录后续追加到本目录；原失败和原轮报告继续保持。[执行方案](../../../plans/语义系统对照.md#sembench-native-model-missing-dual)说明新窗口与原样本的关系。
