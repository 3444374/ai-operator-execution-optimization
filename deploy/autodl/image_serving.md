# 图像模型与原生 CLIP 入口

图像任务按实际模型、processor、输入与执行系统声明配置。
环境按[runtime](../runtime/README.md)和[平台入口](README.md)检查，driver 与模型环境保持隔离。

[run_lotus_image_retrieval.py](../../code/scripts/experiments/run_lotus_image_retrieval.py)与
[qualify_lotus_clip.py](../../code/scripts/experiments/qualify_lotus_clip.py)保留原生 CLIP 查询和资格检查入口。
模型、processor revision、dtype、decode／resize／normalize、输出维度及归一化由实际任务绑定；
共享 worker 的职责和支持范围见[原生 CLIP 方法](../../code/src/semantic_methods/lotus/README.md#原生-clip-图像检索)。

CLIP embedding、独立 vLLM pooling 服务与 SemBench Movie Q2b 的视觉模型分别核对。
已有 COCO／CLIP 结果不能代替 Q2b 接线或任意多卡、DAG 与新模型资格；当前限制见[代码状态](../../code/INFRA_STATUS.md)。
COCO importer 仍使用[workload schema](image_documents_workload_key.sql)，因此此共享 SQL 保留。

运行需绑定输入身份、图像处理归属、系统原生图、资源 owner、质量与最终关闭证据。
按实际边界区分 source、CPU 预处理、传输、模型、fan-in 和完整结果可用时钟。
历史 60K、多 Job、双卡静态点和旧命令已退出当前指引，原字节与恢复来源见
[旧配方归档](../../experiments/reproduction/legacy_recipes_20261105/README.md)。
