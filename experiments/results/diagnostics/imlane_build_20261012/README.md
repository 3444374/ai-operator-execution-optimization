# IMLane作者构建与Arrow软件检查

固定作者源码`072f8257db80569e42c9f5727d839587df529652`，本项目交付为`fdd6a227`，
接续分支中对应`fa53eb16`。构建入口见[IMLane目录](../../../../code/integrations/imlane/README.md)。
原源码与patched源码均完成编译；patched版处理嵌入式Python事件循环和Ray信号处理器恢复。
pristine版完整运行尚未检查。

Python与作者Ray各完成三次真实DBEnd／Coordinator／Arrow调用；每种模式315行，
共630行、132次函数调用。Arrow16.1.0与Ray2.56.1，独立8CPU、Ray2CPU／0GPU、128MiB对象存储。
两个模式的Coordinator正常退出，自有存活进程和命名共享内存均为0；无真实模型、重试或GPU任务。
这份记录只支持作者构建、连续Arrow调用及所测退出，不证明原SQL／LOTUS方法接入、任意UDF异常或查询取消。
后续完整方法接入由[原生执行报告](../../postgresql/native_executor_baselines_20261012/README.md)维护。

原件位于仓库外`native-executor-20261012/imlane/imlane-evidence-20261012.tar.gz`，
大小7,756,695字节，SHA256 `ad2a353480e48750230b6b44ea290deff3b02f66b6d49ba648a1554d3d17ed4c`。
恢复时先核摘要，再读取包内`artifacts/evidence-summary.json`、build identity及成功／失败日志；
源码、环境与二进制继续放在仓库外。
