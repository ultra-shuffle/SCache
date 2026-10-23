# 在 223 上构建与运行 URPC 模拟实验

工作目录：

```text
/home/xinzhang/urpc-spark-scache-20261021
```

这是独立实验，不要覆盖原来的 RM/CTP 交付包。
实现边界见 [ARCHITECTURE_CN.md](ARCHITECTURE_CN.md)。

## 已有源码与依赖

`repos/SCache`、`repos/spark-3.5-scache`、
`repos/OpenURMA`、`repos/umdk` 是独立副本/新分支；
`repos/systemc`、`repos/OpenClickNP` 是模拟器依赖。
`runtime/package` 是独立运行包副本。
旧包、旧源码、旧 qcow2 和真实主机网络/驱动均不需要调整。

当前构建复用 223 已有 JDK 17 和离线 sbt 依赖缓存。
新的 SCache JAR、测试 JAR、libScacheUrpc.so 已有构建入口，
但重新构建不能与正在运行的 cluster 同时进行；
构建脚本会用同一锁拒绝覆盖活跃实验使用的二进制。

## 分层检查

在 223 执行：

```bash
cd /home/xinzhang/urpc-spark-scache-20261021
bash scripts/build-provider.sh
bash scripts/build-jni.sh
bash scripts/build-scache.sh
bash scripts/jni-smoke.sh
bash scripts/scache-service-smoke.sh
python3 scripts/run-logical-cluster.py --stage smoke
```

这些脚本的含义分别是：

- provider 构建：编译新分支中的模拟 provider；需要已有 SystemC facade 构建产物。
- JNI 构建：连接固定的官方 URPC framework，不构建旧直接 URMA JNI。
- SCache 构建：包含新 backend 和集成测试，产物在 repos/SCache/target。
- JNI smoke：独立两个 JVM，跨分块读写、越界拒绝、错误 generation 拒绝、关闭检查。
- service smoke：实际 SCache arena/lease/consumer 流程，双向远端块传输。
- Spark smoke：两个 SCache 节点、两个 Spark Worker；要求实际远端 URPC 字节大于零。

注意：任何源码更改后，不能只重编 JNI 而忘记配套 SCache JAR。
若使用候选 native/Java class override，那只是独立组件实验；
最终必须重新 assembly 并验证最终交付组合。

## 全套测试

```bash
cd /home/xinzhang/urpc-spark-scache-20261021
python3 scripts/run-logical-cluster.py --stage bench
```

脚本会创建新的 logs/csm-* 工作目录，自动启动并清理本轮自己的进程：

1. SCache Master、A/B 两个 SCache 节点。
2. Spark Master、A/B 两个 Worker（各 2 cores）。
3. URPC Spark Shuffle smoke。
4. 标准 Spark 的 WordCount、Sort、TeraSort、Repartition 和 TPC-DS。
5. URPC/SCache 的相同工作负载。
6. 检验 HiBench 输出，比较 103 份 TPC-DS 结果和 schema。
7. 先停止 Spark，再并发停止两端 SCache，并确认 native 关闭成功。

HiBench 输入是 100,000 行文本 / 10,000 条 TeraSort 记录；
TPC-DS 使用现有 SF1 Parquet 数据、99 个 SQL 文件、103 条实际语句。
这是功能和结果一致性验证，不是可信硬件性能测试。

当前实验不需要 A/B 免密 SSH、共享文件系统挂载或现场 UMDK 安装，
因为 A/B 是 223 上的两个逻辑节点；不能拿这个结论替代真实双机部署条件。

## 看什么才算成功

终端最后必须出现 `URPC_CLUSTER_RESULT=PASS`。
同时检查打印路径下的 `REPORT.json`：

- result 为 PASS，所有 application exit 为 0；
- bench 模式要求 tpcds_reference_matches 为 103；
- cleanup_errors 为空；
- 每个 urpc application 的 remote_urpc_bytes 大于 0；
- 两端指标中 urpc.rpcFailed、checksumErrors、fallbacks、TCP/Netty payload 为零；
- 两端 scache 日志有 SCACHE_URPC_CLOSE_PASS。

只看官方例程 PASS、只看标准 Spark 通过、只看一个 SQL 成功，均不足以验收。

## 失败后保留哪些信息

直接发送本轮终端输出、REPORT.json 和失败 application 的 *.log；
另附对应的 *-before-metrics.json / *-after-metrics.json、
scache-A.log、scache-B.log。每个进程的命令和 PID 也保存在同目录。

不要删除当前工作目录、盲目重跑安装或强行解除活跃锁。
若提示端口占用，先核对上一轮 REPORT.json / PID / 进程命令，
不要终止其他用户的 Spark、VM 或任务。

预编译包的无编译运行入口见 DEPLOY_SIMULATION_CN.md；
这里是 223 开发环境入口，不能直接当成真机 UB 部署文档。
