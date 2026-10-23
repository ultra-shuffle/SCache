# Spark / SCache：6 月 24 日 UMQ RM/CTP 预编译模拟包

本包用于 ARM Linux 上的两个逻辑节点功能验证，不是现场真实 UB 双机安装包。
它不安装内核、不替换系统 UMDK、不配置 SSH/网络，也不修改旧交付目录。

## 固定版本

UMDK 公开源码固定为 099343116b63aad37e2f60f99b71e772bf6bf009，
内核源码参照固定为 41b5f26ff0e91e8af102aa248117e6210b486a40（6 月 24 日）。
本包不依赖 10 月版 UMQ 接口，也不使用旧 URPC framework 的 RC 队列。

此 UMDK 的 urma_api.h SHA256：
c334280559eb312982c8a8a3e6250e9413161878a6245be96e928b239bd23d54。
它与此前现场采集的头文件一致，但不能据此宣称与 CTyunOS B103 所有私有 ABI 相同。
公开 SDK 在仓库中保持未修改；模拟接口实现和 SCache 适配位于独立源码分支。

## 运行方式

在一台空闲的 aarch64 Linux 测试机解压到新目录，然后执行：

```bash
tar -xzf spark-3.5.7-scache-umq-rm-ctp-simulator-aarch64.tar.gz
cd spark-3.5.7-scache-umq-rm-ctp-simulator-aarch64
bash scripts/run-simulation.sh smoke
bash scripts/run-simulation.sh bench
```

不需要编译，不需要 sudo，也不需要真实 UB 设备。包内已有 JDK17、Spark3.5.7、
SCache/JNI、UMQ 库、HiBench JAR 和 TPC-DS SF1 数据/SQL。
系统仍需 Python3、bash、基础动态加载器与足够新的 glibc。
实际 ELF 所需 glibc 下限写在 metadata/umq-manifest.json；入口会检查，
不建议替换系统 glibc。至少预留 10 GiB 可用空间；建议 16 GiB 以上可用内存。
实验会使用两端各约 1 GiB UMQ 初始缓冲池，外加 JVM、SCache arena 和 Spark。

smoke 只运行 Spark Shuffle 冒烟。bench 先运行冒烟，再分别运行标准 Spark 和 UMQ
路径的 HiBench WordCount/Sort/TeraSort/Repartition，以及 TPC-DS 99 个 SQL 文件中的
103 条语句。标准 Spark 是结果对照，不是 UB 模式失败后的回退。
HiBench 是小规模功能数据，TPC-DS 使用 SF1；不是生产性能评测。

成功标志为 UMQ_CLUSTER_RESULT=PASS。每次打印 RUN=...，完整报告位于该目录：

```bash
cat 日志目录/REPORT.json
python3 scripts/audit-cluster-report.py 日志目录/REPORT.json
```

审计脚本只适用于完整 bench 报告。审计检查两个 Executor 均执行任务、每个 UMQ
应用有远端字节、RPC 无失败、无数据回退、103 条结果及 schema 一致，且两端正常关闭。
失败请发终端输出和该目录 REPORT.txt/REPORT.json、scache-A.log、scache-B.log、
失败应用日志；无需先打包整个安装目录。保留失败工作目录，先确认进程状态再重跑。
若启动端口已占用，入口拒绝启动，不会结束其他任务。

## 实际链路与模拟边界

Spark Shuffle → 本机 SCache IPC → SCache UMQ backend → JNI → 官方 libumq /
libumq_ub → 官方 liburma → 新 RM/CTP 功能 provider → 本机 UNIX datagram 模拟通道 →
对端同样的官方库 → 对端 SCache arena。

官方 UMQ 队列显式配置 UB + RM + CTP，优先级 6。模拟 provider 拒绝 RC、
非 CTP 导入和 RC bind；旧 RTP flit 提交路径在此 profile 中为硬失败。
UNIX 通道是模拟设备内部连线，不是可部署到两台真实机器的网络协议。
127.0.0.1 / 127.0.0.2 是同一个 Linux 内核上的两个逻辑节点，不是两台物理服务器。

本轮采用 UMQ BASE 消息 API 实现 READ/WRITE 请求响应。应用 arena 是软件授权窗口，
不是向对端直接导入的 DMA MR；数据经 UMQ 已注册缓冲池拷贝，最大分片 3840 字节，
单 peer、单在途请求。因此验证了软件调用/数据一致性，不能证明零拷贝或性能优势。
也没有模拟 CTP 的硬件报文、拥塞、重传和时序，更没有验证 UBAGG/UBMAD 私有控制通路。

## 实机仍需完成

用现场配套的 6 月 24 日 SDK/厂商 B103 头文件及库重新构建 JNI并核对符号；
选择双方可达的设备/EID/CTP 优先级；先运行官方 UMQ 双机小消息测试，再运行 SCache
和 Spark。真实 provider 不得使用本包的 LD_PRELOAD / 模拟设备目录。
若聚合设备的 UMQ bind 本身也触发 UBMAD 的不支持 TP 类型，仍需厂商修复或确认
可用物理 UDMA 路径；本轮通过不能直接证明现场导入问题已解决。
