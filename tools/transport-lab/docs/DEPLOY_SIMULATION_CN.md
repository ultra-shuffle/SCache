# 预编译 URPC Spark/SCache 模拟包部署

本包在同一台 aarch64 Linux 上运行两个逻辑节点，每个节点有独立 SCache JVM、
Spark Worker 和 Executor。数据经过官方 URPC framework 和 OpenURMA 模拟器；
不是物理 UB，也不是两个独立操作系统。不要覆盖原 RM/CTP 交付目录。

## 条件与包含内容

- aarch64 Linux，glibc 至少 2.38；不要替换系统 glibc。
- Python 3、bash、常用系统命令；建议至少 8 cores、16 GiB 可用内存，
  解压后的工作目录至少 10 GiB 空闲磁盘。
- 不需要 root、硬件 UMDK、UB 设备、SSH 双机互信或共享目录挂载。
- 已包含 JDK 17、Spark 3.5.7、SCache/测试 JAR、HiBench、TPC-DS SF1 数据、
  JNI、固定官方 URPC/liburma、模拟器、OpenSSL/libstdc++/libgcc；目标机不编译。
- 系统加载器和 glibc 仍由目标系统提供，实际启动是最终兼容性检查。

固定本机端口：19070–19073、19181–19182、19777–19780、19880–19882、
25190–25191。节点绑定 127.0.0.1/127.0.0.2；有占用时先协调，不要杀其他服务。

## 在 223 上直接运行

```bash
cd /home/xinzhang/urpc-spark-scache-20261021/delivery/spark-3.5.7-scache-urpc-simulator-aarch64
python3 scripts/run-logical-cluster.py --stage smoke
python3 scripts/run-logical-cluster.py --stage bench
```

smoke 仅测 Spark Shuffle，bench 包含 smoke、HiBench 四项和 TPC-DS 全量。
脚本校验文件哈希、glibc、架构、磁盘并自行启动/清理本轮进程。
无需手工设置 JAVA_HOME、LD_PRELOAD、EID、config.env 或编译 JNI。

## 移到其他兼容主机

在保存最终压缩包的目录执行，解压到新建的专用目录：

```bash
mkdir -p "$HOME/urpc-simulation"
tar -xzf spark-3.5.7-scache-urpc-simulator-aarch64.tar.gz -C "$HOME/urpc-simulation"
cd "$HOME/urpc-simulation/spark-3.5.7-scache-urpc-simulator-aarch64"
python3 scripts/run-logical-cluster.py --stage smoke
python3 scripts/run-logical-cluster.py --stage bench
```

模拟 wire 自动使用短 /tmp 路径，运行包可换父目录。使用无空格的可写路径。
本包不修改内核、驱动或网络，也不运行真实 UB 设备。

## 验收与失败日志

最后必须是 `URPC_CLUSTER_RESULT=PASS`，对应 REPORT.json 的 result=PASS、
cleanup_errors 为空。bench 还需 tpcds_reference_matches=103 和四项 HiBench 校验。
完整模式先跑标准 Spark，再跑 URPC/SCache，对比 103 份结果和 schema。
每个 URPC 应用需实际远端字节大于零，错误/fallback/TCP/Netty payload 为零，
且两端日志均有 SCACHE_URPC_CLOSE_PASS；不能用标准 Spark 成功冒充 URPC 成功。

失败时发送本轮终端输出和 logs/csm-*/REPORT.json，再附失败应用 *.log、
scache-A/B.log、该应用前后 *-metrics.json。命令、PID、库映射和事件日志均保留。
不要删除工作目录或盲目重装；重跑新建日志目录，不覆盖上轮证据。

本包只验证模拟功能，不能证明生产故障恢复、零拷贝、真实 RM/CTP 或硬件性能，
尤其不能证明现场 UBAGG/UBMAD 的 UM/UTP 兼容问题已经解决。
