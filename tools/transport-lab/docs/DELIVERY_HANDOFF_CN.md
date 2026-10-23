# 223 交付位置与最终核验

以下均在 10.0.3.223，不在 controller；工作根目录：

```text
/home/xinzhang/urpc-spark-scache-20261021
```

## 文件

- 预编译压缩包：delivery/spark-3.5.7-scache-urpc-simulator-aarch64.tar.gz，约 925 MiB。
- 完整部署说明：docs/DEPLOY_SIMULATION_CN.md；包内同内容为 README_CN.md。
- 改动/原理/构建依据：docs/CHANGELOG_CN.md、ARCHITECTURE_CN.md、BUILD_PROVENANCE_CN.md。
- 产品独立分支：experiment/urpc-sim-20261021；源码在 repos/，补丁在 patches/。

压缩包 SHA256：

```text
a1df6eba38b909e9580c4911005d9cef2bc2f1181281baeb8908c519a32a4345
```

## 已实际通过

最终预编译副本全测 logs/csm-qry_6c32（位于 delivery 解压目录内）：
HiBench 四项、TPC-DS SF1 的 99 文件/103 语句、全部结果/schema 对比通过。
独立审计两端均有实际执行的 Executor、实际远端 URPC 字节、无 fallback 或
RPC/校验/lease 错误，双方 native 正常关闭。完整摘要随包放在 evidence/。

该压缩包又实际解压到 delivery/relocation-check-ntFDNS，未经编译或修改路径，
运行 Spark Shuffle smoke 成功，远端 URPC READ 37,308 字节、cleanup_errors=[]。
库映射确认 JNI、官方 framework 和 provider 全部来自新的解压目录。
复验终端日志：logs/relocation-smoke.log；报告在新解压目录 logs/csm-rvjev5gj/REPORT.json。

## 当前直接运行

```bash
cd /home/xinzhang/urpc-spark-scache-20261021/delivery/spark-3.5.7-scache-urpc-simulator-aarch64
python3 scripts/run-logical-cluster.py --stage bench
```

无需编译或补下载 JAR/JDK；仍要求 aarch64、glibc>=2.38、Python 3 及足够内存/磁盘。
这是同一 Linux 内核上的两个逻辑节点模拟，不是物理双机，也不是现场 RM/CTP
通过。固定官方 URPC framework 使用 RC，真实 UB、故障恢复与性能仍需另行验证。
