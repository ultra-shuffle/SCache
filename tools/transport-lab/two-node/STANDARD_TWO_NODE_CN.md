# 双机标准 Spark 与 SCache 节点测试

本版修改第 60 阶段：两台机器各启动一个 SCache 数据节点和一个 Spark Worker；Spark Master、SCache Master 位于 A。只修改测试脚本，不修改 Spark、SCache、JNI 产品代码，不覆盖原单机脚本。

## 验证范围

1. 检查 A/B 是两个独立系统，不能用同一机器的两个 IP 代替。
2. 启动真实 SCache 进程，以 Netty/TCP 分别完成 A→B、B→A 的 1 MiB 远端取数、内容校验及删除。
3. 启动两个 Spark Worker，检查两端各有且仅有一个本次 SCache 数据节点、一个 Worker。
4. 执行标准 Spark smoke、HiBench WordCount/Sort/TeraSort/Repartition、TPC-DS 全部 99 个 SQL 文件（103 条语句）。检查内容及任务事件，要求两端均执行任务且没有失败任务。
5. 再次检查节点拓扑，收集日志，清理本次启动的进程。

**Spark 使用标准 Shuffle，不通过 SCache。SCache 是独立的双向通信测试。本阶段不使用 UB、UMDK 或 JNI，不能代表 RM/CTP 链路通过。**

## 两台目标机如何运行

两端解压本次独立 RM/CTP 交付包，保留其中的 JDK、Spark、SCache、测试 JAR 和数据。在 A 上配置并运行；无需分别手工启动 B。包的完整名称和解压顺序见随包交付说明。

```bash
TEST_HOME="$PACKAGE_HOME/tools/ub-two-node-rm-ctp"
cp "$TEST_HOME/config.env.example" "$TEST_HOME/config.env"
```

编辑 `config.env` 中这些部署信息，其余保持默认：

| 配置 | 含义 |
|---|---|
| `B_SSH`、`SSH_OPTIONS` | A 登录 B 的账号、密钥；必须能够免交互登录 |
| `A_IP`、`B_IP` | 两台机器互通的管理网 IP |
| `A_PACKAGE`、`B_PACKAGE` | 两端已解压的 `spark-3.5.7-rm-ctp-benchmark-aarch64` 目录 |
| `A_WORK`、`B_WORK` | 两端可写工作目录，不能放在原包目录内 |
| `SHARED_ROOT` | 两端均可读写、相同绝对路径的共享文件系统目录 |

脚本会实际检查共享目录的双向可见性，不会自动配置 NFS。两端运行账号需要相应读写权限，并允许测试端口互通。第 60 阶段无需填写 UB 设备和 provider，默认 `MODE=rm-ctp` 也不会触发 UB 测试。

```bash
bash "$TEST_HOME/60-standard-benchmarks.sh" \
  --config "$TEST_HOME/config.env"
```

不要在 B 再运行一遍该入口，A 会自动部署并控制 B。失败后修正环境、使用相同命令重跑；每轮保留独立日志，不重编产品代码。

## 成功标志与日志

```text
SCACHE_NETTY_TWO_NODE_PASS
TWO_NODE_TOPOLOGY_PASS
HIBENCH_PASS
TPCDS_PASS files=99 statements=103
TWO_NODE_STAGE_PASS phase=60
```

以最终第 60 阶段 PASS 为准，不能只看某一项 PASS。A 上 `$A_WORK/logs/phase-60-*/REPORT.txt` 汇总结果、两端进程和任务分布；失败时终端打印诊断，同时保留完整日志。需要反馈问题时，优先发送本次 `REPORT.txt` 和 `terminal.log`。

## 本次双虚拟机验证方式

开发测试使用两台独立 ARM KVM 虚拟机，每台 8 vCPU、16 GiB，通过虚拟网卡跨机通信，共享测试目录使用 9p。不是一个系统里的双进程模拟，也不是 UB 设备模拟。

`tests/run-two-vm-standard.sh` 是开发机专用的一键测试入口：新建两份增量镜像、启动两台虚拟机、配置隔离网络与共享挂载、执行第 60 阶段、归档日志并关机。它依赖已有 ARM 基础镜像和 SSH 初始化文件，不是目标硬件部署入口。具体实测结果见 `VALIDATION.md`。
