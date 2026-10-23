# 2026-10-21 最终功能验证记录

工作位置：10.0.3.223 的 /home/xinzhang/urpc-spark-scache-20261021。
原源码、原交付包、现场内核/驱动未修改，没有推送 Git。
独立分支：experiment/urpc-sim-20261021；精确提交见 BUILD_PROVENANCE_CN.md。

## 最终完整测试：通过

最终预编译交付副本直接运行（不调用编译器）：
delivery/spark-3.5.7-scache-urpc-simulator-aarch64/logs/csm-qry_6c32。
终端记录：logs/portable-bench.log；独立审计：logs/portable-bench-audit.json。
REPORT.json 的 result=PASS、cleanup_errors=[]、tpcds_reference_matches=103。

| 应用 | 退出码 | 远端 URPC READ 字节 |
|---|---:|---:|
| Spark Shuffle smoke | 0 | 37,212 |
| HiBench WordCount | 0 | 4,406 |
| HiBench Sort | 0 | 168,985 |
| HiBench TeraSort | 0 | 66,851 |
| HiBench Repartition | 0 | 84,842 |
| TPC-DS SF1 全 103 条语句 | 0 | 698,218,979 |

标准 Spark 同样运行四项 HiBench 和全部 SQL；四项输出通过内容校验。
TPC-DS 99 个 SQL 文件 / 103 条语句的结果和 schema 共 206 份文件逐字节一致。
独立审计 Spark event log，确认上述每个应用两端 Executor 均实际执行任务。
这里不是只启动了两个空闲 Worker，也不是只跑标准 Spark。

A 节点 11,525 次 RPC，读取 348,602,933 字节；
B 节点 11,725 次 RPC，读取 349,978,342 字节。
两端 rpcCalls=rpcCompleted，rpcFailed=0；fallback、TCP/Netty payload、
checksumErrors、leaseErrors、generationErrors、activeLeases、outstanding 均为零。
两端均打印 SCACHE_URPC_CLOSE_PASS staging_live=0 arena_grants=0。
加载映射明确来自本交付副本的 libScacheUrpc.so、liburpc_framework 和模拟 provider。

完整报告与独立审计摘要随最终运行包放在 evidence/，完整原始日志保留在 223。
结果只适用于本记录的最终二进制组合；不要用早期部分通过日志代替。

## 分层证据与修复

- logs/jsm-x5KL88：最终标准构建的 JNI 双 JVM 读写、分块、负例与关闭通过。
- logs/jsm-y6thAj：独立 32 KiB 候选的 64 MiB 全内容回读通过，不作为硬件性能指标。
- logs/ssm-xBM4Pn：实际 SCache 双向三块、CRC/lease 与关闭通过，每端 1,400,065 字节。
- logs/csm-xjpo2nf0：旧 4 KiB 版本全测失败证据，保留，不是当前通过报告。

旧版在 q50 出现控制连接超时。将 raw payload 提至 32 KiB，并在获取远端
lease 前串行准入，避免调用已持 lease 还在阻塞队列中排队。本轮 q50 已通过，
并完成剩余语句和全部结果校验。没有以放宽正确性检查或 Netty 回退绕过失败。
完整改动与官方依据见 CHANGELOG_CN.md 和 ARCHITECTURE_CN.md。

## 生成器来源复核

重新编译固定 OpenClickNP 生成器，与正在使用的 SystemC 拓扑比较：
仅源路径注释和一条 UB flit include 不同，归一化后逻辑代码相同。
这避免把旧生成目录当成未知来源；详细边界见 BUILD_PROVENANCE_CN.md。

## 不能外推的结论

本次是同一 ARM Linux 内核上的两个逻辑节点，不是两台 VM 或真实双机。
官方固定 URPC framework 使用 RC；不能证明目标硬件 RM/CTP 通过，
不能解决或否定现场 UBAGG/UBMAD UM/UTP 导入兼容问题。
RPC 使用 staging 和响应拷贝，不是原来的 one-sided URMA READ，不是零拷贝。
每进程一个 peer、阻塞传输；崩溃、重连、恶意输入、长期压力未全面验证。
不要把本测试时间当成真实 UB 硬件性能。

初始里程碑记录另保存在开发目录 HISTORY_INITIAL_MILESTONE_CN.md；
它仅为历史排错材料，不代表当前完成状态，也不作为交付验收依据。
