# 验证记录

日期：2026-09-29。

## 2026-09-29 独立 preflight 包追加验证

- 在隔离副本中增加硬件无关的 `00-package-preflight.sh` 和双机共享目录 `05-shared-check.sh`。前者在 ARM 主机 10.0.3.223 完成架构、包构件指纹、JDK17、Spark3.5.7、包内工具链、HiBench/TPC-DS 资产、脚本/控制流检查，及参考 UMDK 下重新构建并通过 8/8 项 native CTest，整体 `PACKAGE_PREFLIGHT_PASS`；空包负例被明确拒绝。前者不访问 UB 设备。
- 包内 `libSparkUrma.so` 已在 223 上以参考 UMDK、包内 GCC12/CMake/JDK17 **重新编译链接**并得到 `LIBSPARKURMA_BUILD_PASS`。这是参考软件 ABI 检查，不是厂商 UMDK 或硬件测试。
- 第60阶段在两个全新、独立 boot ID 的 ARM 虚拟机上完整重跑：会话 `phase-60-20260929-192233-DCogdG`，最终 `TWO_NODE_STAGE_PASS`。标准 Spark Shuffle 完成小规模 HiBench WordCount、Sort、TeraSort、Repartition 全部四项；完整记录内容一致，TeraSort 全局有序。四项 Spark 任务均分布到 A/B，失败任务0。TPC-DS SF1 99个SQL文件、103条语句、12,675输出行，3,396个任务A2,266/B1,130、失败0。
- TeraSort/Repartition 使用包内 Hadoop 3.3.4 examples JAR 和确定性 10,000 条固定宽度记录。首轮 Repartition 因1 MiB输入只有少量任务，全部由 A 执行而被双节点门禁拒绝；调整 Hadoop 输入切片为64 KiB后完整重跑并通过。失败轮保留，不计入通过结果。
- 证据位于 223 的 `/home/xinzhang/rm-ctp-preflight-20260929/vm-standard-four-workloads-split/`，含终端日志、`evidence/`两端日志和 `shared/`输出。该虚拟机测试不模拟 UB，不能证明 RM/CTP 数据传输。
- 第66阶段在另一对新建ARM虚拟机完整重跑，最终会话 `phase-66-20260929-193419-fZEMh3` 为 `TWO_NODE_STAGE_PASS`：Spark/SCache Netty smoke 的 driver/两端 executor 均 strict 初始化并分别发布 Shuffle 块；标准参考与 SCache Netty 各完成四项HiBench的全部内容校验，以及TPC-DS SF1的103条语句。SCache Netty的3,396个TPC-DS任务A2,204/B1,192、失败0；103份结果JSONL与schema逐文件匹配标准参考，`TPCDS_REFERENCE_MATCH_103`。证据位于223的 `/home/xinzhang/rm-ctp-preflight-20260929/vm-scache-netty-four-workloads/`。此阶段不使用URMA/UB。
- 证据归档排除 Spark Worker 中重复复制的 JAR，保留 executor stdout/stderr、两端运行日志和阶段报告。新建双虚拟机的第65阶段复测 `phase-65-20260929-200251-Jk8KVs` 通过，主控归档约 232 KiB，`LAB_EXIT_CODE=0`；B 节点日志已汇集于 A 的阶段归档。此项只验证无 UB 的集成和日志可交付性。
- 第40阶段的显式远端 FETCH 探测改为每个 map 注册两个 reduce 分区但只发布其中一个，避免 map-complete 自动预取先于显式 FETCH 使本地未命中断言误失败。控制流测试已覆盖新的 REGISTER 参数；这是测试布局修正，尚未在硬件上验证。
- 第40阶段双向探测还要求 A、B 各自相对于运行前都有新增 URMA 与目的池字节；故障注入模拟了 B 只有历史计数、此次无新传输，阶段正确拒绝。该断言用于防止单向数据路径通过双向测试。
- 三包重新打包并在 223 的新目录合并解压后，8,361 个普通文件 SHA-256 全部通过；从新解压目录运行 ARM 本机预检，包内 Java17、Spark3.5.7/Scala2.13、工具链和8/8项参考UMDK native CTest均通过。再以该新解压包创建两台全新 ARM 虚拟机执行第66阶段，会话 `phase-66-20260929-200953-xwjVuX` 为 `TWO_NODE_STAGE_PASS` 且宿主实验退出码0：标准与 SCache Netty 各通过四项 HiBench 和 TPC-DS 103/103；SCache Netty 的TPC-DS任务A2,147/B1,249、失败0，103份结果及schema与标准参考逐文件匹配。证据位于223的 `/home/xinzhang/rm-ctp-preflight-extracted-v2-full-bench/`，本地终端副本在 `evidence/fresh-package-phase66-terminal.log`。该复测仍不涉及UB设备。
- 路径含空格的故意测试暴露出包内 CMake/GCC 在 `TryCompile` Makefile 中将编译器路径截断；包内版本、文件和脚本检查均通过，但 JNI/native 构建未通过。因目标交付路径无需空格，安装、本机预检和双机配置改为前置拒绝空白路径并给出具体原因，避免部署后才出现难定位的 `gmake` 失败。此处没有声称已支持含空格路径。
- 针对 `ERR_DMA_UNRESOLVED`/`UB_CLOSE_QUARANTINED` 增加失败清理安全门禁：脚本先查本次两端日志，一旦出现未确认完成的 provider-owned 请求，保留**两端**进程与租约，不执行自动 stop/release；报告明确要求先确认 DMA 所有权。故障注入控制流已验证保留行为。正常失败仍自动清理；真实设备故障处理尚待现场验证。
- 原生 `urma_perftest` 脚本固定使用 `-j true` 以匹配产品 Jetty 的 shared JFR 设置；这是基于目标机先前的 `URMA_TRANSPORT_UB only support share_jfr` 错误和工具帮助，但**尚未在真实硬件验证**。第10阶段提前检查厂商工具是否具有该参数。

本文件保留 2026-09-28 旧运行包与旧 RC 应用门禁的历史结果，并新增下面的 2026-09-29 **新构件**验证。独立副本已改用 RM/CTP SCache/Spark 源码，40/50/70 仍仅通过模拟调度测试，尚未在真实双机 UB 硬件上验证。

## 2026-09-29 新构件验证

- 新包 `spark-3.5.7-rm-ctp-benchmark-aarch64` 在 10.0.3.223 上通过本地标准 Spark smoke、HiBench WordCount/Sort 与 TPC-DS SF1 全部 103 条语句；此检查关闭 SCache/UB。
- 从新构件启动的双 ARM 虚拟机完整执行第 60 阶段，会话 `phase-60-20260929-161939-4QJD8U`，整体 `TWO_NODE_STAGE_PASS`。两个不同 boot ID、两端各一个 SCache Netty 节点和 Spark Worker；SCache 双向 1 MiB 数据校验通过。
- 双机 Spark smoke：40 个任务，A27/B13，失败0。HiBench WordCount/Sort 各6个任务、A3/B3，内容校验通过。TPC-DS：99个SQL文件、103条语句、12,675输出行；3,396个任务，A2,247/B1,149，失败0。
- 新包关键 JAR 的 SHA-256 在两端一致，且双机脚本拒绝没有本次 RM/CTP 构建标识的旧 U1 包。证据位于 223 的 `/home/xinzhang/rm-ctp-build-20260929/standard-two-vm/`，其中 `benchmark-terminal.log`、`evidence/`、`shared/` 保存运行记录。标准路径不是 UB 路径。
- 为隔离验证上层集成，新增第65阶段并在两个独立 ARM 虚拟机运行两次；最终会话 `phase-65-20260929-170355-Veasvw` 为 `TWO_NODE_STAGE_PASS`。40个Spark任务A14/B26，失败0；driver及两端executor均 `SCACHE_READY strict=true`，两端SCache分别记录70/130个Spark Shuffle块，未见 `SCACHE_FALLBACK`。这证明Spark/SCache的Netty集成和双机类加载可运行，不证明UB传输。
- 第66阶段会话 `phase-66-20260929-171115-HR8zc3` 为 `TWO_NODE_STAGE_PASS`：标准参考和 SCache Netty 均完成 HiBench WordCount/Sort 内容校验及 TPC-DS SF1 全部99个SQL文件、103条语句，分别输出12,675行。SCache Netty TPC-DS 的3,396个任务分布A2,216/B1,180、失败0；103份结果JSONL和schema与同轮标准参考逐文件一致。此阶段仅验证 Spark/SCache Netty 全工作负载集成，不含 URMA/UB。
- 本地脚本的16项控制流测试、节点执行器检查、Shell语法检查通过。8项 JNI/native 离线测试及 SCache pool/协议测试通过；参考 UMDK JNI 可编译。真实厂商 JNI、跨机 RM/CTP READ/WRITE、SCache UB fetch、Spark UB HiBench/TPC-DS 仍未实测通过。

## 本轮设备自动发现追加验证

- 新增独立 `discover.sh`、`discover.py`、`device-probe.c`，10确认、30重新查询并复用；原单机包不改。
- `tests/test-discovery.py` 的15项测试通过：设备可见性、UDMA/UBAGG匹配、歧义优先级、多EID、错误provider、RC能力为0、权限与端口状态、保存后复用及环境变更阻断。输入为合成的目标机报告格式，不是硬件实测。
- 原有14项调度测试和节点执行器检查重新通过；全部Shell语法检查通过。
- 查询程序在223的aarch64编译器上以 `-Wall -Wextra -Werror` 编译成ARM目标文件，使用包内参考公开头文件；这不等于与目标机B103库链接/运行成功。
- 新设备发现流程尚未在真实UB双机上执行；此前第60阶段实测不能替代本轮硬件发现和第30阶段链路验证。实际加载库映射、厂商能力、EID和优先级仍需目标机10的结果确认。
- 原单机 `bin/real-ub` 与本轮前保存的 `base-helpers.tar.gz` 执行 `tar --compare` 一致（退出码0）。

- 全部新 Shell 文件通过 `bash -n`；10–70 七个入口的 `--help` 可运行。
- `tests/run-control-flow.sh`：14 个调度场景通过，涵盖10/20/30/40/50/60/70、三个RM应用门禁、两端同时检测失败、原生失败、清理失败及必需指标缺失。SSH/硬件/Spark用桩替代，仅证明流程和状态处理。
- `tests/run-node-checks.sh`：使用真实节点执行器验证 RM/CTP CLI 构造、对端IP、工作目录锁、两端任务事件识别、进程会话归属、孤儿进程组清理、JNI缓存不覆盖新设备名及拒绝错误厂商库缓存。perftest和JNI文件为桩，进程清理使用本次创建的真实 `sleep` 进程。
- 原 `spark-3.5.7-benchmark-runtime-aarch64/bin/real-ub` 全部文件的排序SHA清单摘要在改造前后均为 `000805d2883b2db99f76b331291272fa8bae79c8db05c49ffea64694cd6627ca`，原目录未改。
- 新脚本存放在独立目录；未修改 Spark/SCache/JNI 产品源码；未连接或配置目标硬件机器。

## 双 ARM 虚拟机实测

在 10.0.3.223 的 KVM 上创建两个独立增量磁盘实例，未覆盖原镜像。各8 vCPU、16 GiB，openEuler 24.03、内核6.6；两台分别使用192.168.77.1/2，独立内核和Spark Worker，经隔离虚拟网卡通信。测试共享目录使用9p，先验证双向可见。没有真实 UB 设备，不把这些结果计作硬件验证。

第一轮会话：`phase-60-20260928-123944-OgDh8S`。

- 第60阶段整体 `TWO_NODE_STAGE_PASS`，双机标准Shuffle smoke通过。
- HiBench WordCount、Sort：100,000行、300,000 tokens、1,000 keys校验通过。
- TPC-DS SF1：99 SQL文件、103 statements全部完成，输出12,675行。
- TPC-DS事件日志：3,396个任务，A端2,219、B端1,177，失败0。
- 无UB机器执行第10/20阶段，能够同时报告两端检测失败；第40阶段正确阻断RM/CTP；退出后释放两个工作目录锁。
- 正在运行第60阶段时再次进入同一工作目录，明确拒绝重复运行，未停止原任务。

第二轮会话：`phase-60-20260928-124901-GWb3Vc`。

- 故意注入无效的旧 `JAVA_HOME`、`SPARK_HOME`、`SPARK_CONF_DIR`、`CLASSPATH`、Java启动选项及Spark主机名变量，原配置和产品代码未修改。
- 仍使用包内Java17.0.20.1、Spark3.5.7、Scala2.13.8；Driver与Executor的运行时检查通过。
- 第60阶段再次整体通过，HiBench两项和TPC-DS103条语句完成。
- TPC-DS事件日志：3,396个任务，A端2,318、B端1,078，失败0。
- 两轮103条语句的结果JSONL及schema逐文件比较一致：`REPEATED_TPCDS_MATCH statements=103`。
- 两端均无本次Spark Master、Worker、Executor残留，无测试端口监听，无工作目录会话锁残留。

完整ARM测试证据保存在223的 `/home/xinzhang/ub-two-node-validation-20260928/evidence.tar.gz`；测试会话见上文。新增脚本的本地控制流与节点专项证据在 `/home/mshou/ub-source/ub-two-node-validation-20260928/`。这些是测试记录位置，不是部署所需固定路径。

## 本轮发现并修复的脚本问题

1. 原生客户端纳入会话进程跟踪，便于异常退出后清理。
2. 启动器先退出时仍检查并清理其存活子进程组；保留不停止无关进程的校验。
3. 环境检测不在A失败后遗漏B；两端问题一并汇总。
4. `REPORT.txt`保留明确失败原因、最后步骤及失败命令，避免前置错误仅留在终端。
5. 指标必须唯一且为非负整数；缺失指标不再可能通过空字符串相等检查。
6. JNI复用指纹覆盖SDK头文件和构建脚本；旧JNI状态不再覆盖本次设备选择，厂商库路径不一致时拒绝复用。
7. 清理旧Java/Spark及主机名等环境变量，固定使用包内运行时。

截至旧版本验证，尚未完成真实双机 UB、厂商 JNI 编译或 UB 应用端到端实测；当时 RM/CTP 仅允许原生验证。2026-09-29 独立副本已移除此旧门禁，后续必须以新构件的真实结果判定。标准 Spark 通过不能证明 UB Shuffle 通过。

## 第60阶段追加：每台机器一个 SCache 数据节点与一个 Spark Worker

此次只调整独立双机脚本，未修改 Spark、SCache、JNI 产品代码或原单机脚本。新增 SCache Netty 双向 1 MiB 取数校验、进程拓扑校验，以及自动创建两台独立 ARM 虚拟机的一键测试入口。

实测环境：223 上两台独立 ARM KVM 虚拟机，每台8 vCPU、16 GiB，独立增量磁盘和内核；IP 为192.168.76.11/12，通过虚拟网卡通信；9p提供两端相同路径的共享测试目录。复用原包的Java17、Spark3.5.7、SCache assembly及测试JAR，不重新编译产品。此处不是虚拟UB设备测试。

初轮完整会话 `phase-60-20260928-172053-ybu405`：SCache双向取数、双节点拓扑、Spark smoke、HiBench WordCount/Sort、TPC-DS103条语句全部通过。TPC-DS共3,396个任务，A端2,308、B端1,088，失败0；输出12,675行。103条语句的结果JSONL及schema与上午标准Spark基线逐文件一致。

随后发现并修复：

1. 虚拟机原NetworkManager DHCP配置会撤销临时静态IP；改为在测试虚拟机内建立明确的静态网络配置，宿主机网络不改。
2. SCache Netty原使用随机数据端口；测试配置固定为19074并核对实际监听结果，便于目标机放行端口。
3. `phase-60-20260928-173148-i1YTa6`正确报告失败：完整map发布触发自动预取，显式FETCH之前数据已经抵达消费者。测试改为注册两个reduce分区、只发布其中一个，避免map完成预取与远端FETCH竞争；仍要求消费者取数前本地未命中、数据长度和CRC正确。产品预取机制不改；该服务探测不覆盖map完成预取。
4. 为测试进程提供脚本自带的log4j配置，避免旧包缺少该配置时不断输出无关FileNotFound堆栈。
5. `REPORT.txt`增加两端实际PID、boot ID、Master注册Worker、取数和任务分布证据。每轮日志单独保存，测试结束后清理本次进程；虚拟机测试入口归档两端工作目录并关机。

本地与ARM主机的14项调度测试、节点执行器检查、4项进程拓扑测试通过；15项设备发现测试通过。拓扑测试覆盖正确进程、缺少SCache、重复Worker、PID不匹配。Shell语法检查通过。

### 最终版本完整重跑

会话 `phase-60-20260928-173543-d0urRL`：由 `tests/run-two-vm-standard.sh` 一次启动全流程，在两台新建的增量磁盘虚拟机上执行，未在运行中手工修改文件或配置。第60阶段最终PASS。

| 检查 | 结果 |
|---|---|
| A进程 | SCache PID2009；Spark Worker PID2257，各1个 |
| B进程 | SCache PID1995；Spark Worker PID2347，各1个 |
| 系统身份 | 两端boot ID不同，IP分别为192.168.76.11/12 |
| SCache | 两端Netty数据端口19074；双向1 MiB，取数前本地未命中，长度与CRC一致 |
| Spark smoke | 40个任务，A30/B10，失败0 |
| HiBench WordCount | 6个任务，A3/B3，失败0 |
| HiBench Sort | 6个任务，A3/B3，失败0 |
| HiBench内容 | 100,000行、300,000 tokens、1,000 keys，校验PASS |
| TPC-DS SF1 | 99文件、103条语句，12,675输出行，PASS |
| TPC-DS任务 | 3,396个，A2,290/B1,106，失败0 |
| 查询内容回归 | 与上午标准基线103份结果JSONL及103份schema逐文件一致 |

本轮证据位于223的 `/home/xinzhang/standard-two-vm-verified-20260928/`：`benchmark-terminal.log`保留完整终端输出，`evidence/A-logs.tar.gz`和`evidence/B-logs.tar.gz`保留两端工作目录，`shared/`保留查询输出。Spark/SCache原产品仓库的工作区仍无改动。

注意：此处SCache走Netty/TCP，Spark走标准Shuffle，两项分开测试。不能据此宣称Spark通过SCache取数、RM/CTP成功或真实UB硬件性能已验证。
