# Spark 3.5.7 双机 UB 分阶段测试

这是基于 2026-09-29 RM/CTP 构件、于 2026-10-15 对照官方 UMDK v25.12.0 审查的独立副本，不覆盖原单机脚本或固定三包。在机器 A 执行入口，通过 SSH 在 B 执行对应操作。两端安装目录、工作目录、设备名、provider 和 EID 可分别指定。

**RM/CTP 的 10–70 阶段现已具备调度入口，但 30/40/50/70 尚未在真实双机 UB 硬件上通过。** 40/50/70 必须使用本次预编译的 RM/CTP SCache/Spark JAR 与包内对应 JNI 源文件，不能指向旧 RC 运行包；20 会针对目标机厂商 SDK 编译 JNI，不要求另备完整 Spark/SCache 源码包。脚本的模拟测试只验证调度，不证明 UB 链路可用。60 使用标准 Spark Shuffle，与 UB 无关。

## 1. 各阶段的递进关系

新包解压后，先在 **A、B 各自**运行不依赖 UB/UMDK 的本机预检，确认包内
Spark 3.5.7/JDK 17、关键 JAR 指纹、脚本语法、离线控制流测试，以及参考 UMDK 下的 JNI/native 构建与9项 CTest：

```bash
bash "$PACKAGE_HOME/tools/ub-two-node-rm-ctp/00-package-preflight.sh" \
  --workspace "$HOME/ub-source/rm-ctp-preflight-work"
```

成功标志是 `PACKAGE_PREFLIGHT_PASS`，终端会给出 `REPORT.txt`。内部的 `01-offline-native-tests.sh` 也可单独重跑，并保留完整 CMake/CTest 日志。它不建立
UB 连接，也不替代下面的 10–70 阶段。预检失败时直接转发该报告；不要继续
排查设备，因为此时交付包或主机基础命令尚未通过。

| 入口 | 双机版本执行内容 | 前提与结果 |
|---|---|---|
| `05-shared-check.sh` | A/B 相互读写同一路径的随机标记，提前确认 benchmark 所需共享目录 | 不依赖 UB/UMDK；只验证目录双向可见，不创建 benchmark 数据 |
| `10-detect.sh` | 两端运行环境、Java/Spark 构件一致性、厂商 UMDK、指定设备及 provider 检测 | 查询通过不等于链路通过 |
| `20-build-vendor-jni.sh` | 分别使用 A/B 的厂商 UMDK 编译各自 JNI；输入未变时复用 | 不把 A 编译的 `.so` 直接复制给 B；只验证编译与链接 |
| `30-native-cross-node.sh` | A server/B client，以及 B server/A client，各跑 READ、WRITE | 默认测 4 KiB 与两端较小的应用子请求上限；无 SCache/Spark；保存两端命令、输出、退出结果 |
| `40-scache-transfer.sh` | 原生测试后，A 启动 SCache Master，两端各一个数据节点，双向 PUT/FETCH 和校验、URMA 对账、资源回收 | 使用新构建的 RM/CTP SCache/JNI；成功与否待真机确认 |
| `50-spark-ub-smoke.sh` | 40 后，A 启动 Spark Master、两端各一个 Worker，执行分布式 Shuffle smoke | 必须两台都有成功任务、UB 字节和目的池字节增加、无 fallback、资源回收 |
| `60-standard-benchmarks.sh` | 每台一个 SCache Netty 数据节点、一个 Spark Worker；SCache 双向取数后跑标准 Shuffle smoke、HiBench WordCount/Sort/TeraSort/Repartition、TPC-DS 99 文件/103 statements | 不依赖 UB；核对进程数、两个 Worker 注册及两端实际任务；需要共享目录 |
| `65-scache-netty-smoke.sh` | Spark driver 与两端 executor 以 strict 模式接入 SCache Netty，验证双机 Shuffle smoke、两端块发布和任务分布 | 检查 Spark↔SCache 软件集成；仍不涉及 UB，不需要共享目录 |
| `66-scache-netty-benchmarks.sh` | 用同一双 Worker 集群完成标准参考与 SCache Netty 两套 HiBench WordCount/Sort/TeraSort/Repartition、TPC-DS 103 条语句，并逐条比较 SQL 结果 | 软件集成全量预检；需要共享目录，仍不涉及 UB |
| `70-ub-benchmarks.sh` | 40/50 后，在同一个双 Worker 集群先跑标准结果参考，再跑 UB HiBench/TPC-DS，比较结果及 UB 指标 | 需要共享目录和已通过的真实 SCache 传输；尚未真机验证 |

每个入口是独立运行。成功时先确认 **A、B 两端应用与 Spark Worker 都已停止**，再扫描两端本会话进程，避免 PID 文件丢失时漏掉仍活着的子进程；确认后才停止任一端 SCache。如果一端仍有应用进程，两端 SCache 均保留。单独执行 50、70 时会执行所需的前置链路验证。JNI 在依赖路径、依赖文件和 JNI 源码未变时复用，不复用上次的 Spark/SCache 进程。**40/50/70 中 SCache 启动后的失败默认进入保守隔离，不会自动停止两端 SCache 或释放注册内存**，因为仅凭脚本退出码不能证明厂商 provider 已完成所有 DMA。

正常停止向本会话进程组发 TERM，最多等待 30 秒；若仍存活，日志列出 PID/PPID/PGID、进程状态与已运行时间，不使用 KILL，也不停止 SCache。Spark Master 的关闭钩子可能超过原先的 10 秒等待窗口；延长窗口是为避免把正常退出误判成清理失败，并不放宽生命周期门禁。

## 2. 两台机器需要准备什么

- A/B 各有新的 `spark-3.5.7-rm-ctp-benchmark-aarch64` 安装目录，包含本次重新编译的 Spark/SCache、RM/CTP JNI 源码及原三包的测试依赖和数据；两端应用构件版本一致，厂商 UMDK、驱动和 provider 分别在本机安装。
- **仅执行60时不要求安装UMDK、UB驱动或JNI。** 新包已包含 `runtime/scache/SCache-ub-integration-test.jar`，用于启动和控制 SCache 测试节点。
- A 到 B 的 SSH 密钥登录已配置；脚本使用 BatchMode，不记录密码，不修改 sshd、驱动、EID、路由或防火墙。
- 两个不同的管理网 IPv4 地址；脚本核对 IP 属于对应机器，并通过 boot ID 拒绝误把同一运行内核当双机。
- UB fabric、两端 EID 及模式由厂商配置。30 用真实对端地址，不使用 `127.0.0.1`。
- 60/70 需要已挂载的共享文件系统，A/B 在**同一个绝对路径**读写。现有测试 JAR 使用本地文件 API；只复制两份输入不能解决 HiBench 分布式输出校验。脚本在两端相互读写随机运行标记，确认确实共享后才执行，并将 TPC-DS 输入复制到本次共享目录。

依赖系统命令：`bash`、`ssh`、`tar`、`timeout`、`flock`、`ss`、`ip`、`setsid`、`nohup`、`ps`、`sha256sum`。脚本使用包内 JDK/Spark，清除常见旧 Java/Spark 环境覆盖；不自动安装软件。

## 3. 配置与执行

本目录随新包放在 `tools/ub-two-node-rm-ctp`。脚本会把本次节点执行器及配置复制到 B 的工作目录，不覆盖 B 的原安装包。

```bash
TEST_HOME="$PACKAGE_HOME/tools/ub-two-node-rm-ctp"
cp "$TEST_HOME/config.env.example" "$TEST_HOME/config.env"
vi "$TEST_HOME/config.env"
```

必须填写 `B_SSH`、`A_IP/B_IP`、两端 `PACKAGE/WORK` 目录。`PACKAGE` 必须指向新目录 `spark-3.5.7-rm-ctp-benchmark-aarch64`；脚本还会验证该包的构建标识和三个关键 JAR 的 SHA-256，拒绝旧 U1 包。设备、provider、EID 和优先级默认 `auto`。示例 IP 是占位地址，脚本拒绝原样运行。
安装、工作、共享数据及厂商 SDK 路径均使用不含空格/制表符的绝对路径；包内 native 构建工具链不支持含空白路径，脚本会提前给出明确错误。

当前目标机报告 RM/CTP 能力为 1，先设置 `MODE=rm-ctp`。10 查询两端实际能力、有效 EID 和优先级表；只在候选唯一时自动选取。若打印 `PRIORITY_SELECTION_REQUIRED candidates=[6, 7]`，确认采用的优先级后只填写对应节点的 `A_PRIORITY` 或 `B_PRIORITY`，再运行10；其他自动字段不必填写。不能仅凭 A 的报告替 B 选择。

10 按选定设备的 `driver_name` 匹配 UDMA/UBAGG，检查实际加载库、有效 EID、模式能力、设备节点权限和活动状态；不再将 `/sys/class/ubcore/ubcore` 当设备。确认结果保存在两端 `WORK/state/device-selection.json`。30 重新查询并比对，自动复用选择；库、头文件、内核、能力或选定 EID 变化时停止并打印差异文件位置，要求重新运行10，不静默切换。

系统需有 `python3`。10 使用包内 GCC（缺失时查系统 C 编译器）编译一个只读能力查询程序，不编译或修改 Spark/SCache/JNI。当前按同版本 RPM 管理的厂商 SDK 校验；非 RPM 安装需先确认其版本证明与安装方式，不能当作已验证支持。额外安装位置可配置 `A/B_SDK_ROOT` 或 `A/B_SEARCH_ROOTS`。不自动安装驱动。

10另做两项只编译/链接、不执行的可选接口检查，输出 `SDK_LINK_INTERFACES` 和 `discovery/sdk-link-interfaces.json`：完整Jetty/MR上下文的get/put接口，以及配套 `urma_ubagg.h` 的显式聚合context接口。缺少这些可选接口不直接判为硬件失败；完整编译错误留在各自sdk日志。检查通过也不证明provider实现或物理链路可用。

10还保存官方 `urma_admin show topo` 与 `/sys/bus/ub/ub_feature` 到 `discovery/urma-admin-topology.log`、`platform-ub-feature.log`，不猜配对EID、不修改路由。读不到或工具不支持只保留错误，能力状态记unknown；选中UBAGG且明确观察到UTP-UNO关闭时，提示其内部管理通信可能受影响，不把应用RM/CTP说成UM/UTP，也不单凭该提示宣布互通失败。

新版JNI在配套SDK具备明确接口时，按官方B081/6月24日perftest的默认配置，在创建队列前设置 `standalone + IODIE`；设置失败立即报告并清理，不偷偷继续使用PORT默认值。没有该配套接口的旧SDK保持provider默认并打印 `level=unverified`，不能认为已与另一版工具完全同路径。编译状态记录 `bond_context_policy`；不硬编码user_ctl opcode，也不使用B081中存在opcode歧义的通用setter。这项对齐不消除旧聚合MR交换内部的UM/UTP依赖，现场仍须经过30/40验证。

JNI提交前按配置QD控制未完成WR数量，队列满先在原提交deadline内poll CQ；不依赖所有provider都以EAGAIN表示SQ满，也不盲目重试ENOMEM。容量等待超时或CQ错误仍保留已接受请求的内存与lease，按原quarantine流程处理，不以提前回收换取快速返回。

`NATIVE_EXTRA_ARGS` 仅用于不改变当前应用链路的测量参数；它不会传给 SCache。不能覆盖模式、设备/EID/priority、共享 JFR、端口或测试大小，也不能只给 perftest 加 simplex/flat API、TP-aware/user TP、异步导入、保序、token policy、`aggr_mode/bond_mode/bond_level` 等切换。否则30测试的不是40阶段JNI使用的同一路径；应先做独立官方工具诊断，确认后再实现对应JNI适配，不能将30的通过直接推广到SCache。

原生测试固定传入 `-j true`，与应用 Jetty 的共享 JFR 配置一致。固定官方v25.12.0会在检测到UB设备时将share_jfr设为true；脚本显式传参是为避免依赖厂商版本的默认处理。此前终端中的 `URMA_TRANSPORT_UB only support share_jfr` 是调整提示，本身不能当作导致退出的根因。目标工具若没有共享JFR参数，第10阶段会明确报告。

`UB_CHUNK_BYTES` 默认10MiB，是JNI配置上限。当前通用JNI还公开SEND，端点协议仅协商一个共同raw上限，因此暂按 `max_msg_size`、`max_read_size`、`max_write_size`、配置值和对端上限取小值；这比单独READ所需更保守，不代表官方单边读写只有消息上限或MTU那么大。要去掉这一额外限制，需先分别协商SEND与READ/WRITE上限，不能只改本机计算丢失对端SEND约束。第30阶段默认 `NATIVE_SIZES=(4096 application)` 测试4KiB与两端较小的当前应用子请求上限，相同则去重，计算与JNI一致。若perftest另有限制，保留其实际失败日志及第40阶段应用门禁，不仅凭4KiB结果宣称应用路径已验证。

按顺序执行：

```bash
bash "$TEST_HOME/05-shared-check.sh" --config "$TEST_HOME/config.env"
bash "$TEST_HOME/10-detect.sh" --config "$TEST_HOME/config.env"
bash "$TEST_HOME/20-build-vendor-jni.sh" --config "$TEST_HOME/config.env"
bash "$TEST_HOME/30-native-cross-node.sh" --config "$TEST_HOME/config.env"
```

原生阶段不依赖 JNI，可以跳过20，但必须先完成10的设备确认，再执行30。

第30阶段日志中的 `AB/BA` 是历史测试组名，不单独表示 payload 方向；以 `server/client`、`initiator/target` 和 `payload_from/payload_to` 为准。READ 的数据从 target 流向 initiator，WRITE 则相反。两个测试组分别交换服务端/客户端，因此 READ 和 WRITE 都覆盖了双向数据传输。

填写并验证 `SHARED_ROOT` 后，可独立运行完整标准测试：

```bash
bash "$TEST_HOME/60-standard-benchmarks.sh" --config "$TEST_HOME/config.env"
```

60会自动启动A端SCache Master、Spark Master，两端各一个SCache数据节点和Spark Worker；先完成两个方向的1MiB SCache Netty远端取数及内容校验。之后Spark明确设置`scache.enable=false`并使用标准Shuffle，不会将SCache独立服务检查计作Spark经过SCache或UB。四项HiBench使用小规模确定性输入，TeraSort/Repartition附带Hadoop 3.3.4 examples JAR，检查记录数与完整内容；TeraSort额外检查全局有序。它们是正确性回归，不是吞吐性能对比。

若需在上真实 UB 前单独确认 Spark 与 SCache 的集成，再运行：

```bash
bash "$TEST_HOME/65-scache-netty-smoke.sh" --config "$TEST_HOME/config.env"
```

65 使用 SCache Netty 后端，要求 driver/两端 executor 均在 strict 模式初始化 SCache、两端实际发布 Shuffle 块且任务均参与；不将其结果计作 UB。

若需继续跑完整软件集成工作负载，并已有两端同路径的共享目录：

```bash
bash "$TEST_HOME/66-scache-netty-benchmarks.sh" --config "$TEST_HOME/config.env"
```

66 会同时生成标准与 SCache Netty 结果，校验 HiBench 内容及 TPC-DS 全部 103 条语句。它不使用厂商 URMA，也不是 UB 性能测试。

应用开始和结束时分别检查两台机器的进程数，要求每台恰好一个测试SCache节点、一个Spark Worker，且PID属于本次运行；检查Spark Master实际注册的两个ALIVE Worker地址。每个应用的事件日志要求两端都有成功任务，无失败任务。相同boot ID、多启动Worker、只在一台执行等情况不能算通过。

模拟环境复现使用`tests/run-two-vm-standard.sh NEW_LAB_DIR PREPARED_ARM_IMAGE PACKAGE_DIR [60|65|66]`（默认60）：创建两个独立ARM虚拟机增量磁盘，通过独立虚拟网卡互通，9p只用于提供包和共享测试数据。它不模拟UB，实际执行标准Spark及SCache Netty，产品源码不变。该实验室脚本只用于具备KVM/QEMU及既有镜像的研发宿主机，不应在生产目标机执行。

RM/CTP 原生跨机读写通过后，使用本次重新构建的应用包继续执行：

```bash
bash "$TEST_HOME/40-scache-transfer.sh" --config "$TEST_HOME/config.env"
bash "$TEST_HOME/50-spark-ub-smoke.sh" --config "$TEST_HOME/config.env"
bash "$TEST_HOME/70-ub-benchmarks.sh" --config "$TEST_HOME/config.env"
```

聚合设备的 RC/RTP 仍需厂商确认单路径支持；目标机当前报告 RM/CTP 能力，不能把 `--bond_level port` 当作 RC 单路径的等价替代。RM/CTP 应用源码已在独立副本中完成接口适配和离线测试，但是否与目标设备实际互通必须由 30、40、50 的真机结果判定。

## 4. 网络端口与目录

| 用途 | 端口（管理网络 TCP） |
|---|---|
| A→B SSH | 按 SSH 配置，通常22 |
| 原生测试 | 默认两个不同大小时19190–19197；若 `application` 与4096相同会去重为19190–19193。第30阶段打印实际 `NATIVE_PORT_RANGE`，两端均需可达 |
| SCache | A 的19070；A/B各自19071、19072；UB控制19073；60的Netty数据端口19074 |
| Spark | A的19777、19778、19779、19880；A/B各自19181、19780、19881 |

这些 TCP 端口用于控制和标准 Spark 通信，UB 流量需要另外配置的 UB 链路。脚本不自动开放端口。固定端口被占用时保留原进程并报错。

```text
A_WORK/
├── state/                         # 独立双机状态、JNI构建结果
├── jni-vendor/                    # A本机编译结果
├── runs/<session>/                # A本次进程、配置、日志、Worker目录
└── logs/<session>/
    ├── REPORT.txt                 # 可直接转发的一份汇总
    ├── terminal.log
    ├── status.txt
    ├── A-logs.tar.gz / B-logs.tar.gz
    └── A-evidence/ / B-evidence/   # 两端本次完整进程日志
B_WORK/                            # B对应构建与运行目录
SHARED_ROOT/<session>/             # 本次benchmark输入、输出、SQL结果
```

失败会自动打印 `REPORT.txt`，含两端日志末尾；全部日志保留。远端失联时收集可能失败，终端会注明 B 的工作目录仍需保留。测试成功只接受 `TWO_NODE_STAGE_PASS phase=XX`；不要将 `STEP_PASS` 当作整阶段成功。

正常完成时停止本次创建且会话标识匹配的进程组，保留日志。若终端被强行关闭、SSH中断或停止失败，先查看本次 `REPORT.txt` 和两端进程状态；仅对已确认没有未完成 UB 请求的会话，使用原 `SESSION` 手动清理：

```bash
bash "$TEST_HOME/90-stop.sh" --config "$TEST_HOME/config.env" \
  --session phase-30-YYYYMMDD-HHMMSS-XXXXXX
```

`SESSION` 从实际终端输出复制；脚本不会按名称杀死所有 Spark 或 SCache。停止失败会保留日志和会话目录；互斥锁只在调度器或人工停止进程运行期间持有，需处理原会话后再继续。
人工停止入口会先尝试获取与阶段调度器相同的工作目录锁；若原测试仍在运行，打印 `REFUSE_ACTIVE_ORCHESTRATOR` 并拒绝并发清理。确认原调度器退出后再处理原会话，不要为了绕过互斥保护而使用另一份工作目录。

若报告出现 `DMA_UNRESOLVED_QUARANTINE`，表示脚本**无法证明**已接受请求全部完成，不等于已证明存在未完成 DMA。脚本保留两端进程、注册内存和租约，并标记阶段失败；`90-stop.sh` 默认拒绝清理。不要直接重启节点或删除工作目录。先让设备/驱动负责人按其恢复流程确认 provider 所有权已解除；确有明确确认后，才可由操作员运行 `90-stop.sh ... --provider-cleared`。此参数只记录人工确认，脚本自身无法验证 CQ 状态。隔离期间若资源不足，应停止后续测试，不通过强制清理制造一次“通过”。

## 5. 已验证与未验证

本次新构件已在两个独立 ARM 虚拟机上跑通第60阶段：双机标准 Spark smoke、HiBench WordCount/Sort/TeraSort/Repartition 及 TPC-DS 全部103条语句，两台 Worker 均参与。第65/66阶段也已在双机通过，确认 Spark/SCache 的 Netty smoke 和完整工作负载结果；10–70 调度脚本通过离线控制流和节点检查。第20阶段的厂商 JNI、第30/40/50/70阶段的真实 UB 测试尚未执行。详见 `VALIDATION.md`。

虚拟机测试不包含真实UB硬件。第30阶段的真实跨机RC/RM链路、第20阶段的厂商JNI编译，以及第40/50/70阶段的UB应用运行仍待目标机验证。不能将第60阶段通过理解为UB Shuffle通过。

模式与工具依据：[openEuler URMA 用户指南](https://docs.openeuler.org/zh/docs/24.03_LTS_SP3/unifiedbus/unifiedbus/urma/URMA%20User%20Guide.ch.html)。该文档说明 `--share_jfr` 的作用和默认关闭状态；它不等同于 CTyunOS B103 的硬件实测结果。
