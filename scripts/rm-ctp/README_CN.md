# SCache RM/CTP 实验测试

仅用于本实验分支，不运行Spark，不覆盖原U1或现有三包。不代表已完成真实双机验证。

当前完整交付使用运行包的 `tools/ub-two-node-rm-ctp/10–70` 入口。本目录保留的是早期仅SCache适配入口，不要把两套 state/配置或成功标志混用。2026-10-15 同步为多CTP优先级需明确选择，不再默认猜6。

## 一键运行（目标机使用这个入口）

两台机器均保留已解压的原单机三包，并解压本次独立 RM/CTP 测试包。测试包包含本分支编译好的测试类与匹配的 Java 依赖；目标机不用再准备 sbt/Maven。

在两台机器各执行一次下面的命令，填写各自的对端管理网 IP：

```bash
read -r -p '对端管理网IPv4地址：' PEER_IP
bash "$HOME/ub-source/scache-rm-ctp-20260928/scripts/rm-ctp/run-all.sh" \
  --peer-ip "$PEER_IP"
```

入口自动完成：

1. 从已保存路径、原包默认位置、测试包附近及 `$HOME/ub-source` 查找完整工具包与版本匹配的测试类；失效的旧路径重新查找。
2. 调用原单机检测脚本查找目标机厂商 UMDK、设备和 provider；使用包内 JDK 17、GCC/G++、CMake、Make 构建 JNI。输入及产物不变则复用。
3. 本地 JNI 加载及协议/生命周期自检。唯一 ACTIVE 设备和唯一有效、地址非全零的 EID 自动选择（索引可为0）；CTP 优先级候选唯一才自动选择，多候选时用 `--priority` 明确指定并保存复用。
4. 根据路由确定本机管理网 IP，较小 IPv4 为 A，自动选择空闲控制端口。两端准备完成后协调启动，无需盯着 READY 手动启动另一端。
5. 运行双向 SCache RM/CTP 数据读取、内容校验及资源检查；两端均退出成功才输出 `SCACHE_RM_CTP_TWO_NODE_PASS`。不启动 Spark。

准备期间两端可相差最多 30 分钟（`--wait-seconds` 可调整）。同步端口默认 TCP 19173；控制端口在 TCP 19171–19272 中选择相邻两个空闲端口。需允许两端管理网相互访问这些端口，脚本不修改防火墙。

默认工作目录：`$HOME/ub-source/ultrashulfe-work/scache-rm-ctp`。完整输出既在终端显示，也保存在 `logs/auto-*/summary.log`，末尾打印 `FINAL_REPORT`。失败发送两端各自这一个汇总文件即可；下层原始日志仍保留。

非默认安装位置使用 `--search-root /实际父目录`，工作目录使用 `--workspace /实际工作目录`。多设备/多 EID 无法唯一确定时，可加 `--device`、`--eid`；脚本不随意改选硬件。已经选定的参数会保存并在后续准备阶段重新验证。

只做构建和本地自检、不启动设备：

```bash
bash "$HOME/ub-source/scache-rm-ctp-20260928/scripts/rm-ctp/run-all.sh" --prepare-only
```

目录变化、已有旧 Java、有效的 JNI 缓存等由入口处理。缺少厂商 SDK/驱动、权限不够、硬件能力不支持或实际 DMA 失败仍必须停下并给出证据，不能靠跳过校验或反复重试变成成功。实际 UB 请求发出后不自动重跑。

## 开发者：原单机包适配与分阶段入口

`run-package.sh`直接调用原单机包的`00-detect-vendor-umdk.sh`和`build-lib-spark-urma.sh`，不调用旧RC测试。JDK17、GCC/G++、CMake、Make均使用包内版本；实际UMDK来自目标机。旧包不改，新JNI在独立工作目录生成。

构建机先导出本分支测试类和全部Java依赖（第二个参数为导出父目录）：

```bash
bash scripts/rm-ctp/prepare-tests.sh "$HOME/scache-rm-ctp-build/classes" \
  "$HOME/scache-rm-ctp-build/exports"
```

该步骤仍需构建机已有的`sbt 1.11.7` launcher（设置`SBT_LAUNCH`）。输出`RM_CTP_RUNTIME_EXPORTED=...`；将这个完整目录和本实验源码一起复制到目标机。目标机不用sbt、Maven或下载Java依赖，不能拿旧包SCache JAR代替导出目录。

两台目标机分别在本实验仓库根目录执行：

```bash
PACKAGE_HOME="$HOME/ub-source/ultrashuffle/spark-3.5.7-benchmark-runtime-aarch64"
TEST_WORK="$HOME/ub-source/ultrashulfe-work/scache-rm-ctp"
read -r -p '新RM/CTP运行目录的绝对路径：' CLASSES
bash scripts/rm-ctp/run-package.sh --phase check \
  --package-home "$PACKAGE_HOME" --workspace "$TEST_WORK" --classes "$CLASSES"
```

此步自动搜索厂商SDK、用包内工具编译新JNI、运行离线检查；不要求设备/EID/priority选择。源码、SDK、编译入口和产物校验不变时输出`RM_CTP_VENDOR_JNI_REUSED`，不重复编译。运行目录选择保存到工作目录。

再确认设备参数：

```bash
bash scripts/rm-ctp/run-package.sh --phase detect \
  --package-home "$PACKAGE_HOME" --workspace "$TEST_WORK"
```

优先查询`urma_admin show`唯一ACTIVE设备，再由原单机脚本匹配provider；从`show --all --whole`查询有效EID和CTP优先级。设备/EID/priority均需候选唯一或显式指定；例如同时列出6、7时，用`--priority`选择现场确认的一项，后续重新验证并复用。不修改驱动、网络或设备配置。此阶段只确认选择，RM/CTP细粒度能力由新JNI在初始化时再次校验，不代表链路通过。

两端设备确认后，先运行A，看到READY后60秒内运行B：

```bash
read -r -p '本机角色A或B：' ROLE
read -r -p '对端管理网IPv4地址：' PEER_IP
bash scripts/rm-ctp/run-package.sh --phase node \
  --package-home "$PACKAGE_HOME" --workspace "$TEST_WORK" \
  --role "$ROLE" --peer-ip "$PEER_IP"
```

本机IP根据到对端的路由发现，可用`--local-ip`覆盖；默认控制端口19171、A协调端口19172，先检查占用，不停止已有进程。尝试提高当前进程memlock，不修改系统配置；至少预留3MiB池+64MiB余量，provider实际需求可能更高。每端需不同工作目录，不能共享state。

成功要求两端`RM_CTP_SERVICE_PASS`且退出0。失败直接返回终端输出或本次`logs/package-*/terminal.log`，命令、检测、编译及Java异常均保留。未接Spark，也不会自动运行旧10–70应用阶段。下面是原有开发者底层入口，通常无需逐项手工配置。

## 1. 每台机器准备

两端均使用本分支相同提交、JDK17。准备匹配各自机器的厂商UMDK公开头文件、运行库、provider及驱动。先由厂商工具确认双机RM/CTP读写可用；设备、EID及CTP优先级不能照抄另一台机器。

在本仓库根目录操作，设置以下环境变量为该机器的实际绝对路径：

| 变量 | 内容 |
|---|---|
| `JAVA_HOME` | JDK17根目录 |
| `SBT_LAUNCH` | sbt 1.11.7 launcher JAR |
| `URMA_INC` | 包含公开`urma_api.h`的目录 |
| `URMA_LIBRARY` | 厂商`liburma.so`链接文件 |
| `URMA_COMMON_LIBRARY` | 厂商`liburma_common.so`链接文件 |
| `UMDK_LIBRARY_DIRS` | 上述库运行时目录，以冒号分隔 |

不使用包内参考库代替目标机厂商SDK，不要求`ub_util.h`或OpenURMA源码。

```bash
export TEST_WORK="$HOME/scache-rm-ctp-test"
export SPARK_URMA_BUILD_DIR="$TEST_WORK/native-build"
export SPARK_URMA_INSTALL_PREFIX="$TEST_WORK/native-install"
bash native/spark_urma/build_spark_urma.sh
bash scripts/rm-ctp/prepare-tests.sh "$TEST_WORK/classes"
export SCACHE_JNI_DIR="$SPARK_URMA_INSTALL_PREFIX/lib"
export SCACHE_TEST_CLASSPATH_FILE="$TEST_WORK/classes/classpath.txt"
bash scripts/rm-ctp/run-node.sh --check "$TEST_WORK/check"
```

`--check`加载实际JNI，运行协议/生命周期测试；数据路径测试使用测试拷贝后端，不访问设备。成功标志`SCACHE_RM_CTP_LAUNCHER_CHECK_PASS`不能解释成硬件通过。源lease隔离测试会故意打印一次关闭失败堆栈，最后必须出现`SOURCE_LEASE_SAFETY_PASS`。

## 2. 两台机器执行真实服务测试

在各自终端设置`DEVICE`、`EID_INDEX`、`CTP_PRIORITY`、`LOCAL_IP`、`PEER_IP`，对应本机设备和对端管理网地址。沿用上一节环境变量。两端均使用19171作为SCache控制端口；A额外监听19172，仅交换阶段标记。确保端口未占用且防火墙允许；脚本不会修改防火墙或驱动。

每端注册3×1MiB内存池，memlock需留有余量；不足时按系统权限调整后重试，不要绕过错误。实际锁定需求以provider为准。

先在A执行：

```bash
bash scripts/rm-ctp/run-node.sh A "$TEST_WORK/node-A" \
  "$DEVICE" "$EID_INDEX" "$CTP_PRIORITY" "$LOCAL_IP" 19171 "$PEER_IP" 19171
```

看到`RM_CTP_SERVICE_READY`后，60秒内在B执行：

```bash
bash scripts/rm-ctp/run-node.sh B "$TEST_WORK/node-B" \
  "$DEVICE" "$EID_INDEX" "$CTP_PRIORITY" "$LOCAL_IP" 19171 "$PEER_IP" 19171
```

两端都必须出现`RM_CTP_SERVICE_PASS`并退出0。仅一端通过不算成功。本测试使用生产服务、JNI和URMA；不需要SCache Master、Spark、共享文件系统，pool文件只在各自机器本地使用。

覆盖：双向READ，64/700000/700001字节，跨arena，逐字节与CRC，两消费者释放，源/目的块回收、URMA字节对账、失败计数为零、关闭成功。未覆盖：多机超过两个节点、压力性能、真实链路故障恢复和厂商退出时DMA撤销行为。

## 3. 失败时返回

终端会显示`LOG=.../launcher-*`。返回该目录的`terminal.log`和`status.txt`即可，日志包含完整Java异常、选定库及hash、ldd和运行命令。若存在CQ/provider错误，再附同一时间段目标机内核日志。

遇到`ERR_DMA_UNRESOLVED`或`UB_CLOSE_QUARANTINED`，不得将失败当作普通可重试读写，也不要在同一进程释放/复用池。保留日志，确认provider排空或厂商进程退出资源撤销机制后再重新测试；当前实验实现没有自动恢复承诺。

原10–70部署脚本及其RC门禁没有修改。不要直接绕过旧脚本门禁或把本分支JNI单独替换到原U1 JAR中：控制协议2与JNI API3要求两端Java/Scala/JNI一起更新。
