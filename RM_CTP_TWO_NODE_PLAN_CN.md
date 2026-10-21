# SCache 双机 RM/CTP 适配方案与审查记录

状态：审查与实现中，尚未通过真实双机 UB 验证。Spark 不在本轮修改范围内。

## 当前结论（先读这一节）

这是基于U1的独立实验版本，不是已验证可交付的硬件版本。RM/CTP模式、完整MR描述、Jetty读写路径和失败内存保护已实现；ARM编译、6个native软件测试、JNI加载、控制协议及两个服务实例的协议联调通过。审查和测试持续超过一小时。

真实双机尚未执行：223没有UB设备，不能替代controller。`RmCtpServiceNode`和`scripts/rm-ctp/run-node.sh`是供下一阶段真实双节点测试的入口，未宣称硬件通过。

已知限制：每进程一个active transport、每节点一个peer；Java transport调用串行化；未知DMA归属时停止新请求并隔离内存，尚未提供自动故障恢复；两端需同时使用新控制协议2/JNI API3，不能与原U1混用。旧RC测试入口没有全部迁移，不用于本轮RM/CTP验收。

不能省略的后续验证：目标厂商SDK编译；目标机设备/EID/priority与provider能力；两进程/两机真实READ及完整服务测试；真实CQ异常/进程退出时驱动的DMA撤销语义。软件替身通过不证明上述事项。

统一回归入口的最新结果：223 `service-launcher/launcher-Sni4Y3Ev/terminal.log`，`exit_code=0`。依次执行实际JNI加载、端点/MR、22项控制帧、源lease隔离、两个服务实例联调。lease测试故意触发关闭失败并核对资源保留，其异常堆栈是预期断言，不代表真实硬件故障。native六项测试另见`baseline/rm-ctp-init.log`。

## 1. 基线与隔离

- 基线：U1 `08d6083aafbe0c4b8812afcfca7ef4604bd31ff6`。
- 当前交付包 `sources/scache-jni/src/urma_transport.cpp` 与该基线相同，SHA-256 为 `67c123ab9a3cb39a788b91b361616e696d2aa247f067231d3dd55fe85e5949fd`。
- 实验分支：`experiment/rm-ctp-two-node-20260928`。
- 223 独立工作目录：`/home/xinzhang/scache-rm-ctp-20260928`。
- 原 U1 工作目录、原分支、现有三包及单机脚本不变；不推送 GitHub。
- `4d29a90` 与 `22057e6` 等既有实验只作审查参考，不整批合并，避免引入未经核对的行为。
- 本轮要求至少一小时代码审查；以下记录不代表已满足该时长，完成时附实际审查时段和测试记录。

## 2. 要实现的链路

两台独立机器各运行一个 SCache 数据节点；保留 SCache TCP 控制协议、节点级 registered arena、块 descriptor 与 lease，payload 经官方 URMA API 的 RM/CTP READ 读取到本地注册目的池。

先不启动 Spark。独立 SCache 集成测试负责 PUT/FETCH、内容校验、并发消费、指标对账与资源回收；原生小程序负责隔离 provider/API 与上层协议问题。

RM 与 CTP 是两个维度，不能只改 `URMA_TM_RC` 常量，也不能用 perftest 通过替代 SCache 通过。

## 3. 已确认的源码问题

| 位置 | 当前行为 | 适配要求 |
|---|---|---|
| `native/spark_urma/src/urma_transport.cpp:init` | JFS/JFR/Jetty 固定 RC，context 固定 EID 0 | 显式模式、EID、priority；核对实际组合能力 |
| 同上数据提交 | 因 vdev 限制而独立创建 JFS，调用 `urma_post_jfs_wr` | 以官方 Jetty READ/WRITE 路径为准，不依赖模拟设备接口限制 |
| `connect/cleanup_locked` | 固定 RC import、无条件 bind/unbind | RM import 后不执行 RC bind/unbind；远端模式必须匹配 |
| MR 注册与查找 | 本地 CPU VA 和远端地址概念混用，重叠检查遗漏部分交叉 | 本地范围只用 CPU VA；远端 descriptor 使用 provider 返回 UBVA |
| remote import | 将 vdev generation 作为公开 import 的映射地址参数 | 硬件路径使用公开 NOMAP 参数语义；块 generation 留在控制面验证 |
| `EndpointDescriptor` | 72字节、协议1；未完整核对模式/传输类型 | 新协议明确模式、TP 与 MR 身份，旧 peer 明确拒绝，不静默解释 |
| `readBlock/writeBlock/wait` | 子请求提交/完成失败后父请求可能先返回 | 错误返回不能授权复用仍被 DMA 使用的 slice |
| `cleanup_locked` | 清理调用失败后仍清空部分句柄 | 分步清理成功才移除所有权；失败保留可诊断状态 |
| `RealUbTransport` | token 限制为正的有符号 int | 核对官方 token 的32位无符号语义，避免误拒绝合法 token |
| `UBBlockTransferService.fetchBlockToPool` | 捕获异常即 abort 目的 slice、释放远端 lease | 区分“已安全终止”与“仍有 provider 所有权”，阻止提前回收 |
| `serve` 的 `finally` / `RegisteredArena.releaseOwner` | TCP断开直接释放源端lease | 断连不等于DMA排空；保留未确认完成的源块，禁止重用 |
| JNI `nativeConnect` | 不传递Java descriptor中的模式/协议字段 | 不能只修改native端判断；JNI也必须传递并校验协商信息 |
| remote Jetty import | `rjetty.type` 保持零值（JFR） | 官方Jetty路径明确填写 `URMA_JETTY` |
| remote segment import | import flag为零 | 按官方样例明确NOMAP及READ/WRITE访问权限，不能依赖模拟provider忽略字段 |

## 4. 拟实施边界

1. 新的 SCache 配置贯穿 Scala → Java → JNI → native，不通过 Spark 修改传递；配置日志打印实际设备、EID、mode、TP、priority、capability。
2. 默认保留明确的兼容选择；RM/CTP 必须显式启用并通过能力检查，不从失败的 RC 静默 fallback 到 RM。
3. 远端协议必须能拒绝混合版本、模式不一致、无效长度和错误 MR 身份。不要让连接错误进入数据面。
4. 两个 SCache 节点先维持一个 peer 的现有限制；本轮不声称支持任意多节点。
5. 错误回收方案需同时覆盖本地目的 slice、远端源 lease、JNI direct buffer 与 MR，不能只保护 native 注册对象。
6. 不采用“超时后无期限等 CQ”作为完整方案。正常请求有期限；未安全排空时必须进入不可复用的故障状态，并提供明确的恢复/终止边界。具体方案待 native 与控制面审查后定稿。不能仅隔离目的slice而在TCP断开时释放源lease；也不能仅保留native MR句柄而允许JVM回收其DirectByteBuffer。
7. CMake 支持明确指定厂商 SDK 公开头文件和库；硬件构建不要求 `ub_util.h` 或 OpenURMA build tree。测试替身不得链接进生产 `.so`。

## 5. 验证计划

| 层次 | 需验证内容 | 证明范围 |
|---|---|---|
| 静态与构建 | 官方头文件 API、编译警告、Java/JNI签名、协议长度与字节序 | 能构建、接口一致，不证明硬件可用 |
| native 测试替身 | RM无bind、模式错误、EID/priority、不同VA/UBVA、部分提交失败、CQ错误/延迟、清理失败 | 请求和资源状态机，不证明DMA与fabric |
| Java/Scala 合约 | endpoint编解码、配置校验、slice隔离、源lease保留、失败不会返回成功 | 上下层契约 |
| SCache双进程 | PUT/FETCH、双向、跨arena、并发消费者、重连、清理 | 测试后端与完整SCache控制流程 |
| 真实双机 | 厂商RM/CTP perftest → native → JNI → SCache内容/指标/回收 | 只有目标硬件执行成功才证明真实链路 |

每层保留命令、依赖版本、退出码及完整日志。失败不修改原分支、不重用原运行目录。真实硬件不可访问时明确保留未验证项，不以测试替身替代。

## 6. 官方依据与版本边界

- [openEuler URMA API Guide](https://docs.openeuler.org/zh/docs/24.03_LTS_SP3/unifiedbus/unifiedbus/urma/URMA%20API%20Guide.ch.html)：bind限定RC，RM import与RC连接关系不同。
- [openEuler URMA User Guide](https://docs.openeuler.org/zh/docs/24.03_LTS_SP3/unifiedbus/unifiedbus/urma/URMA%20User%20Guide.ch.html)：Jetty、TP配置与使用流程。
- 当前官方网页与master会演进；实现以目标机25.12.0-B103公开头文件/工具能力为约束，不能把最新文档中的新增接口当作B103已支持。
- 223的官方UMDK源码当前为 `0e246a9`；OpenURMA固定子模块为 `4eab3e4ad170b06bfe5d5c1014341e81edb9bf58`。两者与B103不能仅凭版本号视为完全一致，需逐项核对。

## 7. 当前进度

- 已核对交付源码身份并建立独立分支/worktree。
- 已通读 native transport 主实现与头文件，发现上述模式与回收问题。
- 正在核对 Java/Scala 的 buffer/lease 生命周期、官方样例和已有测试覆盖。
- 已开始修改独立分支产品实现；尚未声称 RM/CTP SCache 可运行。

### 配置与协议实现第一轮

- Java/JNI新增显式模式、TP、EID index、priority及超时参数。JNI API版本提升为2，并使用新入口名，拒绝错误搭配旧动态库。
- endpoint协议提升为2（88字节），携带TP及独立segment EID；拒绝旧协议或不同模式组合。双端必须一起升级，不能和原U1互通。
- native查询细粒度设备能力和EID；RM/CTP不执行RC bind/unbind，使用Jetty提交数据，移除初始化对模拟wire role环境变量的要求。
- 223参考ARM UMDK下native/JNI重新编译成功。此处不是目标机B103，也没有真实数据传输。
- `Integration / runMain org.scache.network.ub.RmCtpEndpointContractTest` 实际执行通过：模式枚举映射、无效配置、88字节编解码、segment EID独立性、旧协议及错误模式拒绝。该测试不加载JNI、不访问硬件。
- 第一轮时MR身份传播、DMA失败隔离及双进程测试未完成；后续进度见下文。整体仍不可交付，不要用于硬件benchmark。

### 配置接入及地址检查第二轮

- Scala服务入口已使用 `scache.ub.device`、`scache.ub.mode`、`scache.ub.eidIndex`、`scache.ub.priority`；前四项必须显式配置，没有模拟设备名和优先级猜测。模式为 `rm-ctp` 或 `rc-rtp`。
- `spark.urma.transferTimeoutMs` 控制请求超时；`scache.ub.drainTimeoutMs` 控制失败/超时后的额外排空等待预算（第三轮已实现，见下文）。
- 不再校验 `OPENURMA_WIRE_ROLE/PATH`；管理TCP监听地址仍使用原 `spark.urma.controlBindHost/controlAdvertiseHost`，与URMA模式独立。
- 修复本地注册范围检查使用CPU VA、远端地址使用provider UBVA；修复部分重叠注册未被拒绝的判断；远端import缓存增加generation匹配。
- 第二轮ARM重新编译成功，`local_va_contract_test` 实际通过。当时 `rm_ctp_dma_lifetime_test` 仍失败；第三轮修复结果见下文。
- 第二轮证据：223 `scache-rm-ctp-evidence-20260928/baseline/rm-ctp-build.log`、`rm-ctp-scala.log`，CTest的 `build/Testing/Temporary/LastTest.log`。参考库而非目标硬件测试。

### 首轮基线测试证据（2026-09-28）

目录：223 `/home/xinzhang/scache-rm-ctp-evidence-20260928/baseline`。

- `build-arm.log`：U1 native/JNI及现有native测试程序编译完成。
- `sbt.log`：SCache 95个Scala/97个Java生产文件及Integration编译完成。`sbt test`没有输出实际测试数量，不能据此声称执行了完整合约测试。
- `request-state.log`：既有 `request_state_test` PASS，但该测试容许父请求先失败、子请求后排空，因此不是slice安全证明。
- 新增 `rm_ctp_dma_lifetime_test.cpp` 在U1上按预期失败：`API_RESULT status=-8 provider_outstanding=1`。它通过测试内状态注入，证明普通失败返回时仍可能有provider所有权；没有真实DMA发生。
- 新增 `UbRmCtpLeaseSafetyTest.scala` 在U1上按预期失败：`DISCONNECT_RECLAIM rejected=false freeBytes=4096`。注册替身不提交payload，验证的是实际arena/lease逻辑：断开连接后可立即回收源块。
- 最初复用旧OpenURMA构建目录时，发现其liburma为x86-64，ARM链接失败；未修改该目录。改为223已有ARM构件后完成链接；对应公开头文件SHA一致。该参考UMDK仅用于本地编译/离线测试，不能替代目标B103验证。

后续必须使新增回归测试在修复版本上通过，同时验证正常完成与资源回收没有退化。

### 断链源内存保护（进行中）

- `releaseOwner` 不再把TCP断开视为DMA完成。未释放的READ源lease和WRITE目的lease转为 `OWNER_DISCONNECTED`，不回收slice。
- 普通release、abort、commit及retry不能绕过该隔离；正常完成并显式释放的路径不产生隔离。
- 服务关闭时若arena仍有未决lease，不继续关闭transport；保留服务及buffer强引用并报告 `UB_CLOSE_QUARANTINED`，避免吞掉异常后注销MR或由GC回收内存。
- 这是安全边界的部分实现，不是完整恢复方案。隔离区没有超时自动释放；尚需基于确切provider完成/资源撤销证据设计恢复，不能声称“断开TCP或重启进程即可证明无DMA”。未解决时会占用容量并阻断关闭，不能交付生产。
- 源侧测试通过不代表整个DMA生命周期安全；本地目的slice的专用异常处理已开始实现，仍须覆盖完整传输路径。
- 新增源侧测试同时覆盖：READ断链禁止unpublish、WRITE断链禁止abort/retry、正常release可恢复全部容量、service.close不能绕过arena失败。使用注册替身，无实际DMA。

### 有限排空与目的内存隔离第三轮

- native wait区分API终态和provider所有权，检查父请求的所有子请求；失败/超时后继续有限等待CQ，超出排空预算报告 `ERR_DMA_UNRESOLVED` 并阻止新提交，不无限等待。
- JNI映射为 `UrmaDmaUnresolvedException`。SCache接收到该异常后保留buffer强引用及目的reservation，不发RELEASE/ABORT、不释放scratch；控制连接结束后远端按上述规则保留源lease。
- 请求记录在提交WR前分配；逻辑块预留子请求数组，并在同一锁内提交及关联子请求，避免成功提交后丢失所有权记录。
- aggregate终态不立即解除child记录保护；缓存回收只能在确认无provider所有权后解除，避免迟到完成对应记录被提前淘汰。
- 223上native/JNI、Java/Scala构建通过；CTest两项 `local_va_contract_test`、`rm_ctp_dma_lifetime_test` 实测PASS；扩展后的 `UbRmCtpLeaseSafetyTest` PASS。证据为 `build/Testing/Temporary/LastTest.log` 和 `baseline/rm-ctp-dma-scala.log`。测试中的关闭异常是刻意注入未决lease后的预期结果。
- 验证仍不充分：需要覆盖真实API调用替身、延迟CQ/部分提交/轮询错误、Java关闭并发、JNI buffer强引用、MR完整身份、多进程控制流程及隔离恢复。不得把上述少量状态注入测试当作完整正确性证明。

### 公开SDK与资源清理第四轮

- 移除native中 `openurma_vdev_q5_mr_info` 私有符号/结构及动态加载逻辑。MR远端地址、EID、UASID读取公开 `urma_seg_t.ubva`，不再以模拟设备的EID index冒充UASID。
- NOMAP import映射地址明确为0，指定READ/WRITE访问权限；generation改为软件注册身份，仅参与缓存匹配，不传给provider作为映射地址。
- CMake不再强制OpenURMA build tree或内部 `ub_util.h`，可使用系统公开头文件/库或显式 `URMA_INC`、`URMA_LIBRARY`、`URMA_COMMON_LIBRARY`。自动发现不等于认证SDK版本，目标机仍须核对实际链接路径。
- 全新目录223 `scache-rm-ctp-evidence-20260928/public-sdk-build` 仅指定公开头文件和两项ARM参考库，native/JNI及测试程序构建通过，不配置vdev头文件或common内部头文件。参考头文件实际存储在历史子模块目录，仅作为文件来源，不作为构建树依赖。
- 注销/删除失败时保留句柄及依赖资源，成功的阶段置空后重试不会重复删除；RC绑定单独跟踪。该失败注入测试尚待补充，编译通过不构成清理安全证明。
- 下一步必须解决JNI对DirectByteBuffer的强引用、部分注册失败后的记录保留，以及每个MR独立身份的传递/约束；当前endpoint中保存最后注册MR身份尚不足以证明所有provider下多arena正确。

### JNI持有与无符号token第五轮

- JNI owner持有已注册buffer的GlobalRef；注销成功后才删除对应引用，整个transport关闭失败时保留全部引用。注册前预分配Java结果数组、引用容器和native MR记录，避免MR已注册却无法记录或返回时失去buffer所有权。
- 原生关闭成功但Java结果数组分配失败时保留可重试的已关闭owner，不返回悬空句柄。Java包装器在native销毁成功后先清空句柄，再创建metrics对象。
- Java transport实例方法暂以同一monitor保护native handle，避免close与read/wait/register交叉删除对象。代价是wait期间同实例其他调用会等待；该保守实现的并发性能尚未评价，不能宣称无性能回归。
- uint32 token按32位比特串传递：Java int允许最高位为1，JNI先转uint32再扩展，native注册返回long限定0..0xffffffff。移除把负int token误判非法的检查，保留地址/长度/generation和provider验证。
- ARM构建及端点测试通过；测试增加0x80000000/0xffffffff token边界和公开handle方法同步属性检查。属性检查不能替代真实并发/GC测试；这些仍待补充。
- 证据：223 `baseline/rm-ctp-jni-owner.log`、`baseline/rm-ctp-unsigned-token.log`。真实设备仍未执行。
- `RmCtpJniAbiTest` 实际加载本轮编译的ARM `libSparkUrma.so` 并通过：JNI API版本2、V2入口签名、非法模式在provider初始化前拒绝、空handle异常映射。日志 `baseline/rm-ctp-jni-abi.log`；不初始化设备，不证明MR或DMA成功。

### 多MR描述第六轮（尚在接线）

- 新增不可变 `SegmentDescriptor`：完整MR的EID、UASID、attributes、UBVA、长度、uint32 token、软件generation，固定56字节网络序。block window另行表达，不能把slice当成新注册MR。
- native注册结果扩展为10个long，Java保存每个MR自己的身份；远端window操作保留同一个完整MR描述，不从最后一次注册的endpoint推断。
- 兼容边界：新增类型允许旧测试构造器暂不提供segment，但最终硬件读写入口必须拒绝缺失MR身份；当前控制帧与native导入参数接入尚未完成，因此这一轮不构成多arena可用证明。
- 地址暂明确限制在Java正long可表示范围内，越界拒绝而不截断。如果厂商返回高位UBVA，需要完整扩展无符号地址计算后再支持，不能绕过检查。
- ARM native/JNI与Java/Scala编译通过；多MR身份区分、56字节编解码、防外部修改、window身份保留、越界及generation不匹配测试通过。证据：`baseline/rm-ctp-segment-descriptor.log`。未执行多MR硬件导入。

### 多MR接线第七轮（2026-09-28）

- 控制协议升级为2，GRANT携带完整56字节MR描述；旧协议明确拒绝。JNI API升级为3，四个读写入口改为带MR的独立名称，避免旧签名误调用。
- MR导入改用当前描述中的EID/UASID/attributes/完整UBVA范围，而非端点最后一次注册的信息。缓存按完整MR身份复用，SGE仍使用获授block窗口；导入前检查窗口范围与整数溢出。
- 在provider导入前预分配缓存空间，避免导入成功后容器分配失败丢失资源。该措施不替代其他提交/完成路径的异常安全审查。
- 223上ARM native/JNI、Java和Scala编译通过。22项控制帧测试、实际ARM JNI API3加载测试通过，日志：`/home/xinzhang/scache-rm-ctp-evidence-20260928/baseline/rm-ctp-mr-wire.log`。两项离线native生命周期/VA测试再次通过，端点描述测试再次通过。
- 原U1工作区仍干净，未修改Spark。尚未完成provider模拟故障矩阵、历史诊断调用构造器更新、跨进程完整SCache测试及真实双机硬件验证；不能据本轮编译和协议测试宣称RM/CTP链路已跑通。

### 提交归属第八轮（2026-09-28）

- READ/WRITE/SEND仅在失败返回的`bad_wr`明确指向本次唯一WR时允许EAGAIN重试。失败但`bad_wr`为空或指向其他对象时，不能证明未被接收：保留请求及本地/远端MR引用，进入CLOSING，返回可跟踪request供逻辑块挂接并等待。
- 未排空时wait抛`ERR_DMA_UNRESOLVED`，SCache不得释放slice/远端lease；迟到CQ到达后释放provider引用，但不将先前提交异常改成成功。若永远没有CQ，当前仍需隔离该transport及其内存，不能声称已具备自动恢复。
- SEND补上提交前分配请求记录及请求CQ标志，避免提交成功后分配失败丢失所有权。
- 新增链接包装测试，执行真实transport代码，替换post/poll入口；READ、WRITE、SEND分别覆盖明确拒绝、归属不明后迟到CQ、正常完成，共9例通过。测试不生成真实DMA，不证明厂商provider实现符合假设。
- ARM重新编译通过，3个CTest全部通过；详细日志：`/home/xinzhang/scache-rm-ctp-evidence-20260928/baseline/rm-ctp-post-ownership.log`。审查/实现/测试持续时间现已超过一小时，仍继续审查剩余跨进程和恢复边界。

### 调用入口与构建第九轮

- 修复`UbRegisteredBuffer.descriptor()`遗漏segment的调用路径，增加注册对象导出MR身份的回归断言。
- `build_spark_urma.sh`不再强制要求`OU_UMDK_BUILD`，默认由CMake查系统SDK；可显式传`URMA_ROOT`或`URMA_INC/URMA_LIBRARY/URMA_COMMON_LIBRARY`。JDK从显式`JAVA_HOME`或PATH中的javac解析，强制核对17；不使用x86固定默认目录。安装libdir明确为lib。
- ARM以清除`OU_UMDK_BUILD`、新构建目录和显式公开SDK路径执行该构建入口成功，产物为`build-script-public-sdk/install/lib/libSparkUrma.so`；SHA256 `072355b9b1c6342b8ff68c304d9d997778e1b7d5c95e978eeb67dd15bd493c50`。这是参考SDK编译，不是目标B103验证。
- 原负向诊断`remote_bounds/invalid_token`不再缺失MR身份或捕获任意异常即判PASS。明确仅检查本地窗口/token一致性，不提交DMA、不声称验证provider错误处理。后续还需独立provider故障测试。
- Java/Scala重新编译、端点测试及22项控制协议测试通过；223日志`baseline/rm-ctp-descriptor-export.log`。

### MR导入实参第十轮

- 新增`rm_ctp_mr_import_test`，使用真实transport方法并仅包装URMA import/unimport边界，验证完整MR范围、独立EID/UASID、attributes、uint32 token与NOMAP参数。
- 故意将endpoint设成另一个MR身份，确认导入不误用它；同一MR的不同block只导入一次，不同EID或generation不复用旧项。
- 越界、缺失MR、地址溢出在provider调用前拒绝；import失败不增加有效缓存；unimport失败保留所有权，重试成功后计数归零。
- ARM构建通过，4个CTest全部通过；证据：223 `baseline/rm-ctp-mr-import.log`。这些证明调用实参与软件资源状态，不证明厂商MR导入成功或真实DMA。

### 独立SCache双节点入口第十一轮

- 新增集成主类`org.scache.network.ub.RmCtpServiceNode`，不使用Spark或测试payload替身，调用生产`UBBlockTransferService`与真实JNI。参数依次为：`A|B work-dir device eid-index priority bind-ip control-port peer-ip peer-control-port`。
- 每端3个1MiB arena，发布64/700000/700001字节三块，双方读取对方数据、核对每个字节；每块预留两个消费者，验证第一次释放不提前回收、最后一次释放后目的块消失。
- 两端完成读取后再清理源块及cached imports；要求arena容量恢复、lease/scratch/inflight/providerOutstanding为零、TCP/Netty payload/fallback为零，提交及完成字节数等于测试数据总量。PASS在service.close成功后打印。
- 额外协调TCP仅传PUBLISHED/FETCHED/QUIESCENT标记，A监听自己的control-port+1；先启动A看到READY后再启动B。目录由参数指定，每次创建独立运行子目录，不覆写原pool。
- 223上`Integration / compile`通过，证据`baseline/rm-ctp-service-node-build.log`。该入口尚未实际执行；不能将编译成功写成双机PASS，后续需补启动脚本和真实/替身分层运行记录。

### 启动器第十二轮

- `scripts/rm-ctp/prepare-tests.sh <输出目录>`使用显式`JAVA_HOME`及`SBT_LAUNCH`编译SCache测试并导出classpath；无Test源码时不将sbt虚列的空test-classes当成依赖缺失，其他缺项仍拒绝。
- `scripts/rm-ctp/run-node.sh`要求`JAVA_HOME`、`SCACHE_TEST_CLASSPATH_FILE`、`SCACHE_JNI_DIR`、`UMDK_LIBRARY_DIRS`。清除Java注入变量/旧classpath/LD_PRELOAD，核对JDK17和JNI所解析的UMDK库是否确实来自指定目录；输出库hash、ldd、命令、终端日志和退出状态。
- 先执行`bash scripts/rm-ctp/run-node.sh --check <日志目录>`，实际加载JNI并运行离线协议检查；随后才按上一节9个参数启动A/B。脚本不会自动更改memlock、驱动或系统库。
- 223上准备与`--check`实际通过，日志`/home/xinzhang/scache-rm-ctp-evidence-20260928/service-launcher/launcher-25xVCk6m/`；未初始化硬件。开发中发现的Shell引号和空Test输出目录问题已修复并重跑，不将失败尝试算PASS。

### Jetty连接与重连第十三轮

- 实际检查223：主机`arm-euler-server-2`、aarch64，无`/dev/uburma`及`/sys/class/ubcore`。它是构建/软件测试机，不是controller；真实双机仍需目标硬件环境。
- 修复重连过程中旧MR/Jetty取消导入或RC解绑失败后仍保持CONNECTED的状态；改为CLOSING并保留未释放句柄，阻止继续数据操作。
- 重连前检查所有请求的provider归属及aggregate-building状态，不能仅凭当前inflight计数为零就在逻辑块两个子请求之间替换peer。
- 新增`rm_ctp_connect_test`，包装官方Jetty API边界验证RM不bind/unbind、RC仍bind/unbind、类型为JETTY、模式/TP与身份传递、协商长度、混合协议拒绝、构建中拒绝重连和清理失败保留所有权。
- ARM编译及5个CTest通过；日志223 `baseline/rm-ctp-connect.log`。没有执行设备初始化或真实网络传输。

### 初始化第十四轮

- 对照223官方UMDK `src/urma/examples/urma_sample.c`的JFS/JFR配置，显式设置本地、远端、接收单SGE数量为1，并在创建前检查设备至少支持1个SGE。避免把申请规模隐含交给provider默认值；不据此宣称原零值必然错误。
- 新增`rm_ctp_init_test`，包装初始化相关官方API，实际执行transport.init/close：核对配置EID7、RM队列、priority6、CTP multipath和共享JFR；注入能力缺失、EID不可用、Jetty创建失败，确认部分资源回收与进程active transport slot归零。
- ARM重新编译及6个CTest全部通过；日志223 `baseline/rm-ctp-init.log`。初始化测试使用API替身，不访问设备；厂商B103具体默认值和硬件配置仍需目标机验证。

### 完整服务协议第十五轮

- 新增`RmCtpServiceProtocolTest`：同一JVM两个生产SCache服务实例，真实loopback TCP控制帧、独立3×1MiB mmap arena；通过测试专用反射注入拷贝后端，不修改生产factory或引入可启用的模拟fallback。
- 双向共6块（64/700000/700001字节）实际完成ACQUIRE/GRANT、MR描述编解码、读取、CRC/逐字节校验、RELEASE、两消费者引用、unpublish和关闭；两端容量恢复，测试注册表归零。
- 223实际运行PASS，日志`baseline/rm-ctp-service-protocol.log`。这是两个服务实例而非两个独立进程/机器；payload为测试内存copy，不是JNI/URMA/DMA，不用于性能结论。真实双机仍使用独立`RmCtpServiceNode`。

### 目的池满载异常第十六轮

- 实际复现：ACQUIRE成功后，目的池prepareShared因容量耗尽抛异常，原代码尚未进入清理try，源lease在TCP断开后被隔离；关闭报`UB_CLOSE_QUARANTINED`及1个lease未排空。失败证据：223 `baseline/allocation-failure-before.log`。
- 将目的slice申请及identity检查纳入清理范围；申请前失败也发送RELEASE。已申请slice在确认没有未排空DMA时才abort；清理异常保留为suppressed，不覆盖最初原因，也不跳过源lease释放。未放松DMA unresolved隔离。
- 新增真实服务/TCP/mmap测试：目的池填满3MiB，fetch明确失败、源activeLeases归零、submittedBytes为0；释放容量后重试成功，源/目的容量恢复，关闭成功。原双向6块测试继续通过。证据：223 `baseline/allocation-failure-after.log`。
- 仍为测试拷贝后端，未在真实RM/CTP硬件运行；不能作为双机UB链路通过的证据。

### 建链失败诊断第十七轮

- 初始化输出实际设备、请求mode/TP、EID index、priority及厂商细粒度RC/RTP、RM/CTP能力。context/JFC/JFR/Jetty创建及远端Jetty导入失败保留同组参数和即时errno。
- 调用前清零errno，避免把旧错误当根因；provider未设置errno时明确提示查看provider/内核日志，不凭空推断“不支持”。建链日志不再输出segment token。
- 给真实connect控制逻辑增加provider导入失败注入，验证EINVAL与未设置errno两类错误均可辨认，失败仍保持INITIALIZED且无remote Jetty；随后可重新connect。ARM重编译和6项native契约测试通过，证据：223 `baseline/provider-error-context.log`。
- 上述为测试替身的错误注入，不是厂商provider建链成功。下一步仍须目标双机运行本分支`RmCtpServiceNode`，不把旧RC应用脚本或标准Spark测试作为替代。

### unpublish失败引用记录第十八轮

- `unpublishBlock`先清除importedSharedBlocks/consumer refs，再调用可能因远端lease超时而失败的arena.unpublish。实际复现后，内存仍Published，但两个本地消费者的第一次release已经返回false。证据：223 `baseline/unpublish-failure-before-v2.log`。更早的`unpublish-failure-before.log`仅为新测试调用名错误的编译失败，不算复现证据。
- 调整为持有相同metadata锁先完成arena.unpublish，成功后才清除记录；失败保持原记录。锁顺序与已有releaseImportedPoolBlock一致。
- 回归覆盖：读取后保留两个消费者、额外获取一个远端lease；unpublish超时拒绝，释放远端lease后两个消费者仍依次释放，最后归还slice。该用例使用真实服务/控制协议与测试内存copy，不是硬件。
- ARM编译及该用例、目的池满载重试、原双向6块测试全部通过：223 `baseline/unpublish-failure-after.log`。

### 尚待处理的RM/CTP设备配置边界

- 当前`jfs_cfg.flag.bs.multi_path = (tp_type == CTP)`过于宽泛。固定官方UMDK头文件`urma_types.h`明确该位“for ubagg only”，`urma_sample.c:validate_input_params`也禁止非bonding设备设置multi_path。不能把CTP本身当作UBAGG的证明；物理UDMA路径尚不能据此宣称支持。
- 官方perftest对use_bonding额外增加一个max_sge，而sample仍配置1；这是两种调用路径的差异，尚不足以断言所有UBAGG都必须增加SGE。需结合对应provider处理逻辑确认，不能靠猜测修改。
- 本轮仅修复已复现的unpublish引用丢失；上述设备配置问题未修复、未通过硬件验证，仍是后续代码检查项。

### UBAGG多路径与CTP解耦第十九轮

- 延续检查后，依据固定官方`urma_sample.c:validate_input_params`的bonding名称前缀约定，将multi_path限定为`device->name`以`bonding`开头且请求RM。普通UDMA RM/CTP保持multi_path=0；RC/RTP保持0。模式和TP细粒度能力校验仍保留，不静默切换模式。
- 该选择沿用官方示例的命名约定，不是任意厂商设备的通用身份识别。重命名聚合设备或其他provider尚需适配；`tp_feature.rm_multi_path`记录到诊断日志，但不把未经验证的该字段当作自动启用策略。
- 初始化替身测试新增`udmac0d1e2`，验证RM、CTP优先级保留且multi_path为0；bonding路径为1。ARM重编译及6项native测试通过，证据：223 `baseline/multipath-device-selection.log`。
- 本节解决上节multi_path一律随CTP启用的问题；SGE扩展差异与真实B103厂商库行为仍未验证。没有改变Java/Scala/JNI接口或Spark。

### 读取失败恢复第二十轮

- 为服务级测试增加3种故障：提交前明确拒绝、已排空的失败completion、故意修改一个目的字节造成CRC不匹配。断言对应错误内容，避免把其他异常当作目标场景通过。
- 每种场景均验证目的块未发布、目的池容量恢复、源lease为0；取消失败消费者reservation后重新申请并读取，逐字节校验8193字节，再正常释放。checksum/completion错误计数分别验证。
- ARM实际运行通过：223 `baseline/read-failure-recovery.log`；这些故障注入发生在测试拷贝后端，没有硬件/DMA，也不涵盖provider永不返回的恢复。生产代码本轮未改。

### 回收等待线程中断第二十一轮

- 实际复现：unpublish将状态改为Unpublishing后进入Object.wait，线程中断抛出InterruptedException，原代码没有恢复Published。远端lease仍在，但后续unpublish会以“not published”失败。证据：223 `baseline/unpublish-interrupt-before.log`。
- 修复unpublish与discardForRetry中的lease等待：中断时恢复Published、重新设置当前线程中断标志并重抛，不释放slice、lease或MR。
- 新增两种等待路径的真实线程测试：等待状态可观测后中断并join；验证状态、线程标志、lease和内存保留，正常release后可重新unpublish并恢复全部容量。注册使用测试替身，不接触硬件。
- ARM编译、两种中断恢复、原lease隔离测试及服务正常/故障恢复测试全部通过：223 `baseline/unpublish-interrupt-after.log`。

### 独立容器调试构建第二十二轮

- 223缺少ASan/UBSan运行库，GCC目录中的链接指向不存在的libasan/libubsan文件；未安装或更换系统包，未执行sanitizer测试，不能宣称已通过ASan/UBSan。
- 使用独立`native-debug-containers`目录，Debug构建加`-D_GLIBCXX_DEBUG -D_GLIBCXX_ASSERTIONS`，6项native契约测试全部通过。日志：223 `baseline/native-debug-containers.log`。该检查覆盖被执行路径的容器/迭代器断言，不等于完整内存安全检测。
- 使用新Debug JNI执行统一`run-node.sh --check`，实际JNI ABI检查及现有Java/Scala测试通过，退出0。证据：223 `debug-launcher/launcher-gLQRSvdk/terminal.log`。未初始化UB设备，未替换正常构建产物或原U1。
- Clang17静态分析`urma_transport.cpp`退出0，生成plist的diagnostics为空；证据`native-debug-containers/transport-analysis.plist`。这仅表示分析器未报告问题，不证明所有路径正确。

### 回收操作互斥第二十三轮

- 实际复现：unpublish线程在Object.wait释放锁后，discardForRetry仍接受Unpublishing状态。零等待的retry失败时重置Published，破坏原回收线程状态；若两个操作都等待，则存在重复回收同一entry的风险。实际失败证据：223 `baseline/concurrent-retire-before.log`，断言“retry overwrote another retire operation's state”。重复释放风险本轮未单独触发，不当作实测损坏结论。
- 修复：discardForRetry遇到Unpublishing明确拒绝，不重置状态，也不成为第二个回收等待者。
- 新增真实线程测试：首个unpublish保持等待，第二个retry拒绝且状态不变，新acquire不能进入；原lease释放后首个回收完成、全部容量恢复。注册仍为测试替身。
- ARM编译、该并发用例、两种中断恢复及服务故障恢复回归通过：223 `baseline/concurrent-retire-after.log`。

### native关闭失败可重试第二十四轮

- 已复现：provider删除JFR失败时，cleanup保留句柄，但close提前设为CLOSED；后续close直接返回，不能重试剩余资源。证据：223 `baseline/close-retry-before.log`。
- 修复：仅在全部cleanup成功后进入CLOSED；失败保留CLOSING和资源所有权，不允许JNI把失败当作成功关闭。
- 测试覆盖JFR失败后重试；进一步覆盖Jetty、JFR、JFC、context在同一关闭序列依次失败，确认已成功销毁的对象不重复销毁、runtime slot最后才释放。使用provider替身，不是厂商驱动故障实测。

### arena关闭与回收交错第二十五轮

- 已复现：unpublish等待lease时释放monitor，close可先释放entry；原unpublish恢复后再次freeEntry，重复归还同一slice。证据：223 `baseline/close-retirement-before.log`。
- freeEntry增加entry对象身份校验：仅当前map中的同一个entry有权回收；已关闭或被替换的旧entry不再操作分配器。
- 确定性线程交错测试修复后通过：223 `baseline/close-retirement-after.log`。该测试使用注册替身，不涉及DMA。

### 失败请求计数第二十六轮

- 已复现：nativeWait以异常报告已排空的超时/失败，RealUbTransport只在正常返回时recordTerminal，导致逻辑inflight残留。证据：223 `baseline/logical-metrics-before.log`。
- 对UrmaTimeoutException、UrmaCompletionException记录一次失败并原样重抛；DMA unresolved、协议及其他异常仍保留inflight。计数不授权任何内存或lease回收。
- 新增独立异常注入测试，覆盖超时、completion、unresolved、协议异常、重复wait、成功及close对账；通过日志`baseline/logical-metrics-after.log`。已加入run-node.sh --check。

### 内存检测与一小时并发测试

- 使用223软件源下载与GCC12.3.1-38匹配的libasan/libubsan RPM，只解压到证据目录，不安装或替换系统包。独立ASan/UBSan构建6项native测试通过，日志`baseline/asan-native.log`。本节更新第二十二轮“当时缺少运行库”的状态。
- 使用固定JDK17、256MiB heap/128MiB direct memory运行`UbRmCtpLeaseSafetyTest --stress-seconds 3600`。每轮检查两种回收中断、竞争回收和close交错，共4个场景。日志`baseline/lease-concurrency-hour.log`；最终结果需以该日志的LEASE_STRESS_PASS和进程退出码为准，未完成前不宣称一小时通过。
- 以上均为离线软件检查，不包含真实UB provider、DMA、双机链路或性能结论。原U1及Spark不修改，现有三包不替换。

### JNI符号边界补充

- RmCtpJniAbiTest遍历13个以native handle为首参数的JNI入口，以空handle验证符号可解析且明确拒绝，覆盖READ/WRITE、分块请求、SEND、注册/反注册、wait、metrics、endpoint、connect和close。
- 在223使用实际ARM JNI、JDK17和`-Xcheck:jni`执行通过，无JNI检查告警：`baseline/jni-xcheck.log`。该检查不会创建URMA设备对象，不等于正常数据操作通过。
- GCC12 `-fanalyzer`实际编译退出0；仍有官方头文件零长度数组和本项目C++20 designated-initializer扩展警告。未修改厂商头文件，未把静态检查当作全部路径正确的证明。日志`baseline/gcc-analyzer-compile.log`。

### 复用原单机包的脚本适配

- 新增`run-package.sh`，复用原单机`00-detect-vendor-umdk.sh`的软件/设备检测及`build-lib-spark-urma.sh`的私有工具链、隔离环境和完整日志。不改原单机脚本，不接Spark，不把旧RC应用或旧JNI源码作为新路径。
- 在独立build-view中指向本分支native源码、包内dependencies及明确检测的厂商公开头文件。首次实际编译发现CMake包装入口缺少顶层minimum/project声明，已在新适配脚本修复；不改产品CMake。失败/通过证据分别`baseline/package-private-toolchain.log`和`baseline/package-private-toolchain-fixed.log`。
- 编译缓存包括产品源码、SDK头文件/库、原构建入口、新适配入口、JDK版本文件和工具链清单；产物损坏或输入变化重新编译。仅编译阶段不以多设备/EID/优先级为门禁。
- 使用原单机设备/provider检测，再解析厂商admin的EID和CTP priority；唯一候选自动选择，歧义明确提示并保存人工选择。原单机固定EID0/RC参数不沿用。对端地址由用户提供，本机管理IP从路由查询；不扫描网络猜对端。
- prepare-tests支持导出完整相对classpath运行目录，包含本分支新类及Java依赖。本次导出约44MiB；目标机使用包内JDK17，不需sbt/Maven，也不混入旧SCache JAR。源码身份和文件校验后才加载。
- 7项参数选择测试、4项包适配控制流测试通过；后者为假工具fixture，仅验证复用/失效/日志/环境选择，不冒充真实编译。223另以实际包内GCC12.3.1/CMake/JDK17完成新JNI编译，UMDK使用ARM参考库，非目标B103厂商库。
- 导出目录复制到含空格的新路径，在错误JAVA_HOME/JAVA_TOOL_OPTIONS干扰下使用实际包内JDK17.0.20.1、新JNI完成全部离线检查；证据`baseline/package-relocation.log`。真实设备自动选择和双机DMA尚未实测。
