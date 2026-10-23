# 三版本源码目录与 UB 链路索引

核对日期：2026-10-23。源码浏览工作区位于 223：
`/home/xinzhang/ub-source-versions-20261023`。

## GitHub 发布状态

2026-10-23 已使用 aloooooooha 身份提交并推送。新提交使用该账号的 GitHub noreply
邮箱；原有历史不重写。不覆盖 master，不强推，不删除原远端分支。
旧源码、已验证核心文件及运行包保持不变。

| 源码主体 | 原 GitHub 仓库 | 新工作区子目录 |
|---|---|---|
| SCache，包括三个 Java/JNI/Native 后端 | https://github.com/ultra-shuffle/SCache | SCache |
| Spark 3.5.7 的 SCache Shuffle 集成 | https://github.com/ultra-shuffle/spark-3.5-scache | spark-3.5-scache |
| OpenURMA 模拟 provider / 设备功能模型 | https://github.com/aloooooooha/OpenURMA | OpenURMA |

## 版本分支：SCache/OpenURMA 三版，Spark 只用一个

| 版本 | SCache 分支/已验证源码基线 | OpenURMA 分支/已验证源码基线 |
|---|---|---|
| 原直接 URMA | [versions/direct-urma](https://github.com/ultra-shuffle/SCache/tree/versions/direct-urma) / e3db8f0 | [versions/direct-urma](https://github.com/aloooooooha/OpenURMA/tree/versions/direct-urma) / 673421a |
| URPC | [versions/urpc](https://github.com/ultra-shuffle/SCache/tree/versions/urpc) / 1b12682 | [versions/urpc](https://github.com/aloooooooha/OpenURMA/tree/versions/urpc) / 9b74b0c |
| UMQ RM/CTP | [versions/umq](https://github.com/ultra-shuffle/SCache/tree/versions/umq) / 7af7f75 | [versions/umq](https://github.com/aloooooooha/OpenURMA/tree/versions/umq) / 146289c |

Spark 唯一的本次共用分支：
[versions/scache-shared](https://github.com/ultra-shuffle/spark-3.5-scache/tree/versions/scache-shared)。
已验证源码基线 99357e52267d1466c7421836f40fdffcade24fc1，
发布提交 69c91e1a5a58c56f96037b8f71e9e0725b51453f（只增加索引）。
没有把同一份 Spark 重复发布为三个版本分支。

上表短 SHA 指已验证源码基线，不是新增索引/脚本归档后的分支最新提交。
直接 URMA 指原 10 月 15 日审查版；冻结 URPC 是 RC framework 实验，
并非 RM/CTP；UMQ 是六月官方 UMQ RM/CTP 消息功能模拟。
UMQ 分支保留旧后端以维持兼容，实际选择由 scache.ub.transport 决定。

在新浏览工作区切换（不会改旧交付，也不会自动重编译/切换依赖）：

```bash
cd /home/xinzhang/ub-source-versions-20261023
bash switch-version.sh direct-urma
bash switch-version.sh urpc
bash switch-version.sh umq
```

每次只执行想查看的版本那一行。脚本先检查三个仓库均干净，再切换 SCache/OpenURMA；
Spark 始终保持 versions/scache-shared。发现修改会停住，不覆盖修改。当前默认查看 umq。
也可只在 SCache 或 OpenURMA 执行 `git switch versions/umq`；
Spark 应执行 `git switch versions/scache-shared`。单仓库切换不会自动同步其他仓库。
GitHub 中各分支根目录 UB_SOURCE_GUIDE_CN.md、Spark 的 SCACHE_UB_SOURCE_GUIDE_CN.md
也标注了相应调用文件。

## 先看这里：三版共用的 Spark / SCache 入口

路径相对于上表对应仓库；可在对应 GitHub 分支中查看。

| 仓库 | 文件 | 作用 |
|---|---|---|
| Spark | core/src/main/scala/org/apache/spark/shuffle/scache/ScacheDaemon.scala | Spark 对接本机 SCache 的入口、配置及严格/回退门禁 |
| Spark | core/src/main/java/org/apache/spark/shuffle/sort/UnsafeShuffleWriter.java | Shuffle 写入 SCache 的相关入口 |
| Spark | core/src/main/java/org/apache/spark/shuffle/sort/ShuffleExternalSorter.java | Shuffle 排序/落块与 SCache 发布路径 |
| Spark | core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala | Shuffle 拉取、SCache pool slice 消费及释放 |
| SCache | src/main/scala/org/scache/deploy/ScacheClient.scala | Spark 与本机 SCache daemon 的客户端桥接 |
| SCache | src/main/scala/org/scache/network/ub/UBBlockTransferService.scala | UB block 服务：发布、发现、lease、远端读取、CRC、回收；三后端共用 |
| SCache | src/main/java/org/scache/network/ub/UbTransportFactory.java | 根据 real / urpc / umq 显式选择后端 |
| SCache | src/main/java/org/scache/network/ub/UbTransport.java | 后端接口合同 |
| SCache | src/main/java/org/scache/network/ub/UrmaConnectionConfig.java | RM/CTP、RC/RTP、EID index、priority 等配置 |

## 原版 direct-URMA：最直接展现 URMA 调用的文件

全部在 SCache 仓库：

- `src/main/java/org/scache/network/ub/RealUbTransport.java`：服务层到直接 URMA transport。
- `src/main/java/org/scache/network/ub/UrmaTransport.java`：Java API、buffer 与请求生命周期。
- `src/main/java/org/scache/network/ub/UrmaNative.java`：JNI 声明。
- `native/spark_urma/src/spark_urma_jni.cpp`：Java / C++ 桥接及 direct buffer pin。
- **`native/spark_urma/src/urma_transport.cpp`**：真正的 URMA 调用核心。
- `native/spark_urma/include/urma_transport.h`：传输对象与资源状态定义。
- `native/spark_urma/tests/`：native 合同、所有权、超时及 RM/CTP 用例。

核心调用顺序：
urma_init → 查设备/能力/EID → urma_create_context → JFC/JFR/Jetty →
urma_import_jetty（RC 才 bind）→ urma_register_seg → 交换描述符 →
urma_import_seg → urma_post_jetty_send_wr(READ/WRITE) → urma_poll_jfc → 回收。

现场导入失败重点仍是 urma_import_seg 及下层 UBAGG/UBMAD；不能因为源码分支存在就说硬件已通。

## URPC 版：官方 framework RPC

全部在 SCache 仓库：

- `src/main/java/org/scache/network/ub/UrpcUbTransport.java`
- `src/main/java/org/scache/network/ub/UrpcNative.java`
- `native/spark_urpc/urpc_jni.cpp`
- **`native/spark_urpc/urpc_bridge.cpp`**
- `src/integration/java/org/scache/network/ub/UrpcTransportProbe.java`
- `src/integration/scala/org/scache/network/ub/UrpcServiceNode.scala`

调用 urpc_init、内存池注册、queue/server/channel 建立、
urpc_func_register / urpc_func_call / urpc_func_poll / urpc_func_return。
READ/WRITE 是应用 RPC 语义，不是直接单边 DMA。
冻结 URPC framework 源码为 4eab3e4a（2025-12-31），使用 RC；
它不是后续六月 UMQ RM/CTP 方案，不要混称。

## UMQ 版：六月官方 UMQ RM/CTP

全部在 SCache 仓库：

- `src/main/java/org/scache/network/ub/UmqUbTransport.java`
- `src/main/java/org/scache/network/ub/UmqNative.java`
- `native/spark_umq/umq_jni.cpp`
- **`native/spark_umq/umq_bridge.cpp`**
- `native/spark_umq/umq_smoke.cpp`
- `src/integration/java/org/scache/network/ub/UmqTransportProbe.java`
- `src/integration/scala/org/scache/network/ub/UmqServiceNode.scala`

核心调用：umq_init → umq_create(UB + RM + CTP) →
umq_bind_info_get / umq_bind → umq_buf_alloc / umq_enqueue / umq_dequeue →
umq_buf_free → umq_unbind / umq_destroy / umq_uninit。
官方 UMQ 内部再调用 URMA。此实验使用 BASE 消息 API，
由对端复制授权 arena 窗口，不直接导入远端应用 arena MR；不是零拷贝 READ/WRITE。
单 peer、单在途、3840 字节消息 payload；真实硬件仍待验证。

## OpenURMA 中的 UB 相关位置

- `integration/umdk/provider/openurma_provider.c`：URMA provider 操作、队列、MR、SEND/CQ 等功能模拟。
- `integration/umdk/provider/openurma_nic_functional.cpp`：UMQ 分支新增的 RM/CTP verbs 功能模拟连线。
- `integration/umdk/shim/openurma_ioctl_624.c`：UMQ 分支新增的六月 TLV 设备查询模拟。
- `integration/umdk/tools/openurma_mkdev.sh`：创建隔离 fake sysfs/device。
- `integration/umdk/`：其他旧 RTP 模拟接口和集成源文件。

UMQ functional profile 不是 CTP 硬件时序、重传或拥塞模型，不拿旧 RTP 模型改名当 CTP。

## 官方依赖源码（不与我们的三个应用版本分支混淆）

- UMDK：https://github.com/openeuler-mirror/umdk
- 六月固定源码：https://github.com/openeuler-mirror/umdk/tree/099343116b63aad37e2f60f99b71e772bf6bf009
- UMQ API：src/urpc/include/umq/
- UMQ 实现：src/urpc/umq/umq_ub/core/umq_ub_impl.c、
  src/urpc/umq/umq_ub/core/private/umq_ub.c
- URPC framework：src/urpc/framework/；注意使用其冻结 URPC 版本，而非套用 UMQ 版本结论。
- URMA API：src/urma/lib/urma/core/include/urma_api.h
- URMA 控制/数据接口：src/urma/lib/urma/core/urma_cp_api.c、urma_dp_api.c
- 内核六月参照：https://github.com/openeuler-mirror/kernel/tree/41b5f26ff0e91e8af102aa248117e6210b486a40
- 内核 UB 子系统目录：drivers/ub/urma/
- 旧模拟依赖 SystemC：https://github.com/accellera-official/systemc
- 旧模拟依赖 OpenClickNP：https://github.com/bojieli/OpenClickNP

直接 URMA 审查版与 UMQ 使用六月 24 日参考；UMQ 不使用十月最新版。
公开 API 头文件匹配不等于全部厂商 B103 私有 ABI 已验证。

## 已发布的编排/构建脚本源码

这些脚本也已随对应 SCache 分支发布，但归档不等于已适配新安装目录：

| 版本 | 对应 SCache 分支中的位置 |
|---|---|
| direct-URMA 自带脚本 | scripts/rm-ctp/ |
| direct-URMA 双机交付编排 | tools/transport-lab/two-node/ |
| URPC 构建/运行/审计脚本及说明 | tools/transport-lab/scripts/、tools/transport-lab/docs/ |
| UMQ 构建/运行/审计脚本及说明 | tools/transport-lab/scripts/、tools/transport-lab/docs/ |

归档说明在 tools/transport-lab/README_CN.md。
开发脚本保留原实验布局与工具链依赖，不能把 GitHub clone 目录冒充完整预编译安装包；
具体运行仍应按原交付文档，或先恢复对应开发工作区。
不提交日志、密钥、实际 config.env、JAR/SO 或构建缓存。

本源码浏览切换脚本也保存在 SCache 的 tools/source-versions/；
从那个归档位置执行时必须设置 SOURCE_VERSIONS_ROOT，见同目录 README_CN.md。
版本切换只切代码，不会切换 SDK、运行包、硬件参数或编译产物。
