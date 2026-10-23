# 官方 URPC 版 Spark/SCache：实现与验证边界

本实验在 10.0.3.223 的独立目录实施，原工作区和旧交付包未修改。
所有产品实验仓库使用 `experiment/urpc-sim-20261021` 分支，没有推送。

## 1. 什么路径发生了变化

数据路径是：

`Spark Shuffle → 本节点 SCache mmap/IPC → SCache URPC backend → JNI
→ 官方 liburpc_framework → 官方 liburma → OpenURMA 模拟 provider
→ 对端 provider / URPC → 对端已授权的 arena`

Spark 和 SCache 的控制 RPC、元数据和连接建立仍可使用 TCP/Netty。
禁止的是 Shuffle payload 回退到 Netty/普通 Spark 文件传输，不是禁止整个进程使用 TCP。
OpenURMA 的模拟 wire 是用户态 UNIX datagram，承载模拟 flit 和实际测试 payload；
它不是物理 UB，不能据此声称硬件带宽、时延或现场 UBAGG 问题已解决。

目前是同一台 ARM Linux 主机上的两个逻辑节点，各有独立 SCache JVM、
Spark Worker 和 Executor；不是两台独立 OS/VM，更不是实机双机链路通过。
两个地址 `127.0.0.1 / 127.0.0.2` 是模拟拓扑，不是交付现场 IP。

## 2. 关键源码

| 新分支中的位置 | 作用 |
|---|---|
| SCache/src/main/java/org/scache/network/ub/UbTransportFactory.java | 只有显式选择 `urpc` 才进入新 backend；原 `real` 路径不改成 URPC |
| 同目录 UrpcUbTransport.java | arena 授权、窗口校验、peer 身份、分块、逻辑完成和指标 |
| 同目录 UrpcNative.java | 独立加载 `libScacheUrpc.so`；不是旧 `libSparkUrma.so` |
| SCache/native/spark_urpc/urpc_bridge.cpp | 官方 URPC 初始化、队列、函数调用/响应、I/O 线程和关闭 |
| 同目录 urpc_jni.cpp | JNI 生命周期、DirectByteBuffer 范围核对及数据复制 |
| SCache/src/main/scala/org/scache/network/ub/UBBlockTransferService.scala | 复用原有 block/lease/generation/CRC/arena 协议，允许显式 URPC 并导出指标 |

Spark 产品源码没有新增 URPC 调用：既有改造已经把 Shuffle 接入 SCache；
本次保持该接口不变，在 SCache 的传输入口加入官方 URPC backend。
实验使用与基线一致的预编译 Spark 3.5.7 / Scala 2.13.8。
SCache、测试类和新 JNI 必须重新编译，不能使用旧 SCache JAR 搭配新 JNI。

## 3. 官方实现依据

固定 UMDK 提交：
`4eab3e4ad170b06bfe5d5c1014341e81edb9bf58`，
其来源为 `https://gitee.com/openeuler/umdk.git`，保存在本目录 repos/umdk。

对应源码：

- `src/urpc/include/framework/urpc_framework_api.h`：公开 framework API。
- `src/urpc/examples/framework/urpc_framework_example.c`：初始化、内存、队列和配对。
- 同目录 `urpc_framework_client.c / urpc_framework_server.c`：call / poll / return。
- `src/urpc/framework/core/queue/jetty/jetty_public_func.c`：该版本底层 RC Jetty 路径。

实际调用包括 `urpc_init`、`urpc_allocator_register`、
`urpc_mem_seg_register`、`urpc_queue_create`、
`urpc_channel_server_attach`、`urpc_channel_queue_add/pair`、
`urpc_queue_rx_post`、`urpc_func_call`、`urpc_func_poll`、
`urpc_func_return`，以及对应的 flush/unpair/remove/destroy/unregister。

没有把旧 URMA backend 或 Netty 包装后命名为 URPC。
也没有修改官方 framework 来绕过校验。OpenURMA 模拟器另有明确适配记录。

## 4. 不能把哪些东西混为一谈

- 该固定官方 framework 的队列实现使用 RC；新 backend 显式要求
  `rc-rtp / EID index 0 / priority 0`，不接受 rm-ctp 后悄悄换成 RC。
- 应用 arena 通过地址范围、generation、cookie 和 endpoint UUID 授权。
  它不是把整个 arena 暴露为 one-sided RDMA MR；URPC staging pool 才注册给底层 URMA。
- READ/WRITE 是新的 RPC 语义：服务端核验授权范围后复制并响应。
  它与原来的直接 `urma_import_seg / urma_post_jetty_send_wr(READ)` 不等价。
- 因此这次通过不能证明“改用 URPC 就能解决现场 UBMAD UM/UTP 不兼容”。
  真机 RM/CTP 必须另选/适配支持该模式的官方版本，再做设备、跨机与故障验证。

## 5. 已发现并处理的适配问题

1. 旧模拟 provider 的 async_fd 为 -1：官方 framework 会监听它，需提供 eventfd。
2. 旧模型只记最后创建的 Jetty：改为按目标 ID 路由，完成项带正确源身份。
3. 退出需要 query_jfr 和接收队列 flush：补齐模拟器的状态查询和 flush。
4. 双向通信不能复用同一个 RC 配对队列：每进程分离 client / server 队列。
5. 用户队列接收完成后需要补投 RX：不补投会在初始接收深度耗尽后超时。
6. I/O 初始化 promise 必须活到线程结束：改用共享所有权，避免异常分支悬空。
7. 小块 RPC 调度开销：本版将最大 raw payload 提到 32 KiB，同时保持
   staging 总量 32 MiB；JAR/JNI 交叉核验 raw limit，并与 peer/用户配置取最小值。
   最终是否交付以最终验证报告对应的二进制和哈希为准。
8. 阻塞 URPC 的准入锁放在远端 lease 获取之前，避免并发排队的调用
   先持有远端 lease 后长时间闲置控制连接；仅新 urpc 路径启用。

这些修改仅发生在新分支，既有 URMA 产品路径和现场内核/驱动没有被修改。

## 6. 保留的限制

每进程一个固定 peer、一个 URPC runtime，native 调用由一个 I/O 线程串行处理；
当前 Java read/write 是阻塞到 RPC 数据返回后再提供完成 ID，不是高并发异步实现。
存在 staging、响应缓冲和 arena 之间的额外复制，不能宣称零拷贝。
32 KiB 是本模拟 wire 的保守限制，不是 URMA 硬件的统一上限。

`urpc.stagingCopyBytes` 统计的是经过 staging 的逻辑 payload 字节，
不是所有 memcpy 的字节总和。现有 `urma.*` 键为上层保留的历史指标名；
URPC 下部分 request/chunk 指标是 RPC 投影，不应解读为硬件 CQ/WQE 统计。

失联、异常完成或所有权未明时不会尝试 Netty fallback；会失败并保留隔离状态。
初始化失败及异常关闭不是“自动修复硬件”的机制。失败报告必须保留，
不要以单个 PASS 字样代替完整结果。
