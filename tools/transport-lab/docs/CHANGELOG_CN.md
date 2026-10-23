# 2026-10-21 改动与依据

全部工作位于 223 的 urpc-spark-scache-20261021，产品仓库分支
experiment/urpc-sim-20261021；没有修改旧交付或推送代码。

## SCache：新 backend，不是重命名旧 URMA

1. 新增 UrpcNative、UrpcUbTransport、native/spark_urpc，通过官方 framework
   call/poll/return 实现 READ/WRITE RPC；工厂显式选择 urpc，保留原 real 路径。
2. 能力标为 OFFICIAL_URPC，不把模拟 RPC 宣称为真实硬件 backend。
3. 沿用原 arena、block generation、CRC、lease/consumer 协议，增加 endpoint
   UUID、窗口/授权/generation/cookie 核对及实际 RPC 指标。
4. 双队列分别负责 client/server，接收后补投 RX；关闭显式 flush、unpair、
   remove/destroy/unregister，检查 staging 与 arena 授权没有遗留。
5. 初始化 promise 改 shared_ptr，修复 I/O 线程生命周期问题。
6. 最大 raw payload 从 4 KiB 改为 32 KiB，staging 仍为 32 MiB；Java/JNI
   交叉核验尺寸，并与用户/peer 限制取最小值。
7. URPC 的准入锁放在获取远端 lease 之前，避免已持有 lease 的控制连接
   在阻塞传输队列中长时间无通信；原 real 路径不受此串行锁影响。
8. 拒绝只读目标/注册缓冲，授权登记失败时撤销 native grant。

第 6、7 项依据实际全测失败：旧 4 KiB 版本通过 HiBench 四项，但 TPC-DS
q50 期间出现控制连接超时及 lease 连带错误；证据保留在
logs/csm-xjpo2nf0 和 logical-cluster-bench.log。
修正后的最终全测另看对应 REPORT，不能用旧的部分通过代替。

## OpenURMA 模拟 provider

为 async_fd 提供 eventfd；按 Jetty ID 路由多端点；完成项带源身份；
补齐 query_jfr 与 ERROR 状态下接收队列 flush。没有修改真实 UDMA 驱动。

## 未修改部分与交付

Spark 产品源码没有新增 URPC 调用：已有 Spark→SCache 接口足够承载新 backend。
复用相同基线的预编译 Spark，重新编译 SCache、测试类和 JNI。
固定官方 UMDK/URPC 源码未改，未绕过 RC 配对行为，未实现现场 RM/CTP URPC。
没有修改 UBMAD 默认 TP 类型，也不声称解决了现场硬件问题。

新增分层 smoke、完整 logical-cluster 和预编译打包入口；与旧硬件脚本隔离。
清理只针对本轮创建的进程，保留成功/失败的独立证据和实际库加载记录。
