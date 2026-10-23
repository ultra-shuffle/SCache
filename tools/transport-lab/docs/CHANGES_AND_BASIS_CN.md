# 本轮改动与依据

所有新工作位于 /home/xinzhang/umq-spark-scache-20261021。
旧 URPC 交付与原直接 URMA 交付保持独立。Spark 源码另建分支，但沿用已构建的
Spark/SCache 接口集成；此次更换的是 SCache 传输实现，不重写 Spark SQL。

## 依据

- [UMDK 固定源码](https://github.com/openeuler-mirror/umdk/tree/099343116b63aad37e2f60f99b71e772bf6bf009)：
  src/urpc/include/umq/umq_api.h、umq_types.h、examples/umq/，
  以及 umq/umq_ub/core/umq_ub_impl.c、private/umq_ub.c。
- [内核固定源码参照](https://github.com/openeuler-mirror/kernel/tree/41b5f26ff0e91e8af102aa248117e6210b486a40)：
  仅作接口/实现对照，不把它安装到223或现场。
- SDK 使用公开 UMQ 初始化、队列创建、bind info 交换、bind、enqueue/dequeue、
  unbind/destroy/uninit；显式选择 RM/CTP。未把官方 URPC RC 路径改名冒充 RM/CTP。
  UMQ 官方源码仍复用部分 urpc_manage / urpc_thread 公共组件，日志出现这些函数名
  不等于使用了 liburpc_framework 的 RC 传输。应以实际加载库、队列和导入参数核验。
  BASE 模式不开启 remote-memory feature，因此此消息方案不直接导入对端应用 MR。

## 新增或修改

1. SCache native/spark_umq：UMQ 消息请求/响应引擎、JNI 与原生测试。
   软件 grant 校验地址、长度、generation/cookie；分片读写，超时禁止回退并保留资源。
2. Java UmqNative / UmqUbTransport：新增显式 umq backend，适配已有 UbTransport。
   Scala UBBlockTransferService 接入指标及传输准入；原 real backend 不改模式。
3. 集成测试：JNI 正常读写/负例、SCache 双 JVM 双向取块、
   Spark 两逻辑节点及标准结果对照。不是只跑一个独立 C 示例。
4. OpenURMA 新 functional profile：RM/CTP 能力、SEND/接收/ACK 完成、严格模式拒绝，
   六月 TLV 设备查询适配；不是升级或修改现场驱动。
5. 脚本/包：独立构建、库归档、预编译入口、结果审计和版本清单。

## 已查明并修正的适配问题

- JVM 加载 JNI 为局部符号，而六月 UMQ 插件需要 libumq 的公共工具符号：
  仅将已加载的配套 libumq 提升到全局可见；不更换 SDK、不修改官方库源码。
- 六月 umq_ub_dequeue_with_poll_rx 会合并多条接收消息：
  不能把 qbuf_next 非空当作分片错误。逐条拆链、校验长度并释放，支持双向并发。
- 小块池必须扣除元数据和自定义协议头；请求 payload 上限为 3840。
- 六月 UMQ 初始池为 1 GiB，不能配置成小于初始池的 max；使用 2 GiB 上限，
  内存开销明确记录，不声称已做性能优化。
- 原 RTP SystemC 不具备 CTP 硬件模型：新增明确标注的 verbs 功能模拟，
  不修改模式标识伪造硬件支持。官方 SDK 源码保持干净。

详细通过情况以本次 REPORT/AUDIT 为准；历史失败日志保留在工作目录，
不能用模拟 PASS 替代真实 UB 双机验收。
