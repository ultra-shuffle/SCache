# versions/direct-urma：UB 链路源码入口

原 10 月 15 日审查版；JNI 直接调用 URMA，支持代码配置 RM/CTP 或 RC/RTP。

## 版本身份

- 已验证应用源码基线：`e3db8f0b12bcc97f97a211894512c4189b0defae`。
- 本次发布提交由 aloooooooha 署名；原有历史不重写。
- 固定官方 UMDK：`099343116b63aad37e2f60f99b71e772bf6bf009`。
- [共用 Spark 分支](https://github.com/ultra-shuffle/spark-3.5-scache/tree/versions/scache-shared)：`versions/scache-shared`，Spark 3.5.7，源码基线 `99357e52267d1466c7421836f40fdffcade24fc1`。
- 现场 UBAGG/UBMAD 导入问题未证明解决；不能把离线合同/标准网络测试视为真实 UB 通过。

## 调用链文件

| 文件 | 作用 |
|---|---|
| [UBBlockTransferService.scala](src/main/scala/org/scache/network/ub/UBBlockTransferService.scala) | 发布/发现/lease、远端读取、校验与回收 |
| [UbTransportFactory.java](src/main/java/org/scache/network/ub/UbTransportFactory.java) | 显式选择传输后端，不隐式回退 |
| [RealUbTransport.java](src/main/java/org/scache/network/ub/RealUbTransport.java) | 服务层到当前 Java transport |
| [native 核心](native/spark_urma/src/urma_transport.cpp) | 直接查看官方通信接口调用顺序 |
| [native 目录](native/) | JNI、Native 实现与测试 |
| [集成测试](src/integration/) | Java/Scala 合同与服务测试 |
| [开发脚本快照](tools/transport-lab/) | 对应版本构建、运行及审计脚本源码 |

三个 SCache 版本分支是：
[直接 URMA](https://github.com/ultra-shuffle/SCache/tree/versions/direct-urma)、
[URPC](https://github.com/ultra-shuffle/SCache/tree/versions/urpc)、
[UMQ](https://github.com/ultra-shuffle/SCache/tree/versions/umq)。

分支可能保留旧后端代码以保持兼容；实际使用由配置与运行日志决定，不是看目录是否存在。
这些发布提交只增加源码索引/开发脚本快照，不改变已验证核心源码。
