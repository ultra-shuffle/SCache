# 本轮验证结果

验证日期：2026-10-21。地点：10.0.3.223。全部为隔离模拟实验，不代表真实 UB 硬件通过。

## 冻结的源码

- SCache 新分支提交：7af7f75（新增 UMQ 后端与测试）。
- OpenURMA 新分支提交：146289c（RM/CTP functional profile 与六月 TLV 查询）。
- Spark 独立源码分支：99357e52267d1466c7421836f40fdffcade24fc1；沿用已编译集成版本。
- UMDK 原始公开参考：099343116b63aad37e2f60f99b71e772bf6bf009；
  本地原样快照提交 3a6a345373780bdfa370031b61d2f54d104ae803。
  与 upstream/umdk-20260624 逐文件比较无差异。
- 内核源码参照：41b5f26ff0e91e8af102aa248117e6210b486a40；未安装或修改任何内核。

## 实际验收

| 项目 | 结果 | 证据目录（相对工作根目录） |
|---|---|---|
| 官方 UMQ 原生两进程读/写/回读 | PASS，210000 字节，双方关闭通过 | logs/umq-native-wfwr7G |
| JNI 两 JVM 读写及负例 | PASS，越界、失效 generation 拒绝后可继续正常请求 | logs/jsm-dZVWbI |
| SCache 两 JVM 双向取块 | PASS，两端各 3 块，共各 1400065 字节 | logs/ssm-xHfaPw |
| Spark 两逻辑节点 Shuffle | PASS，两端 Executor 均执行任务 | logs/csm-bex9hsr9 |
| 标准 Spark / UMQ HiBench 四项 | 两组均 PASS | 同上 |
| 标准 Spark / UMQ TPC-DS SF1 | 两组均完成 103 语句，103 份结果与 103 份 schema 一致 | 同上 |
| 独立证据审计 | PASS，两端无 RPC 失败/数据回退，正常关闭 | 同上 AUDIT.json |
| 预编译交付目录直接运行 | PASS，未调用编译器 | logs/delivery-smoke-terminal.log |
| 端口冲突负例 | 按预期 FAIL，并输出简短终端摘要和 REPORT.txt | 交付目录的独立失败 RUN |

TPC-DS UMQ 应用记录远端读 720528225 字节。
整组 UMQ 应用合计：A 101279 次 RPC / 385814006 字节，B 88044 次 RPC /
335023183 字节；RPC failures 均为 0。CQ/DMA、lease、generation、checksum、fallback、
TCP/Netty payload 错误/回退指标与最终活跃资源按审计规则归零。

JNI 负例中有意发送一次失效 generation，因此其测试专用 rpcFailed=1 是期望的拒绝；
不能把它和完整基准中必须为 0 的 rpcFailed 混为一谈。
日志也保留控制连接关闭时的 Connection reset、重复清理 block 的 WARN；
它们未造成该轮任务失败或结果不一致，不宣称整个软件已不存在其他 bug。
模拟 GET_EID_LIST ioctl 尚走官方 sysfs fallback，日志中的 cmd=37 / ENOTTY
是当前 fake kernel 的边界，不代表测试机真实驱动出错。

所有应用实际加载新隔离目录 dependencies/umq/lib 下的官方六月 libumq、libumq_ub、
liburma 和 functional provider。未加载旧 liburpc_framework / libScacheUrpc。
模拟 provider 记录 RM/CTP 队列和远端端点导入，不调用旧 RTP flit 通路。

## 仍未证明的事情

没有真实 UB 网卡、两机物理链路、UMQ 在现场 B103 库上的 ABI 验证或 UBAGG/UBMAD
控制面验证；没有 CTP 时序/重传/拥塞模型。消息 READ/WRITE 不是单边零拷贝 DMA。
仅支持单 peer、单在途请求的实验版本，不能直接推广为四节点生产方案。
现场仍须按部署文档的实机验证流程推进，不能替换厂商 UMDK 后直接宣称问题已解决。
