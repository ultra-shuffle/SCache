# UMQ RM/CTP 模拟实验：基线与验收

全部工作在 10.0.3.223 的 /home/xinzhang/umq-spark-scache-20261021，
独立分支 experiment/umq-rm-ctp-20261021。旧 URPC 交付与旧源码不修改。

用户要求沿用现场 6 月 24 日内核对应基线：公开内核参照为
41b5f26ff0e91e8af102aa248117e6210b486a40；UMDK 参照为
099343116b63aad37e2f60f99b71e772bf6bf009。
现场版本为 6.6.0-0001.2.950.ctl4.aarch64 / 25.12.0-B103.ctl4。
公开 API 头文件曾确认与现场哈希一致；这不证明整个公开 provider 与厂商二进制相同。
10 月源码仅作对照，放在 repos/umdk-oct20-reference，不是交付依赖。

验证必须真实调用官方 libumq / libumq_ub，选择 UB + RM + CTP，
禁止使用 URPC framework 的 RC 队列或 Netty payload 冒充通过。
旧 OpenURMA 只有 RTP SystemC 模型；本轮增加明确的 RM/CTP 功能模拟后端，
不把 RTP flit 改个名字当 CTP，也不宣称模拟了 CTP 硬件时序/重传。

逐层验收：原生双进程 UMQ → JNI 读写及负例 → SCache 双向传输 →
Spark 两逻辑节点 → HiBench 四项 → TPC-DS SF1 全 103 语句与标准结果/schema 比较。
还必须检查实际模式、两端任务、无 fallback、错误指标和双方资源清理。
最终文档应包含可运行入口、精确依赖版本、日志和未验证的硬件兼容边界。

这是同一 ARM Linux 内核上的两进程/两逻辑节点功能模拟。
未来真机部署必须重新对现场配套库构建/核对符号，不能用本包覆盖现场 UMDK。
