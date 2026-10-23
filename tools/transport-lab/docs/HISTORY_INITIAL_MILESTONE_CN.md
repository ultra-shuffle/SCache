# 实施记录与验证边界（2026-10-21）

所有命令在 10.0.3.223 的新目录执行：
`/home/xinzhang/urpc-spark-scache-20261021`。没有修改旧源码、旧交付包、
现场内核/驱动，未推送 Git。当前不是可交付完成版。

## 基线与分支

四个独立仓库分支均为 `experiment/urpc-sim-20261021`：

- SCache：克隆旧 Git 历史后覆盖 10 月 15 日 reviewed source，
  保存基线提交 `e3db8f0`，实验提交 `7fcada3`。
- Spark：同样保存 reviewed source，基线提交 `99357e52`，尚未修改传输调用。
- OpenURMA：原提交 `673421a`，仅在新副本中适配模拟 provider。
- UMDK/URPC：原提交 `4eab3e4ad170b06bfe5d5c1014341e81edb9bf58`；
  官方 framework 源码目前未改动。

额外依赖：OpenClickNP 克隆其官方仓库；SystemC 使用 Accellera 2.3.4
（`5fc1469339775b54b1fcc2020ac744a58be5b50e`），仅安装到本目录 build。
SystemC topology 复用旧 OpenURMA 的已生成代码，后续需固定生成器版本、
验证与本分支元素源码的一致性，不能把生成代码视作独立权威源码。

## 已经实际执行

1. 原生 ARM 编译完整官方 UMDK、URPC framework、示例、perftest。
2. 原生 ARM 编译 OpenURMA SystemC provider。
3. 运行官方 `urpc_framework_example` 两进程测试，退出码 0，
   请求/响应、示例中的 reference READ 得到数据。
4. 运行新 C++ 桥接，两进程模拟设备 openurma0 / openurma1：
   READ 4096 字节逐字节核对，WRITE 4096 字节后 READ 回读核对。

证据：

- `logs/umdk-build.log`
- `logs/provider-sc-build.log`
- `logs/fsm-He56cx/{server,client}.log`：官方示例数据测试通过；
  **退出清理仍有 query_jfr / 未释放队列错误，不能称生命周期通过。**
- `logs/bsm-31Ck24/{server,client}.log`：桥接数据测试通过，
  `SCACHE_OFFICIAL_URPC_READ_WRITE_PASS bytes=12288`；
  桥接尚未实现完整关闭流程，不能当成最终测试通过。

早期失败日志保留用于排错，不属于通过证据。最早的 smoke 脚本曾因 shell
AND-list 的退出语义误打印 PASS，已改成显式失败分支；只能使用这里列出的
数据证据及后续重新验证结果。

## 为什么修改模拟器

官方框架需要的接口多于旧直接 URMA demo：

- 设备 async_fd 必须能被 epoll 监听：改用 eventfd，仍不伪造硬件错误事件。
- 多个 Jetty 必须按目标 ID 路由，不能始终投递到最后创建的队列。
- 接收完成项必须携带源 Jetty identity，否则框架无法找到响应目标。

这些是 OpenURMA **模拟器**适配，不是给真实 UDMA/UBAGG 打补丁；
不代表解决了现场的 UBMAD UM/UTP 导入问题。

## 新桥接的数据路径

应用授权 arena 地址范围（带 generation/cookie）→ URPC 请求 → 服务端
核验范围和身份 → 官方 `urpc_func_return` 返回数据 → 调用端校验长度。
底层使用官方 `urpc_func_call / urpc_func_poll / urpc_func_return`；
URPC 的缓冲区由 `urpc_mem_seg_register` 注册。

第一版是 RPC READ/WRITE 语义，不是直接 one-sided URMA READ/WRITE；
arena 内容通过注册 staging pool 搬运，有额外内存拷贝。
当前固定 4 KiB 分块、每进程一个 peer、仅模拟环境验证，
没有宣称真实 RM/CTP、零拷贝或硬件带宽。
所用官方 framework 此版本的 Jetty 路径本身使用 RC：
`src/urpc/framework/core/queue/jetty/jetty_public_func.c`。

## 尚未完成，不能省略

- 桥接显式关闭、缓冲区所有权归还及故障隔离测试。
- JNI 和 SCache UbTransport 适配、无 fallback 的选择及统计证据。
- 两节点 SCache 数据面校验，包括并发、越界、错误身份及失联。
- Spark Shuffle smoke；HiBench 四项与 TPC-DS 全部 SQL 的实际运行及结果核对。
- 可重跑的一键构建/启动/收集日志、最终部署文档。

目前不需要现场人员运行这套实验原型。
