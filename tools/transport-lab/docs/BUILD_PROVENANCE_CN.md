# 源码、构建与复现边界

本文件针对 223 上的开发目录，不是要求部署者重新编译。
预编译包运行见 DEPLOY_SIMULATION_CN.md。

## 固定来源

- SCache：基线 e3db8f0，实验 HEAD 1b1268242a4f4c2bd400490daf9833c9618c904c。
- Spark：基线与实验 HEAD 均为 99357e52267d1466c7421836f40fdffcade24fc1。
- 官方 UMDK/URPC：4eab3e4ad170b06bfe5d5c1014341e81edb9bf58，源码未修改。
- OpenURMA：基线 673421a，实验 9b74b0c82684a31e756b5085662707fbe9bfbe10。
- Accellera SystemC 2.3.4：5fc1469339775b54b1fcc2020ac744a58be5b50e。
- OpenClickNP：c1c6acc58032a1894507d88659b3cca668b0e1a5。

官方 framework API/例程在 repos/umdk/src/urpc；不是本项目自定义的同名库。
最终二进制 SHA256 与上述精确提交写入运行包 metadata/urpc-manifest.json。
Spark 使用原 reviewed package 对应的预编译版本，没有声称本次重新编译 Spark。

## 223 上的增量构建

```bash
cd /home/xinzhang/urpc-spark-scache-20261021
bash scripts/build-provider.sh
bash scripts/build-jni.sh
bash scripts/build-scache.sh
bash scripts/jni-smoke.sh
bash scripts/scache-service-smoke.sh
python3 scripts/run-logical-cluster.py --stage bench
```

构建脚本与开发 cluster 使用同一个锁，禁止边运行边覆盖二进制。
预编译交付目录是独立副本，开发构建不会修改正在测试的交付副本。
JDK/sbt launcher 来自 223 的 codex-fullkit-runtime-20260918；
sbt 依赖缓存只复制到本实验 build/sbt-cache，然后离线构建。
缓存与基线包属于开发前置依赖，不包含在无需编译的交付入口中。

## 从源码重建模拟依赖的说明

SystemC 使用 Release、BUILD_SHARED_LIBS=ON、CMAKE_CXX_STANDARD=17，
安装前缀只为本目录 build/systemc-install；没有安装到系统目录。
官方 UMDK 的 CMake 选项为 BUILD_ALL=disable、BUILD_URMA=enable、
BUILD_URPC=enable、BUILD_UDMA=disable，输出到 build/umdk。
这里仅使用构建目录的用户态库，没有运行 cmake --install 写入 /usr/local。

OpenURMA 的 build-provider.sh 是增量入口，要求已有 PIC 的 SystemC facade。
首次构建要设置 OPENCLICKNP_ROOT、SystemC include/lib 路径，再执行
repos/OpenURMA/scripts/build_libsc.sh 和 integration/umdk/build_provider.sh，
选择 NIC_IMPL=sc、UMDK_BUILD_DIR 指向本实验 build/umdk。
不要把默认 NIC_IMPL=stub 当成 SystemC 验证。

当前使用的 topology.cpp 最初从旧生成目录复制，已进行独立来源复核：
用上述固定 OpenClickNP 源码重新构建 openclicknp-cc，并对本分支
examples/openurma/topology.clnp 重新生成到 build/regenerated-topology。
与实际使用代码比较，只存在源路径注释以及增加的
`#include "openurma/ub_flit.hpp"`；忽略这两项后 diff 为空。
topology_tlm.cpp 仅源路径注释不同。不能把这个人工 include 差异隐去。
生成/编译记录见 logs/generator-{configure,build,run}.log；重新从干净目录
生成时需要保留 UB flit include 适配，避免把旧生成目录当成必然存在。

## 分层测试与完整验收

JNI probe 有意发送一个错误 generation，必须被服务端拒绝；其 rpcFailed=1
是负例预期，不是生产 benchmark 容许出错。完整 benchmark 要求错误计数为零。
64 MiB 分块测试核对逐字节模式，SCache service 测试核对 CRC/lease 与双方关闭。
最终完整流程还要独立审计 Spark 事件日志，确认两端 Executor 均实际执行任务，
再核对所有 TPC-DS 结果/schema 与无 fallback、无遗留 lease、无 outstanding RPC。

```bash
python3 scripts/audit-cluster-report.py /本轮日志目录/REPORT.json
```

初始化失败的回滚、长期运行、恶意输入、节点崩溃和真实硬件故障恢复尚未完整验证。
native 单个 RPC 的超时目前固定 15 秒，不等同于全部 SCache 配置超时；
每 JVM 一个固定 peer、一次 runtime 生命周期，不支持进程内重新初始化。
