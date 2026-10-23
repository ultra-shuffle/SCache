# 最终交付核验

2026-10-21，在 10.0.3.223 完成。

包：spark-3.5.7-scache-umq-rm-ctp-simulator-aarch64.tar.gz，约 925 MiB。
SHA256：776dc36cf80208a8cd1e536f8b1c1707f9fbda63aeb3ccf9c266269242f2a1d9

交付目录：/home/xinzhang/umq-spark-scache-20261021/delivery/

1. 完整 HiBench 四项 + TPC-DS 103 条结果/schema 对照与独立审计通过：
   logs/csm-bex9hsr9/REPORT.json、AUDIT.json。
2. 包内 JNI/SCache JAR 与完整基准使用的文件哈希相同。
3. 压缩包 gzip 完整性检查通过。
4. 实际从最终压缩包解压到新的 build/umq-relocation-ND22X2 目录，
   不编译运行 Spark 双逻辑节点 Shuffle 冒烟通过：
   logs/archive-smoke-terminal.log。
5. 新目录两端只从其自身 dependencies/umq/lib 加载 UMQ/URMA/JNI/provider，
   未加载开发目录的库或旧 URPC framework，两端关闭通过。
6. 端口占用负例按预期停止，终端汇总约 1.4 KiB，
   完整 REPORT.json 与简短 REPORT.txt 均生成。
7. 旧 URPC 压缩包 SHA256 仍为
   a1df6eba38b909e9580c4911005d9cef2bc2f1181281baeb8908c519a32a4345，
   原直接 URMA 源文件哈希也未变化。

基线为六月 24 日 UMDK 0993431 / 内核参照 41b5f26，系统 glibc 最低 2.38。
这是功能模拟预编译包，不是现场硬件已适配/通过的证明。部署步骤与未验证边界见
部署与运行_CN.md；完整修改依据和测试情况见包内 docs。
