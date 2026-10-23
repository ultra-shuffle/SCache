# versions/umq 开发脚本源码快照

此目录随源码发布，用于审阅和保留完整构建/运行/审计逻辑。
它不是新的预编译安装包，也不承诺在本目录直接运行所有脚本。

- `scripts/` 与 `docs/` 原样保存本实验的开发脚本、验证说明及历史边界。
- 原工作区：`/home/xinzhang/umq-spark-scache-20261021`。
- 原脚本以实验根目录为 R，要求 `repos/SCache`、`repos/OpenURMA`、`repos/umdk`、
  `build/`、`runtime/package/` 和已准备的工具链/构建缓存等布局。
- 部分开发构建脚本引用 223 上的固定工具链/缓存；它们不因此变为可跨机直接执行的安装器。
- 若迁移开发环境，需先恢复该布局、固定 SDK 及依赖，再按源码调整本地路径；
  不能直接把本目录当作实验根运行，也不能套用另一个版本的 SDK。
- 预编译运行方式与测试边界见 `docs/DEPLOY_SIMULATION_CN.md`。

不提交运行日志、JAR/SO、密钥、实际现场 config.env 或构建缓存。
