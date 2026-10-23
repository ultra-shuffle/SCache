# 源码浏览工作区切换

此目录是浏览辅助脚本，不是运行包安装器。三个仓库需要放在同一个 ROOT 下：

```text
ROOT/
  SCache/
  spark-3.5-scache/
  OpenURMA/
```

Spark 只使用 versions/scache-shared；SCache 和 OpenURMA 分别选择
versions/direct-urma、versions/urpc、versions/umq。

已有 223 浏览目录：
`/home/xinzhang/ub-source-versions-20261023`。
可在其根目录执行 `bash switch-version.sh umq`（或 direct-urma、urpc）。

若从本 GitHub 归档位置执行，必须指定包含三个仓库的 ROOT：

```bash
SOURCE_VERSIONS_ROOT=/你的源码浏览根目录 \
  bash SCache/tools/source-versions/switch-version.sh umq
```

第一次从 GitHub clone 时，可分别使用：
```bash
git clone -b versions/umq git@github.com:ultra-shuffle/SCache.git
git clone -b versions/scache-shared git@github.com:ultra-shuffle/spark-3.5-scache.git
git clone -b versions/umq git@github.com:aloooooooha/OpenURMA.git
for repo in SCache OpenURMA; do
  git -C "$repo" branch --track versions/direct-urma origin/versions/direct-urma
  git -C "$repo" branch --track versions/urpc origin/versions/urpc
done
```

以上 branch --track 两行仅在新 clone、尚未创建对应本地分支时执行。
脚本拒绝覆盖未提交修改；不会自动切换 SDK、运行包或编译产物，也不执行 GitHub push。
完整源码/文件位置索引见仓库根 SOURCE_VERSIONS_CN.md。
