# SCache

SCache is a shuffle cache/daemon integrated with the local Hadoop + Spark trees in this workspace.

The previous (upstream-style) README is archived as `README.old.md`.

## Dependencies (local git repos)

SCache now depends on these repositories being present at the following paths:

- `${HOME}/hadoop/`: [Hadoop](https://github.com/XingZYu/hadoop)
- `${HOME}/spark-3.5/`: [Spark](https://github.com/XingZYu/spark-3.5-scache)
- `${HOME}/spark-apps/`: [Spark Apps](https://github.com/XingZYu/spark-apps)
- `${HOME}/spark-apps/HiBench-7.1.1/`: [HiBench Suite](https://github.com/XingZYu/hibench)

## Build

### Build SCache

```bash
cd ${HOME}/SCache
sbt publishM2   # publishes org.scache to ~/.m2 (needed by spark and hadoop)
sbt assembly    # fat jar for deployment
```

Artifacts:

- `target/scala-2.13/scache_2.13-0.1.0-SNAPSHOT.jar`
- `target/scala-2.13/SCache-assembly-0.1.0-SNAPSHOT.jar`

### Build Hadoop

```bash
cd ${HOME}/hadoop
mvn -DskipTests -Pdist -Dtar package
# For faster build
mvn package -T 1C -Pdist -DskipTests -Dtar -Dmaven.javadoc.skip=true -Denforcer.skip=true
```

### Build Spark 3.5

```bash
cd $HOME/spark-3.5
# Verified 
./build/sbt -Phadoop-3 -Pscala-2.13 package
# Not verified yet
./dev/make-distribution.sh -DskipTests
```

## Deploy / Run

### Start SCache

1. Configure cluster hosts in `conf/slaves` and settings in `conf/scache.conf`.
2. Distribute SCache to the cluster and start it:

```bash
cd $HOME/SCache
sbin/copy-dir.sh
sbin/start-scache.sh
```

Stop:

```bash
cd $HOME/SCache
sbin/stop-scache.sh
```

### Enable in Hadoop MapReduce

- Put `target/scala-2.13/SCache-assembly-0.1.0-SNAPSHOT.jar` on the YARN classpath (for example, copy it to `$HADOOP_HOME/share/hadoop/yarn/lib/` on every node).
- Set the following in `$HADOOP_HOME/etc/hadoop/mapred-site.xml`:

```
mapreduce.job.map.output.collector.class=org.apache.hadoop.mapred.MapTask$ScacheOutputBuffer
mapreduce.job.reduce.shuffle.consumer.plugin.class=org.apache.hadoop.mapreduce.task.reduce.ScacheShuffle
mapreduce.scache.home=$HOME/SCache
```

### Enable in Spark

- Make the SCache jar visible to drivers/executors (either copy to `$SPARK_HOME/jars/` or set `spark.scache.jars`).
- Set (for example in `$SPARK_HOME/conf/spark-defaults.conf`):

```
spark.scache.enable true
spark.scache.home $HOME/SCache
spark.scache.jars $HOME/SCache/target/scala-2.13/SCache-assembly-0.1.0-SNAPSHOT.jar
spark.shuffle.useOldFetchProtocol true
```

### Workloads / benchmarks

- Standalone Spark scripts and HiBench in `$HOME/spark-apps/` (see `$HOME/spark-apps/README.md`).
