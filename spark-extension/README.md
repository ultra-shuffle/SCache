# SCache Spark extension

This module contains the parts of the UltraShuffle experiments that can live outside Spark core
without changing normal shuffle behavior. It is a separate, thin jar built for Spark 3.5.7 and
Scala 2.12; it is not included in the SCache daemon assembly.

The extension provides five independent pieces:

- an opt-in remote-fetch cost model exposed as a `SortShuffleManager` subtype;
- a Spark-independent adaptive telemetry model with explicit persistence;
- a pure native/PBR/record-PBR planner with immutable inputs and explainable decisions;
- a driver-only adaptive telemetry `SparkPlugin` backed by standard listener metrics;
- an opt-in `SparkPlugin` bridge for on-demand JVM/process gauges.

All features default to disabled. Adding the jar to a class path does not register a listener,
start a thread, read or write a telemetry file, poll an MXBean, or change a shuffle path.

## Build

Use JDK 9 or newer to build; the resulting classes target Java 8. Spark and Scala are provided by
the runtime and are not bundled into the jar.

```bash
cd /path/to/SCache
sbt "sparkExtension/packageBin"
```

Artifact:

```text
spark-extension/target/scala-2.12/scache-spark-extension_2.12-0.1.0-SNAPSHOT.jar
```

The root project does not aggregate or depend on `sparkExtension`, so the existing `sbt assembly`
command still builds only the SCache daemon. The extension deliberately has no assembly target.

Run the framework-free checks with:

```bash
sbt \
  "sparkExtension/Test/runMain org.apache.spark.shuffle.scache.RemoteFetchCostSelfTest" \
  "sparkExtension/Test/runMain org.scache.adaptive.AdaptiveShufflePlannerSelfTest" \
  "sparkExtension/Test/runMain org.scache.telemetry.AdaptiveTelemetryRegistrySelfTest" \
  "sparkExtension/Test/runMain org.scache.telemetry.JvmProcessMetricsSelfTest" \
  "sparkExtension/Test/runMain org.scache.spark.telemetry.ScacheAdaptiveTelemetryPluginSelfTest" \
  "sparkExtension/Test/runMain org.scache.spark.telemetry.ScacheProcessMetricsPluginSelfTest"
```

## Deployment

Place the thin jar on the initial driver and executor class paths, for example with `--jars` or in
`$SPARK_HOME/jars`. `spark.scache.jars` alone is insufficient: that setting belongs to SCache's
later reflection loader, while Spark loads a shuffle manager and `SparkPlugin` during startup.

This module is compiled against the official Spark 3.5.7 / Scala 2.12 API. The remote-cost manager
subclasses Spark's internal `SortShuffleManager`, so treat that part as version-locked and rebuild
it when changing Spark or Scala versions.

## Remote-fetch cost injection

On an otherwise unmodified Spark 3.5.7 distribution, configure:

```text
spark.shuffle.manager org.apache.spark.shuffle.scache.RemoteFetchCostShuffleManager
spark.shuffle.remoteFetchCost.enabled true
spark.shuffle.remoteFetchCost.bandwidth 12g
spark.shuffle.remoteFetchCost.fixedLatency 5us
```

The manager remains a `SortShuffleManager`, preserving Spark SQL's serialized/no-copy shuffle
checks. It wraps only the read metrics reporter: when Spark reports a successfully consumed remote
buffer, the task thread reserves shared per-executor bandwidth, waits for the modeled deadline,
and adds the actual wait to Spark's existing fetch-wait metric. Sub-millisecond waits are retained
in the reader-local reporter and carried into later whole milliseconds instead of being truncated
per buffer. Local and host-local blocks are not charged. Fixed latency may overlap across tasks;
transfer bandwidth is shared.

When `spark.shuffle.remoteFetchCost.enabled=false` (the default), the original metrics reporter is
passed directly to Spark and no wrapper is installed. For an untouched baseline, also leave
`spark.shuffle.manager` unset so Spark does not load the extension at all.

Do not combine this manager with the Spark branch that already contains
`org.apache.spark.storage.RemoteShuffleFetchCost`: both implementations use the same three
settings. The extension detects that core implementation and refuses to enable rather than charge
every block twice. A Spark application can have only one custom shuffle manager, so this manager
also cannot be stacked with another manager such as Celeborn.

## Adaptive telemetry and persistence

`org.scache.telemetry.AdaptiveTelemetryRegistry` is a pure Java, instance-scoped model. It exposes
explicit `observeMap`, `observeStage`, `observeRead`, `observeCxlReadStage`, `snapshot`, and `flush`
methods. It does not install Spark callbacks; existing SCache/Spark instrumentation must call it at
the already-established observation points.

`TelemetryConfig.from(Map<String, String>)` accepts the following Spark-friendly keys, equivalent
unprefixed `scache.telemetry.*` keys, and the old `spark.ultrashuffle.adaptive.*` model keys:

```text
spark.scache.telemetry.adaptive.enabled false
spark.scache.telemetry.adaptive.readOnly false
spark.scache.telemetry.adaptive.file ""
spark.scache.telemetry.adaptive.profile default
spark.scache.telemetry.adaptive.ewmaAlpha 0.25
spark.scache.telemetry.adaptive.autoPersistEverySamples 0
spark.scache.telemetry.adaptive.fullConfidenceSamples 16
spark.scache.telemetry.adaptive.cxlBandwidthBytesPerSec 2684354560
spark.scache.telemetry.adaptive.recordPbrBandwidthBytesPerSec 2684354560
spark.scache.telemetry.adaptive.pbrCostPerBlockMs 0.05
```

The model and all updates are disabled by default. Persistence is also disabled unless a file is
set and `flush()` is called. A positive `autoPersistEverySamples` explicitly permits synchronous
file I/O from observation calls. `readOnly=true` supports warm-start snapshots without updates or
writes.

When a warm-start file is configured, call `registry.preload()` during driver/plugin
initialization. This keeps the first scheduler planning or observation callback from loading the
properties file on the scheduler thread.

The properties format is compatible with the former Spark-side 5/7/9/14/24/26-field rows. Writes
use a temporary file and same-directory atomic replacement when the filesystem supports it;
persistence is best effort, not fsync/crash durable. Use one writer and a distinct file per Spark
application. Multiple JVMs or registry instances writing one file are unsupported and may
overwrite one another's newest observations.

### Adaptive telemetry SparkPlugin

The driver-only plugin moves successful shuffle-map telemetry aggregation out of DAGScheduler and
uses only Spark's standard asynchronous listener events and `TaskMetrics`:

```text
spark.plugins org.scache.spark.telemetry.ScacheAdaptiveTelemetryPlugin
spark.scache.telemetry.adaptive.enabled true
spark.scache.telemetry.adaptive.file /application-specific/path/adaptive.properties
```

Append the class to `spark.plugins` when another plugin is already configured. The plugin creates
one registry per driver application, preloads its optional warm-start file during driver
initialization, and installs no executor component, background thread, or RPC handler. It groups
events by stage attempt and accepts only the first successful attempt for each logical
`TaskInfo.index`; speculative duplicates and out-of-range events are ignored. Each active stage
retains only a bit set and constant-size totals, not one metrics object per task. The first task
success updates the map EWMA and totals in O(1), while stage completion emits one aggregate sample
in O(1). A failed or incomplete attempt emits no stage-throughput sample, although its already
successful logical maps remain valid map-size observations.

Only the exact new key `spark.scache.telemetry.adaptive.enabled=true` starts this observer. Neither
the unprefixed alias nor `spark.ultrashuffle.adaptive.online.enabled` starts it implicitly. If the
legacy Spark-core registry is both enabled and writable, the plugin refuses to create a registry
or listener, preventing two registries from writing the same telemetry file.
On the patched Spark branch that registry defaults to enabled and writable when its two mode keys
are omitted, so the plugin also treats that omitted configuration as a conflict. Explicitly set
the core registry read-only or disabled before enabling the plugin.

For the current patched Spark branch, the recommended coexistence configuration is:

```text
spark.ultrashuffle.adaptive.online.enabled true
spark.ultrashuffle.adaptive.online.readOnly true
spark.ultrashuffle.adaptive.online.telemetryFile /application-specific/path/adaptive.properties
spark.scache.telemetry.adaptive.enabled true
spark.scache.telemetry.adaptive.file /application-specific/path/adaptive.properties
```

The legacy core registry then consumes the warm-start snapshot read-only for the current
application. The plugin owns the only writer and trains the file for the next application; its
asynchronous updates are not fed back into the core registry in the same application. Internally,
the observer always forces `adaptiveEnabled=true`, `readOnly=false`, and
`autoPersistEverySamples=0`. It is intentionally not a read-only preload service.

The current `spark-3.5-scache` adapter supplies map-stage identity with these StageSubmitted
properties:

```text
spark.ultrashuffle.task.plan.shuffleId
spark.ultrashuffle.task.plan.signature
spark.ultrashuffle.task.mode
spark.ultrashuffle.task.plan.numMaps
spark.ultrashuffle.task.plan.numReducers
```

New adapters may instead use the equivalent
`spark.scache.telemetry.stage.{shuffleId,signature,mode,numMaps,numReducers}` properties. A stage
without both a shuffle ID and signature is ignored; this avoids mixing unrelated vanilla Spark
stages into an `unknown` adaptive signature.

Successful attempts aggregate standard shuffle bytes, records, write time, and map-stage duration.
The duration ends at the latest accepted task finish time, avoiding push-shuffle finalization and
scheduler cleanup time; StageInfo completion is only a fallback for historical events without a
task finish time. Block cost uses the maps actually run by that attempt, so an AQE/partial-output
rebuild does not charge already-available maps again. Listener callbacks perform only bounded
in-memory aggregation and registry
arithmetic. The plugin explicitly flushes only after removing the listener during driver shutdown,
so no task or listener callback performs file I/O. With the explicit observer key absent or false
(the default), the plugin does not create a registry, install a listener, start a thread, or touch
the telemetry file.

Listener delivery is asynchronous, so these observations are suitable for later-stage or future
application learning. They cannot replace the synchronous planning/completion hook required when
the immediately following stage in the same DAG must see the update.

## Adaptive planner

`org.scache.adaptive.AdaptiveShufflePlanner` is the Spark-independent part of the adaptive
shuffle selector. It accepts an immutable `AdaptivePlannerConfig` and `ShufflePlanningInput`, and
returns an immutable `ShufflePlan`. Planning performs only local arithmetic: it owns no global
decision map, does no file or network I/O, starts no thread, and does not read `SparkConf` or
`TaskContext`.

The safe default is `AdaptivePlannerConfig.nativeOnly()`. Adaptive selection is explicit:

```java
AdaptivePlannerConfig config = AdaptivePlannerConfig.builder()
    .mode(AdaptiveShufflePlanner.ADAPTIVE)
    .estimatedShuffleBytes(32L << 30)
    .estimateConfidence(0.95)
    .build();
ShufflePlanningInput input = ShufflePlanningInput.builder(shuffleId, maps, reducers)
    .telemetry(registry.snapshot(signature))
    .parentReads(parentReads)
    .build();
ShufflePlan plan = AdaptiveShufflePlanner.choose(config, input);
```

The module deliberately does not install this plan into Spark. A version-specific adapter must
collect parent/capability information and freeze one plan before publishing the shuffle handle.
That adapter is also responsible for releasing its per-shuffle state on `unregisterShuffle`.

`RecordPbrCostModel` is a smaller experimental fixed-width-record selector. It is also pure and
uses `RecordPbrPlannerConfig` instead of `SparkConf`.

`org.scache.telemetry.ScacheProfilingMetrics` is the matching task-local counter DTO. The class
does not time, register, or export anything by itself; an adapter should construct it only when
detailed profiling is enabled and batch its values at a task or stage boundary.

## JVM/process metrics

The sampler can be used directly through `org.scache.telemetry.JvmProcessMetrics`, or exposed in
Spark's plugin metric registry:

```text
spark.plugins org.scache.spark.telemetry.ScacheProcessMetricsPlugin
spark.scache.telemetry.process.enabled true
spark.scache.telemetry.process.minSampleIntervalMs 1000
```

If other plugins are configured, append this class to the comma-separated `spark.plugins` list.
The plugin registers CPU, heap/non-heap, direct/mapped buffer, GC, thread, class, physical memory,
swap, and file-descriptor gauges for both driver and executors. It starts no sampler thread: a
metrics-system scrape samples synchronously, and all gauges reuse one snapshot during the minimum
interval. Unsupported or access-restricted MXBeans degrade to unavailable values and never fail
Spark initialization. Leave the plugin or `process.enabled` unset for no polling overhead.
Sampling intervals below 100 ms are clamped to 100 ms so one metrics scrape cannot poll all
MXBeans once per individual gauge.
