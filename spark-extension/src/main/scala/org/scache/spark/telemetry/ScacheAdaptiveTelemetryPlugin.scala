/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.scache.spark.telemetry

import java.util.{Collections, Locale, Map => JMap, Properties}

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.{SparkContext, Success}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted,
  SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.scache.telemetry.{AdaptiveTelemetryRegistry, TelemetryConfig}
import org.slf4j.LoggerFactory

/**
 * Driver-only bridge from Spark's standard listener events to the SCache adaptive model.
 *
 * Merely putting this class on the class path has no effect. Even when Spark instantiates the
 * plugin, the disabled default installs no listener, creates no registry or thread, and performs
 * no file I/O. The enabled path collects metrics on Spark's asynchronous listener thread; it
 * never sends a per-task RPC and never persists from a listener callback.
 */
final class ScacheAdaptiveTelemetryPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new ScacheAdaptiveTelemetryDriverPlugin

  override def executorPlugin(): ExecutorPlugin = null
}

private final class ScacheAdaptiveTelemetryDriverPlugin extends DriverPlugin {
  private val logger = LoggerFactory.getLogger(classOf[ScacheAdaptiveTelemetryDriverPlugin])

  @volatile private var sparkContext: SparkContext = _
  @volatile private var registry: AdaptiveTelemetryRegistry = _
  @volatile private var listener: ScacheAdaptiveTelemetryListener = _

  override def init(
      sc: SparkContext,
      pluginContext: PluginContext): JMap[String, String] = {
    val settings = AdaptiveTelemetryPluginConfig.from(pluginContext)
    if (settings.legacyWriterConflict) {
      logger.warn(
        "SCache adaptive telemetry plugin is disabled because the legacy Spark-core telemetry " +
          "writer is enabled; set spark.ultrashuffle.adaptive.online.readOnly=true or " +
          "spark.ultrashuffle.adaptive.online.enabled=false before enabling the plugin observer")
    }
    if (!settings.observerEnabled) {
      return Collections.emptyMap[String, String]()
    }

    val config = settings.registryConfig
    val applicationRegistry = new AdaptiveTelemetryRegistry(config)
    // This is the only read from the persistence file. It deliberately happens before the
    // listener is installed, so neither planning nor listener callbacks can trigger lazy I/O.
    applicationRegistry.preload()
    registry = applicationRegistry

    val applicationListener = new ScacheAdaptiveTelemetryListener(applicationRegistry)
    sparkContext = sc
    listener = applicationListener
    sc.addSparkListener(applicationListener)
    Collections.emptyMap[String, String]()
  }

  override def shutdown(): Unit = {
    val applicationListener = listener
    val sc = sparkContext
    listener = null
    sparkContext = null

    if (applicationListener != null) {
      if (sc != null) {
        try {
          sc.removeSparkListener(applicationListener)
        } catch {
          case NonFatal(_) => // Telemetry cleanup must not make Spark shutdown fail.
        }
      }
      applicationListener.closeAndDiscard()
    }

    val applicationRegistry = registry
    registry = null
    if (applicationRegistry != null) {
      // Persistence is explicit and confined to driver shutdown. flush() is best effort and
      // never throws into Spark shutdown.
      applicationRegistry.flush()
    }
  }
}

private[telemetry] final case class AdaptiveTelemetryPluginConfig(
    observerEnabled: Boolean,
    legacyWriterConflict: Boolean,
    registryConfig: TelemetryConfig)

private[telemetry] object AdaptiveTelemetryPluginConfig {
  val ExplicitObserverEnabled = "spark.scache.telemetry.adaptive.enabled"
  val LegacyEnabled = "spark.ultrashuffle.adaptive.online.enabled"
  val LegacyReadOnly = "spark.ultrashuffle.adaptive.online.readOnly"
  val LegacyFile = "spark.ultrashuffle.adaptive.online.telemetryFile"
  private val PatchedCoreRegistryClass =
    "org.apache.spark.shuffle.adaptive.AdaptiveTelemetryRegistry"

  def from(pluginContext: PluginContext): AdaptiveTelemetryPluginConfig = {
    val values = new java.util.HashMap[String, String]()
    pluginContext.conf().getAll.foreach { case (key, value) => values.put(key, value) }
    from(values, patchedCoreRegistryAvailable())
  }

  def from(configuredValues: JMap[String, String]): AdaptiveTelemetryPluginConfig = {
    from(configuredValues, patchedCoreRegistryAvailable())
  }

  private[telemetry] def from(
      configuredValues: JMap[String, String],
      patchedCoreRegistryAvailable: Boolean): AdaptiveTelemetryPluginConfig = {
    val values = new java.util.HashMap[String, String]()
    if (configuredValues != null) values.putAll(configuredValues)

    // Do not let TelemetryConfig's compatibility aliases accidentally start a Spark listener.
    // Observer activation is deliberately owned by one new, fully-qualified plugin key.
    val observerRequested = enabled(values.get(ExplicitObserverEnabled))
    // The patched Spark-core registry defaults to enabled=true/readOnly=false. Once that class is
    // present, omitted legacy keys therefore describe an active writer rather than a disabled
    // feature. Official Spark has no such registry, so keep the standalone plugin usable there.
    val coreRegistryConfigured = patchedCoreRegistryAvailable ||
      values.containsKey(LegacyEnabled) || values.containsKey(LegacyReadOnly) ||
      nonEmpty(values.get(LegacyFile))
    val legacyWriter = coreRegistryConfigured &&
      enabledOrDefault(values.get(LegacyEnabled), default = true) &&
      !enabledOrDefault(values.get(LegacyReadOnly), default = false)
    val conflict = observerRequested && legacyWriter
    if (!observerRequested || conflict) {
      return AdaptiveTelemetryPluginConfig(
        observerEnabled = false,
        legacyWriterConflict = conflict,
        registryConfig = TelemetryConfig.disabled())
    }

    // AdaptiveTelemetryRegistry supports opt-in auto-persistence for non-Spark callers. A Spark
    // listener must never inherit that setting because observeMap/observeStage would then perform
    // synchronous file I/O on the shared listener thread. The plugin is a writer/trainer, not a
    // read-only preload service, so its private registry is always update-enabled.
    values.put(TelemetryConfig.ADAPTIVE_ENABLED, "true")
    values.put(TelemetryConfig.ADAPTIVE_READ_ONLY, "false")
    values.put(TelemetryConfig.ADAPTIVE_AUTO_PERSIST_EVERY_SAMPLES, "0")
    AdaptiveTelemetryPluginConfig(
      observerEnabled = true,
      legacyWriterConflict = false,
      registryConfig = TelemetryConfig.from(values))
  }

  private def enabled(value: String): Boolean =
    value != null && value.trim.equalsIgnoreCase("true")

  private def enabledOrDefault(value: String, default: Boolean): Boolean =
    if (value == null) default else enabled(value)

  private def nonEmpty(value: String): Boolean =
    value != null && value.trim.nonEmpty

  private def patchedCoreRegistryAvailable(): Boolean = {
    try {
      Class.forName(
        PatchedCoreRegistryClass,
        false,
        classOf[ScacheAdaptiveTelemetryPlugin].getClassLoader)
      true
    } catch {
      case _: ClassNotFoundException => false
      case _: LinkageError => false
    }
  }
}

private[telemetry] final class ScacheAdaptiveTelemetryListener(
    registry: AdaptiveTelemetryRegistry) extends SparkListener {
  private val coordinator = new AdaptiveTelemetryCoordinator(
    new RegistryAdaptiveTelemetrySink(registry))

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    AdaptiveStageMetadata.from(event).foreach { metadata =>
      coordinator.stageSubmitted(
        event.stageInfo.stageId,
        event.stageInfo.attemptNumber(),
        metadata)
    }
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    if (event.reason != Success || event.taskMetrics == null) return

    val taskInfo = event.taskInfo
    val write = event.taskMetrics.shuffleWriteMetrics
    coordinator.taskSucceeded(
      event.stageId,
      event.stageAttemptId,
      taskInfo.index,
      nonnegative(write.bytesWritten),
      nonnegative(write.recordsWritten),
      nonnegative(write.writeTime),
      nonnegative(taskInfo.finishTime))
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val info = event.stageInfo
    coordinator.stageCompleted(
      info.stageId,
      info.attemptNumber(),
      info.failureReason.isEmpty,
      info.completionTime.getOrElse(0L))
  }

  def closeAndDiscard(): Unit = coordinator.closeAndDiscard()

  private def nonnegative(value: Long): Long = math.max(0L, value)
}

private[telemetry] final case class AdaptiveStageMetadata(
    signature: String,
    mode: String,
    expectedTasks: Int,
    numMaps: Int,
    numReducers: Int,
    submissionTimeMs: Long)

private[telemetry] object AdaptiveStageMetadata {
  // Prefer SCache-owned stage keys in new adapters while accepting the properties emitted by the
  // current spark-3.5-scache DAGScheduler. Keeping the strings here avoids linking the extension
  // to the patched ShuffleDataMode class.
  private val SignatureKeys = Seq(
    "spark.scache.telemetry.stage.signature",
    "spark.ultrashuffle.task.plan.signature")
  private val ModeKeys = Seq(
    "spark.scache.telemetry.stage.mode",
    "spark.ultrashuffle.task.mode")
  private val ShuffleIdKeys = Seq(
    "spark.scache.telemetry.stage.shuffleId",
    "spark.ultrashuffle.task.plan.shuffleId")
  private val NumMapsKeys = Seq(
    "spark.scache.telemetry.stage.numMaps",
    "spark.ultrashuffle.task.plan.numMaps")
  private val NumReducersKeys = Seq(
    "spark.scache.telemetry.stage.numReducers",
    "spark.ultrashuffle.task.plan.numReducers")

  def from(event: SparkListenerStageSubmitted): Option[AdaptiveStageMetadata] = {
    val properties = event.properties
    // Only shuffle-map stages carrying adapter metadata are observed. Spark's public StageInfo
    // does not expose shuffleDepId, and inventing a cross-application "unknown" signature would
    // mix unrelated stages in one adaptive EWMA.
    val shuffleId = property(properties, ShuffleIdKeys).flatMap(nonnegativeInt)
    val signature = property(properties, SignatureKeys).map(_.trim).filter(_.nonEmpty)
    if (shuffleId.isEmpty || signature.isEmpty) return None

    val info = event.stageInfo
    val expectedTasks = math.max(0, info.numTasks)
    val numMaps = property(properties, NumMapsKeys).flatMap(positiveInt)
      .getOrElse(math.max(1, expectedTasks))
    val numReducers = property(properties, NumReducersKeys).flatMap(positiveInt).getOrElse(0)
    val mode = property(properties, ModeKeys)
      .map(_.trim.toLowerCase(Locale.ROOT))
      .filter(value => value == AdaptiveTelemetryRegistry.NATIVE ||
        value == AdaptiveTelemetryRegistry.PBR ||
        value == AdaptiveTelemetryRegistry.RECORD_PBR)
      .getOrElse(AdaptiveTelemetryRegistry.NATIVE)

    Some(AdaptiveStageMetadata(
      signature.get,
      mode,
      expectedTasks,
      numMaps,
      numReducers,
      info.submissionTime.getOrElse(0L)))
  }

  private def property(properties: Properties, keys: Seq[String]): Option[String] = {
    if (properties == null) return None
    keys.iterator.map(properties.getProperty).find(_ != null)
  }

  private def nonnegativeInt(value: String): Option[Int] = parseInt(value).filter(_ >= 0)

  private def positiveInt(value: String): Option[Int] = parseInt(value).filter(_ > 0)

  private def parseInt(value: String): Option[Int] = {
    try {
      Some(value.trim.toInt)
    } catch {
      case _: RuntimeException => None
    }
  }
}

private[telemetry] trait AdaptiveTelemetrySink {
  def observeMap(
      signature: String,
      mode: String,
      bytes: Long,
      records: Long,
      writeTimeNs: Long): Unit

  def observeStage(
      signature: String,
      mode: String,
      bytes: Long,
      records: Long,
      writeTimeNs: Long,
      elapsedNs: Long,
      blocks: Long): Unit
}

private[telemetry] final class RegistryAdaptiveTelemetrySink(
    registry: AdaptiveTelemetryRegistry) extends AdaptiveTelemetrySink {
  override def observeMap(
      signature: String,
      mode: String,
      bytes: Long,
      records: Long,
      writeTimeNs: Long): Unit = {
    registry.observeMap(signature, mode, bytes, records, writeTimeNs, 0L, 0L)
  }

  override def observeStage(
      signature: String,
      mode: String,
      bytes: Long,
      records: Long,
      writeTimeNs: Long,
      elapsedNs: Long,
      blocks: Long): Unit = {
    // The current adaptive model derives write bandwidth from end-to-end stage time. records and
    // writeTimeNs remain in this boundary so a future sidecar can consume them without changing
    // the listener or returning to Spark TaskMetrics patches.
    registry.observeStage(signature, mode, bytes, 0L, elapsedNs, blocks)
  }
}

/**
 * Attempt-aware in-memory aggregation isolated from Spark event classes for deterministic tests.
 *
 * All methods are synchronized because shutdown may race Spark's listener thread. Sink calls are
 * intentionally made while holding the lock: closeAndDiscard then forms a strict boundary before
 * the driver's final flush. The registry sink does no I/O because the plugin forces auto-persist
 * to zero and preloads before constructing this coordinator.
 */
private[telemetry] final class AdaptiveTelemetryCoordinator(sink: AdaptiveTelemetrySink) {
  private final class StageState(val metadata: AdaptiveStageMetadata) {
    val seenTaskIndexes: mutable.BitSet = mutable.BitSet.empty
    var acceptedTasks: Int = 0
    var totalBytes: Long = 0L
    var totalRecords: Long = 0L
    var totalWriteTimeNs: Long = 0L
    var latestTaskFinishTimeMs: Long = 0L
  }

  private val stages = mutable.LongMap.empty[StageState]
  private var closed = false

  def stageSubmitted(
      stageId: Int,
      stageAttemptId: Int,
      metadata: AdaptiveStageMetadata): Unit = synchronized {
    if (!closed && metadata.expectedTasks > 0) {
      val key = stageKey(stageId, stageAttemptId)
      if (!stages.contains(key)) stages.update(key, new StageState(metadata))
    }
  }

  def taskSucceeded(
      stageId: Int,
      stageAttemptId: Int,
      logicalTaskIndex: Int,
      bytes: Long,
      records: Long,
      writeTimeNs: Long,
      finishTimeMs: Long): Unit = synchronized {
    if (closed) return
    val state = stages.getOrNull(stageKey(stageId, stageAttemptId))
    if (state != null) {
      // TaskInfo.index is Spark's logical map-partition index and is stable across attempts. A
      // partial/reconstructed attempt may submit one task whose index is, for example, 137 of
      // 200, so validate it against the full shuffle map count rather than this attempt's task
      // count. expectedTasks remains the completion cardinality for this particular attempt.
      if (logicalTaskIndex >= 0 && logicalTaskIndex < state.metadata.numMaps &&
          !state.seenTaskIndexes(logicalTaskIndex)) {
        state.seenTaskIndexes += logicalTaskIndex
        state.acceptedTasks += 1
        sink.observeMap(
          state.metadata.signature,
          state.metadata.mode,
          bytes,
          records,
          writeTimeNs)
        state.totalBytes = saturatedAdd(state.totalBytes, bytes)
        state.totalRecords = saturatedAdd(state.totalRecords, records)
        state.totalWriteTimeNs = saturatedAdd(state.totalWriteTimeNs, writeTimeNs)
        state.latestTaskFinishTimeMs = math.max(
          state.latestTaskFinishTimeMs, finishTimeMs)
      }
    }
  }

  def stageCompleted(
      stageId: Int,
      stageAttemptId: Int,
      succeeded: Boolean,
      completionTimeMs: Long): Unit = synchronized {
    if (closed) return
    val state = stages.remove(stageKey(stageId, stageAttemptId)).orNull
    if (state == null || !succeeded ||
        state.acceptedTasks != state.metadata.expectedTasks) return

    // StageInfo.completionTime may include push-shuffle finalization and scheduler cleanup after
    // all map tasks finished. Use the last accepted task finish for write throughput, retaining
    // stage completion only as a compatibility fallback for historical events without finishTime.
    val writeFinishedTimeMs = if (state.latestTaskFinishTimeMs > 0L) {
      state.latestTaskFinishTimeMs
    } else {
      completionTimeMs
    }
    val elapsedNs = elapsedNanos(state.metadata.submissionTimeMs, writeFinishedTimeMs)
    // A recreated/AQE stage attempt may execute only the missing map outputs. Charge the blocks
    // actually written by this attempt, not every map in the logical shuffle.
    val blocks = saturatedMultiply(
      state.metadata.expectedTasks.toLong, state.metadata.numReducers.toLong)
    sink.observeStage(
      state.metadata.signature,
      state.metadata.mode,
      state.totalBytes,
      state.totalRecords,
      state.totalWriteTimeNs,
      elapsedNs,
      blocks)
  }

  def closeAndDiscard(): Unit = synchronized {
    closed = true
    stages.clear()
  }

  private[telemetry] def activeStageCount: Int = synchronized { stages.size }

  private def stageKey(stageId: Int, stageAttemptId: Int): Long =
    (stageId.toLong << 32) ^ (stageAttemptId.toLong & 0xffffffffL)

  private def elapsedNanos(startMs: Long, endMs: Long): Long = {
    if (startMs <= 0L || endMs <= 0L || endMs < startMs) return 0L
    saturatedMultiply(math.max(1L, endMs - startMs), 1000000L)
  }

  private def saturatedAdd(left: Long, right: Long): Long = {
    if (right > 0L && left > Long.MaxValue - right) Long.MaxValue else left + right
  }

  private def saturatedMultiply(left: Long, right: Long): Long = {
    if (left <= 0L || right <= 0L) 0L
    else if (left > Long.MaxValue / right) Long.MaxValue
    else left * right
  }
}
