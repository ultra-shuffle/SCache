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

import scala.collection.mutable.ArrayBuffer

import org.scache.telemetry.TelemetryConfig

/** Framework-free checks for attempt isolation, partition deduplication, and aggregation. */
object ScacheAdaptiveTelemetryPluginSelfTest {
  private final case class MapObservation(
      signature: String,
      mode: String,
      bytes: Long,
      records: Long,
      writeTimeNs: Long)

  private final case class StageObservation(
      signature: String,
      mode: String,
      bytes: Long,
      records: Long,
      writeTimeNs: Long,
      elapsedNs: Long,
      blocks: Long)

  private final class RecordingSink extends AdaptiveTelemetrySink {
    val maps: ArrayBuffer[MapObservation] = ArrayBuffer.empty
    val stages: ArrayBuffer[StageObservation] = ArrayBuffer.empty

    override def observeMap(
        signature: String,
        mode: String,
        bytes: Long,
        records: Long,
        writeTimeNs: Long): Unit = {
      maps += MapObservation(signature, mode, bytes, records, writeTimeNs)
    }

    override def observeStage(
        signature: String,
        mode: String,
        bytes: Long,
        records: Long,
        writeTimeNs: Long,
        elapsedNs: Long,
        blocks: Long): Unit = {
      stages += StageObservation(
        signature, mode, bytes, records, writeTimeNs, elapsedNs, blocks)
    }
  }

  def main(args: Array[String]): Unit = {
    pluginRequiresExplicitNonConflictingEnablement()
    successfulAttemptIsAggregatedOnce()
    partialRetryAcceptsHighLogicalIndex()
    failedAndIncompleteAttemptsDoNotEmitStageSamples()
    attemptsAreIsolatedAndShutdownIsFinal()
    println("ScacheAdaptiveTelemetryPluginSelfTest: PASS")
  }

  private def pluginRequiresExplicitNonConflictingEnablement(): Unit = {
    val configured = new java.util.HashMap[String, String]()
    configured.put(AdaptiveTelemetryPluginConfig.ExplicitObserverEnabled, "true")
    configured.put(TelemetryConfig.ADAPTIVE_READ_ONLY, "true")
    configured.put(TelemetryConfig.ADAPTIVE_AUTO_PERSIST_EVERY_SAMPLES, "1")
    configured.put("spark.scache.telemetry.adaptive.readOnly", "true")
    configured.put("spark.scache.telemetry.adaptive.autoPersistEverySamples", "2")
    val enabled = AdaptiveTelemetryPluginConfig.from(configured)
    assert(enabled.observerEnabled)
    assert(!enabled.legacyWriterConflict)
    assert(enabled.registryConfig.adaptiveEnabled())
    assert(!enabled.registryConfig.adaptiveReadOnly())
    assert(enabled.registryConfig.autoPersistEverySamples() == 0)

    val disabled = AdaptiveTelemetryPluginConfig.from(
      new java.util.HashMap[String, String]())
    assert(!disabled.observerEnabled)
    assert(!disabled.registryConfig.adaptiveEnabled())

    val unprefixedOnly = new java.util.HashMap[String, String]()
    unprefixedOnly.put(TelemetryConfig.ADAPTIVE_ENABLED, "true")
    assert(!AdaptiveTelemetryPluginConfig.from(unprefixedOnly).observerEnabled)

    val legacyOnly = new java.util.HashMap[String, String]()
    legacyOnly.put(AdaptiveTelemetryPluginConfig.LegacyEnabled, "true")
    assert(!AdaptiveTelemetryPluginConfig.from(legacyOnly).observerEnabled)

    val patchedCoreDefaults = new java.util.HashMap[String, String]()
    patchedCoreDefaults.put(AdaptiveTelemetryPluginConfig.ExplicitObserverEnabled, "true")
    val conflict = AdaptiveTelemetryPluginConfig.from(
      patchedCoreDefaults, patchedCoreRegistryAvailable = true)
    assert(!conflict.observerEnabled)
    assert(conflict.legacyWriterConflict)
    assert(!conflict.registryConfig.adaptiveEnabled())

    val sharedLegacyFile = new java.util.HashMap[String, String]()
    sharedLegacyFile.put(AdaptiveTelemetryPluginConfig.ExplicitObserverEnabled, "true")
    sharedLegacyFile.put(AdaptiveTelemetryPluginConfig.LegacyFile, "/tmp/telemetry.properties")
    val implicitCoreWriter = AdaptiveTelemetryPluginConfig.from(sharedLegacyFile)
    assert(!implicitCoreWriter.observerEnabled)
    assert(implicitCoreWriter.legacyWriterConflict)

    val coreReadOnly = new java.util.HashMap[String, String]()
    coreReadOnly.put(AdaptiveTelemetryPluginConfig.ExplicitObserverEnabled, "true")
    coreReadOnly.put(AdaptiveTelemetryPluginConfig.LegacyReadOnly, "true")
    val compatible = AdaptiveTelemetryPluginConfig.from(
      coreReadOnly, patchedCoreRegistryAvailable = true)
    assert(compatible.observerEnabled)
    assert(!compatible.legacyWriterConflict)
    assert(compatible.registryConfig.adaptiveEnabled())
    assert(!compatible.registryConfig.adaptiveReadOnly())

    val coreDisabled = new java.util.HashMap[String, String]()
    coreDisabled.put(AdaptiveTelemetryPluginConfig.ExplicitObserverEnabled, "true")
    coreDisabled.put(AdaptiveTelemetryPluginConfig.LegacyEnabled, "false")
    val disabledCoreIsCompatible = AdaptiveTelemetryPluginConfig.from(
      coreDisabled, patchedCoreRegistryAvailable = true)
    assert(disabledCoreIsCompatible.observerEnabled)
    assert(!disabledCoreIsCompatible.legacyWriterConflict)
  }

  private def successfulAttemptIsAggregatedOnce(): Unit = {
    val sink = new RecordingSink
    val coordinator = new AdaptiveTelemetryCoordinator(sink)
    coordinator.stageSubmitted(7, 0, metadata(expectedTasks = 2, submittedMs = 1000L))

    coordinator.taskSucceeded(7, 0, 0, 100L, 10L, 20L, 1800L)
    coordinator.taskSucceeded(7, 0, 0, 999L, 999L, 999L, 2400L)
    coordinator.taskSucceeded(7, 0, -1, 888L, 888L, 888L, 2400L)
    coordinator.taskSucceeded(7, 0, 4, 777L, 777L, 777L, 2400L)
    coordinator.taskSucceeded(7, 0, 1, 300L, 30L, 40L, 2200L)
    // Map samples are emitted on the first logical-task success, not replayed at stage end.
    assert(sink.maps.sortBy(_.bytes) == Seq(
      MapObservation("sig", "native", 100L, 10L, 20L),
      MapObservation("sig", "native", 300L, 30L, 40L)))
    coordinator.stageCompleted(7, 0, succeeded = true, completionTimeMs = 2500L)

    assert(sink.maps.sortBy(_.bytes) == Seq(
      MapObservation("sig", "native", 100L, 10L, 20L),
      MapObservation("sig", "native", 300L, 30L, 40L)))
    // numMaps=4 describes the logical shuffle, but this attempt ran only two missing maps.
    assert(sink.stages == Seq(
      StageObservation("sig", "native", 400L, 40L, 60L, 1200000000L, 6L)))
    assert(coordinator.activeStageCount == 0)
  }

  private def partialRetryAcceptsHighLogicalIndex(): Unit = {
    val sink = new RecordingSink
    val coordinator = new AdaptiveTelemetryCoordinator(sink)
    coordinator.stageSubmitted(
      12,
      3,
      AdaptiveStageMetadata(
        signature = "partial",
        mode = "pbr",
        expectedTasks = 1,
        numMaps = 200,
        numReducers = 5,
        submissionTimeMs = 1000L))

    // A partial retry preserves the original logical map index even though this attempt has one
    // submitted task. The duplicate and the index at numMaps must both be ignored.
    coordinator.taskSucceeded(12, 3, 137, 20L, 2L, 3L, 1600L)
    coordinator.taskSucceeded(12, 3, 137, 999L, 999L, 999L, 1700L)
    coordinator.taskSucceeded(12, 3, 200, 888L, 888L, 888L, 1800L)
    coordinator.stageCompleted(12, 3, succeeded = true, completionTimeMs = 1900L)

    assert(sink.maps == Seq(MapObservation("partial", "pbr", 20L, 2L, 3L)))
    // Only one missing map ran, so the attempt wrote one map times five reducer blocks.
    assert(sink.stages == Seq(
      StageObservation("partial", "pbr", 20L, 2L, 3L, 600000000L, 5L)))
    assert(coordinator.activeStageCount == 0)
  }

  private def failedAndIncompleteAttemptsDoNotEmitStageSamples(): Unit = {
    val sink = new RecordingSink
    val coordinator = new AdaptiveTelemetryCoordinator(sink)

    coordinator.stageSubmitted(8, 0, metadata(expectedTasks = 1, submittedMs = 1000L))
    coordinator.taskSucceeded(8, 0, 0, 100L, 10L, 20L, 1500L)
    coordinator.stageCompleted(8, 0, succeeded = false, completionTimeMs = 2000L)

    coordinator.stageSubmitted(9, 0, metadata(expectedTasks = 2, submittedMs = 1000L))
    coordinator.taskSucceeded(9, 0, 0, 100L, 10L, 20L, 1500L)
    coordinator.stageCompleted(9, 0, succeeded = true, completionTimeMs = 2000L)

    // Per-map observations are useful once that logical map succeeds. Failure/incompleteness only
    // prevents publishing an invalid aggregate stage-throughput sample.
    assert(sink.maps.map(_.bytes) == Seq(100L, 100L))
    assert(sink.stages.isEmpty)
    assert(coordinator.activeStageCount == 0)
  }

  private def attemptsAreIsolatedAndShutdownIsFinal(): Unit = {
    val sink = new RecordingSink
    val coordinator = new AdaptiveTelemetryCoordinator(sink)
    coordinator.stageSubmitted(10, 0, metadata(expectedTasks = 1, submittedMs = 1000L))
    coordinator.stageSubmitted(10, 1, metadata(expectedTasks = 1, submittedMs = 2000L))
    coordinator.taskSucceeded(10, 0, 0, 1L, 1L, 1L, 1500L)
    // A zero finish time exercises the StageInfo.completionTime compatibility fallback.
    coordinator.taskSucceeded(10, 1, 0, 2L, 2L, 2L, 0L)
    coordinator.stageCompleted(10, 0, succeeded = false, completionTimeMs = 2500L)
    coordinator.stageCompleted(10, 1, succeeded = true, completionTimeMs = 3000L)

    assert(sink.maps.map(_.bytes) == Seq(1L, 2L))
    assert(sink.stages.map(_.bytes) == Seq(2L))

    coordinator.stageSubmitted(
      11, 0, metadata(expectedTasks = 1, submittedMs = 3000L))
    assert(coordinator.activeStageCount == 1)
    coordinator.closeAndDiscard()
    assert(coordinator.activeStageCount == 0)
    coordinator.taskSucceeded(11, 0, 0, 3L, 3L, 3L, 3500L)
    coordinator.stageCompleted(11, 0, succeeded = true, completionTimeMs = 4000L)
    assert(sink.maps.map(_.bytes) == Seq(1L, 2L))
  }

  private def metadata(expectedTasks: Int, submittedMs: Long): AdaptiveStageMetadata = {
    AdaptiveStageMetadata(
      signature = "sig",
      mode = "native",
      expectedTasks = expectedTasks,
      numMaps = 4,
      numReducers = 3,
      submissionTimeMs = submittedMs)
  }
}
