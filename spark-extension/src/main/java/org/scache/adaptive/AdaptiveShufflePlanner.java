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
package org.scache.adaptive;

import org.scache.telemetry.AdaptiveTelemetrySnapshot;

/**
 * Pure native/PBR/record-PBR cost planner.
 *
 * <p>The planner owns no state, performs no I/O, creates no threads, and has no Spark dependency.
 * A caller invokes it once before publishing a shuffle handle and owns the resulting plan's
 * lifecycle.</p>
 */
public final class AdaptiveShufflePlanner {
  public static final String NATIVE = "native";
  public static final String PBR = "pbr";
  public static final String RECORD_PBR = "record-pbr";
  public static final String ADAPTIVE = "adaptive";

  private AdaptiveShufflePlanner() { }

  public static ShufflePlan choose(
      AdaptivePlannerConfig config, ShufflePlanningInput input) {
    if (config == null) throw new NullPointerException("config");
    if (input == null) throw new NullPointerException("input");

    int shuffleId = input.shuffleId();
    int numMaps = input.numMaps();
    int numReducers = input.numReducers();
    AdaptiveTelemetrySnapshot telemetry = input.telemetry();
    ParentShuffleReadEstimate parentReads = input.parentReads();

    if (!ADAPTIVE.equals(config.mode())) {
      String selectedMode = RECORD_PBR.equals(config.mode()) && !input.hasRecordPbrCapability()
          ? PBR : config.mode();
      String reason = RECORD_PBR.equals(config.mode()) && !input.hasRecordPbrCapability()
          ? "record_pbr_capability_missing_fallback_to_pbr"
          : "forced_" + config.mode();
      return plan(shuffleId, selectedMode, numMaps, numReducers, 0L,
          blockCount(numMaps, numReducers), 0.0, 0.0, 0.0, 0.0,
          1.0, 1.0, 1.0, 1, 1.0, reason, parentReads, 0.0,
          parentReads.cxlBytes());
    }

    String stageModeOverride = config.stageModeOverrides().get(shuffleId);
    if (stageModeOverride == null && config.strictStageModeOverrides()) {
      throw new IllegalArgumentException("no stage mode override for shuffle ID=" + shuffleId);
    }
    if (stageModeOverride != null) {
      if (RECORD_PBR.equals(stageModeOverride) && !input.hasRecordPbrCapability()) {
        stageModeOverride = PBR;
      }
      long estimatedBytes = estimateBytes(config, telemetry, numMaps, false);
      return plan(shuffleId, stageModeOverride, numMaps, numReducers, estimatedBytes,
          blockCount(numMaps, numReducers), 0.0, 0.0, 0.0, 0.0,
          1.0, 1.0, 1.0, 1, 1.0, "stage_mode_override", parentReads, 0.0,
          saturatedAdd(estimatedBytes, parentReads.cxlBytes()));
    }

    boolean useOnlineSize = telemetry.hasSizeEstimate()
        && telemetry.samples() >= config.onlineMinSamples();
    if (config.estimatedShuffleBytes() == 0L && config.estimatedBytesPerMap() == 0L
        && !useOnlineSize) {
      return plan(shuffleId, NATIVE, numMaps, numReducers, 0L,
          blockCount(numMaps, numReducers), 0.0, 0.0, 0.0, 0.0,
          1.0, 1.0, 1.0, 1, 0.0, "missing_shuffle_size_estimate", parentReads, 0.0,
          parentReads.cxlBytes());
    }

    long estimatedBytes = estimateBytes(config, telemetry, numMaps, useOnlineSize);
    long estimatedCxlWorkingSetBytes = saturatedAdd(estimatedBytes, parentReads.cxlBytes());
    if (estimatedCxlWorkingSetBytes > config.cxlCapacityBytes()) {
      return plan(shuffleId, NATIVE, numMaps, numReducers, estimatedBytes,
          blockCount(numMaps, numReducers), 0.0, 0.0, 0.0, 0.0,
          1.0, 1.0, 1.0, 1, config.estimateConfidence(),
          parentReads.cxlBytes() > 0L ? "cxl_working_set_exceeded" : "cxl_capacity_exceeded",
          parentReads, 0.0, estimatedCxlWorkingSetBytes);
    }

    long observedStageBytes = Math.max(telemetry.nativeStageBytes(),
        Math.max(telemetry.pbrStageBytes(), telemetry.recordPbrStageBytes()));
    if (useOnlineSize && observedStageBytes > 0L
        && estimatedBytes > observedStageBytes * config.maxExtrapolationFactor()) {
      return plan(shuffleId, NATIVE, numMaps, numReducers, estimatedBytes,
          blockCount(numMaps, numReducers), 0.0, 0.0, 0.0, 0.0,
          1.0, 1.0, 1.0, 1,
          Math.min(config.estimateConfidence(), telemetry.confidence()),
          "telemetry_scale_extrapolation", parentReads, 0.0, estimatedCxlWorkingSetBytes);
    }

    double pbrFixedMs = config.pbrFixedCostMs();
    if (input.hasAggregator()) {
      pbrFixedMs = config.pbrAggregateFixedCostMs();
    } else if (input.hasKeyOrdering() && !input.rangePartitioned()) {
      pbrFixedMs = config.pbrOrderedHashFixedCostMs();
    }
    double pbrTransferMultiplier = input.rangePartitioned()
        ? config.pbrRangeTransferMultiplier() : 1.0;
    double pbrBlockMs = config.pbrCostPerBlockMs();
    if (config.useRuntimeReadTelemetry() && telemetry.pbrBlockCostMs() > 0.0) {
      pbrBlockMs = telemetry.pbrBlockCostMs();
    }
    double confidence = useOnlineSize
        ? Math.min(config.estimateConfidence(), telemetry.confidence())
        : config.estimateConfidence();
    double pbrUtilization = config.useRuntimeReadTelemetry()
        ? Math.max(config.cxlUtilization(), bandwidthUtilization(
            telemetry.pbrReadBandwidth(), telemetry.pbrIdleReadBandwidth()))
        : config.cxlUtilization();
    double recordPbrUtilization = config.useRuntimeReadTelemetry()
        ? Math.max(config.cxlUtilization(), bandwidthUtilization(
            telemetry.recordPbrReadBandwidth(), telemetry.recordPbrIdleReadBandwidth()))
        : config.cxlUtilization();

    double baseConcurrencyPenalty = config.nearSlotFraction() < config.targetNearSlotFraction()
        ? config.targetNearSlotFraction() / config.nearSlotFraction()
        : 1.0 + Math.max(pbrUtilization, recordPbrUtilization)
            * (config.nearSlotFraction() - config.targetNearSlotFraction())
            / Math.max(0.01, 1.0 - config.targetNearSlotFraction());
    double pbrConcurrencyPenalty = baseConcurrencyPenalty
        * (config.useRuntimeReadTelemetry() && config.useContentionMultiplier()
            ? telemetry.pbrContentionPenalty() : 1.0);
    double recordPbrConcurrencyPenalty = baseConcurrencyPenalty
        * (config.useRuntimeReadTelemetry() && config.useContentionMultiplier()
            ? telemetry.recordPbrContentionPenalty() : 1.0);

    double analyticalNativeSeconds = (double) estimatedBytes * config.nativeRemoteFraction()
        * config.expectedReadCount() / config.nativeBandwidthBytesPerSec();
    double learnedNativeSeconds = telemetry.nativeCostSeconds(estimatedBytes);
    long estimatedBlocks = blockCount(numMaps, numReducers);
    double configuredAvailableCxlBandwidth = config.cxlBandwidthBytesPerSec()
        * Math.max(0.05, 1.0 - config.cxlUtilization());
    double availableCxlBandwidth = config.useRuntimeReadTelemetry()
        && telemetry.pbrReadBandwidth() > 0.0
        ? Math.min(configuredAvailableCxlBandwidth, telemetry.pbrReadBandwidth())
        : configuredAvailableCxlBandwidth;
    double configuredAvailableRecordBandwidth = config.recordPbrBandwidthBytesPerSec()
        * Math.max(0.05, 1.0 - config.cxlUtilization());
    double availableRecordBandwidth = config.useRuntimeReadTelemetry()
        && telemetry.recordPbrReadBandwidth() > 0.0
        ? Math.min(configuredAvailableRecordBandwidth, telemetry.recordPbrReadBandwidth())
        : configuredAvailableRecordBandwidth;
    long estimatedRecords = useOnlineSize && telemetry.recordsPerMap() > 0.0
        ? saturatedMultiply(Math.max(1L, Math.round(telemetry.recordsPerMap())),
            Math.max(1, numMaps))
        : 0L;
    analyticalNativeSeconds += estimatedRecords * config.nativeRecordCpuNs()
        / 1_000_000_000.0;

    double parentNativeReadSeconds = (double) parentReads.nativeBytes()
        * config.nativeRemoteFraction() / config.nativeBandwidthBytesPerSec();
    double parentPbrReadSeconds = (double) parentReads.pbrBytes() / availableCxlBandwidth
        * pbrConcurrencyPenalty;
    double parentRecordPbrReadSeconds = (double) parentReads.recordPbrBytes()
        / availableRecordBandwidth * recordPbrConcurrencyPenalty;
    double parentReadSeconds = parentNativeReadSeconds + parentPbrReadSeconds
        + parentRecordPbrReadSeconds;
    boolean hasExplicitParentReads = parentReads.totalBytes() > 0L;
    double learnedCommonSeconds = !hasExplicitParentReads && learnedNativeSeconds >= 0.0
        ? Math.max(0.0, learnedNativeSeconds - analyticalNativeSeconds) : 0.0;
    double commonStageSeconds = Math.max(parentReadSeconds, learnedCommonSeconds);
    double nativeSeconds = analyticalNativeSeconds + commonStageSeconds;
    double pbrSpecificResidualSeconds = !hasExplicitParentReads
        && telemetry.pbrResidualSeconds() > 0.0
        ? Math.max(0.0, telemetry.pbrResidualSeconds() - commonStageSeconds)
        : pbrFixedMs / 1000.0;
    double pbrSeconds = (pbrTransferMultiplier * (1.0 + config.expectedReadCount())
        * (double) estimatedBytes / availableCxlBandwidth
        + pbrSpecificResidualSeconds
        + (double) estimatedBlocks * config.expectedReadCount()
            * pbrBlockMs / 1000.0) * pbrConcurrencyPenalty + commonStageSeconds;

    double recordPbrSeconds = Double.POSITIVE_INFINITY;
    if (input.hasRecordPbrCapability()) {
      double recordCpuNs = config.recordPbrCpuNs();
      if (config.useRuntimeReadTelemetry() && telemetry.recordDecodeNsPerRecord() > 0.0) {
        recordCpuNs = telemetry.recordDecodeNsPerRecord()
            + telemetry.recordMaterializeNsPerRecord();
      }
      double recordBlockMs = config.useRuntimeReadTelemetry()
          ? telemetry.recordPbrBlockCostMs() : 0.0;
      double directComputeMultiplier = input.supportsDirectCompute()
          ? config.directComputeReadMultiplier() : 1.0;
      double recordSpecificResidualSeconds = !hasExplicitParentReads
          && telemetry.recordPbrResidualSeconds() > 0.0
          ? Math.max(0.0, telemetry.recordPbrResidualSeconds() - commonStageSeconds)
          : config.recordPbrFixedCostMs() / 1000.0;
      recordPbrSeconds = ((double) estimatedBytes / availableRecordBandwidth
          + directComputeMultiplier * config.expectedReadCount() * (double) estimatedBytes
              / availableRecordBandwidth
          + estimatedRecords * recordCpuNs / 1_000_000_000.0
          + estimatedBlocks * config.expectedReadCount() * recordBlockMs / 1000.0
          + recordSpecificResidualSeconds) * recordPbrConcurrencyPenalty + commonStageSeconds;
    }

    boolean pbrEligible = estimatedBytes >= config.minPbrStageBytes();
    boolean hasGain = pbrEligible
        && pbrSeconds < nativeSeconds * (1.0 - config.minGainFraction());
    boolean recordHasGain = pbrEligible
        && recordPbrSeconds < nativeSeconds * (1.0 - config.minGainFraction());
    String decision = NATIVE;
    if (confidence >= config.minConfidence()) {
      if (recordHasGain && recordPbrSeconds < pbrSeconds) {
        decision = RECORD_PBR;
      } else if (hasGain) {
        decision = PBR;
      }
    }
    String reason = confidence < config.minConfidence() ? "low_estimate_confidence"
        : !pbrEligible ? "native_small_stage"
        : RECORD_PBR.equals(decision)
            ? (input.supportsDirectCompute()
                ? "predicted_direct_record_pbr_gain" : "predicted_record_pbr_gain")
        : PBR.equals(decision) ? "predicted_pbr_gain" : "native_cost_or_margin";
    double selectedUtilization = RECORD_PBR.equals(decision)
        ? recordPbrUtilization : pbrUtilization;
    double selectedContentionPenalty = RECORD_PBR.equals(decision)
        ? recordPbrConcurrencyPenalty : pbrConcurrencyPenalty;
    return plan(shuffleId, decision, numMaps, numReducers, estimatedBytes, estimatedBlocks,
        nativeSeconds * 1000.0, pbrSeconds * 1000.0,
        Double.isFinite(recordPbrSeconds) ? recordPbrSeconds * 1000.0 : 0.0,
        config.nativeRemoteFraction(), selectedUtilization, config.nearSlotFraction(),
        selectedContentionPenalty, config.expectedReadCount(), confidence, reason,
        parentReads, parentReadSeconds * 1000.0, estimatedCxlWorkingSetBytes);
  }

  private static long estimateBytes(
      AdaptivePlannerConfig config,
      AdaptiveTelemetrySnapshot telemetry,
      int numMaps,
      boolean useOnlineSize) {
    if (useOnlineSize) {
      return saturatedMultiply(Math.max(1L, Math.round(telemetry.bytesPerMap())),
          Math.max(1, numMaps));
    }
    if (config.estimatedShuffleBytes() > 0L) return config.estimatedShuffleBytes();
    if (config.estimatedBytesPerMap() > 0L) {
      return saturatedMultiply(config.estimatedBytesPerMap(), Math.max(1, numMaps));
    }
    if (telemetry.hasSizeEstimate()) {
      return saturatedMultiply(Math.max(1L, Math.round(telemetry.bytesPerMap())),
          Math.max(1, numMaps));
    }
    return 0L;
  }

  private static ShufflePlan plan(
      int shuffleId,
      String mode,
      int numMaps,
      int numReducers,
      long estimatedShuffleBytes,
      long estimatedBlockCount,
      double predictedNativeMs,
      double predictedPbrMs,
      double predictedRecordPbrMs,
      double remoteFraction,
      double cxlUtilization,
      double nearSlotFraction,
      double pbrConcurrencyPenalty,
      int expectedReadCount,
      double confidence,
      String reason,
      ParentShuffleReadEstimate parentReads,
      double predictedParentReadMs,
      long estimatedCxlWorkingSetBytes) {
    return new ShufflePlan(shuffleId, mode, numMaps, numReducers, estimatedShuffleBytes,
        estimatedBlockCount, predictedNativeMs, predictedPbrMs, predictedRecordPbrMs,
        remoteFraction, cxlUtilization, nearSlotFraction, pbrConcurrencyPenalty,
        expectedReadCount, confidence, reason, parentReads.totalBytes(), parentReads.cxlBytes(),
        parentReads.parentShuffleCount(), predictedParentReadMs, estimatedCxlWorkingSetBytes);
  }

  private static double bandwidthUtilization(double recentBandwidth, double idleBandwidth) {
    if (recentBandwidth <= 0.0 || idleBandwidth <= 0.0) return 0.0;
    return Math.max(0.0, Math.min(1.0, 1.0 - recentBandwidth / idleBandwidth));
  }

  private static long blockCount(int numMaps, int numReducers) {
    return saturatedMultiply(Math.max(1, numMaps), Math.max(1, numReducers));
  }

  private static long saturatedMultiply(long left, long right) {
    if (left == 0L || right == 0L) return 0L;
    if (left > Long.MAX_VALUE / right) return Long.MAX_VALUE;
    return left * right;
  }

  private static long saturatedAdd(long left, long right) {
    if (left >= Long.MAX_VALUE - right) return Long.MAX_VALUE;
    return left + right;
  }
}
