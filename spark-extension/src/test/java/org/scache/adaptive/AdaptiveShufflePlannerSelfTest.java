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

import org.scache.telemetry.AdaptiveTelemetryRegistry;
import org.scache.telemetry.AdaptiveTelemetrySnapshot;
import org.scache.telemetry.ScacheProfilingMetrics;
import org.scache.telemetry.TelemetryConfig;

/** Framework-free regression tests for the Spark-independent adaptive planner. */
public final class AdaptiveShufflePlannerSelfTest {
  private AdaptiveShufflePlannerSelfTest() { }

  public static void main(String[] args) {
    defaultIsNativeAndStateless();
    forcedModesAndOverrides();
    explainsColdAndLowConfidencePlans();
    choosesAcrossAllThreeModes();
    pricesLargeShuffleGridsWithoutOverflow();
    accountsForParentReadsAndCapacity();
    pricesRecordPbrParentReadsIndependently();
    consumesOnlineTelemetry();
    recordPbrCostModelIsCapabilityGated();
    profilingDtoEscapesJson();
    System.out.println("AdaptiveShufflePlannerSelfTest: PASS");
  }

  private static void defaultIsNativeAndStateless() {
    ShufflePlan first = AdaptiveShufflePlanner.choose(
        AdaptivePlannerConfig.nativeOnly(), input(1, 4, 8));
    ShufflePlan second = AdaptiveShufflePlanner.choose(
        AdaptivePlannerConfig.nativeOnly(), input(1, 4, 8));
    check(AdaptiveShufflePlanner.NATIVE.equals(first.mode()), "default must force native");
    check("forced_native".equals(first.reason()), "default reason must be explicit");
    check(first != second, "planner must not install decisions in global state");
  }

  private static void forcedModesAndOverrides() {
    AdaptivePlannerConfig record = AdaptivePlannerConfig.builder()
        .mode(AdaptiveShufflePlanner.RECORD_PBR)
        .build();
    ShufflePlan fallback = AdaptiveShufflePlanner.choose(record, input(2, 2, 2));
    check(AdaptiveShufflePlanner.PBR.equals(fallback.mode()),
        "record-PBR without capability must fall back to block PBR");

    AdaptivePlannerConfig override = AdaptivePlannerConfig.builder()
        .mode(AdaptiveShufflePlanner.ADAPTIVE)
        .stageModeOverride(7, AdaptiveShufflePlanner.RECORD_PBR)
        .strictStageModeOverrides(true)
        .estimatedShuffleBytes(1024L)
        .build();
    ShufflePlan plan = AdaptiveShufflePlanner.choose(override,
        ShufflePlanningInput.builder(7, 3, 5).hasRecordPbrCapability(true).build());
    check(AdaptiveShufflePlanner.RECORD_PBR.equals(plan.mode()), "override was not honored");
    check("stage_mode_override".equals(plan.reason()), "override reason changed");
    check(plan.estimatedBlockCount() == 15L, "block count changed");
    expectFailure(() -> AdaptiveShufflePlanner.choose(override, input(8, 3, 5)));
  }

  private static void explainsColdAndLowConfidencePlans() {
    AdaptivePlannerConfig cold = AdaptivePlannerConfig.builder()
        .mode(AdaptiveShufflePlanner.ADAPTIVE)
        .build();
    ShufflePlan missing = AdaptiveShufflePlanner.choose(cold, input(3, 4, 4));
    check("missing_shuffle_size_estimate".equals(missing.reason()),
        "cold plan must fail safe with an explanation");

    AdaptivePlannerConfig lowConfidence = AdaptivePlannerConfig.builder()
        .mode(AdaptiveShufflePlanner.ADAPTIVE)
        .estimatedShuffleBytes(1L << 30)
        .nativeBandwidthBytesPerSec(100_000_000.0)
        .nativeRemoteFraction(1.0)
        .pbrFixedCostMs(0.0)
        .estimateConfidence(0.5)
        .build();
    ShufflePlan plan = AdaptiveShufflePlanner.choose(lowConfidence, input(4, 8, 8));
    check(AdaptiveShufflePlanner.NATIVE.equals(plan.mode()), "low confidence must use native");
    check("low_estimate_confidence".equals(plan.reason()), "low-confidence reason changed");
  }

  private static void choosesAcrossAllThreeModes() {
    AdaptivePlannerConfig config = AdaptivePlannerConfig.builder()
        .mode(AdaptiveShufflePlanner.ADAPTIVE)
        .estimatedShuffleBytes(32L << 30)
        .estimateConfidence(0.95)
        .build();
    ShufflePlan block = AdaptiveShufflePlanner.choose(config, input(5, 32, 32));
    check(AdaptiveShufflePlanner.PBR.equals(block.mode()), "large shuffle should select PBR");

    ShufflePlan record = AdaptiveShufflePlanner.choose(config,
        ShufflePlanningInput.builder(6, 32, 32)
            .hasAggregator(true)
            .hasRecordPbrCapability(true)
            .supportsDirectCompute(true)
            .build());
    check(AdaptiveShufflePlanner.RECORD_PBR.equals(record.mode()),
        "direct record-capable shuffle should select record-PBR");
    check(record.predictedRecordPbrMs() < record.predictedPbrMs(),
        "record-PBR estimate should beat block PBR");
    check(record.toJson().contains("\"shuffleId\":6"), "decision JSON is incomplete");
  }

  private static void pricesLargeShuffleGridsWithoutOverflow() {
    AdaptivePlannerConfig config = AdaptivePlannerConfig.builder()
        .mode(AdaptiveShufflePlanner.ADAPTIVE)
        .estimatedShuffleBytes(1L << 30)
        .estimateConfidence(1.0)
        .pbrFixedCostMs(0.0)
        .build();
    ShufflePlan plan = AdaptiveShufflePlanner.choose(config, input(40, 50_000, 50_000));

    check(plan.estimatedBlockCount() == 2_500_000_000L,
        "large shuffle block count overflowed");
    check(plan.predictedPbrMs() >= 125_000_000.0,
        "large shuffle PBR block cost overflowed");
    check(AdaptiveShufflePlanner.NATIVE.equals(plan.mode()),
        "overflowed PBR cost must not select PBR");
  }

  private static void accountsForParentReadsAndCapacity() {
    AdaptivePlannerConfig config = AdaptivePlannerConfig.builder()
        .mode(AdaptiveShufflePlanner.ADAPTIVE)
        .estimatedShuffleBytes(16L << 30)
        .cxlCapacityBytes(48L << 30)
        .estimateConfidence(0.95)
        .build();
    ParentShuffleReadEstimate parents = new ParentShuffleReadEstimate(0L, 40L << 30, 0L, 1);
    ShufflePlan plan = AdaptiveShufflePlanner.choose(config,
        ShufflePlanningInput.builder(7, 32, 32).parentReads(parents).build());
    check(AdaptiveShufflePlanner.NATIVE.equals(plan.mode()), "capacity guard must use native");
    check("cxl_working_set_exceeded".equals(plan.reason()), "working-set reason changed");
    check(plan.estimatedCxlWorkingSetBytes() == (56L << 30), "working-set estimate changed");
    check(plan.estimatedParentReadBytes() == (40L << 30), "parent estimate was lost");
  }

  private static void pricesRecordPbrParentReadsIndependently() {
    long oneSecondOfPbrBytes = 1_000_000_000L;
    AdaptivePlannerConfig config = AdaptivePlannerConfig.builder()
        .mode(AdaptiveShufflePlanner.ADAPTIVE)
        .estimatedShuffleBytes(1L << 30)
        .estimateConfidence(0.95)
        .cxlBandwidthBytesPerSec(1_000_000_000.0)
        .recordPbrBandwidthBytesPerSec(100_000_000.0)
        .useRuntimeReadTelemetry(false)
        .build();
    ParentShuffleReadEstimate parents = new ParentShuffleReadEstimate(
        0L, oneSecondOfPbrBytes, oneSecondOfPbrBytes, 2);

    ShufflePlan plan = AdaptiveShufflePlanner.choose(config,
        ShufflePlanningInput.builder(9, 4, 4).parentReads(parents).build());

    check(Math.abs(plan.predictedParentReadMs() - 11_000.0) < 0.001,
        "record-PBR parent bytes must use record-PBR bandwidth");

    AdaptiveTelemetrySnapshot differentiatedContention = new AdaptiveTelemetrySnapshot(
        0L, 0.0, 0.0, 0.0, 0.0, 0.0, 0L, 0L, 0L,
        0L, 0.0, 0.0, 0.0, 0.0,
        0.0, 0.0, 0.0, 0.0,
        1_000_000_000.0, 1_000_000_000.0,
        1_000_000_000.0, 1_000_000_000.0,
        1.0, 3.0, 0.0, 0.0, 0.0);
    AdaptivePlannerConfig contentionConfig = AdaptivePlannerConfig.builder()
        .mode(AdaptiveShufflePlanner.ADAPTIVE)
        .estimatedShuffleBytes(1L << 30)
        .estimateConfidence(0.95)
        .cxlBandwidthBytesPerSec(1_000_000_000.0)
        .recordPbrBandwidthBytesPerSec(1_000_000_000.0)
        .useRuntimeReadTelemetry(true)
        .useContentionMultiplier(true)
        .build();
    ShufflePlan contentionPlan = AdaptiveShufflePlanner.choose(contentionConfig,
        ShufflePlanningInput.builder(10, 4, 4)
            .parentReads(parents)
            .telemetry(differentiatedContention)
            .build());
    check(Math.abs(contentionPlan.predictedParentReadMs() - 4_000.0) < 0.001,
        "record-PBR parent bytes must use record-PBR contention penalty");
  }

  private static void consumesOnlineTelemetry() {
    AdaptiveTelemetryRegistry registry = new AdaptiveTelemetryRegistry(
        TelemetryConfig.builder()
            .adaptiveEnabled(true)
            .ewmaAlpha(1.0)
            .fullConfidenceSamples(1)
            .build());
    registry.observeMap("online", 1L << 30, 1_000_000L);
    AdaptiveTelemetrySnapshot telemetry = registry.snapshot("online");
    AdaptivePlannerConfig config = AdaptivePlannerConfig.builder()
        .mode(AdaptiveShufflePlanner.ADAPTIVE)
        .onlineMinSamples(1)
        .estimateConfidence(0.95)
        .build();
    ShufflePlan plan = AdaptiveShufflePlanner.choose(config,
        ShufflePlanningInput.builder(8, 2, 4).telemetry(telemetry).build());
    check(plan.estimatedShuffleBytes() == (2L << 30), "online size estimate changed");
    check(plan.confidence() == 0.95, "planner confidence should cap telemetry confidence");
  }

  private static void recordPbrCostModelIsCapabilityGated() {
    long oneGiB = 1L << 30;
    RecordPbrPlannerConfig capable = RecordPbrPlannerConfig.builder()
        .estimateConfidence(0.9)
        .fixedWidthFormat(true)
        .binaryAggregate(true)
        .poolCapacityBytes(2L << 30)
        .build();
    RecordPbrPlan selected = RecordPbrCostModel.choose(
        capable, oneGiB, 100_000_000L, oneGiB + 4096L);
    check(AdaptiveShufflePlanner.RECORD_PBR.equals(selected.mode()),
        "capable record plan should select record-PBR");
    RecordPbrPlan unsupported = RecordPbrCostModel.choose(
        RecordPbrPlannerConfig.builder().build(), oneGiB, 100_000_000L, oneGiB);
    check("unsupported_record_capability".equals(unsupported.reason()),
        "unsupported capability must fail safe");
  }

  private static void profilingDtoEscapesJson() {
    ScacheProfilingMetrics metrics = new ScacheProfilingMetrics(true);
    metrics.fetchCount = 2L;
    metrics.batchLookupCount = 1L;
    String json = metrics.toJson("read\"side", 9, 3L, 12L);
    check(json.contains("\"side\":\"read\\\"side\""), "profiling JSON did not escape side");
    check(json.contains("\"batchLookupCount\":1"), "profiling JSON omitted migrated counters");
  }

  private static ShufflePlanningInput input(int shuffleId, int maps, int reducers) {
    return ShufflePlanningInput.builder(shuffleId, maps, reducers).build();
  }

  private static void expectFailure(Runnable runnable) {
    try {
      runnable.run();
      throw new AssertionError("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      // Expected.
    }
  }

  private static void check(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
  }
}
