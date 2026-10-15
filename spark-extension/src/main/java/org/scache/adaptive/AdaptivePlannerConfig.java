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

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Immutable parameters for {@link AdaptiveShufflePlanner}.
 *
 * <p>This object deliberately has no Spark dependency. A version-specific adapter should read
 * Spark configuration once, build this object, and reuse it for shuffle-level planning.</p>
 */
public final class AdaptivePlannerConfig {
  private final String mode;
  private final Map<Integer, String> stageModeOverrides;
  private final boolean strictStageModeOverrides;
  private final long estimatedShuffleBytes;
  private final long estimatedBytesPerMap;
  private final int onlineMinSamples;
  private final long cxlCapacityBytes;
  private final double maxExtrapolationFactor;
  private final double nativeBandwidthBytesPerSec;
  private final double cxlBandwidthBytesPerSec;
  private final double nativeRemoteFraction;
  private final double pbrFixedCostMs;
  private final double pbrAggregateFixedCostMs;
  private final double pbrOrderedHashFixedCostMs;
  private final double pbrRangeTransferMultiplier;
  private final double pbrCostPerBlockMs;
  private final boolean useRuntimeReadTelemetry;
  private final double minGainFraction;
  private final double estimateConfidence;
  private final double minConfidence;
  private final double cxlUtilization;
  private final double nearSlotFraction;
  private final double targetNearSlotFraction;
  private final int expectedReadCount;
  private final boolean useContentionMultiplier;
  private final double nativeRecordCpuNs;
  private final double recordPbrBandwidthBytesPerSec;
  private final double recordPbrFixedCostMs;
  private final double recordPbrCpuNs;
  private final double directComputeReadMultiplier;
  private final long minPbrStageBytes;

  private AdaptivePlannerConfig(Builder builder) {
    mode = normalizeMode(builder.mode, true);
    strictStageModeOverrides = builder.strictStageModeOverrides;
    stageModeOverrides = immutableOverrides(builder.stageModeOverrides);
    estimatedShuffleBytes = Math.max(0L, builder.estimatedShuffleBytes);
    estimatedBytesPerMap = Math.max(0L, builder.estimatedBytesPerMap);
    onlineMinSamples = Math.max(1, builder.onlineMinSamples);
    cxlCapacityBytes = Math.max(0L, builder.cxlCapacityBytes);
    maxExtrapolationFactor = atLeast(builder.maxExtrapolationFactor, 1.0, 4.0);
    nativeBandwidthBytesPerSec = positiveOr(builder.nativeBandwidthBytesPerSec, 750_000_000.0);
    cxlBandwidthBytesPerSec = positiveOr(builder.cxlBandwidthBytesPerSec, 2_684_354_560.0);
    nativeRemoteFraction = clamp01(builder.nativeRemoteFraction, 0.75);
    pbrFixedCostMs = nonnegativeOr(builder.pbrFixedCostMs, 3000.0);
    pbrAggregateFixedCostMs = Double.isNaN(builder.pbrAggregateFixedCostMs)
        ? pbrFixedCostMs : nonnegativeOr(builder.pbrAggregateFixedCostMs, pbrFixedCostMs);
    pbrOrderedHashFixedCostMs = nonnegativeOr(builder.pbrOrderedHashFixedCostMs, 200.0);
    pbrRangeTransferMultiplier = atLeast(builder.pbrRangeTransferMultiplier, 1.0, 1.5);
    pbrCostPerBlockMs = nonnegativeOr(builder.pbrCostPerBlockMs, 0.05);
    useRuntimeReadTelemetry = builder.useRuntimeReadTelemetry;
    minGainFraction = nonnegativeOr(builder.minGainFraction, 0.10);
    estimateConfidence = clamp01(builder.estimateConfidence, 0.5);
    minConfidence = clamp01(builder.minConfidence, 0.60);
    cxlUtilization = clamp01(builder.cxlUtilization, 0.0);
    nearSlotFraction = clamp(builder.nearSlotFraction, 0.01, 1.0, 0.75);
    targetNearSlotFraction = clamp(builder.targetNearSlotFraction, 0.01, 1.0, 0.75);
    expectedReadCount = Math.max(1, builder.expectedReadCount);
    useContentionMultiplier = builder.useContentionMultiplier;
    nativeRecordCpuNs = nonnegativeOr(builder.nativeRecordCpuNs, 65.0);
    recordPbrBandwidthBytesPerSec = Double.isNaN(builder.recordPbrBandwidthBytesPerSec)
        ? cxlBandwidthBytesPerSec
        : positiveOr(builder.recordPbrBandwidthBytesPerSec, cxlBandwidthBytesPerSec);
    recordPbrFixedCostMs = nonnegativeOr(builder.recordPbrFixedCostMs, 200.0);
    recordPbrCpuNs = nonnegativeOr(builder.recordPbrCpuNs, 5.0);
    directComputeReadMultiplier = nonnegativeOr(builder.directComputeReadMultiplier, 0.55);
    minPbrStageBytes = Math.max(0L, builder.minPbrStageBytes);
  }

  /** Safe default: force the native path and perform no adaptive selection. */
  public static AdaptivePlannerConfig nativeOnly() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public String mode() { return mode; }
  public Map<Integer, String> stageModeOverrides() { return stageModeOverrides; }
  public boolean strictStageModeOverrides() { return strictStageModeOverrides; }
  public long estimatedShuffleBytes() { return estimatedShuffleBytes; }
  public long estimatedBytesPerMap() { return estimatedBytesPerMap; }
  public int onlineMinSamples() { return onlineMinSamples; }
  public long cxlCapacityBytes() { return cxlCapacityBytes; }
  public double maxExtrapolationFactor() { return maxExtrapolationFactor; }
  public double nativeBandwidthBytesPerSec() { return nativeBandwidthBytesPerSec; }
  public double cxlBandwidthBytesPerSec() { return cxlBandwidthBytesPerSec; }
  public double nativeRemoteFraction() { return nativeRemoteFraction; }
  public double pbrFixedCostMs() { return pbrFixedCostMs; }
  public double pbrAggregateFixedCostMs() { return pbrAggregateFixedCostMs; }
  public double pbrOrderedHashFixedCostMs() { return pbrOrderedHashFixedCostMs; }
  public double pbrRangeTransferMultiplier() { return pbrRangeTransferMultiplier; }
  public double pbrCostPerBlockMs() { return pbrCostPerBlockMs; }
  public boolean useRuntimeReadTelemetry() { return useRuntimeReadTelemetry; }
  public double minGainFraction() { return minGainFraction; }
  public double estimateConfidence() { return estimateConfidence; }
  public double minConfidence() { return minConfidence; }
  public double cxlUtilization() { return cxlUtilization; }
  public double nearSlotFraction() { return nearSlotFraction; }
  public double targetNearSlotFraction() { return targetNearSlotFraction; }
  public int expectedReadCount() { return expectedReadCount; }
  public boolean useContentionMultiplier() { return useContentionMultiplier; }
  public double nativeRecordCpuNs() { return nativeRecordCpuNs; }
  public double recordPbrBandwidthBytesPerSec() { return recordPbrBandwidthBytesPerSec; }
  public double recordPbrFixedCostMs() { return recordPbrFixedCostMs; }
  public double recordPbrCpuNs() { return recordPbrCpuNs; }
  public double directComputeReadMultiplier() { return directComputeReadMultiplier; }
  public long minPbrStageBytes() { return minPbrStageBytes; }

  private static Map<Integer, String> immutableOverrides(Map<Integer, String> source) {
    if (source.isEmpty()) return Collections.emptyMap();
    Map<Integer, String> copy = new HashMap<>();
    for (Map.Entry<Integer, String> entry : source.entrySet()) {
      if (entry.getKey() == null || entry.getKey() < 0) {
        throw new IllegalArgumentException("stage override shuffle ID must be nonnegative");
      }
      copy.put(entry.getKey(), normalizeMode(entry.getValue(), false));
    }
    return Collections.unmodifiableMap(copy);
  }

  private static String normalizeMode(String value, boolean allowAdaptive) {
    String normalized = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    if (AdaptiveShufflePlanner.NATIVE.equals(normalized)
        || AdaptiveShufflePlanner.PBR.equals(normalized)
        || AdaptiveShufflePlanner.RECORD_PBR.equals(normalized)
        || (allowAdaptive && AdaptiveShufflePlanner.ADAPTIVE.equals(normalized))) {
      return normalized;
    }
    throw new IllegalArgumentException("unsupported shuffle mode: " + value);
  }

  private static double atLeast(double value, double minimum, double fallback) {
    if (!Double.isFinite(value)) return fallback;
    return Math.max(minimum, value);
  }

  private static double positiveOr(double value, double fallback) {
    return value > 0.0 && Double.isFinite(value) ? value : fallback;
  }

  private static double nonnegativeOr(double value, double fallback) {
    return value >= 0.0 && Double.isFinite(value) ? value : fallback;
  }

  private static double clamp01(double value, double fallback) {
    return clamp(value, 0.0, 1.0, fallback);
  }

  private static double clamp(double value, double minimum, double maximum, double fallback) {
    if (!Double.isFinite(value)) return fallback;
    return Math.max(minimum, Math.min(maximum, value));
  }

  public static final class Builder {
    private String mode = AdaptiveShufflePlanner.NATIVE;
    private final Map<Integer, String> stageModeOverrides = new HashMap<>();
    private boolean strictStageModeOverrides = false;
    private long estimatedShuffleBytes = 0L;
    private long estimatedBytesPerMap = 0L;
    private int onlineMinSamples = 4;
    private long cxlCapacityBytes = Long.MAX_VALUE;
    private double maxExtrapolationFactor = 4.0;
    private double nativeBandwidthBytesPerSec = 750_000_000.0;
    private double cxlBandwidthBytesPerSec = 2_684_354_560.0;
    private double nativeRemoteFraction = 0.75;
    private double pbrFixedCostMs = 3000.0;
    private double pbrAggregateFixedCostMs = Double.NaN;
    private double pbrOrderedHashFixedCostMs = 200.0;
    private double pbrRangeTransferMultiplier = 1.5;
    private double pbrCostPerBlockMs = 0.05;
    private boolean useRuntimeReadTelemetry = true;
    private double minGainFraction = 0.10;
    private double estimateConfidence = 0.5;
    private double minConfidence = 0.60;
    private double cxlUtilization = 0.0;
    private double nearSlotFraction = 0.75;
    private double targetNearSlotFraction = 0.75;
    private int expectedReadCount = 1;
    private boolean useContentionMultiplier = false;
    private double nativeRecordCpuNs = 65.0;
    private double recordPbrBandwidthBytesPerSec = Double.NaN;
    private double recordPbrFixedCostMs = 200.0;
    private double recordPbrCpuNs = 5.0;
    private double directComputeReadMultiplier = 0.55;
    private long minPbrStageBytes = 256L * 1024L * 1024L;

    public Builder mode(String value) { mode = value; return this; }
    public Builder stageModeOverride(int shuffleId, String value) {
      stageModeOverrides.put(shuffleId, value);
      return this;
    }
    public Builder stageModeOverrides(Map<Integer, String> values) {
      stageModeOverrides.clear();
      if (values != null) stageModeOverrides.putAll(values);
      return this;
    }
    public Builder strictStageModeOverrides(boolean value) {
      strictStageModeOverrides = value; return this;
    }
    public Builder estimatedShuffleBytes(long value) { estimatedShuffleBytes = value; return this; }
    public Builder estimatedBytesPerMap(long value) { estimatedBytesPerMap = value; return this; }
    public Builder onlineMinSamples(int value) { onlineMinSamples = value; return this; }
    public Builder cxlCapacityBytes(long value) { cxlCapacityBytes = value; return this; }
    public Builder maxExtrapolationFactor(double value) {
      maxExtrapolationFactor = value; return this;
    }
    public Builder nativeBandwidthBytesPerSec(double value) {
      nativeBandwidthBytesPerSec = value; return this;
    }
    public Builder cxlBandwidthBytesPerSec(double value) {
      cxlBandwidthBytesPerSec = value; return this;
    }
    public Builder nativeRemoteFraction(double value) { nativeRemoteFraction = value; return this; }
    public Builder pbrFixedCostMs(double value) { pbrFixedCostMs = value; return this; }
    public Builder pbrAggregateFixedCostMs(double value) {
      pbrAggregateFixedCostMs = value; return this;
    }
    public Builder pbrOrderedHashFixedCostMs(double value) {
      pbrOrderedHashFixedCostMs = value; return this;
    }
    public Builder pbrRangeTransferMultiplier(double value) {
      pbrRangeTransferMultiplier = value; return this;
    }
    public Builder pbrCostPerBlockMs(double value) { pbrCostPerBlockMs = value; return this; }
    public Builder useRuntimeReadTelemetry(boolean value) {
      useRuntimeReadTelemetry = value; return this;
    }
    public Builder minGainFraction(double value) { minGainFraction = value; return this; }
    public Builder estimateConfidence(double value) { estimateConfidence = value; return this; }
    public Builder minConfidence(double value) { minConfidence = value; return this; }
    public Builder cxlUtilization(double value) { cxlUtilization = value; return this; }
    public Builder nearSlotFraction(double value) { nearSlotFraction = value; return this; }
    public Builder targetNearSlotFraction(double value) {
      targetNearSlotFraction = value; return this;
    }
    public Builder expectedReadCount(int value) { expectedReadCount = value; return this; }
    public Builder useContentionMultiplier(boolean value) {
      useContentionMultiplier = value; return this;
    }
    public Builder nativeRecordCpuNs(double value) { nativeRecordCpuNs = value; return this; }
    public Builder recordPbrBandwidthBytesPerSec(double value) {
      recordPbrBandwidthBytesPerSec = value; return this;
    }
    public Builder recordPbrFixedCostMs(double value) {
      recordPbrFixedCostMs = value; return this;
    }
    public Builder recordPbrCpuNs(double value) { recordPbrCpuNs = value; return this; }
    public Builder directComputeReadMultiplier(double value) {
      directComputeReadMultiplier = value; return this;
    }
    public Builder minPbrStageBytes(long value) { minPbrStageBytes = value; return this; }

    public AdaptivePlannerConfig build() {
      return new AdaptivePlannerConfig(this);
    }
  }
}
