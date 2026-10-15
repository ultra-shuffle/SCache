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
package org.scache.telemetry;

/** Immutable online measurements that a caller may use when planning a later shuffle stage. */
public final class AdaptiveTelemetrySnapshot {
  public static final AdaptiveTelemetrySnapshot EMPTY = new AdaptiveTelemetrySnapshot(
      0L, 0.0, 0.0, 0.0, 0.0, 0.0, 0L, 0L, 0L,
      0L, 0.0, 0.0, 0.0, 0.0,
      0.0, 0.0, 0.0, 0.0,
      0.0, 0.0, 0.0, 0.0, 1.0, 1.0,
      0.0, 0.0, 0.0);

  private final long samples;
  private final double bytesPerMap;
  private final double recordsPerMap;
  private final double nativeWriteBandwidth;
  private final double pbrWriteBandwidth;
  private final double recordPbrWriteBandwidth;
  private final long nativeStageBytes;
  private final long pbrStageBytes;
  private final long recordPbrStageBytes;
  private final long nativeCostSamples;
  private final double nativeCostSumBytes;
  private final double nativeCostSumSeconds;
  private final double nativeCostSumBytesSquared;
  private final double nativeCostSumByteSeconds;
  private final double pbrBlockCostMs;
  private final double recordPbrBlockCostMs;
  private final double recordDecodeNsPerRecord;
  private final double recordMaterializeNsPerRecord;
  private final double pbrReadBandwidth;
  private final double recordPbrReadBandwidth;
  private final double pbrIdleReadBandwidth;
  private final double recordPbrIdleReadBandwidth;
  private final double pbrContentionPenalty;
  private final double recordPbrContentionPenalty;
  private final double pbrResidualSeconds;
  private final double recordPbrResidualSeconds;
  private final double confidence;

  public AdaptiveTelemetrySnapshot(
      long samples,
      double bytesPerMap,
      double recordsPerMap,
      double nativeWriteBandwidth,
      double pbrWriteBandwidth,
      double recordPbrWriteBandwidth,
      long nativeStageBytes,
      long pbrStageBytes,
      long recordPbrStageBytes,
      long nativeCostSamples,
      double nativeCostSumBytes,
      double nativeCostSumSeconds,
      double nativeCostSumBytesSquared,
      double nativeCostSumByteSeconds,
      double pbrBlockCostMs,
      double recordPbrBlockCostMs,
      double recordDecodeNsPerRecord,
      double recordMaterializeNsPerRecord,
      double pbrReadBandwidth,
      double recordPbrReadBandwidth,
      double pbrIdleReadBandwidth,
      double recordPbrIdleReadBandwidth,
      double pbrContentionPenalty,
      double recordPbrContentionPenalty,
      double pbrResidualSeconds,
      double recordPbrResidualSeconds,
      double confidence) {
    this.samples = Math.max(0L, samples);
    this.bytesPerMap = nonnegative(bytesPerMap);
    this.recordsPerMap = nonnegative(recordsPerMap);
    this.nativeWriteBandwidth = nonnegative(nativeWriteBandwidth);
    this.pbrWriteBandwidth = nonnegative(pbrWriteBandwidth);
    this.recordPbrWriteBandwidth = nonnegative(recordPbrWriteBandwidth);
    this.nativeStageBytes = Math.max(0L, nativeStageBytes);
    this.pbrStageBytes = Math.max(0L, pbrStageBytes);
    this.recordPbrStageBytes = Math.max(0L, recordPbrStageBytes);
    this.nativeCostSamples = Math.max(0L, nativeCostSamples);
    this.nativeCostSumBytes = nonnegative(nativeCostSumBytes);
    this.nativeCostSumSeconds = nonnegative(nativeCostSumSeconds);
    this.nativeCostSumBytesSquared = nonnegative(nativeCostSumBytesSquared);
    this.nativeCostSumByteSeconds = nonnegative(nativeCostSumByteSeconds);
    this.pbrBlockCostMs = nonnegative(pbrBlockCostMs);
    this.recordPbrBlockCostMs = nonnegative(recordPbrBlockCostMs);
    this.recordDecodeNsPerRecord = nonnegative(recordDecodeNsPerRecord);
    this.recordMaterializeNsPerRecord = nonnegative(recordMaterializeNsPerRecord);
    this.pbrReadBandwidth = nonnegative(pbrReadBandwidth);
    this.recordPbrReadBandwidth = nonnegative(recordPbrReadBandwidth);
    this.pbrIdleReadBandwidth = nonnegative(pbrIdleReadBandwidth);
    this.recordPbrIdleReadBandwidth = nonnegative(recordPbrIdleReadBandwidth);
    this.pbrContentionPenalty = Math.max(1.0, finiteOr(pbrContentionPenalty, 1.0));
    this.recordPbrContentionPenalty = Math.max(
        1.0, finiteOr(recordPbrContentionPenalty, 1.0));
    this.pbrResidualSeconds = nonnegative(pbrResidualSeconds);
    this.recordPbrResidualSeconds = nonnegative(recordPbrResidualSeconds);
    this.confidence = Math.max(0.0, Math.min(1.0, finiteOr(confidence, 0.0)));
  }

  public long samples() { return samples; }
  public double bytesPerMap() { return bytesPerMap; }
  public double recordsPerMap() { return recordsPerMap; }
  public double nativeWriteBandwidth() { return nativeWriteBandwidth; }
  public double pbrWriteBandwidth() { return pbrWriteBandwidth; }
  public double recordPbrWriteBandwidth() { return recordPbrWriteBandwidth; }
  public long nativeStageBytes() { return nativeStageBytes; }
  public long pbrStageBytes() { return pbrStageBytes; }
  public long recordPbrStageBytes() { return recordPbrStageBytes; }
  public long nativeCostSamples() { return nativeCostSamples; }
  public double nativeCostSumBytes() { return nativeCostSumBytes; }
  public double nativeCostSumSeconds() { return nativeCostSumSeconds; }
  public double nativeCostSumBytesSquared() { return nativeCostSumBytesSquared; }
  public double nativeCostSumByteSeconds() { return nativeCostSumByteSeconds; }
  public double pbrBlockCostMs() { return pbrBlockCostMs; }
  public double recordPbrBlockCostMs() { return recordPbrBlockCostMs; }
  public double recordDecodeNsPerRecord() { return recordDecodeNsPerRecord; }
  public double recordMaterializeNsPerRecord() { return recordMaterializeNsPerRecord; }
  public double pbrReadBandwidth() { return pbrReadBandwidth; }
  public double recordPbrReadBandwidth() { return recordPbrReadBandwidth; }
  public double pbrIdleReadBandwidth() { return pbrIdleReadBandwidth; }
  public double recordPbrIdleReadBandwidth() { return recordPbrIdleReadBandwidth; }
  public double pbrContentionPenalty() { return pbrContentionPenalty; }
  public double recordPbrContentionPenalty() { return recordPbrContentionPenalty; }
  public double pbrResidualSeconds() { return pbrResidualSeconds; }
  public double recordPbrResidualSeconds() { return recordPbrResidualSeconds; }
  public double confidence() { return confidence; }

  public boolean hasSizeEstimate() {
    return samples > 0L && bytesPerMap > 0.0;
  }

  /** Returns the learned affine native stage-cost estimate, or -1 if underdetermined. */
  public double nativeCostSeconds(long bytes) {
    if (nativeCostSamples < 2L) return -1.0;
    double denominator = nativeCostSamples * nativeCostSumBytesSquared
        - nativeCostSumBytes * nativeCostSumBytes;
    if (denominator <= 0.0 || Double.isInfinite(denominator)) return -1.0;
    double slope = Math.max(0.0, (nativeCostSamples * nativeCostSumByteSeconds
        - nativeCostSumBytes * nativeCostSumSeconds) / denominator);
    double intercept = Math.max(
        0.0, (nativeCostSumSeconds - slope * nativeCostSumBytes) / nativeCostSamples);
    double estimate = intercept + slope * Math.max(0L, bytes);
    return Double.isNaN(estimate) || Double.isInfinite(estimate) ? -1.0 : estimate;
  }

  private static double nonnegative(double value) {
    return Math.max(0.0, finiteOr(value, 0.0));
  }

  private static double finiteOr(double value, double fallback) {
    return Double.isNaN(value) || Double.isInfinite(value) ? fallback : value;
  }
}
