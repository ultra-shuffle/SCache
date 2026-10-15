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

/** Immutable inputs to the experimental fixed-width record-PBR cost model. */
public final class RecordPbrPlannerConfig {
  private final double estimateConfidence;
  private final double minConfidence;
  private final boolean fixedWidthFormat;
  private final boolean binaryAggregate;
  private final long poolCapacityBytes;
  private final double nativeBandwidthBytesPerSec;
  private final double cxlBandwidthBytesPerSec;
  private final double nativeRemoteFraction;
  private final double cxlUtilization;
  private final double nativeRecordCpuNs;
  private final double binaryRecordCpuNs;
  private final double minGainFraction;

  private RecordPbrPlannerConfig(Builder builder) {
    estimateConfidence = clamp01(builder.estimateConfidence);
    minConfidence = clamp01(builder.minConfidence);
    fixedWidthFormat = builder.fixedWidthFormat;
    binaryAggregate = builder.binaryAggregate;
    poolCapacityBytes = Math.max(0L, builder.poolCapacityBytes);
    nativeBandwidthBytesPerSec = positiveOr(builder.nativeBandwidthBytesPerSec, 3_125_000_000.0);
    cxlBandwidthBytesPerSec = positiveOr(builder.cxlBandwidthBytesPerSec, 2_684_354_560.0);
    nativeRemoteFraction = clamp01(builder.nativeRemoteFraction);
    cxlUtilization = clamp01(builder.cxlUtilization);
    nativeRecordCpuNs = nonnegativeOr(builder.nativeRecordCpuNs, 65.0);
    binaryRecordCpuNs = nonnegativeOr(builder.binaryRecordCpuNs, 25.0);
    minGainFraction = nonnegativeOr(builder.minGainFraction, 0.10);
  }

  public static Builder builder() { return new Builder(); }

  public double estimateConfidence() { return estimateConfidence; }
  public double minConfidence() { return minConfidence; }
  public boolean fixedWidthFormat() { return fixedWidthFormat; }
  public boolean binaryAggregate() { return binaryAggregate; }
  public long poolCapacityBytes() { return poolCapacityBytes; }
  public double nativeBandwidthBytesPerSec() { return nativeBandwidthBytesPerSec; }
  public double cxlBandwidthBytesPerSec() { return cxlBandwidthBytesPerSec; }
  public double nativeRemoteFraction() { return nativeRemoteFraction; }
  public double cxlUtilization() { return cxlUtilization; }
  public double nativeRecordCpuNs() { return nativeRecordCpuNs; }
  public double binaryRecordCpuNs() { return binaryRecordCpuNs; }
  public double minGainFraction() { return minGainFraction; }

  private static double clamp01(double value) {
    if (!Double.isFinite(value)) return 0.0;
    return Math.max(0.0, Math.min(1.0, value));
  }

  private static double positiveOr(double value, double fallback) {
    return value > 0.0 && Double.isFinite(value) ? value : fallback;
  }

  private static double nonnegativeOr(double value, double fallback) {
    return value >= 0.0 && Double.isFinite(value) ? value : fallback;
  }

  public static final class Builder {
    private double estimateConfidence = 0.5;
    private double minConfidence = 0.6;
    private boolean fixedWidthFormat;
    private boolean binaryAggregate;
    private long poolCapacityBytes;
    private double nativeBandwidthBytesPerSec = 3_125_000_000.0;
    private double cxlBandwidthBytesPerSec = 2_684_354_560.0;
    private double nativeRemoteFraction = 0.75;
    private double cxlUtilization;
    private double nativeRecordCpuNs = 65.0;
    private double binaryRecordCpuNs = 25.0;
    private double minGainFraction = 0.10;

    public Builder estimateConfidence(double value) { estimateConfidence = value; return this; }
    public Builder minConfidence(double value) { minConfidence = value; return this; }
    public Builder fixedWidthFormat(boolean value) { fixedWidthFormat = value; return this; }
    public Builder binaryAggregate(boolean value) { binaryAggregate = value; return this; }
    public Builder poolCapacityBytes(long value) { poolCapacityBytes = value; return this; }
    public Builder nativeBandwidthBytesPerSec(double value) {
      nativeBandwidthBytesPerSec = value; return this;
    }
    public Builder cxlBandwidthBytesPerSec(double value) {
      cxlBandwidthBytesPerSec = value; return this;
    }
    public Builder nativeRemoteFraction(double value) { nativeRemoteFraction = value; return this; }
    public Builder cxlUtilization(double value) { cxlUtilization = value; return this; }
    public Builder nativeRecordCpuNs(double value) { nativeRecordCpuNs = value; return this; }
    public Builder binaryRecordCpuNs(double value) { binaryRecordCpuNs = value; return this; }
    public Builder minGainFraction(double value) { minGainFraction = value; return this; }

    public RecordPbrPlannerConfig build() { return new RecordPbrPlannerConfig(this); }
  }
}
