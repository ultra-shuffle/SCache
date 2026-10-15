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

/** Pure cost model for the capability-gated fixed-width record-PBR experiment. */
public final class RecordPbrCostModel {
  private RecordPbrCostModel() { }

  public static RecordPbrPlan choose(
      RecordPbrPlannerConfig config,
      long estimatedBytes,
      long estimatedRecords,
      long requiredPoolBytes) {
    if (config == null) throw new NullPointerException("config");
    long bytes = Math.max(0L, estimatedBytes);
    long records = Math.max(0L, estimatedRecords);
    if (bytes == 0L || records == 0L) {
      return nativePlan(bytes, records, config.estimateConfidence(), "missing_record_estimate");
    }
    if (!config.fixedWidthFormat() || !config.binaryAggregate()) {
      return nativePlan(bytes, records, config.estimateConfidence(),
          "unsupported_record_capability");
    }
    if (config.poolCapacityBytes() < Math.max(0L, requiredPoolBytes)) {
      return nativePlan(bytes, records, config.estimateConfidence(),
          "insufficient_pool_capacity");
    }
    if (config.estimateConfidence() < config.minConfidence()) {
      return nativePlan(bytes, records, config.estimateConfidence(),
          "low_estimate_confidence");
    }

    double nativeSeconds = bytes * config.nativeRemoteFraction()
        / config.nativeBandwidthBytesPerSec()
        + records * config.nativeRecordCpuNs() / 1_000_000_000.0;
    double availableCxlBandwidth = config.cxlBandwidthBytesPerSec()
        * Math.max(0.05, 1.0 - config.cxlUtilization());
    double recordSeconds = 2.0 * bytes / availableCxlBandwidth
        + records * config.binaryRecordCpuNs() / 1_000_000_000.0;
    boolean hasGain = recordSeconds
        < nativeSeconds * (1.0 - config.minGainFraction());
    return new RecordPbrPlan(
        hasGain ? AdaptiveShufflePlanner.RECORD_PBR : AdaptiveShufflePlanner.NATIVE,
        bytes, records, nativeSeconds * 1000.0, recordSeconds * 1000.0,
        config.estimateConfidence(),
        hasGain ? "predicted_record_pbr_gain" : "native_cost_or_margin");
  }

  private static RecordPbrPlan nativePlan(
      long bytes, long records, double confidence, String reason) {
    return new RecordPbrPlan(AdaptiveShufflePlanner.NATIVE, bytes, records,
        0.0, 0.0, confidence, reason);
  }
}
