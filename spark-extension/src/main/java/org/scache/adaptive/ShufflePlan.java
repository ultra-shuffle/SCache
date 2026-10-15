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

import java.io.Serializable;
import java.util.Locale;

/** Immutable, explainable decision for one shuffle. */
public final class ShufflePlan implements Serializable {
  private static final long serialVersionUID = 1L;

  private final int shuffleId;
  private final String mode;
  private final int numMaps;
  private final int numReducers;
  private final long estimatedShuffleBytes;
  private final long estimatedBlockCount;
  private final double predictedNativeMs;
  private final double predictedPbrMs;
  private final double predictedRecordPbrMs;
  private final double remoteFraction;
  private final double cxlUtilization;
  private final double nearSlotFraction;
  private final double pbrConcurrencyPenalty;
  private final int expectedReadCount;
  private final double confidence;
  private final String reason;
  private final long estimatedParentReadBytes;
  private final long estimatedPbrParentReadBytes;
  private final int parentShuffleCount;
  private final double predictedParentReadMs;
  private final long estimatedCxlWorkingSetBytes;

  public ShufflePlan(
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
      long estimatedParentReadBytes,
      long estimatedPbrParentReadBytes,
      int parentShuffleCount,
      double predictedParentReadMs,
      long estimatedCxlWorkingSetBytes) {
    this.shuffleId = shuffleId;
    this.mode = mode == null ? "" : mode;
    this.numMaps = numMaps;
    this.numReducers = numReducers;
    this.estimatedShuffleBytes = estimatedShuffleBytes;
    this.estimatedBlockCount = estimatedBlockCount;
    this.predictedNativeMs = predictedNativeMs;
    this.predictedPbrMs = predictedPbrMs;
    this.predictedRecordPbrMs = predictedRecordPbrMs;
    this.remoteFraction = remoteFraction;
    this.cxlUtilization = cxlUtilization;
    this.nearSlotFraction = nearSlotFraction;
    this.pbrConcurrencyPenalty = pbrConcurrencyPenalty;
    this.expectedReadCount = expectedReadCount;
    this.confidence = confidence;
    this.reason = reason == null ? "" : reason;
    this.estimatedParentReadBytes = estimatedParentReadBytes;
    this.estimatedPbrParentReadBytes = estimatedPbrParentReadBytes;
    this.parentShuffleCount = parentShuffleCount;
    this.predictedParentReadMs = predictedParentReadMs;
    this.estimatedCxlWorkingSetBytes = estimatedCxlWorkingSetBytes;
  }

  public int shuffleId() { return shuffleId; }
  public String mode() { return mode; }
  public int numMaps() { return numMaps; }
  public int numReducers() { return numReducers; }
  public long estimatedShuffleBytes() { return estimatedShuffleBytes; }
  public long estimatedBlockCount() { return estimatedBlockCount; }
  public double predictedNativeMs() { return predictedNativeMs; }
  public double predictedPbrMs() { return predictedPbrMs; }
  public double predictedRecordPbrMs() { return predictedRecordPbrMs; }
  public double remoteFraction() { return remoteFraction; }
  public double cxlUtilization() { return cxlUtilization; }
  public double nearSlotFraction() { return nearSlotFraction; }
  public double pbrConcurrencyPenalty() { return pbrConcurrencyPenalty; }
  public int expectedReadCount() { return expectedReadCount; }
  public double confidence() { return confidence; }
  public String reason() { return reason; }
  public long estimatedParentReadBytes() { return estimatedParentReadBytes; }
  public long estimatedPbrParentReadBytes() { return estimatedPbrParentReadBytes; }
  public int parentShuffleCount() { return parentShuffleCount; }
  public double predictedParentReadMs() { return predictedParentReadMs; }
  public long estimatedCxlWorkingSetBytes() { return estimatedCxlWorkingSetBytes; }

  public String toJson() {
    return String.format(Locale.ROOT,
        "{\"event\":\"scache_shuffle_decision\",\"shuffleId\":%d,\"mode\":\"%s\"," +
            "\"maps\":%d,\"reducers\":%d,\"estimatedShuffleBytes\":%d," +
            "\"estimatedBlockCount\":%d,\"predictedNativeMs\":%.3f," +
            "\"predictedPbrMs\":%.3f,\"predictedRecordPbrMs\":%.3f," +
            "\"remoteFraction\":%.4f,\"cxlUtilization\":%.4f," +
            "\"nearSlotFraction\":%.4f,\"pbrConcurrencyPenalty\":%.4f," +
            "\"expectedReadCount\":%d,\"confidence\":%.4f," +
            "\"estimatedParentReadBytes\":%d,\"estimatedPbrParentReadBytes\":%d," +
            "\"parentShuffleCount\":%d,\"predictedParentReadMs\":%.3f," +
            "\"estimatedCxlWorkingSetBytes\":%d,\"reason\":\"%s\"}",
        shuffleId, escape(mode), numMaps, numReducers, estimatedShuffleBytes,
        estimatedBlockCount, predictedNativeMs, predictedPbrMs, predictedRecordPbrMs,
        remoteFraction, cxlUtilization, nearSlotFraction, pbrConcurrencyPenalty,
        expectedReadCount, confidence, estimatedParentReadBytes,
        estimatedPbrParentReadBytes, parentShuffleCount, predictedParentReadMs,
        estimatedCxlWorkingSetBytes, escape(reason));
  }

  private static String escape(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
