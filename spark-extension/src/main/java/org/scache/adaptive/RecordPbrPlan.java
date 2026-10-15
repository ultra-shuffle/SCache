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

/** Explainable, capability-gated native versus record-PBR decision. */
public final class RecordPbrPlan implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String mode;
  private final long estimatedBytes;
  private final long estimatedRecords;
  private final double predictedNativeMs;
  private final double predictedRecordPbrMs;
  private final double confidence;
  private final String reason;

  RecordPbrPlan(
      String mode,
      long estimatedBytes,
      long estimatedRecords,
      double predictedNativeMs,
      double predictedRecordPbrMs,
      double confidence,
      String reason) {
    this.mode = mode;
    this.estimatedBytes = estimatedBytes;
    this.estimatedRecords = estimatedRecords;
    this.predictedNativeMs = predictedNativeMs;
    this.predictedRecordPbrMs = predictedRecordPbrMs;
    this.confidence = confidence;
    this.reason = reason;
  }

  public String mode() { return mode; }
  public long estimatedBytes() { return estimatedBytes; }
  public long estimatedRecords() { return estimatedRecords; }
  public double predictedNativeMs() { return predictedNativeMs; }
  public double predictedRecordPbrMs() { return predictedRecordPbrMs; }
  public double confidence() { return confidence; }
  public String reason() { return reason; }

  public String toJson() {
    return String.format(Locale.ROOT,
        "{\"event\":\"scache_record_decision\",\"mode\":\"%s\"," +
            "\"estimatedBytes\":%d,\"estimatedRecords\":%d," +
            "\"predictedNativeMs\":%.3f,\"predictedRecordPbrMs\":%.3f," +
            "\"confidence\":%.4f,\"reason\":\"%s\"}",
        mode, estimatedBytes, estimatedRecords, predictedNativeMs,
        predictedRecordPbrMs, confidence, reason);
  }
}
