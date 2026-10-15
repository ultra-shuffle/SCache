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

/** Immutable facts supplied to the pure shuffle planner for one shuffle. */
public final class ShufflePlanningInput {
  private final int shuffleId;
  private final int numMaps;
  private final int numReducers;
  private final boolean hasAggregator;
  private final boolean hasKeyOrdering;
  private final boolean rangePartitioned;
  private final boolean hasRecordPbrCapability;
  private final boolean supportsDirectCompute;
  private final AdaptiveTelemetrySnapshot telemetry;
  private final ParentShuffleReadEstimate parentReads;

  private ShufflePlanningInput(Builder builder) {
    shuffleId = builder.shuffleId;
    numMaps = Math.max(0, builder.numMaps);
    numReducers = Math.max(0, builder.numReducers);
    hasAggregator = builder.hasAggregator;
    hasKeyOrdering = builder.hasKeyOrdering;
    rangePartitioned = builder.rangePartitioned;
    hasRecordPbrCapability = builder.hasRecordPbrCapability;
    supportsDirectCompute = builder.supportsDirectCompute;
    telemetry = builder.telemetry == null
        ? AdaptiveTelemetrySnapshot.EMPTY : builder.telemetry;
    parentReads = builder.parentReads == null
        ? ParentShuffleReadEstimate.EMPTY : builder.parentReads;
  }

  public static Builder builder(int shuffleId, int numMaps, int numReducers) {
    return new Builder(shuffleId, numMaps, numReducers);
  }

  public int shuffleId() { return shuffleId; }
  public int numMaps() { return numMaps; }
  public int numReducers() { return numReducers; }
  public boolean hasAggregator() { return hasAggregator; }
  public boolean hasKeyOrdering() { return hasKeyOrdering; }
  public boolean rangePartitioned() { return rangePartitioned; }
  public boolean hasRecordPbrCapability() { return hasRecordPbrCapability; }
  public boolean supportsDirectCompute() { return supportsDirectCompute; }
  public AdaptiveTelemetrySnapshot telemetry() { return telemetry; }
  public ParentShuffleReadEstimate parentReads() { return parentReads; }

  public static final class Builder {
    private final int shuffleId;
    private final int numMaps;
    private final int numReducers;
    private boolean hasAggregator;
    private boolean hasKeyOrdering;
    private boolean rangePartitioned;
    private boolean hasRecordPbrCapability;
    private boolean supportsDirectCompute;
    private AdaptiveTelemetrySnapshot telemetry = AdaptiveTelemetrySnapshot.EMPTY;
    private ParentShuffleReadEstimate parentReads = ParentShuffleReadEstimate.EMPTY;

    private Builder(int shuffleId, int numMaps, int numReducers) {
      this.shuffleId = shuffleId;
      this.numMaps = numMaps;
      this.numReducers = numReducers;
    }

    public Builder hasAggregator(boolean value) { hasAggregator = value; return this; }
    public Builder hasKeyOrdering(boolean value) { hasKeyOrdering = value; return this; }
    public Builder rangePartitioned(boolean value) { rangePartitioned = value; return this; }
    public Builder hasRecordPbrCapability(boolean value) {
      hasRecordPbrCapability = value; return this;
    }
    public Builder supportsDirectCompute(boolean value) {
      supportsDirectCompute = value; return this;
    }
    public Builder telemetry(AdaptiveTelemetrySnapshot value) { telemetry = value; return this; }
    public Builder parentReads(ParentShuffleReadEstimate value) {
      parentReads = value; return this;
    }

    public ShufflePlanningInput build() {
      return new ShufflePlanningInput(this);
    }
  }
}
