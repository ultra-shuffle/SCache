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

/**
 * Mutable, single-task counters for SCache shuffle operations.
 *
 * <p>The object has no Spark dependency and performs no timing itself. Data-plane adapters should
 * create it only when detailed profiling is enabled and should avoid {@code nanoTime()} calls on
 * the disabled path.</p>
 */
public final class ScacheProfilingMetrics {
  public final boolean enabled;

  public long poolAllocCount;
  public long poolAllocTimeNs;
  public long poolAllocBytes;
  public long poolCommitCount;
  public long poolCommitTimeNs;
  public long poolCommitAsyncCount;
  public long poolCommitAwaitTimeNs;
  public long ipcWriteTimeNs;
  public long ipcWriteBytes;
  public long ipcReadBytes;
  public long ipcFileReadBytes;
  public long ipcPoolReadBytes;
  public long directCxlReadBytes;
  public long directCxlReadTimeNs;
  public long byteArrayReadBytes;
  public long fetchCount;
  public long fetchTimeNs;
  public long fetchIpcHitCount;
  public long fetchByteArrayCount;
  public long fetchMissCount;
  public long fetchRetryCount;
  public long fetchRetryTimeNs;
  public long batchLookupCount;
  public long cxlReservationCount;
  public long cxlReservationWaitNs;
  public long byteArrayAllocCount;
  public long byteArrayAllocBytes;
  public long bufferCopyCount;
  public long bufferCopyBytes;
  public long poolChannelOpenCount;
  public long serTimeNs;
  public long serBytes;
  public long serRecords;
  public long spillCount;
  public long spillTimeNs;
  public long spillBytes;
  public long spillMergeTimeNs;
  public long spillMergeBytes;

  public ScacheProfilingMetrics(boolean enabled) {
    this.enabled = enabled;
  }

  /** Produces detailed JSON only when an enabled caller explicitly requests it. */
  public String toJson(String side, int shuffleId, long partitionId, long taskAttemptId) {
    StringBuilder json = new StringBuilder(640);
    json.append("{\"event\":\"scache_profiling\"");
    json.append(",\"side\":\"").append(escape(side)).append('"');
    json.append(",\"shuffleId\":").append(shuffleId);
    json.append(",\"partitionId\":").append(partitionId);
    json.append(",\"taskAttemptId\":").append(taskAttemptId);
    json.append(",\"timestampMs\":").append(System.currentTimeMillis());
    json.append(",\"poolAlloc\":{\"count\":").append(poolAllocCount)
        .append(",\"timeNs\":").append(poolAllocTimeNs)
        .append(",\"bytes\":").append(poolAllocBytes).append('}');
    json.append(",\"poolCommit\":{\"count\":").append(poolCommitCount)
        .append(",\"timeNs\":").append(poolCommitTimeNs)
        .append(",\"asyncCount\":").append(poolCommitAsyncCount)
        .append(",\"awaitTimeNs\":").append(poolCommitAwaitTimeNs).append('}');
    json.append(",\"ipcWrite\":{\"timeNs\":").append(ipcWriteTimeNs)
        .append(",\"bytes\":").append(ipcWriteBytes).append('}');
    json.append(",\"ipcReadBytes\":").append(ipcReadBytes);
    json.append(",\"readPathBytes\":{\"ipcFile\":").append(ipcFileReadBytes)
        .append(",\"ipcPool\":").append(ipcPoolReadBytes)
        .append(",\"directCxl\":").append(directCxlReadBytes)
        .append(",\"directCxlTimeNs\":").append(directCxlReadTimeNs)
        .append(",\"byteArray\":").append(byteArrayReadBytes).append('}');
    json.append(",\"fetch\":{\"count\":").append(fetchCount)
        .append(",\"timeNs\":").append(fetchTimeNs)
        .append(",\"ipcHitCount\":").append(fetchIpcHitCount)
        .append(",\"byteArrayCount\":").append(fetchByteArrayCount)
        .append(",\"missCount\":").append(fetchMissCount)
        .append(",\"retryCount\":").append(fetchRetryCount)
        .append(",\"retryTimeNs\":").append(fetchRetryTimeNs)
        .append(",\"batchLookupCount\":").append(batchLookupCount)
        .append(",\"cxlReservationCount\":").append(cxlReservationCount)
        .append(",\"cxlReservationWaitNs\":").append(cxlReservationWaitNs).append('}');
    json.append(",\"gcPressure\":{\"byteArrayAllocCount\":").append(byteArrayAllocCount)
        .append(",\"byteArrayAllocBytes\":").append(byteArrayAllocBytes)
        .append(",\"bufferCopyCount\":").append(bufferCopyCount)
        .append(",\"bufferCopyBytes\":").append(bufferCopyBytes)
        .append(",\"poolChannelOpenCount\":").append(poolChannelOpenCount).append('}');
    json.append(",\"serialization\":{\"timeNs\":").append(serTimeNs)
        .append(",\"bytes\":").append(serBytes)
        .append(",\"records\":").append(serRecords).append('}');
    json.append(",\"spill\":{\"count\":").append(spillCount)
        .append(",\"timeNs\":").append(spillTimeNs)
        .append(",\"bytes\":").append(spillBytes)
        .append(",\"mergeTimeNs\":").append(spillMergeTimeNs)
        .append(",\"mergeBytes\":").append(spillMergeBytes).append("}}");
    return json.toString();
  }

  private static String escape(String value) {
    if (value == null) return "";
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
