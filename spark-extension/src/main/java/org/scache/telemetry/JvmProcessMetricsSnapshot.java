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

/** Immutable, point-in-time JVM and operating-system process measurements. */
public final class JvmProcessMetricsSnapshot {
  public static final JvmProcessMetricsSnapshot DISABLED = new JvmProcessMetricsSnapshot(
      false, 0L, -1L, -1L, -1,
      -1L, -1.0, -1.0, -1.0,
      -1L, -1L, -1L, -1L,
      -1L, -1L, -1L,
      -1L, -1L, -1L,
      -1L, -1L,
      -1, -1, -1, -1,
      -1L, -1L, -1L, -1L, -1L,
      -1L, -1L);

  private final boolean sampled;
  private final long timestampMs;
  private final long uptimeMs;
  private final long processId;
  private final int availableProcessors;
  private final long processCpuTimeNs;
  private final double processCpuLoad;
  private final double systemCpuLoad;
  private final double systemLoadAverage;
  private final long heapUsedBytes;
  private final long heapCommittedBytes;
  private final long nonHeapUsedBytes;
  private final long nonHeapCommittedBytes;
  private final long directBufferCount;
  private final long directBufferUsedBytes;
  private final long directBufferCapacityBytes;
  private final long mappedBufferCount;
  private final long mappedBufferUsedBytes;
  private final long mappedBufferCapacityBytes;
  private final long gcCollectionCount;
  private final long gcCollectionTimeMs;
  private final int liveThreadCount;
  private final int daemonThreadCount;
  private final int peakThreadCount;
  private final int loadedClassCount;
  private final long committedVirtualMemoryBytes;
  private final long totalPhysicalMemoryBytes;
  private final long freePhysicalMemoryBytes;
  private final long totalSwapBytes;
  private final long freeSwapBytes;
  private final long openFileDescriptorCount;
  private final long maxFileDescriptorCount;

  JvmProcessMetricsSnapshot(
      boolean sampled,
      long timestampMs,
      long uptimeMs,
      long processId,
      int availableProcessors,
      long processCpuTimeNs,
      double processCpuLoad,
      double systemCpuLoad,
      double systemLoadAverage,
      long heapUsedBytes,
      long heapCommittedBytes,
      long nonHeapUsedBytes,
      long nonHeapCommittedBytes,
      long directBufferCount,
      long directBufferUsedBytes,
      long directBufferCapacityBytes,
      long mappedBufferCount,
      long mappedBufferUsedBytes,
      long mappedBufferCapacityBytes,
      long gcCollectionCount,
      long gcCollectionTimeMs,
      int liveThreadCount,
      int daemonThreadCount,
      int peakThreadCount,
      int loadedClassCount,
      long committedVirtualMemoryBytes,
      long totalPhysicalMemoryBytes,
      long freePhysicalMemoryBytes,
      long totalSwapBytes,
      long freeSwapBytes,
      long openFileDescriptorCount,
      long maxFileDescriptorCount) {
    this.sampled = sampled;
    this.timestampMs = timestampMs;
    this.uptimeMs = uptimeMs;
    this.processId = processId;
    this.availableProcessors = availableProcessors;
    this.processCpuTimeNs = processCpuTimeNs;
    this.processCpuLoad = processCpuLoad;
    this.systemCpuLoad = systemCpuLoad;
    this.systemLoadAverage = systemLoadAverage;
    this.heapUsedBytes = heapUsedBytes;
    this.heapCommittedBytes = heapCommittedBytes;
    this.nonHeapUsedBytes = nonHeapUsedBytes;
    this.nonHeapCommittedBytes = nonHeapCommittedBytes;
    this.directBufferCount = directBufferCount;
    this.directBufferUsedBytes = directBufferUsedBytes;
    this.directBufferCapacityBytes = directBufferCapacityBytes;
    this.mappedBufferCount = mappedBufferCount;
    this.mappedBufferUsedBytes = mappedBufferUsedBytes;
    this.mappedBufferCapacityBytes = mappedBufferCapacityBytes;
    this.gcCollectionCount = gcCollectionCount;
    this.gcCollectionTimeMs = gcCollectionTimeMs;
    this.liveThreadCount = liveThreadCount;
    this.daemonThreadCount = daemonThreadCount;
    this.peakThreadCount = peakThreadCount;
    this.loadedClassCount = loadedClassCount;
    this.committedVirtualMemoryBytes = committedVirtualMemoryBytes;
    this.totalPhysicalMemoryBytes = totalPhysicalMemoryBytes;
    this.freePhysicalMemoryBytes = freePhysicalMemoryBytes;
    this.totalSwapBytes = totalSwapBytes;
    this.freeSwapBytes = freeSwapBytes;
    this.openFileDescriptorCount = openFileDescriptorCount;
    this.maxFileDescriptorCount = maxFileDescriptorCount;
  }

  public boolean sampled() { return sampled; }
  public long timestampMs() { return timestampMs; }
  public long uptimeMs() { return uptimeMs; }
  public long processId() { return processId; }
  public int availableProcessors() { return availableProcessors; }
  public long processCpuTimeNs() { return processCpuTimeNs; }
  public double processCpuLoad() { return processCpuLoad; }
  public double systemCpuLoad() { return systemCpuLoad; }
  public double systemLoadAverage() { return systemLoadAverage; }
  public long heapUsedBytes() { return heapUsedBytes; }
  public long heapCommittedBytes() { return heapCommittedBytes; }
  public long nonHeapUsedBytes() { return nonHeapUsedBytes; }
  public long nonHeapCommittedBytes() { return nonHeapCommittedBytes; }
  public long directBufferCount() { return directBufferCount; }
  public long directBufferUsedBytes() { return directBufferUsedBytes; }
  public long directBufferCapacityBytes() { return directBufferCapacityBytes; }
  public long mappedBufferCount() { return mappedBufferCount; }
  public long mappedBufferUsedBytes() { return mappedBufferUsedBytes; }
  public long mappedBufferCapacityBytes() { return mappedBufferCapacityBytes; }
  public long gcCollectionCount() { return gcCollectionCount; }
  public long gcCollectionTimeMs() { return gcCollectionTimeMs; }
  public int liveThreadCount() { return liveThreadCount; }
  public int daemonThreadCount() { return daemonThreadCount; }
  public int peakThreadCount() { return peakThreadCount; }
  public int loadedClassCount() { return loadedClassCount; }
  public long committedVirtualMemoryBytes() { return committedVirtualMemoryBytes; }
  public long totalPhysicalMemoryBytes() { return totalPhysicalMemoryBytes; }
  public long freePhysicalMemoryBytes() { return freePhysicalMemoryBytes; }
  public long totalSwapBytes() { return totalSwapBytes; }
  public long freeSwapBytes() { return freeSwapBytes; }
  public long openFileDescriptorCount() { return openFileDescriptorCount; }
  public long maxFileDescriptorCount() { return maxFileDescriptorCount; }

  /** Compact JSON for logs or a plugin-owned metrics sink; it does not depend on a JSON library. */
  public String toJson() {
    StringBuilder value = new StringBuilder(768);
    value.append('{');
    append(value, "sampled", sampled);
    append(value, "timestampMs", timestampMs);
    append(value, "uptimeMs", uptimeMs);
    append(value, "processId", processId);
    append(value, "availableProcessors", availableProcessors);
    append(value, "processCpuTimeNs", processCpuTimeNs);
    append(value, "processCpuLoad", processCpuLoad);
    append(value, "systemCpuLoad", systemCpuLoad);
    append(value, "systemLoadAverage", systemLoadAverage);
    append(value, "heapUsedBytes", heapUsedBytes);
    append(value, "heapCommittedBytes", heapCommittedBytes);
    append(value, "nonHeapUsedBytes", nonHeapUsedBytes);
    append(value, "nonHeapCommittedBytes", nonHeapCommittedBytes);
    append(value, "directBufferCount", directBufferCount);
    append(value, "directBufferUsedBytes", directBufferUsedBytes);
    append(value, "directBufferCapacityBytes", directBufferCapacityBytes);
    append(value, "mappedBufferCount", mappedBufferCount);
    append(value, "mappedBufferUsedBytes", mappedBufferUsedBytes);
    append(value, "mappedBufferCapacityBytes", mappedBufferCapacityBytes);
    append(value, "gcCollectionCount", gcCollectionCount);
    append(value, "gcCollectionTimeMs", gcCollectionTimeMs);
    append(value, "liveThreadCount", liveThreadCount);
    append(value, "daemonThreadCount", daemonThreadCount);
    append(value, "peakThreadCount", peakThreadCount);
    append(value, "loadedClassCount", loadedClassCount);
    append(value, "committedVirtualMemoryBytes", committedVirtualMemoryBytes);
    append(value, "totalPhysicalMemoryBytes", totalPhysicalMemoryBytes);
    append(value, "freePhysicalMemoryBytes", freePhysicalMemoryBytes);
    append(value, "totalSwapBytes", totalSwapBytes);
    append(value, "freeSwapBytes", freeSwapBytes);
    append(value, "openFileDescriptorCount", openFileDescriptorCount);
    append(value, "maxFileDescriptorCount", maxFileDescriptorCount);
    value.append('}');
    return value.toString();
  }

  private static void append(StringBuilder value, String name, boolean field) {
    prefix(value, name).append(field);
  }

  private static void append(StringBuilder value, String name, long field) {
    prefix(value, name).append(field);
  }

  private static void append(StringBuilder value, String name, double field) {
    prefix(value, name).append(field);
  }

  private static StringBuilder prefix(StringBuilder value, String name) {
    if (value.length() > 1) value.append(',');
    return value.append('"').append(name).append("\":");
  }
}
