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

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Collections;
import java.util.List;

/**
 * Opt-in, on-demand JVM process metrics sampler.
 *
 * <p>The sampler creates no thread and registers no callback. When disabled, {@link #sample()}
 * returns a shared constant after one boolean branch. When enabled, repeated calls inside the
 * configured minimum interval return the same cached snapshot without polling MXBeans again.</p>
 */
public final class JvmProcessMetrics {
  private final boolean enabled;
  private final long minimumIntervalNs;
  private final RuntimeMXBean runtimeBean;
  private final MemoryMXBean memoryBean;
  private final ThreadMXBean threadBean;
  private final ClassLoadingMXBean classLoadingBean;
  private final java.lang.management.OperatingSystemMXBean operatingSystemBean;
  private final List<GarbageCollectorMXBean> garbageCollectors;
  private final List<BufferPoolMXBean> bufferPools;

  private volatile JvmProcessMetricsSnapshot cached = JvmProcessMetricsSnapshot.DISABLED;
  private volatile long cachedAtNs = Long.MIN_VALUE;

  public JvmProcessMetrics(TelemetryConfig config) {
    TelemetryConfig safeConfig = config == null ? TelemetryConfig.disabled() : config;
    minimumIntervalNs = millisToNanosSaturated(safeConfig.processMinSampleIntervalMs());
    boolean active = false;
    RuntimeMXBean runtime = null;
    MemoryMXBean memory = null;
    ThreadMXBean threads = null;
    ClassLoadingMXBean classes = null;
    java.lang.management.OperatingSystemMXBean operatingSystem = null;
    List<GarbageCollectorMXBean> collectors = Collections.emptyList();
    List<BufferPoolMXBean> pools = Collections.emptyList();
    if (safeConfig.processMetricsEnabled()) {
      try {
        runtime = ManagementFactory.getRuntimeMXBean();
        memory = ManagementFactory.getMemoryMXBean();
        threads = ManagementFactory.getThreadMXBean();
        classes = ManagementFactory.getClassLoadingMXBean();
        operatingSystem = ManagementFactory.getOperatingSystemMXBean();
        collectors = safeGarbageCollectors();
        pools = safeBufferPools();
        active = true;
      } catch (RuntimeException | LinkageError ignored) {
        // Metrics are observability only; an unsupported or restricted MXBean must not stop Spark.
      }
    }
    enabled = active;
    runtimeBean = runtime;
    memoryBean = memory;
    threadBean = threads;
    classLoadingBean = classes;
    operatingSystemBean = operatingSystem;
    garbageCollectors = collectors;
    bufferPools = pools;
  }

  public boolean enabled() {
    return enabled;
  }

  /** Returns a cached or newly collected process snapshot. This method never starts a thread. */
  public JvmProcessMetricsSnapshot sample() {
    if (!enabled) return JvmProcessMetricsSnapshot.DISABLED;
    long nowNs = System.nanoTime();
    JvmProcessMetricsSnapshot local = cached;
    if (withinInterval(nowNs, cachedAtNs, minimumIntervalNs)) return local;
    synchronized (this) {
      nowNs = System.nanoTime();
      local = cached;
      if (withinInterval(nowNs, cachedAtNs, minimumIntervalNs)) return local;
      JvmProcessMetricsSnapshot sampled;
      try {
        sampled = collect();
      } catch (RuntimeException | LinkageError ignored) {
        // Exporters must not be able to fail an application because a platform MXBean broke.
        sampled = JvmProcessMetricsSnapshot.DISABLED;
      }
      cached = sampled;
      cachedAtNs = nowNs;
      return sampled;
    }
  }

  /** Drops only the cached value; the next call samples synchronously. */
  public synchronized void invalidate() {
    cached = JvmProcessMetricsSnapshot.DISABLED;
    cachedAtNs = Long.MIN_VALUE;
  }

  private JvmProcessMetricsSnapshot collect() {
    long timestampMs = System.currentTimeMillis();
    long uptimeMs = unavailableLong();
    long processId = unavailableLong();
    int availableProcessors = unavailableInt();
    long processCpuTimeNs = unavailableLong();
    double processCpuLoad = unavailableDouble();
    double systemCpuLoad = unavailableDouble();
    double systemLoadAverage = unavailableDouble();
    long committedVirtualMemoryBytes = unavailableLong();
    long totalPhysicalMemoryBytes = unavailableLong();
    long freePhysicalMemoryBytes = unavailableLong();
    long totalSwapBytes = unavailableLong();
    long freeSwapBytes = unavailableLong();
    long openFileDescriptorCount = unavailableLong();
    long maxFileDescriptorCount = unavailableLong();

    try {
      uptimeMs = runtimeBean.getUptime();
      processId = parseProcessId(runtimeBean.getName());
    } catch (RuntimeException ignored) {
      // Unsupported or access-restricted fields remain -1.
    }
    try {
      availableProcessors = operatingSystemBean.getAvailableProcessors();
      systemLoadAverage = finiteOrUnavailable(operatingSystemBean.getSystemLoadAverage());
    } catch (RuntimeException ignored) {
      // Unsupported or access-restricted fields remain -1.
    }
    if (operatingSystemBean instanceof com.sun.management.OperatingSystemMXBean) {
      com.sun.management.OperatingSystemMXBean extended =
          (com.sun.management.OperatingSystemMXBean) operatingSystemBean;
      try {
        processCpuTimeNs = extended.getProcessCpuTime();
        processCpuLoad = rateOrUnavailable(extended.getProcessCpuLoad());
        systemCpuLoad = rateOrUnavailable(extended.getSystemCpuLoad());
        committedVirtualMemoryBytes = extended.getCommittedVirtualMemorySize();
        totalPhysicalMemoryBytes = extended.getTotalPhysicalMemorySize();
        freePhysicalMemoryBytes = extended.getFreePhysicalMemorySize();
        totalSwapBytes = extended.getTotalSwapSpaceSize();
        freeSwapBytes = extended.getFreeSwapSpaceSize();
      } catch (RuntimeException ignored) {
        // The basic MXBean measurements are still useful.
      }
    }
    if (operatingSystemBean instanceof com.sun.management.UnixOperatingSystemMXBean) {
      com.sun.management.UnixOperatingSystemMXBean unix =
          (com.sun.management.UnixOperatingSystemMXBean) operatingSystemBean;
      try {
        openFileDescriptorCount = unix.getOpenFileDescriptorCount();
        maxFileDescriptorCount = unix.getMaxFileDescriptorCount();
      } catch (RuntimeException ignored) {
        // Non-Unix or access-restricted runtimes report -1.
      }
    }

    MemoryUsage heap = safeHeapUsage();
    MemoryUsage nonHeap = safeNonHeapUsage();
    BufferTotals direct = bufferTotals("direct");
    BufferTotals mapped = bufferTotals("mapped");
    LongTotals gc = garbageCollectionTotals();

    int liveThreadCount = unavailableInt();
    int daemonThreadCount = unavailableInt();
    int peakThreadCount = unavailableInt();
    int loadedClassCount = unavailableInt();
    try {
      liveThreadCount = threadBean.getThreadCount();
      daemonThreadCount = threadBean.getDaemonThreadCount();
      peakThreadCount = threadBean.getPeakThreadCount();
      loadedClassCount = classLoadingBean.getLoadedClassCount();
    } catch (RuntimeException ignored) {
      // Unsupported or access-restricted fields remain -1.
    }

    return new JvmProcessMetricsSnapshot(
        true,
        timestampMs,
        uptimeMs,
        processId,
        availableProcessors,
        processCpuTimeNs,
        processCpuLoad,
        systemCpuLoad,
        systemLoadAverage,
        usageValue(heap, true),
        usageValue(heap, false),
        usageValue(nonHeap, true),
        usageValue(nonHeap, false),
        direct.count,
        direct.used,
        direct.capacity,
        mapped.count,
        mapped.used,
        mapped.capacity,
        gc.first,
        gc.second,
        liveThreadCount,
        daemonThreadCount,
        peakThreadCount,
        loadedClassCount,
        committedVirtualMemoryBytes,
        totalPhysicalMemoryBytes,
        freePhysicalMemoryBytes,
        totalSwapBytes,
        freeSwapBytes,
        openFileDescriptorCount,
        maxFileDescriptorCount);
  }

  private MemoryUsage safeHeapUsage() {
    try {
      return memoryBean.getHeapMemoryUsage();
    } catch (RuntimeException ignored) {
      return null;
    }
  }

  private MemoryUsage safeNonHeapUsage() {
    try {
      return memoryBean.getNonHeapMemoryUsage();
    } catch (RuntimeException ignored) {
      return null;
    }
  }

  private BufferTotals bufferTotals(String name) {
    for (BufferPoolMXBean pool : bufferPools) {
      if (name.equals(pool.getName())) {
        try {
          return new BufferTotals(pool.getCount(), pool.getMemoryUsed(), pool.getTotalCapacity());
        } catch (RuntimeException ignored) {
          return BufferTotals.UNAVAILABLE;
        }
      }
    }
    return BufferTotals.UNAVAILABLE;
  }

  private LongTotals garbageCollectionTotals() {
    long count = 0L;
    long timeMs = 0L;
    boolean countAvailable = false;
    boolean timeAvailable = false;
    for (GarbageCollectorMXBean collector : garbageCollectors) {
      try {
        long collectorCount = collector.getCollectionCount();
        long collectorTime = collector.getCollectionTime();
        if (collectorCount >= 0L) {
          count += collectorCount;
          countAvailable = true;
        }
        if (collectorTime >= 0L) {
          timeMs += collectorTime;
          timeAvailable = true;
        }
      } catch (RuntimeException ignored) {
        // Continue with the remaining collectors.
      }
    }
    return new LongTotals(countAvailable ? count : -1L, timeAvailable ? timeMs : -1L);
  }

  private static List<GarbageCollectorMXBean> safeGarbageCollectors() {
    try {
      return ManagementFactory.getGarbageCollectorMXBeans();
    } catch (RuntimeException | LinkageError ignored) {
      return Collections.emptyList();
    }
  }

  private static List<BufferPoolMXBean> safeBufferPools() {
    try {
      return ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
    } catch (RuntimeException | LinkageError ignored) {
      return Collections.emptyList();
    }
  }

  private static long usageValue(MemoryUsage usage, boolean used) {
    return usage == null ? -1L : (used ? usage.getUsed() : usage.getCommitted());
  }

  private static long parseProcessId(String runtimeName) {
    if (runtimeName == null) return -1L;
    int separator = runtimeName.indexOf('@');
    String candidate = separator < 0 ? runtimeName : runtimeName.substring(0, separator);
    try {
      return Long.parseLong(candidate);
    } catch (NumberFormatException ignored) {
      return -1L;
    }
  }

  private static boolean withinInterval(long nowNs, long previousNs, long intervalNs) {
    if (intervalNs <= 0L || previousNs == Long.MIN_VALUE) return false;
    long elapsed = nowNs - previousNs;
    return elapsed >= 0L && elapsed < intervalNs;
  }

  private static long millisToNanosSaturated(long milliseconds) {
    if (milliseconds <= 0L) return 0L;
    return milliseconds > Long.MAX_VALUE / 1_000_000L
        ? Long.MAX_VALUE : milliseconds * 1_000_000L;
  }

  private static double rateOrUnavailable(double value) {
    if (Double.isNaN(value) || Double.isInfinite(value) || value < 0.0) return -1.0;
    return Math.min(1.0, value);
  }

  private static double finiteOrUnavailable(double value) {
    return Double.isNaN(value) || Double.isInfinite(value) ? -1.0 : value;
  }

  private static long unavailableLong() { return -1L; }
  private static int unavailableInt() { return -1; }
  private static double unavailableDouble() { return -1.0; }

  private static final class BufferTotals {
    private static final BufferTotals UNAVAILABLE = new BufferTotals(-1L, -1L, -1L);
    private final long count;
    private final long used;
    private final long capacity;

    private BufferTotals(long count, long used, long capacity) {
      this.count = count;
      this.used = used;
      this.capacity = capacity;
    }
  }

  private static final class LongTotals {
    private final long first;
    private final long second;

    private LongTotals(long first, long second) {
      this.first = first;
      this.second = second;
    }
  }
}
