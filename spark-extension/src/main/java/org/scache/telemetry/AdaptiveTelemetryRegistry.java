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

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Spark-independent EWMA telemetry model for adaptive shuffle experiments.
 *
 * <p>The registry is deliberately instance-scoped: an SCache daemon or Spark plugin can keep one
 * registry per application/profile without sharing static state across class loaders. No
 * scheduler listener or shuffle hook is installed here. Updates and persistence happen only when
 * a caller invokes an observation or flush method.</p>
 */
public final class AdaptiveTelemetryRegistry implements AutoCloseable {
  public static final String NATIVE = "native";
  public static final String PBR = "pbr";
  public static final String RECORD_PBR = "record-pbr";

  private static final String CXL_DEVICE_SIGNATURE = "__cxl_device__";
  private static final int CURRENT_PERSISTED_FIELDS = 26;

  private final TelemetryConfig config;
  private final ConcurrentHashMap<String, MutableTelemetry> telemetry = new ConcurrentHashMap<>();
  private final Object loadLock = new Object();
  private final Object persistenceLock = new Object();

  private volatile boolean loadAttempted;
  private volatile int loadedEntries;
  private volatile String lastPersistenceError;

  public AdaptiveTelemetryRegistry(TelemetryConfig config) {
    this.config = config == null ? TelemetryConfig.disabled() : config;
  }

  /** Produces the representation/operator signature used by the original Spark-side model. */
  public static String signature(
      boolean hasAggregator,
      boolean hasKeyOrdering,
      boolean rangePartitioned,
      boolean recordCapable,
      boolean directCompute) {
    return (hasAggregator ? "agg" : "noagg") + ':'
        + (hasKeyOrdering ? "ordered" : "unordered") + ':'
        + (rangePartitioned ? "range" : "hash") + ':'
        + (recordCapable ? "record" : "block") + ':'
        + (directCompute ? "direct" : "materialize");
  }

  /** Adds power-of-four shape buckets so very different stage sizes do not share one EWMA. */
  public static String signature(
      boolean hasAggregator,
      boolean hasKeyOrdering,
      boolean rangePartitioned,
      boolean recordCapable,
      boolean directCompute,
      int numMaps,
      int numReducers,
      int parallelism) {
    return signature(hasAggregator, hasKeyOrdering, rangePartitioned, recordCapable, directCompute)
        + ":maps=" + shapeBucket(numMaps)
        + ":reducers=" + shapeBucket(numReducers)
        + ":parallelism=" + shapeBucket(parallelism);
  }

  /** Records one successful map's output size without timing the caller's hot path. */
  public void observeMap(String signature, long bytes, long records) {
    if (!updatesEnabled() || bytes <= 0L) return;
    ensureLoaded();
    MutableTelemetry state = telemetry.computeIfAbsent(key(signature), ignored ->
        new MutableTelemetry());
    long samples;
    synchronized (state) {
      state.samples++;
      samples = state.samples;
      state.bytesPerMap = ewma(state.bytesPerMap, bytes, config.ewmaAlpha());
      if (records > 0L) {
        state.recordsPerMap = ewma(state.recordsPerMap, records, config.ewmaAlpha());
      }
    }
    maybeAutoPersist(samples);
  }

  /**
   * Compatibility-shaped overload for existing Spark instrumentation. Timing arguments are
   * intentionally not sampled here because the adaptive model only consumed bytes and records.
   */
  public void observeMap(
      String signature,
      String mode,
      long bytes,
      long records,
      long writeTimeNs,
      long ipcBytes,
      long ipcTimeNs) {
    observeMap(signature, bytes, records);
  }

  public void observeStage(
      String signature, String mode, long shuffleBytes, long ipcBytes, long elapsedNs) {
    observeStage(signature, mode, shuffleBytes, ipcBytes, elapsedNs, 0L);
  }

  /** Records aggregate throughput once all successful map tasks in a stage have completed. */
  public void observeStage(
      String signature,
      String mode,
      long shuffleBytes,
      long ipcBytes,
      long elapsedNs,
      long blocks) {
    if (!updatesEnabled() || shuffleBytes <= 0L || elapsedNs <= 0L) return;
    String normalizedMode = normalizeMode(mode);
    if (!NATIVE.equals(normalizedMode)
        && !PBR.equals(normalizedMode)
        && !RECORD_PBR.equals(normalizedMode)) return;

    ensureLoaded();
    MutableTelemetry state = telemetry.computeIfAbsent(key(signature), ignored ->
        new MutableTelemetry());
    long measuredBytes = (PBR.equals(normalizedMode) || RECORD_PBR.equals(normalizedMode))
        && ipcBytes > 0L ? ipcBytes : shuffleBytes;
    double aggregateBandwidth = measuredBytes * 1_000_000_000.0 / elapsedNs;
    synchronized (state) {
      if (NATIVE.equals(normalizedMode)) {
        state.nativeWriteBandwidth = ewma(
            state.nativeWriteBandwidth, aggregateBandwidth, config.ewmaAlpha());
        state.nativeStageBytes = shuffleBytes;
        double elapsedSeconds = elapsedNs / 1_000_000_000.0;
        state.nativeCostSamples++;
        state.nativeCostSumBytes += shuffleBytes;
        state.nativeCostSumSeconds += elapsedSeconds;
        state.nativeCostSumBytesSquared += (double) shuffleBytes * shuffleBytes;
        state.nativeCostSumByteSeconds += shuffleBytes * elapsedSeconds;
      } else if (PBR.equals(normalizedMode)) {
        state.pbrWriteBandwidth = ewma(
            state.pbrWriteBandwidth, aggregateBandwidth, config.ewmaAlpha());
        state.pbrStageBytes = measuredBytes;
        state.pbrResidualSeconds = ewma(
            state.pbrResidualSeconds,
            stageResidualSeconds(normalizedMode, measuredBytes, elapsedNs, blocks,
                state.pbrBlockCostMs),
            config.ewmaAlpha());
      } else {
        state.recordPbrWriteBandwidth = ewma(
            state.recordPbrWriteBandwidth, aggregateBandwidth, config.ewmaAlpha());
        state.recordPbrStageBytes = measuredBytes;
        state.recordPbrResidualSeconds = ewma(
            state.recordPbrResidualSeconds,
            stageResidualSeconds(normalizedMode, measuredBytes, elapsedNs, blocks,
                state.recordPbrBlockCostMs),
            config.ewmaAlpha());
      }
    }
    // Stage-completion persistence is opt-in; the default never performs synchronous file I/O.
    if (config.autoPersistEverySamples() > 0) flush();
  }

  /** Records reduce-side costs already measured by the caller. */
  public void observeRead(
      String signature,
      String mode,
      long readBytes,
      long blocks,
      long fetchTimeNs,
      long reservationWaitNs,
      long records,
      long decodeNs,
      long materializeNs,
      long scanWaitNs) {
    if (!updatesEnabled() || readBytes <= 0L || blocks <= 0L) return;
    String normalizedMode = normalizeMode(mode);
    if (!PBR.equals(normalizedMode) && !RECORD_PBR.equals(normalizedMode)) return;

    long measuredReadNs = scanWaitNs > 0L ? scanWaitNs : fetchTimeNs;
    double observedBandwidth = measuredReadNs > 0L
        ? readBytes * 1_000_000_000.0 / measuredReadNs : 0.0;
    double residualNs = Math.max(reservationWaitNs, Math.max(0L, fetchTimeNs));
    double blockCostMs = residualNs / blocks / 1_000_000.0;

    ensureLoaded();
    MutableTelemetry state = telemetry.computeIfAbsent(key(signature), ignored ->
        new MutableTelemetry());
    synchronized (state) {
      if (PBR.equals(normalizedMode)) {
        state.pbrBlockCostMs = ewma(
            state.pbrBlockCostMs, blockCostMs, config.ewmaAlpha());
        state.pbrReadBandwidth = ewma(
            state.pbrReadBandwidth, observedBandwidth, config.ewmaAlpha());
        state.pbrIdleReadBandwidth = Math.max(
            state.pbrIdleReadBandwidth, observedBandwidth);
        state.pbrContentionPenalty = contentionPenalty(
            state.pbrIdleReadBandwidth, state.pbrReadBandwidth);
      } else {
        state.recordPbrBlockCostMs = ewma(
            state.recordPbrBlockCostMs, blockCostMs, config.ewmaAlpha());
        state.recordPbrReadBandwidth = ewma(
            state.recordPbrReadBandwidth, observedBandwidth, config.ewmaAlpha());
        state.recordPbrIdleReadBandwidth = Math.max(
            state.recordPbrIdleReadBandwidth, observedBandwidth);
        state.recordPbrContentionPenalty = contentionPenalty(
            state.recordPbrIdleReadBandwidth, state.recordPbrReadBandwidth);
        if (records > 0L) {
          state.recordDecodeNsPerRecord = ewma(
              state.recordDecodeNsPerRecord, decodeNs / (double) records, config.ewmaAlpha());
          state.recordMaterializeNsPerRecord = ewma(
              state.recordMaterializeNsPerRecord,
              materializeNs / (double) records,
              config.ewmaAlpha());
        }
      }
    }
  }

  /** Records end-to-end effective bandwidth for the shared CXL path at stage granularity. */
  public void observeCxlReadStage(long readBytes, long elapsedNs) {
    if (!updatesEnabled() || readBytes <= 0L || elapsedNs <= 0L) return;
    double observedBandwidth = readBytes * 1_000_000_000.0 / elapsedNs;
    ensureLoaded();
    MutableTelemetry state = telemetry.computeIfAbsent(key(CXL_DEVICE_SIGNATURE), ignored ->
        new MutableTelemetry());
    long samples;
    synchronized (state) {
      state.samples++;
      samples = state.samples;
      state.pbrReadBandwidth = ewma(
          state.pbrReadBandwidth, observedBandwidth, config.ewmaAlpha());
      state.pbrIdleReadBandwidth = Math.max(
          state.pbrIdleReadBandwidth, observedBandwidth);
      state.pbrContentionPenalty = contentionPenalty(
          state.pbrIdleReadBandwidth, state.pbrReadBandwidth);
    }
    maybeAutoPersist(samples);
  }

  public AdaptiveTelemetrySnapshot snapshot(String signature) {
    if (!config.adaptiveEnabled()) return AdaptiveTelemetrySnapshot.EMPTY;
    ensureLoaded();
    MutableTelemetry state = telemetry.get(key(signature));
    MutableTelemetry device = telemetry.get(key(CXL_DEVICE_SIGNATURE));
    if (state == null && device == null) return AdaptiveTelemetrySnapshot.EMPTY;

    double deviceReadBandwidth = 0.0;
    double deviceIdleReadBandwidth = 0.0;
    double deviceContentionPenalty = 1.0;
    if (device != null) {
      synchronized (device) {
        deviceReadBandwidth = device.pbrReadBandwidth;
        deviceIdleReadBandwidth = device.pbrIdleReadBandwidth;
        deviceContentionPenalty = Math.max(1.0, device.pbrContentionPenalty);
      }
    }
    if (state == null) state = new MutableTelemetry();
    synchronized (state) {
      double confidence = Math.min(
          1.0, state.samples / (double) config.fullConfidenceSamples());
      return new AdaptiveTelemetrySnapshot(
          state.samples,
          state.bytesPerMap,
          state.recordsPerMap,
          state.nativeWriteBandwidth,
          state.pbrWriteBandwidth,
          state.recordPbrWriteBandwidth,
          state.nativeStageBytes,
          state.pbrStageBytes,
          state.recordPbrStageBytes,
          state.nativeCostSamples,
          state.nativeCostSumBytes,
          state.nativeCostSumSeconds,
          state.nativeCostSumBytesSquared,
          state.nativeCostSumByteSeconds,
          state.pbrBlockCostMs,
          state.recordPbrBlockCostMs,
          state.recordDecodeNsPerRecord,
          state.recordMaterializeNsPerRecord,
          mergeDeviceBandwidth(state.pbrReadBandwidth, deviceReadBandwidth),
          mergeDeviceBandwidth(state.recordPbrReadBandwidth, deviceReadBandwidth),
          Math.max(state.pbrIdleReadBandwidth, deviceIdleReadBandwidth),
          Math.max(state.recordPbrIdleReadBandwidth, deviceIdleReadBandwidth),
          Math.max(deviceContentionPenalty, Math.max(1.0, state.pbrContentionPenalty)),
          Math.max(deviceContentionPenalty,
              Math.max(1.0, state.recordPbrContentionPenalty)),
          state.pbrResidualSeconds,
          state.recordPbrResidualSeconds,
          confidence);
    }
  }

  /**
   * Atomically writes the current model. Persistence is best effort and never throws into the
   * application; callers can inspect {@link #lastPersistenceError()} or the boolean result.
   */
  public boolean flush() {
    if (!updatesEnabled() || config.persistenceFile().isEmpty()) return false;
    ensureLoaded();
    synchronized (persistenceLock) {
      Path temporary = null;
      try {
        Path path = persistencePath();
        Path parent = path.getParent();
        if (parent == null) return false;
        Properties properties = snapshotProperties();
        Files.createDirectories(parent);
        temporary = Files.createTempFile(parent, path.getFileName().toString(), ".tmp");
        try (OutputStream output = Files.newOutputStream(temporary)) {
          properties.store(output, "UltraShuffle adaptive telemetry");
        }
        try {
          Files.move(temporary, path, StandardCopyOption.ATOMIC_MOVE,
              StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException ignored) {
          Files.move(temporary, path, StandardCopyOption.REPLACE_EXISTING);
        }
        temporary = null;
        lastPersistenceError = null;
        return true;
      } catch (Exception error) {
        lastPersistenceError = error.getClass().getSimpleName() + ": " + error.getMessage();
        return false;
      } finally {
        if (temporary != null) {
          try {
            Files.deleteIfExists(temporary);
          } catch (Exception ignored) {
            // It is a telemetry-owned temporary file; a stale file is harmless.
          }
        }
      }
    }
  }

  public int loadedEntries() {
    return loadedEntries;
  }

  /**
   * Explicitly loads a configured warm-start file on the calling thread.
   *
   * <p>A Spark adapter should call this during driver/plugin initialization, before installing
   * scheduler observation hooks, so the first planning call never performs file I/O.</p>
   */
  public int preload() {
    if (!config.adaptiveEnabled() || config.persistenceFile().isEmpty()) return 0;
    ensureLoaded();
    return loadedEntries;
  }

  public String lastPersistenceError() {
    return lastPersistenceError;
  }

  @Override
  public void close() {
    flush();
  }

  private boolean updatesEnabled() {
    return config.adaptiveEnabled() && !config.adaptiveReadOnly();
  }

  private String key(String signature) {
    return config.profile() + '|' + (signature == null ? "" : signature);
  }

  private Path persistencePath() {
    return Paths.get(config.persistenceFile()).toAbsolutePath().normalize();
  }

  private void ensureLoaded() {
    if (loadAttempted) return;
    synchronized (loadLock) {
      if (loadAttempted) return;
      int count = 0;
      if (!config.persistenceFile().isEmpty()) {
        Path path = persistencePath();
        if (Files.isRegularFile(path)) {
          Properties properties = new Properties();
          try (InputStream input = Files.newInputStream(path)) {
            properties.load(input);
            for (String encodedKey : properties.stringPropertyNames()) {
              try {
                String logicalKey = new String(
                    Base64.getUrlDecoder().decode(encodedKey), StandardCharsets.UTF_8);
                MutableTelemetry decoded = decode(properties.getProperty(encodedKey));
                if (decoded != null) {
                  telemetry.put(logicalKey, decoded);
                  count++;
                }
              } catch (RuntimeException ignored) {
                // A stale or malformed entry is a cold start for that signature only.
              }
            }
            lastPersistenceError = null;
          } catch (Exception error) {
            lastPersistenceError = error.getClass().getSimpleName() + ": " + error.getMessage();
          }
        }
      }
      loadedEntries = count;
      loadAttempted = true;
    }
  }

  private Properties snapshotProperties() {
    Properties properties = new Properties();
    for (Map.Entry<String, MutableTelemetry> entry : telemetry.entrySet()) {
      MutableTelemetry state = entry.getValue();
      String value;
      synchronized (state) {
        value = encode(state);
      }
      String encodedKey = Base64.getUrlEncoder().withoutPadding().encodeToString(
          entry.getKey().getBytes(StandardCharsets.UTF_8));
      properties.setProperty(encodedKey, value);
    }
    return properties;
  }

  private static String encode(MutableTelemetry state) {
    StringBuilder value = new StringBuilder(384);
    append(value, state.samples);
    append(value, state.bytesPerMap);
    append(value, state.recordsPerMap);
    append(value, state.nativeWriteBandwidth);
    append(value, state.pbrWriteBandwidth);
    append(value, state.recordPbrWriteBandwidth);
    append(value, state.nativeStageBytes);
    append(value, state.pbrStageBytes);
    append(value, state.recordPbrStageBytes);
    append(value, state.nativeCostSamples);
    append(value, state.nativeCostSumBytes);
    append(value, state.nativeCostSumSeconds);
    append(value, state.nativeCostSumBytesSquared);
    append(value, state.nativeCostSumByteSeconds);
    append(value, state.pbrBlockCostMs);
    append(value, state.recordPbrBlockCostMs);
    append(value, state.recordDecodeNsPerRecord);
    append(value, state.recordMaterializeNsPerRecord);
    append(value, state.pbrReadBandwidth);
    append(value, state.recordPbrReadBandwidth);
    append(value, state.pbrIdleReadBandwidth);
    append(value, state.recordPbrIdleReadBandwidth);
    append(value, Math.max(1.0, state.pbrContentionPenalty));
    append(value, Math.max(1.0, state.recordPbrContentionPenalty));
    append(value, state.pbrResidualSeconds);
    append(value, state.recordPbrResidualSeconds);
    return value.substring(1);
  }

  private static MutableTelemetry decode(String encoded) {
    if (encoded == null) return null;
    String[] values = encoded.split(",", -1);
    if (values.length != 5 && values.length != 7 && values.length != 9
        && values.length != 14 && values.length != 24
        && values.length != CURRENT_PERSISTED_FIELDS) return null;

    MutableTelemetry state = new MutableTelemetry();
    state.samples = nonnegativeLong(values[0]);
    state.bytesPerMap = nonnegativeDouble(values[1]);
    state.recordsPerMap = nonnegativeDouble(values[2]);
    state.nativeWriteBandwidth = nonnegativeDouble(values[3]);
    state.pbrWriteBandwidth = nonnegativeDouble(values[4]);
    if (values.length == 7) {
      state.nativeStageBytes = nonnegativeLong(values[5]);
      state.pbrStageBytes = nonnegativeLong(values[6]);
    } else if (values.length >= 9) {
      state.recordPbrWriteBandwidth = nonnegativeDouble(values[5]);
      state.nativeStageBytes = nonnegativeLong(values[6]);
      state.pbrStageBytes = nonnegativeLong(values[7]);
      state.recordPbrStageBytes = nonnegativeLong(values[8]);
      if (values.length >= 14) {
        state.nativeCostSamples = nonnegativeLong(values[9]);
        state.nativeCostSumBytes = nonnegativeDouble(values[10]);
        state.nativeCostSumSeconds = nonnegativeDouble(values[11]);
        state.nativeCostSumBytesSquared = nonnegativeDouble(values[12]);
        state.nativeCostSumByteSeconds = nonnegativeDouble(values[13]);
      }
      if (values.length >= 24) {
        state.pbrBlockCostMs = nonnegativeDouble(values[14]);
        state.recordPbrBlockCostMs = nonnegativeDouble(values[15]);
        state.recordDecodeNsPerRecord = nonnegativeDouble(values[16]);
        state.recordMaterializeNsPerRecord = nonnegativeDouble(values[17]);
        state.pbrReadBandwidth = nonnegativeDouble(values[18]);
        state.recordPbrReadBandwidth = nonnegativeDouble(values[19]);
        state.pbrIdleReadBandwidth = nonnegativeDouble(values[20]);
        state.recordPbrIdleReadBandwidth = nonnegativeDouble(values[21]);
        state.pbrContentionPenalty = Math.max(1.0, nonnegativeDouble(values[22]));
        state.recordPbrContentionPenalty = Math.max(1.0, nonnegativeDouble(values[23]));
        if (values.length == CURRENT_PERSISTED_FIELDS) {
          state.pbrResidualSeconds = nonnegativeDouble(values[24]);
          state.recordPbrResidualSeconds = nonnegativeDouble(values[25]);
        }
      }
    }
    return state;
  }

  private void maybeAutoPersist(long samples) {
    int interval = config.autoPersistEverySamples();
    if (interval > 0 && (samples == 1L || samples % interval == 0L)) flush();
  }

  private double stageResidualSeconds(
      String mode, long bytes, long elapsedNs, long blocks, double learnedBlockCostMs) {
    double idleBandwidth = RECORD_PBR.equals(mode)
        ? config.recordPbrBandwidthBytesPerSec() : config.cxlBandwidthBytesPerSec();
    double blockMs = learnedBlockCostMs > 0.0
        ? learnedBlockCostMs : config.pbrCostPerBlockMs();
    return Math.max(0.0, elapsedNs / 1_000_000_000.0 - bytes / idleBandwidth
        - Math.max(0L, blocks) * blockMs / 1000.0);
  }

  private static String normalizeMode(String mode) {
    return mode == null ? "" : mode.trim().toLowerCase(Locale.ROOT);
  }

  private static String shapeBucket(int value) {
    int bounded = Math.max(1, value);
    long upper = 1L;
    while (upper < bounded) upper *= 4L;
    long lower = upper == 1L ? 1L : upper / 4L + 1L;
    return lower == upper ? Long.toString(upper) : lower + "-" + upper;
  }

  private static double ewma(double oldValue, double observation, double alpha) {
    if (observation <= 0.0 || Double.isNaN(observation) || Double.isInfinite(observation)) {
      return oldValue;
    }
    return oldValue <= 0.0 ? observation : alpha * observation + (1.0 - alpha) * oldValue;
  }

  private static double mergeDeviceBandwidth(double signatureBandwidth, double deviceBandwidth) {
    if (signatureBandwidth <= 0.0) return deviceBandwidth;
    if (deviceBandwidth <= 0.0) return signatureBandwidth;
    return Math.min(signatureBandwidth, deviceBandwidth);
  }

  private static double contentionPenalty(double idleBandwidth, double recentBandwidth) {
    if (idleBandwidth <= 0.0 || recentBandwidth <= 0.0) return 1.0;
    return Math.max(1.0, Math.min(4.0, idleBandwidth / recentBandwidth));
  }

  private static void append(StringBuilder builder, long value) {
    builder.append(',').append(value);
  }

  private static void append(StringBuilder builder, double value) {
    builder.append(',').append(value);
  }

  private static long nonnegativeLong(String value) {
    return Math.max(0L, Long.parseLong(value));
  }

  private static double nonnegativeDouble(String value) {
    double parsed = Double.parseDouble(value);
    if (Double.isNaN(parsed) || Double.isInfinite(parsed)) return 0.0;
    return Math.max(0.0, parsed);
  }

  private static final class MutableTelemetry {
    private long samples;
    private double bytesPerMap;
    private double recordsPerMap;
    private double nativeWriteBandwidth;
    private double pbrWriteBandwidth;
    private double recordPbrWriteBandwidth;
    private long nativeStageBytes;
    private long pbrStageBytes;
    private long recordPbrStageBytes;
    private long nativeCostSamples;
    private double nativeCostSumBytes;
    private double nativeCostSumSeconds;
    private double nativeCostSumBytesSquared;
    private double nativeCostSumByteSeconds;
    private double pbrBlockCostMs;
    private double recordPbrBlockCostMs;
    private double recordDecodeNsPerRecord;
    private double recordMaterializeNsPerRecord;
    private double pbrReadBandwidth;
    private double recordPbrReadBandwidth;
    private double pbrIdleReadBandwidth;
    private double recordPbrIdleReadBandwidth;
    private double pbrContentionPenalty = 1.0;
    private double recordPbrContentionPenalty = 1.0;
    private double pbrResidualSeconds;
    private double recordPbrResidualSeconds;
  }
}
