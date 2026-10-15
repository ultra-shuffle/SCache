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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Immutable configuration for the optional SCache telemetry components.
 *
 * <p>The defaults deliberately disable both adaptive telemetry and JVM process sampling. This
 * keeps merely placing the SCache jar on Spark's class path free of background threads, file I/O,
 * MXBean polling, and shuffle-path behavior changes. Callers may construct a configuration with
 * {@link Builder}, or adapt an existing string map with {@link #from(Map)}.</p>
 */
public final class TelemetryConfig {
  public static final String ADAPTIVE_ENABLED = "scache.telemetry.adaptive.enabled";
  public static final String ADAPTIVE_READ_ONLY = "scache.telemetry.adaptive.readOnly";
  public static final String ADAPTIVE_FILE = "scache.telemetry.adaptive.file";
  public static final String ADAPTIVE_PROFILE = "scache.telemetry.adaptive.profile";
  public static final String ADAPTIVE_EWMA_ALPHA = "scache.telemetry.adaptive.ewmaAlpha";
  public static final String ADAPTIVE_AUTO_PERSIST_EVERY_SAMPLES =
      "scache.telemetry.adaptive.autoPersistEverySamples";
  public static final String ADAPTIVE_FULL_CONFIDENCE_SAMPLES =
      "scache.telemetry.adaptive.fullConfidenceSamples";
  public static final String CXL_BANDWIDTH_BYTES_PER_SEC =
      "scache.telemetry.adaptive.cxlBandwidthBytesPerSec";
  public static final String RECORD_PBR_BANDWIDTH_BYTES_PER_SEC =
      "scache.telemetry.adaptive.recordPbrBandwidthBytesPerSec";
  public static final String PBR_COST_PER_BLOCK_MS =
      "scache.telemetry.adaptive.pbrCostPerBlockMs";
  public static final String PROCESS_ENABLED = "scache.telemetry.process.enabled";
  public static final String PROCESS_MIN_SAMPLE_INTERVAL_MS =
      "scache.telemetry.process.minSampleIntervalMs";

  // Spark accepts arbitrary keys, but the spark.* aliases are friendlier in spark-defaults.conf.
  private static final String SPARK_PREFIX = "spark.";

  // Legacy Spark keys make an existing UltraShuffle configuration easy to adapt at the boundary.
  private static final String LEGACY_ENABLED = "spark.ultrashuffle.adaptive.online.enabled";
  private static final String LEGACY_READ_ONLY = "spark.ultrashuffle.adaptive.online.readOnly";
  private static final String LEGACY_FILE = "spark.ultrashuffle.adaptive.online.telemetryFile";
  private static final String LEGACY_PROFILE = "spark.ultrashuffle.adaptive.online.profileKey";
  private static final String LEGACY_ALPHA = "spark.ultrashuffle.adaptive.online.ewmaAlpha";
  private static final String LEGACY_PERSIST_EVERY =
      "spark.ultrashuffle.adaptive.online.persistEverySamples";
  private static final String LEGACY_FULL_CONFIDENCE =
      "spark.ultrashuffle.adaptive.online.fullConfidenceSamples";
  private static final String LEGACY_CXL_BANDWIDTH =
      "spark.ultrashuffle.adaptive.cxlBandwidthBytesPerSec";
  private static final String LEGACY_RECORD_PBR_BANDWIDTH =
      "spark.ultrashuffle.adaptive.recordPbrBandwidthBytesPerSec";
  private static final String LEGACY_BLOCK_COST =
      "spark.ultrashuffle.adaptive.pbrCostPerBlockMs";

  private final boolean adaptiveEnabled;
  private final boolean adaptiveReadOnly;
  private final String persistenceFile;
  private final String profile;
  private final double ewmaAlpha;
  private final int autoPersistEverySamples;
  private final int fullConfidenceSamples;
  private final double cxlBandwidthBytesPerSec;
  private final double recordPbrBandwidthBytesPerSec;
  private final double pbrCostPerBlockMs;
  private final boolean processMetricsEnabled;
  private final long processMinSampleIntervalMs;

  private TelemetryConfig(Builder builder) {
    adaptiveEnabled = builder.adaptiveEnabled;
    adaptiveReadOnly = builder.adaptiveReadOnly;
    persistenceFile = trim(builder.persistenceFile);
    profile = trim(builder.profile).isEmpty() ? "default" : trim(builder.profile);
    ewmaAlpha = clamp(builder.ewmaAlpha, 0.01, 1.0);
    autoPersistEverySamples = Math.max(0, builder.autoPersistEverySamples);
    fullConfidenceSamples = Math.max(1, builder.fullConfidenceSamples);
    cxlBandwidthBytesPerSec = positiveOrDefault(
        builder.cxlBandwidthBytesPerSec, 2_684_354_560.0);
    recordPbrBandwidthBytesPerSec = positiveOrDefault(
        builder.recordPbrBandwidthBytesPerSec, 2_684_354_560.0);
    pbrCostPerBlockMs = nonnegativeOrDefault(builder.pbrCostPerBlockMs, 0.05);
    processMetricsEnabled = builder.processMetricsEnabled;
    // One exporter scrape asks every gauge for a value.  A zero interval would make each gauge
    // poll all MXBeans independently, so retain a small cache even when a caller requests zero.
    processMinSampleIntervalMs = builder.processMinSampleIntervalMs < 0L
        ? 1000L : Math.max(100L, builder.processMinSampleIntervalMs);
  }

  /** Returns a configuration in which every telemetry component is disabled. */
  public static TelemetryConfig disabled() {
    return new Builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builds a configuration from string values, accepting SCache keys, their {@code spark.}
   * aliases, and the legacy Spark adaptive-telemetry keys. An unprefixed SCache key takes
   * precedence when more than one form is present. Invalid values fall back to safe defaults
   * instead of preventing the SCache process from starting.
   */
  public static TelemetryConfig from(Map<String, String> values) {
    Map<String, String> safeValues = values == null
        ? Collections.<String, String>emptyMap() : values;
    Builder builder = builder();
    builder.adaptiveEnabled(parseBoolean(first(
        safeValues, ADAPTIVE_ENABLED, sparkAlias(ADAPTIVE_ENABLED), LEGACY_ENABLED), false));
    builder.adaptiveReadOnly(parseBoolean(
        first(safeValues, ADAPTIVE_READ_ONLY, sparkAlias(ADAPTIVE_READ_ONLY), LEGACY_READ_ONLY),
        false));
    builder.persistenceFile(firstOrDefault(
        safeValues, "", ADAPTIVE_FILE, sparkAlias(ADAPTIVE_FILE), LEGACY_FILE));
    builder.profile(firstOrDefault(
        safeValues, "default", ADAPTIVE_PROFILE, sparkAlias(ADAPTIVE_PROFILE), LEGACY_PROFILE));
    builder.ewmaAlpha(parseDouble(
        first(safeValues, ADAPTIVE_EWMA_ALPHA, sparkAlias(ADAPTIVE_EWMA_ALPHA), LEGACY_ALPHA),
        0.25));
    builder.autoPersistEverySamples(parseInt(first(
        safeValues, ADAPTIVE_AUTO_PERSIST_EVERY_SAMPLES,
        sparkAlias(ADAPTIVE_AUTO_PERSIST_EVERY_SAMPLES), LEGACY_PERSIST_EVERY), 0));
    builder.fullConfidenceSamples(parseInt(first(
        safeValues, ADAPTIVE_FULL_CONFIDENCE_SAMPLES,
        sparkAlias(ADAPTIVE_FULL_CONFIDENCE_SAMPLES), LEGACY_FULL_CONFIDENCE), 16));
    builder.cxlBandwidthBytesPerSec(parseDouble(first(
        safeValues, CXL_BANDWIDTH_BYTES_PER_SEC,
        sparkAlias(CXL_BANDWIDTH_BYTES_PER_SEC), LEGACY_CXL_BANDWIDTH), 2_684_354_560.0));
    builder.recordPbrBandwidthBytesPerSec(parseDouble(first(
        safeValues, RECORD_PBR_BANDWIDTH_BYTES_PER_SEC,
        sparkAlias(RECORD_PBR_BANDWIDTH_BYTES_PER_SEC), LEGACY_RECORD_PBR_BANDWIDTH),
        2_684_354_560.0));
    builder.pbrCostPerBlockMs(parseDouble(first(
        safeValues, PBR_COST_PER_BLOCK_MS,
        sparkAlias(PBR_COST_PER_BLOCK_MS), LEGACY_BLOCK_COST), 0.05));
    builder.processMetricsEnabled(parseBoolean(first(
        safeValues, PROCESS_ENABLED, sparkAlias(PROCESS_ENABLED)), false));
    builder.processMinSampleIntervalMs(parseLong(first(
        safeValues, PROCESS_MIN_SAMPLE_INTERVAL_MS, sparkAlias(PROCESS_MIN_SAMPLE_INTERVAL_MS)),
        1000L));
    return builder.build();
  }

  /** Convenience helper for callers whose configuration starts as Java properties. */
  public static TelemetryConfig fromProperties(java.util.Properties properties) {
    if (properties == null || properties.isEmpty()) return disabled();
    Map<String, String> values = new HashMap<>();
    for (String key : properties.stringPropertyNames()) {
      values.put(key, properties.getProperty(key));
    }
    return from(values);
  }

  public boolean adaptiveEnabled() { return adaptiveEnabled; }
  public boolean adaptiveReadOnly() { return adaptiveReadOnly; }
  public String persistenceFile() { return persistenceFile; }
  public String profile() { return profile; }
  public double ewmaAlpha() { return ewmaAlpha; }
  public int autoPersistEverySamples() { return autoPersistEverySamples; }
  public int fullConfidenceSamples() { return fullConfidenceSamples; }
  public double cxlBandwidthBytesPerSec() { return cxlBandwidthBytesPerSec; }
  public double recordPbrBandwidthBytesPerSec() { return recordPbrBandwidthBytesPerSec; }
  public double pbrCostPerBlockMs() { return pbrCostPerBlockMs; }
  public boolean processMetricsEnabled() { return processMetricsEnabled; }
  public long processMinSampleIntervalMs() { return processMinSampleIntervalMs; }

  private static String sparkAlias(String key) {
    return SPARK_PREFIX + key;
  }

  private static String first(Map<String, String> values, String... keys) {
    for (String key : keys) {
      String value = values.get(key);
      if (value != null) return value;
    }
    return null;
  }

  private static String firstOrDefault(
      Map<String, String> values, String defaultValue, String... keys) {
    String value = first(values, keys);
    return value == null ? defaultValue : value;
  }

  private static boolean parseBoolean(String value, boolean defaultValue) {
    if (value == null) return defaultValue;
    String normalized = value.trim();
    if ("true".equalsIgnoreCase(normalized)) return true;
    if ("false".equalsIgnoreCase(normalized)) return false;
    return defaultValue;
  }

  private static int parseInt(String value, int defaultValue) {
    if (value == null) return defaultValue;
    try {
      return Integer.parseInt(value.trim());
    } catch (RuntimeException ignored) {
      return defaultValue;
    }
  }

  private static long parseLong(String value, long defaultValue) {
    if (value == null) return defaultValue;
    try {
      return Long.parseLong(value.trim());
    } catch (RuntimeException ignored) {
      return defaultValue;
    }
  }

  private static double parseDouble(String value, double defaultValue) {
    if (value == null) return defaultValue;
    try {
      double parsed = Double.parseDouble(value.trim());
      return Double.isNaN(parsed) || Double.isInfinite(parsed) ? defaultValue : parsed;
    } catch (RuntimeException ignored) {
      return defaultValue;
    }
  }

  private static String trim(String value) {
    return value == null ? "" : value.trim();
  }

  private static double clamp(double value, double minimum, double maximum) {
    if (Double.isNaN(value) || Double.isInfinite(value)) return minimum;
    return Math.max(minimum, Math.min(maximum, value));
  }

  private static double positiveOrDefault(double value, double defaultValue) {
    return value > 0.0 && !Double.isInfinite(value) && !Double.isNaN(value)
        ? value : defaultValue;
  }

  private static double nonnegativeOrDefault(double value, double defaultValue) {
    return value >= 0.0 && !Double.isInfinite(value) && !Double.isNaN(value)
        ? value : defaultValue;
  }

  public static final class Builder {
    private boolean adaptiveEnabled = false;
    private boolean adaptiveReadOnly = false;
    private String persistenceFile = "";
    private String profile = "default";
    private double ewmaAlpha = 0.25;
    private int autoPersistEverySamples = 0;
    private int fullConfidenceSamples = 16;
    private double cxlBandwidthBytesPerSec = 2_684_354_560.0;
    private double recordPbrBandwidthBytesPerSec = 2_684_354_560.0;
    private double pbrCostPerBlockMs = 0.05;
    private boolean processMetricsEnabled = false;
    private long processMinSampleIntervalMs = 1000L;

    public Builder adaptiveEnabled(boolean value) {
      adaptiveEnabled = value;
      return this;
    }

    public Builder adaptiveReadOnly(boolean value) {
      adaptiveReadOnly = value;
      return this;
    }

    public Builder persistenceFile(String value) {
      persistenceFile = value;
      return this;
    }

    public Builder profile(String value) {
      profile = value;
      return this;
    }

    public Builder ewmaAlpha(double value) {
      ewmaAlpha = value;
      return this;
    }

    public Builder autoPersistEverySamples(int value) {
      autoPersistEverySamples = value;
      return this;
    }

    public Builder fullConfidenceSamples(int value) {
      fullConfidenceSamples = value;
      return this;
    }

    public Builder cxlBandwidthBytesPerSec(double value) {
      cxlBandwidthBytesPerSec = value;
      return this;
    }

    public Builder recordPbrBandwidthBytesPerSec(double value) {
      recordPbrBandwidthBytesPerSec = value;
      return this;
    }

    public Builder pbrCostPerBlockMs(double value) {
      pbrCostPerBlockMs = value;
      return this;
    }

    public Builder processMetricsEnabled(boolean value) {
      processMetricsEnabled = value;
      return this;
    }

    public Builder processMinSampleIntervalMs(long value) {
      processMinSampleIntervalMs = value;
      return this;
    }

    public TelemetryConfig build() {
      return new TelemetryConfig(this);
    }
  }
}
