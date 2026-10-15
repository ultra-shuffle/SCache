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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Framework-free test entry point so the extension can be checked without adding test jars. */
public final class AdaptiveTelemetryRegistrySelfTest {
  private AdaptiveTelemetryRegistrySelfTest() { }

  public static void main(String[] args) throws Exception {
    testDisabledByDefault();
    testConfigurationAndSignatures();
    testOnlineModel();
    testPersistenceAndReadOnlyWarmStart();
    testLegacyAndMalformedPersistence();
    System.out.println("AdaptiveTelemetryRegistrySelfTest: PASS");
  }

  private static void testDisabledByDefault() {
    TelemetryConfig config = TelemetryConfig.disabled();
    check(!config.adaptiveEnabled(), "adaptive telemetry must default to disabled");
    check(!config.processMetricsEnabled(), "process metrics must default to disabled");
    check(config.autoPersistEverySamples() == 0, "automatic persistence must default off");

    AdaptiveTelemetryRegistry registry = new AdaptiveTelemetryRegistry(config);
    registry.observeMap("disabled", 100L, 10L);
    check(registry.snapshot("disabled") == AdaptiveTelemetrySnapshot.EMPTY,
        "disabled registry must return the shared empty snapshot");
    check(!registry.flush(), "disabled registry must not persist");
  }

  private static void testConfigurationAndSignatures() {
    Map<String, String> values = new HashMap<>();
    values.put("spark.ultrashuffle.adaptive.online.enabled", "true");
    values.put("spark.ultrashuffle.adaptive.online.ewmaAlpha", "0.75");
    values.put("spark.ultrashuffle.adaptive.online.profileKey", "legacy-profile");
    values.put(TelemetryConfig.PROCESS_ENABLED, "true");
    TelemetryConfig config = TelemetryConfig.from(values);
    check(config.adaptiveEnabled(), "legacy enabled key was not adapted");
    close(config.ewmaAlpha(), 0.75, 0.0, "legacy alpha was not adapted");
    check("legacy-profile".equals(config.profile()), "legacy profile was not adapted");
    check(config.processMetricsEnabled(), "process metrics key was not parsed");

    Map<String, String> sparkValues = new HashMap<>();
    sparkValues.put("spark.scache.telemetry.adaptive.enabled", "true");
    sparkValues.put("spark.scache.telemetry.process.enabled", "true");
    check(TelemetryConfig.from(sparkValues).adaptiveEnabled(),
        "Spark-prefixed adaptive key was not parsed");
    check(TelemetryConfig.from(sparkValues).processMetricsEnabled(),
        "Spark-prefixed process key was not parsed");
    sparkValues.put("spark.scache.telemetry.process.minSampleIntervalMs", "-1");
    check(TelemetryConfig.from(sparkValues).processMinSampleIntervalMs() == 1000L,
        "negative process sampling interval must fall back to the safe default");
    sparkValues.put("spark.scache.telemetry.process.minSampleIntervalMs", "0");
    check(TelemetryConfig.from(sparkValues).processMinSampleIntervalMs() == 100L,
        "zero process sampling interval must retain a scrape-wide cache");

    values.put(TelemetryConfig.ADAPTIVE_ENABLED, "false");
    check(!TelemetryConfig.from(values).adaptiveEnabled(),
        "SCache configuration must override the legacy key");

    String signature = AdaptiveTelemetryRegistry.signature(
        true, false, true, true, false, 1, 2, 17);
    check("agg:unordered:range:record:materialize:maps=1:reducers=2-4:parallelism=17-64"
            .equals(signature),
        "unexpected shape signature: " + signature);
    String maximumShape = AdaptiveTelemetryRegistry.signature(
        false, false, false, false, false, Integer.MAX_VALUE, 1, 1);
    check(maximumShape.contains("maps=1073741825-4294967296"),
        "large shape bucket must contain the original value: " + maximumShape);
  }

  private static void testOnlineModel() {
    TelemetryConfig config = TelemetryConfig.builder()
        .adaptiveEnabled(true)
        .ewmaAlpha(0.5)
        .fullConfidenceSamples(4)
        .cxlBandwidthBytesPerSec(1000.0)
        .pbrCostPerBlockMs(0.05)
        .build();
    AdaptiveTelemetryRegistry registry = new AdaptiveTelemetryRegistry(config);
    String signature = "model";

    registry.observeMap(signature, 100L, 10L);
    registry.observeMap(signature, 300L, 30L);
    registry.observeStage(signature, AdaptiveTelemetryRegistry.NATIVE,
        100L, 0L, 1_000_000_000L, 1L);
    registry.observeStage(signature, AdaptiveTelemetryRegistry.NATIVE,
        300L, 0L, 2_000_000_000L, 1L);
    registry.observeStage(signature, AdaptiveTelemetryRegistry.PBR,
        1000L, 800L, 1_000_000_000L, 2L);
    registry.observeRead(signature, AdaptiveTelemetryRegistry.PBR,
        1000L, 10L, 10_000_000L, 5_000_000L,
        0L, 0L, 0L, 1_000_000_000L);
    registry.observeCxlReadStage(500L, 1_000_000_000L);

    AdaptiveTelemetrySnapshot snapshot = registry.snapshot(signature);
    check(snapshot.samples() == 2L, "unexpected map sample count");
    close(snapshot.bytesPerMap(), 200.0, 0.0001, "bytes/map EWMA");
    close(snapshot.recordsPerMap(), 20.0, 0.0001, "records/map EWMA");
    close(snapshot.confidence(), 0.5, 0.0001, "confidence");
    close(snapshot.nativeCostSeconds(500L), 3.0, 0.0001, "native affine cost");
    close(snapshot.pbrBlockCostMs(), 1.0, 0.0001, "PBR block cost");
    close(snapshot.pbrReadBandwidth(), 500.0, 0.0001,
        "device bandwidth must conservatively cap signature bandwidth");
    close(snapshot.pbrResidualSeconds(), 0.1999, 0.0001, "PBR residual");
    check(snapshot.hasSizeEstimate(), "size estimate should be available");
  }

  private static void testPersistenceAndReadOnlyWarmStart() throws Exception {
    Path directory = Files.createTempDirectory("scache-telemetry-test-");
    Path file = directory.resolve("telemetry.properties");
    try {
      TelemetryConfig writerConfig = TelemetryConfig.builder()
          .adaptiveEnabled(true)
          .persistenceFile(file.toString())
          .profile("warm")
          .ewmaAlpha(0.5)
          .build();
      AdaptiveTelemetryRegistry writer = new AdaptiveTelemetryRegistry(writerConfig);
      writer.observeMap("persisted", 4096L, 64L);
      writer.observeStage("persisted", AdaptiveTelemetryRegistry.RECORD_PBR,
          4096L, 2048L, 10_000_000L, 4L);
      check(!Files.exists(file), "default observation path must not perform file I/O");
      check(writer.flush(), "explicit telemetry flush failed: " + writer.lastPersistenceError());
      check(Files.isRegularFile(file), "telemetry file was not created");

      Properties properties = new Properties();
      try (InputStream input = Files.newInputStream(file)) {
        properties.load(input);
      }
      check(properties.size() == 1, "expected exactly one persisted signature");
      String persistedValue = properties.values().iterator().next().toString();
      check(persistedValue.split(",", -1).length == 26,
          "persistence format must contain all 26 adaptive fields");

      TelemetryConfig readerConfig = TelemetryConfig.builder()
          .adaptiveEnabled(true)
          .adaptiveReadOnly(true)
          .persistenceFile(file.toString())
          .profile("warm")
          .build();
      AdaptiveTelemetryRegistry reader = new AdaptiveTelemetryRegistry(readerConfig);
      check(reader.preload() == 1, "explicit warm-start preload failed");
      AdaptiveTelemetrySnapshot restored = reader.snapshot("persisted");
      check(reader.loadedEntries() == 1, "warm-start entry was not loaded");
      check(restored.samples() == 1L, "warm-start sample count changed");
      close(restored.bytesPerMap(), 4096.0, 0.0, "warm-start bytes/map");
      reader.observeMap("persisted", 1L, 1L);
      close(reader.snapshot("persisted").bytesPerMap(), 4096.0, 0.0,
          "read-only registry accepted an update");
      check(!reader.flush(), "read-only registry must not write the telemetry file");
    } finally {
      Files.deleteIfExists(file);
      Files.deleteIfExists(directory);
    }
  }

  private static void testLegacyAndMalformedPersistence() throws Exception {
    Path directory = Files.createTempDirectory("scache-telemetry-legacy-test-");
    Path file = directory.resolve("telemetry.properties");
    try {
      Properties properties = new Properties();
      properties.setProperty(encoded("legacy|v5"), "3,100,10,200,300");
      properties.setProperty(encoded("legacy|v7"), "4,110,11,210,310,1000,900");
      properties.setProperty(encoded("legacy|v9"),
          "5,120,12,220,320,330,1100,1000,950");
      properties.setProperty(encoded("legacy|v14"),
          "6,130,13,230,330,340,1200,1100,1000,2,200,2,25000,250");
      properties.setProperty(encoded("legacy|v24"),
          "7,140,14,240,340,350,1300,1200,1100,2,200,2,25000,250,"
              + "1,2,3,4,500,600,700,800,1.5,2");
      properties.setProperty(encoded("legacy|v26"),
          "8,150,15,250,350,360,1400,1300,1200,2,200,2,25000,250,"
              + "1,2,3,4,500,600,700,800,1.5,2,0.1,0.2");
      properties.setProperty(encoded("legacy|bad"), "not,a,valid,telemetry,row");
      properties.setProperty("not-base64!", "1,1,1,1,1");
      try (OutputStream output = Files.newOutputStream(file)) {
        properties.store(output, "legacy telemetry fixture");
      }

      AdaptiveTelemetryRegistry registry = new AdaptiveTelemetryRegistry(
          TelemetryConfig.builder()
              .adaptiveEnabled(true)
              .adaptiveReadOnly(true)
              .persistenceFile(file.toString())
              .profile("legacy")
              .build());
      AdaptiveTelemetrySnapshot snapshot = registry.snapshot("v5");
      check(snapshot.samples() == 3L, "five-field legacy row was not loaded");
      close(snapshot.bytesPerMap(), 100.0, 0.0, "legacy bytes/map");
      check(registry.snapshot("v7").nativeStageBytes() == 1000L,
          "seven-field legacy row was not loaded");
      close(registry.snapshot("v9").recordPbrWriteBandwidth(), 330.0, 0.0,
          "nine-field record-PBR bandwidth");
      check(registry.snapshot("v14").nativeCostSamples() == 2L,
          "fourteen-field native cost model was not loaded");
      close(registry.snapshot("v24").recordPbrContentionPenalty(), 2.0, 0.0,
          "twenty-four-field contention model");
      close(registry.snapshot("v26").recordPbrResidualSeconds(), 0.2, 0.0,
          "twenty-six-field residual model");
      check(registry.loadedEntries() == 6,
          "malformed entries should be skipped independently: " + registry.loadedEntries());
      check(registry.snapshot("bad") == AdaptiveTelemetrySnapshot.EMPTY,
          "malformed entry should remain a cold start");
    } finally {
      Files.deleteIfExists(file);
      Files.deleteIfExists(directory);
    }
  }

  private static String encoded(String value) {
    return Base64.getUrlEncoder().withoutPadding().encodeToString(
        value.getBytes(StandardCharsets.UTF_8));
  }

  private static void check(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
  }

  private static void close(double actual, double expected, double tolerance, String message) {
    if (Math.abs(actual - expected) > tolerance) {
      throw new AssertionError(message + ": expected=" + expected + ", actual=" + actual);
    }
  }
}
