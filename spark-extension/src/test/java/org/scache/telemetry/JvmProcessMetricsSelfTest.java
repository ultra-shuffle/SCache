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

/** Framework-free smoke and caching tests for the opt-in JVM sampler. */
public final class JvmProcessMetricsSelfTest {
  private JvmProcessMetricsSelfTest() { }

  public static void main(String[] args) {
    testDisabledFastPath();
    testOnDemandSamplingAndCaching();
    System.out.println("JvmProcessMetricsSelfTest: PASS");
  }

  private static void testDisabledFastPath() {
    JvmProcessMetrics metrics = new JvmProcessMetrics(TelemetryConfig.disabled());
    check(!metrics.enabled(), "process sampler must default to disabled");
    check(metrics.sample() == JvmProcessMetricsSnapshot.DISABLED,
        "disabled sampler must return the shared constant");
    check(!metrics.sample().sampled(), "disabled snapshot must not claim to be sampled");
  }

  private static void testOnDemandSamplingAndCaching() {
    TelemetryConfig config = TelemetryConfig.builder()
        .processMetricsEnabled(true)
        .processMinSampleIntervalMs(Long.MAX_VALUE)
        .build();
    JvmProcessMetrics metrics = new JvmProcessMetrics(config);
    check(metrics.enabled(), "explicitly enabled sampler stayed disabled");

    JvmProcessMetricsSnapshot first = metrics.sample();
    JvmProcessMetricsSnapshot cached = metrics.sample();
    check(first == cached, "minimum interval did not reuse the cached snapshot");
    check(first.sampled(), "enabled sampler returned an unsampled snapshot");
    check(first.timestampMs() > 0L, "timestamp was not captured");
    check(first.uptimeMs() >= 0L, "JVM uptime should be available");
    check(first.availableProcessors() > 0, "available processors should be positive");
    check(first.heapUsedBytes() >= 0L, "heap usage should be available");
    check(first.liveThreadCount() > 0, "live thread count should be positive");
    String json = first.toJson();
    check(json.startsWith("{") && json.endsWith("}"), "snapshot JSON is malformed");
    check(json.contains("\"processCpuTimeNs\":"), "snapshot JSON omitted CPU time");
    check(json.contains("\"directBufferUsedBytes\":"), "snapshot JSON omitted direct buffers");

    metrics.invalidate();
    JvmProcessMetricsSnapshot refreshed = metrics.sample();
    check(refreshed != first, "invalidate did not force a new MXBean sample");
  }

  private static void check(boolean condition, String message) {
    if (!condition) throw new AssertionError(message);
  }
}
