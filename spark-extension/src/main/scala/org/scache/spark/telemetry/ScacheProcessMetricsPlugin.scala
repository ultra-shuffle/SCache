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

package org.scache.spark.telemetry

import java.util.{Collections, Map => JMap}

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.scache.telemetry.{JvmProcessMetrics, JvmProcessMetricsSnapshot, TelemetryConfig}

/**
 * Optional Spark metrics bridge for the on-demand SCache JVM process sampler.
 *
 * Merely adding the extension jar has no effect: Spark creates this class only when it is listed
 * in `spark.plugins`, and it registers no gauges unless process metrics are explicitly enabled.
 * Gauge scrapes share the sampler's time-bounded cache, so one exporter scrape does not poll all
 * MXBeans once per gauge.
 */
final class ScacheProcessMetricsPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new ScacheProcessMetricsDriverPlugin

  override def executorPlugin(): ExecutorPlugin = new ScacheProcessMetricsExecutorPlugin
}

private final class ScacheProcessMetricsDriverPlugin extends DriverPlugin {
  @volatile private var processMetrics: JvmProcessMetrics = _

  override def init(
      sparkContext: SparkContext,
      pluginContext: PluginContext): JMap[String, String] = {
    processMetrics = ProcessMetricsBridge.from(pluginContext)
    Collections.emptyMap[String, String]()
  }

  override def registerMetrics(appId: String, pluginContext: PluginContext): Unit = {
    ProcessMetricsBridge.register(pluginContext.metricRegistry(), processMetrics)
  }
}

private final class ScacheProcessMetricsExecutorPlugin extends ExecutorPlugin {
  override def init(
      pluginContext: PluginContext,
      extraConf: JMap[String, String]): Unit = {
    val processMetrics = ProcessMetricsBridge.from(pluginContext)
    ProcessMetricsBridge.register(pluginContext.metricRegistry(), processMetrics)
  }
}

private[telemetry] object ProcessMetricsBridge {
  def from(pluginContext: PluginContext): JvmProcessMetrics = {
    val values = new java.util.HashMap[String, String]()
    pluginContext.conf().getAll.foreach { case (key, value) => values.put(key, value) }
    new JvmProcessMetrics(TelemetryConfig.from(values))
  }

  def register(registry: MetricRegistry, sampler: JvmProcessMetrics): Unit = {
    if (sampler == null || !sampler.enabled()) return

    longGauge(registry, sampler, "process.timestampMs", _.timestampMs())
    longGauge(registry, sampler, "process.uptimeMs", _.uptimeMs())
    longGauge(registry, sampler, "process.id", _.processId())
    longGauge(registry, sampler, "process.availableProcessors", _.availableProcessors().toLong)
    longGauge(registry, sampler, "process.cpuTimeNs", _.processCpuTimeNs())
    doubleGauge(registry, sampler, "process.cpuLoad", _.processCpuLoad())
    doubleGauge(registry, sampler, "system.cpuLoad", _.systemCpuLoad())
    doubleGauge(registry, sampler, "system.loadAverage", _.systemLoadAverage())
    longGauge(registry, sampler, "memory.heapUsedBytes", _.heapUsedBytes())
    longGauge(registry, sampler, "memory.heapCommittedBytes", _.heapCommittedBytes())
    longGauge(registry, sampler, "memory.nonHeapUsedBytes", _.nonHeapUsedBytes())
    longGauge(registry, sampler, "memory.nonHeapCommittedBytes", _.nonHeapCommittedBytes())
    longGauge(registry, sampler, "buffer.directCount", _.directBufferCount())
    longGauge(registry, sampler, "buffer.directUsedBytes", _.directBufferUsedBytes())
    longGauge(registry, sampler, "buffer.directCapacityBytes", _.directBufferCapacityBytes())
    longGauge(registry, sampler, "buffer.mappedCount", _.mappedBufferCount())
    longGauge(registry, sampler, "buffer.mappedUsedBytes", _.mappedBufferUsedBytes())
    longGauge(registry, sampler, "buffer.mappedCapacityBytes", _.mappedBufferCapacityBytes())
    longGauge(registry, sampler, "gc.collectionCount", _.gcCollectionCount())
    longGauge(registry, sampler, "gc.collectionTimeMs", _.gcCollectionTimeMs())
    longGauge(registry, sampler, "threads.live", _.liveThreadCount().toLong)
    longGauge(registry, sampler, "threads.daemon", _.daemonThreadCount().toLong)
    longGauge(registry, sampler, "threads.peak", _.peakThreadCount().toLong)
    longGauge(registry, sampler, "classes.loaded", _.loadedClassCount().toLong)
    longGauge(registry, sampler, "memory.committedVirtualBytes", _.committedVirtualMemoryBytes())
    longGauge(registry, sampler, "memory.totalPhysicalBytes", _.totalPhysicalMemoryBytes())
    longGauge(registry, sampler, "memory.freePhysicalBytes", _.freePhysicalMemoryBytes())
    longGauge(registry, sampler, "memory.totalSwapBytes", _.totalSwapBytes())
    longGauge(registry, sampler, "memory.freeSwapBytes", _.freeSwapBytes())
    longGauge(registry, sampler, "files.openDescriptors", _.openFileDescriptorCount())
    longGauge(registry, sampler, "files.maxDescriptors", _.maxFileDescriptorCount())
  }

  private def longGauge(
      registry: MetricRegistry,
      sampler: JvmProcessMetrics,
      name: String,
      value: JvmProcessMetricsSnapshot => Long): Unit = {
    registry.register(name, new Gauge[java.lang.Long] {
      override def getValue: java.lang.Long = java.lang.Long.valueOf(value(sampler.sample()))
    })
  }

  private def doubleGauge(
      registry: MetricRegistry,
      sampler: JvmProcessMetrics,
      name: String,
      value: JvmProcessMetricsSnapshot => Double): Unit = {
    registry.register(name, new Gauge[java.lang.Double] {
      override def getValue: java.lang.Double = java.lang.Double.valueOf(value(sampler.sample()))
    })
  }
}
