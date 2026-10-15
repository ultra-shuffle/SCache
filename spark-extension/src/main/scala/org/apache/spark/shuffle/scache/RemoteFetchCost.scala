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

package org.apache.spark.shuffle.scache

import java.util.Locale
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.LockSupport

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
 * Optional additive cost model for successful remote shuffle block fetches.
 *
 * A model instance belongs to one shuffle manager (and therefore one Spark executor). Bandwidth
 * reservations share a single link clock, while the fixed per-block latency may overlap. Waiting
 * is done by the task consuming the fetched block, never by a network callback thread.
 */
private[scache] trait RemoteFetchCost {
  /** Wait for the modeled transfer and return the actual wait time in nanoseconds. */
  def acquire(bytes: Long): Long
}

private[scache] object RemoteFetchCost extends Logging {
  val EnabledKey = "spark.shuffle.remoteFetchCost.enabled"
  val BandwidthKey = "spark.shuffle.remoteFetchCost.bandwidth"
  val FixedLatencyKey = "spark.shuffle.remoteFetchCost.fixedLatency"

  private val CoreImplementationClass = "org.apache.spark.storage.RemoteShuffleFetchCost$"

  object Disabled extends RemoteFetchCost {
    override def acquire(bytes: Long): Long = 0L
  }

  def fromSparkConf(conf: SparkConf): RemoteFetchCost = {
    if (!conf.getBoolean(EnabledKey, false)) {
      Disabled
    } else {
      require(!coreImplementationPresent,
        s"$EnabledKey cannot be used through RemoteFetchCostShuffleManager because this " +
          "Spark build already contains the core remote-fetch cost implementation. Remove " +
          "spark.shuffle.manager (preferred for this build) or use an unpatched Spark 3.5.7 " +
          "distribution; otherwise every remote block would be charged twice.")
      val bandwidth = conf.getSizeAsBytes(BandwidthKey, "0b")
      require(bandwidth > 0L,
        s"$BandwidthKey must be positive when $EnabledKey=true")

      val fixedLatencyNs = parseNanos(conf.get(FixedLatencyKey, "0ns"))
      require(fixedLatencyNs >= 0L, s"$FixedLatencyKey must not be negative")

      logWarning("Experimental additive remote shuffle fetch cost is enabled: " +
        s"bandwidth=$bandwidth bytes/s, fixedLatency=$fixedLatencyNs ns. " +
        "This does not enable RDMA or replace Spark's network transport.")
      new SharedBandwidthRemoteFetchCost(bandwidth, fixedLatencyNs)
    }
  }

  private[scache] def coreImplementationPresent: Boolean = {
    try {
      Class.forName(CoreImplementationClass, false, getClass.getClassLoader)
      true
    } catch {
      case _: ClassNotFoundException => false
      case error: LinkageError =>
        throw new IllegalStateException(
          s"Found $CoreImplementationClass but could not link it safely; refusing to risk " +
            "combining two remote-fetch cost implementations.", error)
    }
  }

  /** Parse Spark-style duration strings, including the nanosecond unit needed by this model. */
  private[scache] def parseNanos(value: String): Long = {
    val normalized = value.toLowerCase(Locale.ROOT).trim
    val Duration = "(-?[0-9]+)([a-z]+)?".r
    normalized match {
      case Duration(number, suffix) =>
        val amount = number.toLong
        Option(suffix).getOrElse("ns") match {
          case "ns" => amount
          case "us" => TimeUnit.MICROSECONDS.toNanos(amount)
          case "ms" => TimeUnit.MILLISECONDS.toNanos(amount)
          case "s" => TimeUnit.SECONDS.toNanos(amount)
          case "m" | "min" => TimeUnit.MINUTES.toNanos(amount)
          case "h" => TimeUnit.HOURS.toNanos(amount)
          case "d" => TimeUnit.DAYS.toNanos(amount)
          case invalid =>
            throw new NumberFormatException(
              s"Invalid suffix '$invalid' for $FixedLatencyKey")
        }
      case _ =>
        throw new NumberFormatException(
          s"Invalid duration '$value' for $FixedLatencyKey")
    }
  }
}

private[scache] final class SharedBandwidthRemoteFetchCost(
    val bandwidthBytesPerSecond: Long,
    val fixedLatencyNs: Long,
    nanoTime: () => Long = () => System.nanoTime(),
    parkNanos: Long => Unit = LockSupport.parkNanos) extends RemoteFetchCost {

  require(bandwidthBytesPerSecond > 0L, "bandwidthBytesPerSecond must be positive")
  require(fixedLatencyNs >= 0L, "fixedLatencyNs must not be negative")

  private val nextFreeNs = new AtomicLong(0L)

  private def saturatingAdd(left: Long, right: Long): Long = {
    if (right > 0L && left > Long.MaxValue - right) Long.MaxValue else left + right
  }

  private[scache] def transferTimeNs(bytes: Long): Long = {
    if (bytes <= 0L) {
      0L
    } else {
      val nanos = math.ceil(bytes.toDouble * 1000000000.0 / bandwidthBytesPerSecond).toLong
      math.max(1L, nanos)
    }
  }

  /** Reserve shared link time and return the absolute ready time. */
  private[scache] def reserveDeadlineNs(bytes: Long, nowNs: Long): Long = {
    val transferNs = transferTimeNs(bytes)
    if (transferNs == 0L && fixedLatencyNs == 0L) {
      nowNs
    } else {
      var observed = nextFreeNs.get()
      var transferFinishedNs = 0L
      var reserved = false
      while (!reserved) {
        val transferStartNs = math.max(nowNs, observed)
        transferFinishedNs = saturatingAdd(transferStartNs, transferNs)
        reserved = nextFreeNs.compareAndSet(observed, transferFinishedNs)
        if (!reserved) {
          observed = nextFreeNs.get()
        }
      }
      math.max(transferFinishedNs, saturatingAdd(nowNs, fixedLatencyNs))
    }
  }

  override def acquire(bytes: Long): Long = {
    if (bytes <= 0L) {
      return 0L
    }

    val waitStartNs = nanoTime()
    val deadlineNs = reserveDeadlineNs(bytes, waitStartNs)
    var remainingNs = deadlineNs - nanoTime()
    while (remainingNs > 0L && !Thread.currentThread().isInterrupted) {
      parkNanos(remainingNs)
      remainingNs = deadlineNs - nanoTime()
    }
    math.max(0L, nanoTime() - waitStartNs)
  }
}
