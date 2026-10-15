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

import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import org.apache.spark.SparkConf
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.shuffle.sort.SortShuffleManager

/** Dependency-free checks runnable with `Test / runMain ...RemoteFetchCostSelfTest`. */
object RemoteFetchCostSelfTest {
  def main(args: Array[String]): Unit = {
    disabledByDefault()
    requiresPositiveBandwidth()
    parsesDurations()
    sharesBandwidthButOverlapsLatency()
    waitsWithInjectedClock()
    chargesRemoteBytesAndForwardsMetrics()
    accumulatesSubMillisecondWaits()
    keepsRemaindersReporterLocal()
    accumulatesSubMillisecondWaitsConcurrently()
    disabledReporterPathDoesNotWrap()
    remainsASortShuffleManager()
    println("RemoteFetchCostSelfTest: PASS")
  }

  private def disabledByDefault(): Unit = {
    val model = RemoteFetchCost.fromSparkConf(new SparkConf(false))
    assert(model eq RemoteFetchCost.Disabled)
    assert(model.acquire(Long.MaxValue) == 0L)
  }

  private def requiresPositiveBandwidth(): Unit = {
    val conf = new SparkConf(false)
      .set(RemoteFetchCost.EnabledKey, "true")
      .set(RemoteFetchCost.BandwidthKey, "0")
    val error = intercept[IllegalArgumentException] {
      RemoteFetchCost.fromSparkConf(conf)
    }
    assert(error.getMessage.contains(RemoteFetchCost.BandwidthKey))
  }

  private def parsesDurations(): Unit = {
    assert(RemoteFetchCost.parseNanos("17") == 17L)
    assert(RemoteFetchCost.parseNanos("17ns") == 17L)
    assert(RemoteFetchCost.parseNanos("2us") == 2000L)
    assert(RemoteFetchCost.parseNanos("3ms") == 3000000L)
  }

  private def sharesBandwidthButOverlapsLatency(): Unit = {
    val model = new SharedBandwidthRemoteFetchCost(
      bandwidthBytesPerSecond = 1000L,
      fixedLatencyNs = 1500000000L)

    assert(model.transferTimeNs(1L) == 1000000L)
    assert(model.reserveDeadlineNs(1000L, 100L) == 1500000100L)
    assert(model.reserveDeadlineNs(1000L, 100L) == 2000000100L)
  }

  private def waitsWithInjectedClock(): Unit = {
    val nowNs = new AtomicLong(10L)
    val model = new SharedBandwidthRemoteFetchCost(
      bandwidthBytesPerSecond = 1000000000L,
      fixedLatencyNs = 0L,
      nanoTime = () => nowNs.get(),
      parkNanos = delay => nowNs.addAndGet(delay))

    assert(model.acquire(500000000L) == 500000000L)
    assert(nowNs.get() == 500000010L)
  }

  private def chargesRemoteBytesAndForwardsMetrics(): Unit = {
    val delegate = new RecordingMetrics
    val cost = new RemoteFetchCost {
      override def acquire(bytes: Long): Long = {
        assert(bytes == 4096L)
        2500000L
      }
    }
    val reporter = new RemoteFetchCostMetricsReporter(delegate, cost)

    reporter.incLocalBytesRead(7L)
    reporter.incRemoteBytesRead(4096L)

    assert(delegate.localBytesRead.get() == 7L)
    assert(delegate.remoteBytesRead.get() == 4096L)
    assert(delegate.fetchWaitTimeMs.get() == 2L)
  }

  private def accumulatesSubMillisecondWaits(): Unit = {
    val delegate = new RecordingMetrics
    val reporter = new RemoteFetchCostMetricsReporter(delegate, fixedWait(5000L))

    (0 until 199).foreach(_ => reporter.incRemoteBytesRead(1L))
    assert(delegate.fetchWaitTimeMs.get() == 0L)

    reporter.incRemoteBytesRead(1L)
    assert(delegate.fetchWaitTimeMs.get() == 1L)

    (0 until 199).foreach(_ => reporter.incRemoteBytesRead(1L))
    assert(delegate.fetchWaitTimeMs.get() == 1L)

    reporter.incRemoteBytesRead(1L)
    assert(delegate.fetchWaitTimeMs.get() == 2L)
    assert(delegate.remoteBytesRead.get() == 400L)
  }

  private def keepsRemaindersReporterLocal(): Unit = {
    val delegate = new RecordingMetrics
    val first = new RemoteFetchCostMetricsReporter(delegate, fixedWait(5000L))
    val second = new RemoteFetchCostMetricsReporter(delegate, fixedWait(5000L))

    (0 until 100).foreach(_ => first.incRemoteBytesRead(1L))
    (0 until 100).foreach(_ => second.incRemoteBytesRead(1L))
    assert(delegate.fetchWaitTimeMs.get() == 0L)

    (0 until 100).foreach(_ => first.incRemoteBytesRead(1L))
    assert(delegate.fetchWaitTimeMs.get() == 1L)
  }

  private def accumulatesSubMillisecondWaitsConcurrently(): Unit = {
    val delegate = new RecordingMetrics
    val reporter = new RemoteFetchCostMetricsReporter(delegate, fixedWait(5000L))
    val threadCount = 8
    val callsPerThread = 25
    val start = new CountDownLatch(1)
    val done = new CountDownLatch(threadCount)
    val failure = new AtomicReference[Throwable]()
    val executor = Executors.newFixedThreadPool(threadCount)

    (0 until threadCount).foreach { _ =>
      executor.execute(new Runnable {
        override def run(): Unit = {
          try {
            start.await()
            (0 until callsPerThread).foreach(_ => reporter.incRemoteBytesRead(1L))
          } catch {
            case error: Throwable => failure.compareAndSet(null, error)
          } finally {
            done.countDown()
          }
        }
      })
    }

    start.countDown()
    assert(done.await(10L, TimeUnit.SECONDS))
    executor.shutdownNow()
    if (failure.get() != null) throw failure.get()

    assert(delegate.fetchWaitTimeMs.get() == 1L)
    assert(delegate.remoteBytesRead.get() == threadCount * callsPerThread)
  }

  private def disabledReporterPathDoesNotWrap(): Unit = {
    val delegate = new RecordingMetrics
    val effective = RemoteFetchCostMetricsReporter.wrap(delegate, RemoteFetchCost.Disabled)
    assert(effective eq delegate)
  }

  private def fixedWait(waitNs: Long): RemoteFetchCost = new RemoteFetchCost {
    override def acquire(bytes: Long): Long = waitNs
  }

  private def remainsASortShuffleManager(): Unit = {
    assert(classOf[SortShuffleManager].isAssignableFrom(classOf[RemoteFetchCostShuffleManager]))
  }

  private def intercept[T <: Throwable : Manifest](body: => Unit): T = {
    try {
      body
      throw new AssertionError(s"Expected ${manifest[T].runtimeClass.getName}")
    } catch {
      case error: Throwable if manifest[T].runtimeClass.isInstance(error) =>
        error.asInstanceOf[T]
    }
  }

  private final class RecordingMetrics extends ShuffleReadMetricsReporter {
    val localBytesRead = new AtomicLong(0L)
    val remoteBytesRead = new AtomicLong(0L)
    val fetchWaitTimeMs = new AtomicLong(0L)

    override private[spark] def incRemoteBlocksFetched(v: Long): Unit = ()
    override private[spark] def incLocalBlocksFetched(v: Long): Unit = ()
    override private[spark] def incRemoteBytesRead(v: Long): Unit = remoteBytesRead.addAndGet(v)
    override private[spark] def incRemoteBytesReadToDisk(v: Long): Unit = ()
    override private[spark] def incLocalBytesRead(v: Long): Unit = localBytesRead.addAndGet(v)
    override private[spark] def incFetchWaitTime(v: Long): Unit = fetchWaitTimeMs.addAndGet(v)
    override private[spark] def incRecordsRead(v: Long): Unit = ()
    override private[spark] def incCorruptMergedBlockChunks(v: Long): Unit = ()
    override private[spark] def incMergedFetchFallbackCount(v: Long): Unit = ()
    override private[spark] def incRemoteMergedBlocksFetched(v: Long): Unit = ()
    override private[spark] def incLocalMergedBlocksFetched(v: Long): Unit = ()
    override private[spark] def incRemoteMergedChunksFetched(v: Long): Unit = ()
    override private[spark] def incLocalMergedChunksFetched(v: Long): Unit = ()
    override private[spark] def incRemoteMergedBytesRead(v: Long): Unit = ()
    override private[spark] def incLocalMergedBytesRead(v: Long): Unit = ()
    override private[spark] def incRemoteReqsDuration(v: Long): Unit = ()
    override private[spark] def incRemoteMergedReqsDuration(v: Long): Unit = ()
  }
}
