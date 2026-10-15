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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager

/**
 * A drop-in sort shuffle manager that can add a modeled cost to successful remote fetches.
 *
 * The implementation remains a subtype of Spark's [[SortShuffleManager]] so Spark SQL retains
 * its normal no-copy fast paths. When the cost model is disabled it passes the original read
 * metrics reporter through unchanged, keeping the fetch hot path identical to Spark's.
 */
class RemoteFetchCostShuffleManager(conf: SparkConf, isDriver: Boolean)
  extends SortShuffleManager(conf) {
  private val remoteFetchCost = RemoteFetchCost.fromSparkConf(conf)

  def this(conf: SparkConf) = this(conf, false)

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val effectiveMetrics = RemoteFetchCostMetricsReporter.wrap(metrics, remoteFetchCost)
    super.getReader(
      handle,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition,
      context,
      effectiveMetrics)
  }
}

/**
 * Charges exactly once for each remote-byte increment emitted by Spark's standard block fetcher.
 * Spark 3.5 emits that increment once, with the fetched buffer size, when a successful remote
 * block is handed to the consuming task thread.
 */
private[scache] final class RemoteFetchCostMetricsReporter(
    delegate: ShuffleReadMetricsReporter,
    remoteFetchCost: RemoteFetchCost) extends ShuffleReadMetricsReporter {

  private val modeledWaitRemainderNs = new AtomicLong(0L)

  /**
   * Convert this reporter's modeled waits to Spark's millisecond metric without discarding each
   * buffer's sub-millisecond remainder. A reporter belongs to one shuffle reader, so keeping the
   * remainder here also prevents unrelated tasks from borrowing or consuming each other's wait.
   *
   * Although Spark normally calls a read metrics reporter from the consuming task thread, the
   * CAS loop keeps the accounting exact if a caller invokes it concurrently.
   */
  private def modeledWaitMillis(waitNs: Long): Long = {
    if (waitNs <= 0L) return 0L

    val wholeMillis = waitNs / RemoteFetchCostMetricsReporter.NanosPerMillisecond
    val nanos = waitNs % RemoteFetchCostMetricsReporter.NanosPerMillisecond
    if (nanos == 0L) return wholeMillis

    var observed = modeledWaitRemainderNs.get()
    while (true) {
      // Both operands are below one millisecond, so this addition cannot overflow.
      val accumulated = observed + nanos
      val carriedMillis = accumulated / RemoteFetchCostMetricsReporter.NanosPerMillisecond
      val updatedRemainder =
        accumulated % RemoteFetchCostMetricsReporter.NanosPerMillisecond
      if (modeledWaitRemainderNs.compareAndSet(observed, updatedRemainder)) {
        return wholeMillis + carriedMillis
      }
      observed = modeledWaitRemainderNs.get()
    }
    0L
  }

  override private[spark] def incRemoteBlocksFetched(v: Long): Unit =
    delegate.incRemoteBlocksFetched(v)

  override private[spark] def incLocalBlocksFetched(v: Long): Unit =
    delegate.incLocalBlocksFetched(v)

  override private[spark] def incRemoteBytesRead(v: Long): Unit = {
    val modeledWaitNs = remoteFetchCost.acquire(v)
    val modeledWaitMs = modeledWaitMillis(modeledWaitNs)
    if (modeledWaitMs > 0L) {
      delegate.incFetchWaitTime(modeledWaitMs)
    }
    delegate.incRemoteBytesRead(v)
  }

  override private[spark] def incRemoteBytesReadToDisk(v: Long): Unit =
    delegate.incRemoteBytesReadToDisk(v)

  override private[spark] def incLocalBytesRead(v: Long): Unit =
    delegate.incLocalBytesRead(v)

  override private[spark] def incFetchWaitTime(v: Long): Unit =
    delegate.incFetchWaitTime(v)

  override private[spark] def incRecordsRead(v: Long): Unit =
    delegate.incRecordsRead(v)

  override private[spark] def incCorruptMergedBlockChunks(v: Long): Unit =
    delegate.incCorruptMergedBlockChunks(v)

  override private[spark] def incMergedFetchFallbackCount(v: Long): Unit =
    delegate.incMergedFetchFallbackCount(v)

  override private[spark] def incRemoteMergedBlocksFetched(v: Long): Unit =
    delegate.incRemoteMergedBlocksFetched(v)

  override private[spark] def incLocalMergedBlocksFetched(v: Long): Unit =
    delegate.incLocalMergedBlocksFetched(v)

  override private[spark] def incRemoteMergedChunksFetched(v: Long): Unit =
    delegate.incRemoteMergedChunksFetched(v)

  override private[spark] def incLocalMergedChunksFetched(v: Long): Unit =
    delegate.incLocalMergedChunksFetched(v)

  override private[spark] def incRemoteMergedBytesRead(v: Long): Unit =
    delegate.incRemoteMergedBytesRead(v)

  override private[spark] def incLocalMergedBytesRead(v: Long): Unit =
    delegate.incLocalMergedBytesRead(v)

  override private[spark] def incRemoteReqsDuration(v: Long): Unit =
    delegate.incRemoteReqsDuration(v)

  override private[spark] def incRemoteMergedReqsDuration(v: Long): Unit =
    delegate.incRemoteMergedReqsDuration(v)
}

private[scache] object RemoteFetchCostMetricsReporter {
  private val NanosPerMillisecond = TimeUnit.MILLISECONDS.toNanos(1L)

  /** Preserve the disabled hot path: do not allocate or add a forwarding layer. */
  def wrap(
      delegate: ShuffleReadMetricsReporter,
      remoteFetchCost: RemoteFetchCost): ShuffleReadMetricsReporter = remoteFetchCost match {
    case RemoteFetchCost.Disabled => delegate
    case enabled => new RemoteFetchCostMetricsReporter(delegate, enabled)
  }
}
