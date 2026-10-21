package org.scache.network.ub

import java.nio.ByteBuffer

/** Test-only registration stub. No payload path, JNI, or hardware is used. */
object UbRmCtpLeaseSafetyTest {
  private var quiet = false
  private def report(message: String): Unit = if (!quiet) println(message)
  private final class RegistrationOnly extends UbTransport {
    private var closed = false
    override def capabilities(): UbTransportCapabilities =
      throw new UnsupportedOperationException("registration-only test")
    override def localEndpoint(): Array[Byte] = Array.emptyByteArray
    override def connect(endpoint: Array[Byte]): Unit =
      throw new UnsupportedOperationException("registration-only test")
    override def registerBuffer(buffer: ByteBuffer): UbRegisteredBuffer =
      new UbRegisteredBuffer(this, buffer, 1L, buffer, 4096L, buffer.capacity().toLong, 1)
    override def unregisterBuffer(buffer: UbRegisteredBuffer): Unit = { buffer.markUnregistered(); () }
    override def read(r: RemoteBuffer, o: Long, b: ByteBuffer, l: Int, n: Int): Long =
      throw new UnsupportedOperationException("no test data path")
    override def write(r: RemoteBuffer, o: Long, b: ByteBuffer, l: Int, n: Int): Long =
      throw new UnsupportedOperationException("no test data path")
    override def waitFor(id: Long, timeout: Int): Int =
      throw new UnsupportedOperationException("no test data path")
    override def metrics(): UbTransportMetrics = UbTransportMetrics.builder("registration-test").build()
    override def isClosed(): Boolean = closed
    override def close(): Unit = { closed = true }
  }

  def main(args: Array[String]): Unit = {
    if (args.nonEmpty) {
      require(args.length == 2 && args(0) == "--stress-seconds")
      val seconds = args(1).toLong
      require(seconds > 0 && seconds <= 7200)
      quiet = true
      val start = System.nanoTime()
      var nextReport = start
      var rounds = 0L
      while (System.nanoTime() - start < seconds * 1000000000L) {
        interruptedUnpublishCanRetry(false)
        interruptedUnpublishCanRetry(true)
        concurrentRetirementIsExclusive()
        closeAndRetirementDoNotDoubleFree()
        rounds += 1
        val now = System.nanoTime()
        if (now >= nextReport) {
          println(s"LEASE_STRESS_PROGRESS rounds=$rounds cases=${rounds * 4} elapsed_seconds=${(now - start) / 1000000000L}")
          nextReport = now + 30000000000L
        }
      }
      println(s"LEASE_STRESS_PASS rounds=$rounds cases=${rounds * 4} duration_seconds=$seconds (registration stub; NOT UB)")
      return
    }
    interruptedUnpublishCanRetry(false)
    interruptedUnpublishCanRetry(true)
    concurrentRetirementIsExclusive()
    closeAndRetirementDoNotDoubleFree()
    val transport = new RegistrationOnly
    val arena = new UBBlockTransferService.RegisteredArena(transport, 4096, "source", 1L)
    arena.publish("block", Array.fill[Byte](64)(42))
    val lease = arena.acquire("block", "peer-connection").getOrElse(sys.error("missing lease"))
    // A TCP disconnect says nothing about whether the remote NIC still reads.
    arena.releaseOwner("peer-connection")
    var rejected = false
    try arena.unpublish("block", 0)
    catch { case _: IllegalStateException => rejected = true }
    println(s"DISCONNECT_RECLAIM rejected=$rejected freeBytes=${arena.freeBytes}")
    if (!rejected) {
      arena.close(1)
      throw new AssertionError("remote source slice was reclaimed on TCP disconnect without DMA-drain proof")
    }
    assert(arena.freeBytes == 4096 - 64, "disconnected source memory was reused")
    assert(arena.releaseDetailed(lease.leaseId, lease.generation, "peer-connection") !=
      UBBlockTransferService.Released, "orphan lease accepted ordinary release")
    var closeRejected = false
    try arena.close(1) catch { case _: IllegalStateException => closeRejected = true }
    assert(closeRejected && arena.isRegistered, "close reclaimed unresolved remote DMA memory")

    val normal = new UBBlockTransferService.RegisteredArena(transport, 4096, "normal", 2L)
    normal.publish("block", Array.fill[Byte](64)(42))
    val normalLease = normal.acquire("block", "normal-peer").get
    assert(normal.releaseDetailed(normalLease.leaseId, normalLease.generation, "normal-peer") ==
      UBBlockTransferService.Released)
    normal.releaseOwner("normal-peer")
    normal.unpublish("block", 0)
    assert(normal.freeBytes == 4096)
    normal.close(1)
    val pendingWrite = new UBBlockTransferService.RegisteredArena(transport, 4096, "write", 3L)
    val prepared = pendingWrite.prepare("write-block", 64, -1L, 0, 1, "writer")
    pendingWrite.releaseOwner("writer")
    var abortRejected = false
    try pendingWrite.abort(prepared.leaseId, prepared.generation, "writer")
    catch { case _: IllegalStateException => abortRejected = true }
    assert(abortRejected && pendingWrite.freeBytes == 4032, "orphan WRITE was aborted and reused")
    var retryRejected = false
    try pendingWrite.discardForRetry("write-block", "writer", 0)
    catch { case _: IllegalStateException => retryRejected = true }
    assert(retryRejected, "retry reclaimed an orphan WRITE")

    val serviceTransport = new RegistrationOnly
    val pool = new UBBlockTransferService.RegisteredArenaPool(
      serviceTransport, 4096, 1, "service", 4L, 60000)
    pool.prepare("pending", 64, -1L, 0, 1, "remote-writer")
    pool.releaseOwner("remote-writer")
    val testHome = java.nio.file.Files.createTempDirectory("scache-lease-contract-")
    java.nio.file.Files.createDirectory(testHome.resolve("conf"))
    java.nio.file.Files.write(testHome.resolve("conf/log4j.properties"),
      ("log4j.rootLogger=ERROR, console\n" +
       "log4j.appender.console=org.apache.log4j.ConsoleAppender\n" +
       "log4j.appender.console.layout=org.apache.log4j.PatternLayout\n" +
       "log4j.appender.console.layout.ConversionPattern=%m%n\n")
        .getBytes(java.nio.charset.StandardCharsets.UTF_8))
    println(s"TEST_CONF_HOME=$testHome")
    val conf = new org.scache.util.ScacheConf(testHome.toString)
    conf.set("spark.urma.unpublishTimeoutMs", "1")
    val service = new UBBlockTransferService(conf, "127.0.0.1", 1)
    // Test-only field injection avoids loading JNI and permits testing actual
    // service shutdown after arena quarantine, not a duplicate implementation.
    for ((name, value) <- Seq("arena" -> pool, "transport" -> serviceTransport)) {
      val field = classOf[UBBlockTransferService].getDeclaredField(name)
      field.setAccessible(true)
      field.set(service, value)
    }
    var serviceRejected = false
    try service.close() catch { case _: IllegalStateException => serviceRejected = true }
    assert(serviceRejected && !serviceTransport.isClosed(), "service bypassed arena close failure")
    // The first arena intentionally remains quarantined. Registration is a
    // test-only Java object: no hardware resource or provider handle is leaked.
    println("SOURCE_LEASE_SAFETY_PASS (test-only registration; not hardware verification)")
  }

  private def interruptedUnpublishCanRetry(replace: Boolean): Unit = {
    val arena = new UBBlockTransferService.RegisteredArena(new RegistrationOnly, 4096, "interrupt", 5L)
    arena.publish("block", Array.fill[Byte](64)(1))
    val lease = arena.acquire("block", "reader").get
    val interrupted = new java.util.concurrent.atomic.AtomicBoolean(false)
    val restored = new java.util.concurrent.atomic.AtomicBoolean(false)
    val unexpected = new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val waiter = new Thread(() => {
      try {
        if (replace) arena.discardForRetry("block", "writer", 10000)
        else arena.unpublish("block", 10000)
      }
      catch {
        case _: InterruptedException =>
          interrupted.set(true); restored.set(Thread.currentThread().isInterrupted)
        case e: Throwable => unexpected.set(e)
      }
    }, "test-unpublish-interruption")
    waiter.setDaemon(true); waiter.start()
    val until = System.nanoTime() + 3000000000L
    while (arena.stateOf("block") != "Unpublishing" && waiter.isAlive && System.nanoTime() < until)
      Thread.sleep(1)
    require(arena.stateOf("block") == "Unpublishing", "waiter did not enter lease drain")
    waiter.interrupt(); waiter.join(3000)
    require(!waiter.isAlive && interrupted.get() && unexpected.get() == null, "interruption not propagated")
    require(arena.stateOf("block") == "Published", "interrupted unpublish left block stuck")
    require(restored.get(), "interruption flag not preserved")
    require(arena.freeBytes == 4032 && arena.activeLeases == 1, "interruption freed active source")
    require(arena.releaseDetailed(lease.leaseId, lease.generation, "reader") == UBBlockTransferService.Released)
    arena.unpublish("block", 0)
    require(arena.freeBytes == 4096)
    arena.close(1)
    report(s"INTERRUPTED_UNPUBLISH_RECOVERY_PASS replace=$replace (registration stub; NOT UB)")
  }

  private def concurrentRetirementIsExclusive(): Unit = {
    val arena = new UBBlockTransferService.RegisteredArena(new RegistrationOnly, 4096, "retire", 6L)
    arena.publish("block", Array.fill[Byte](64)(1))
    val lease = arena.acquire("block", "reader").get
    val failure = new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val first = new Thread(() => {
      try arena.unpublish("block", 10000)
      catch { case e: Throwable => failure.set(e) }
    }, "test-first-retirement")
    first.setDaemon(true); first.start()
    try {
      val until = System.nanoTime() + 3000000000L
      while (arena.stateOf("block") != "Unpublishing" && first.isAlive && System.nanoTime() < until)
        Thread.sleep(1)
      require(arena.stateOf("block") == "Unpublishing")
      var rejected = false
      try arena.discardForRetry("block", "writer", 0)
      catch { case _: IllegalStateException => rejected = true }
      require(rejected, "concurrent retirement accepted")
      require(arena.stateOf("block") == "Unpublishing", "retry overwrote another retire operation's state")
      require(arena.acquire("block", "late-reader").isEmpty, "retiring source became readable again")
    } finally {
      arena.releaseDetailed(lease.leaseId, lease.generation, "reader")
      first.join(3000)
    }
    require(!first.isAlive && failure.get() == null, "first retirement failed")
    require(arena.freeBytes == 4096 && arena.stateOf("block") == "FREE")
    arena.close(1)
    report("CONCURRENT_RETIREMENT_EXCLUSIVE_PASS (registration stub; NOT UB)")
  }

  private def closeAndRetirementDoNotDoubleFree(): Unit = {
    val arena = new UBBlockTransferService.RegisteredArena(new RegistrationOnly, 4096, "close-race", 7L)
    arena.publish("block", Array.fill[Byte](64)(1))
    val lease = arena.acquire("block", "reader").get
    val failure = new java.util.concurrent.atomic.AtomicReference[Throwable]()
    val first = new Thread(() => {
      try arena.unpublish("block", 10000)
      catch { case e: Throwable => failure.set(e) }
    }, "test-close-retirement")
    first.setDaemon(true); first.start()
    val until = System.nanoTime() + 3000000000L
    while (arena.stateOf("block") != "Unpublishing" && first.isAlive && System.nanoTime() < until)
      Thread.sleep(1)
    require(arena.stateOf("block") == "Unpublishing")
    // Deterministic interleaving: close wins the monitor after lease release,
    // then the earlier unpublish resumes with a stale Entry reference.
    arena.synchronized {
      arena.releaseDetailed(lease.leaseId, lease.generation, "reader")
      arena.close(1)
    }
    first.join(3000)
    require(!first.isAlive && failure.get() == null, "retirement after close failed")
    require(arena.freeBytes == 4096, "stale retirement double-freed a slice already freed by close")
    require(!arena.isRegistered)
    report("CLOSE_RETIREMENT_NO_DOUBLE_FREE_PASS (registration stub; NOT UB)")
  }
}
