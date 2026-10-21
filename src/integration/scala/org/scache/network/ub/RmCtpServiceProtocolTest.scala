package org.scache.network.ub

import java.io.{DataInputStream, DataOutputStream}
import java.net.{InetAddress, ServerSocket, Socket}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, StandardOpenOption}
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong, AtomicReference}
import scala.util.control.NonFatal
import org.scache.storage.ScacheBlockId
import org.scache.util.ScacheConf

/** Actual service/TCP/arena lifecycle with test-only in-JVM memory copying.
  * Not a UB provider, not DMA, and not a two-machine performance test.
  */
object RmCtpServiceProtocolTest {
  private val seq = new AtomicLong(1)
  private val memory = new ConcurrentHashMap[Long, ByteBuffer]()
  private class CopyTransport extends UbTransport {
    var rejectNextRead = false
    var corruptNextRead = false
    var failNextCompletion = false
    var readStarted: CountDownLatch = _
    var continueRead: CountDownLatch = _
    private val owned = scala.collection.mutable.Set.empty[Long]
    private var operations = 0L
    private var bytes = 0L
    private var closed = false
    private val ep = new EndpointDescriptor()
    ep.transportMode = 1; ep.tpType = 1; ep.jettyId = seq.getAndIncrement().toInt
    override def capabilities(): UbTransportCapabilities = RealUbTransport.capabilitiesFor(32, 4096)
    override def localEndpoint(): Array[Byte] = ep.toBytes()
    override def connect(raw: Array[Byte]): Unit = { EndpointDescriptor.fromBytes(raw); () }
    override def registerBuffer(b: ByteBuffer): UbRegisteredBuffer = synchronized {
      // Exercise the full descriptor/control/read path with a valid UBVA whose
      // high bit is set. Java represents this unsigned address as a negative long.
      val id = seq.getAndIncrement(); val address = Long.MinValue + id * 0x1000000L
      memory.put(address, b); owned += address
      val mr = new SegmentDescriptor(new Array[Byte](16), ep.jettyId, 0, address, b.capacity(), 1, id)
      new UbRegisteredBuffer(this, b, id, id, b, address, b.capacity(), 1, mr)
    }
    override def unregisterBuffer(b: UbRegisteredBuffer): Unit = synchronized {
      require(owned.remove(b.remoteAddress), "unregister wrong owner")
      memory.remove(b.remoteAddress); b.markUnregistered(); ()
    }
    override def read(r: RemoteBuffer, offset: Long, dest: ByteBuffer, local: Int, n: Int): Long = synchronized {
      if (rejectNextRead) {
        rejectNextRead = false
        throw new IllegalStateException("TEST_READ_REJECTED_BEFORE_POST")
      }
      val window = RealUbTransport.window(r, offset, n)
      require(window.segment != null)
      val source = Option(memory.get(window.segment.address)).getOrElse(sys.error("unknown test MR"))
      val start = Math.toIntExact(window.remoteAddress - window.segment.address)
      val src = source.duplicate(); src.position(start); src.limit(start + n)
      val out = dest.duplicate(); out.position(local); out.put(src)
      if (readStarted != null) {
        readStarted.countDown()
        require(continueRead.await(5, TimeUnit.SECONDS), "timed out waiting to finish test read")
      }
      if (corruptNextRead) {
        corruptNextRead = false
        dest.put(local, (dest.get(local) ^ 1).toByte)
      }
      operations += 1; bytes += n; operations
    }
    override def write(r: RemoteBuffer, o: Long, b: ByteBuffer, l: Int, n: Int): Long =
      throw new UnsupportedOperationException("READ service test only")
    override def waitFor(id: Long, timeout: Int): Int = synchronized {
      require(id > 0 && id <= operations)
      // The test copy is synchronous: a failed completion here is already drained.
      if (failNextCompletion) { failNextCompletion = false; -1 } else 0
    }
    override def metrics(): UbTransportMetrics = synchronized {
      UbTransportMetrics.builder("test-copy-not-urma").operations(operations, operations, 0, 0, 0)
        .bytes(bytes, bytes, 0, 0).registeredRegions(owned.size).build()
    }
    override def isClosed(): Boolean = closed
    override def close(): Unit = { require(owned.isEmpty, "registered memory survived close"); closed = true }
  }
  private def field(service: UBBlockTransferService, name: String): java.lang.reflect.Field = {
    val f = classOf[UBBlockTransferService].getDeclaredField(name); f.setAccessible(true); f
  }
  private def start(label: String, strict: Boolean = false): UBBlockTransferService = {
    val home = Files.createTempDirectory("scache-service-contract-")
    Files.createDirectory(home.resolve("conf")); Files.createDirectory(home.resolve("pool"))
    Files.writeString(home.resolve("conf/log4j.properties"), "log4j.rootLogger=OFF\n")
    val conf = new ScacheConf(home.toString)
    conf.set("scache.ub.pool.enabled", "true")
    conf.set("scache.ub.pool.path", home.resolve("pool").toString)
    conf.set("spark.urma.strict", strict.toString)
    conf.set("spark.urma.transferTimeoutMs", "3000")
    conf.set("spark.urma.unpublishTimeoutMs", "1")
    conf.set("spark.urma.nodeId", label)
    val s = new UBBlockTransferService(conf, "127.0.0.1", 1)
    val t = new CopyTransport
    val pool = new UBBlockTransferService.RegisteredArenaPool(t, 1048576, 3, label,
      field(s, "epoch").getLong(s), 30000, Some(home.resolve("pool").toString))
    val socket = new ServerSocket(0, 64, InetAddress.getLoopbackAddress)
    field(s, "transport").set(s, t); field(s, "arena").set(s, pool)
    field(s, "endpoint").set(s, t.localEndpoint()); field(s, "server").set(s, socket)
    val accept = classOf[UBBlockTransferService].getDeclaredMethod("acceptLoop")
    accept.setAccessible(true)
    val thread = new Thread(() => { accept.invoke(s); () }, "test-control-" + label)
    thread.setDaemon(true); field(s, "acceptThread").set(s, thread); thread.start()
    println(s"TEST_SERVICE label=$label port=${s.port} dir=$home")
    s
  }
  def main(args: Array[String]): Unit = {
    val addressHome = Files.createTempDirectory("scache-advertise-host-")
    Files.createDirectory(addressHome.resolve("conf"))
    Files.writeString(addressHome.resolve("conf/log4j.properties"),
      "log4j.rootLogger=OFF\n")
    val addressConf = new ScacheConf(addressHome.toString)
    addressConf.set("spark.urma.controlBindHost", "0.0.0.0")
    addressConf.set("spark.urma.controlAdvertiseHost", "192.0.2.10")
    val addressService = new UBBlockTransferService(addressConf, "127.0.0.1", 1)
    require(addressService.hostName == "192.0.2.10",
      "BlockManager must publish the configured UB control advertise host")
    addressService.close()
    rejectedControlHandoffClosesSocket()
    allocationFailureReleasesSource()
    Seq("submit", "completion", "checksum").foreach(failedReadCanRetry)
    val a = start("test-A"); val b = start("test-B")
    val sizes = Seq(64, 700000, 700001)
    val services = Seq(a, b)
    def id(node: Int, i: Int) = ScacheBlockId("contract", 1, 1, node, i)
    def payload(node: Int, i: Int, n: Int) = Array.tabulate[Byte](n)(j => (j * 31 + node * 7 + i).toByte)
    try {
      for (node <- services.indices; (n, i) <- sizes.zipWithIndex)
        services(node).publishBlock(id(node, i), payload(node, i, n))
      for (node <- services.indices; (n, i) <- sizes.zipWithIndex) {
        val source = services(1 - node); val dest = services(node); val block = id(1 - node, i)
        dest.reservePoolConsumer(block); dest.reservePoolConsumer(block)
        val slice = dest.fetchBlockToPool("127.0.0.1", source.port, block.toString).get
        val f = FileChannel.open(java.nio.file.Paths.get(slice.path), StandardOpenOption.READ)
        try {
          val buffer = ByteBuffer.allocate(n); var pos = slice.offset
          while (buffer.hasRemaining) { val count = f.read(buffer, pos); require(count > 0); pos += count }
          require(java.util.Arrays.equals(buffer.array(), payload(1 - node, i, n)), "byte mismatch")
        } finally f.close()
        if (node == 0 && i == 0) {
          val pool = field(dest, "arena").get(dest)
            .asInstanceOf[UBBlockTransferService.RegisteredArenaPool]
          val remoteLease = pool.acquire(block.toString, "test-reader").get
          var rejected = false
          try dest.unpublishBlock(block)
          catch { case _: IllegalStateException => rejected = true }
          require(rejected, "unpublish must not recycle a remotely leased block")
          require(pool.release(remoteLease.leaseId, remoteLease.generation, "test-reader") ==
            UBBlockTransferService.Released)
          // Rejected unpublish must preserve both existing local consumers.
        }
        require(dest.releaseImportedPoolBlock(block)); require(dest.localPoolBlock(block).nonEmpty)
        require(dest.releaseImportedPoolBlock(block)); require(dest.localPoolBlock(block).isEmpty)
      }
      for (node <- services.indices; i <- sizes.indices) services(node).unpublishBlock(id(node, i))
      for (s <- services) {
        val m = s.urmaMetrics
        require(m("urma.arenaFreeBytes") == 3L * 1048576)
        require(m("urma.activeLeases") == 0 && m("urma.arenaPublishedBlocks") == 0)
        require(m("urma.submittedBytes") == sizes.sum && m("urma.completedBytes") == sizes.sum)
      }
    } finally { try a.close() finally b.close() }
    require(memory.isEmpty, "test registrations leaked")
    releaseAckFailureDoesNotPublish()
    staleGrantReleasesSource()
    localConsumerBlocksRetry()
    closeWaitsForLocalConsumer()
    strictCleanupReportsActiveReader()
    shuffleCleanupRejectsLateFetch()
    shuffleCleanupRejectsLateOwnerCommit()
    applicationCleanupIsScoped()
    println("RM_CTP_SERVICE_PROTOCOL_PASS blocks=6 (two services; TCP control; test memory copy; NOT UB)")
  }

  private def rejectedControlHandoffClosesSocket(): Unit = {
    val service = start("rejected-control-handoff")
    val workers = field(service, "workers").get(service)
      .asInstanceOf[java.util.concurrent.ExecutorService]
    workers.shutdownNow()
    val socket = new Socket("127.0.0.1", service.port)
    socket.setSoTimeout(2000)
    try {
      require(socket.getInputStream.read() == -1,
        "rejected control connection remained open after executor shutdown")
      println("RM_CTP_REJECTED_CONTROL_HANDOFF_PASS (TCP only; NOT UB)")
    } finally {
      socket.close()
      service.close()
    }
  }

  private def applicationCleanupIsScoped(): Unit = {
    val source = start("app-scope-source")
    val dest = start("app-scope-dest")
    val oldBlock = ScacheBlockId("retired-app", 1, 1, 0, 0)
    val liveBlock = ScacheBlockId("live-app", 1, 1, 0, 0)
    try {
      source.publishBlock(oldBlock, Array.fill[Byte](64)(7))
      source.publishBlock(liveBlock, Array.fill[Byte](64)(9))
      dest.fetchBlockToPool("127.0.0.1", source.port, oldBlock.toString).get
      dest.fetchBlockToPool("127.0.0.1", source.port, liveBlock.toString).get
      dest.releaseCachedRemoteImportsForApplication("retired-app")
      require(dest.localPoolBlock(oldBlock).isEmpty, "application cleanup retained its old slice")
      require(dest.localPoolBlock(liveBlock).nonEmpty,
        "application cleanup retired another application's slice")
      var rejected = false
      try dest.reservePoolConsumer(oldBlock)
      catch { case _: IllegalStateException => rejected = true }
      require(rejected, "retired application accepted a new reservation")
      dest.unpublishBlock(liveBlock)
      source.retireApplicationNamespace("retired-app")
      source.unpublishBlock(oldBlock)
      rejected = false
      try source.publishBlock(oldBlock, Array.fill[Byte](64)(1))
      catch { case _: IllegalStateException => rejected = true }
      require(rejected, "retired application accepted a late owner publication")
      source.unpublishBlock(liveBlock)
      println("RM_CTP_APPLICATION_CLEANUP_SCOPE_PASS (arena lifecycle; NOT UB)")
    } finally { try dest.close() finally source.close() }
  }

  private def shuffleCleanupRejectsLateFetch(): Unit = {
    val source = start("cleanup-race-source")
    val dest = start("cleanup-race-dest")
    val block = ScacheBlockId("cleanup-race", 1, 1, 0, 0)
    val transport = field(dest, "transport").get(dest).asInstanceOf[CopyTransport]
    val started = new CountDownLatch(1)
    val proceed = new CountDownLatch(1)
    transport.readStarted = started
    transport.continueRead = proceed
    val failure = new AtomicReference[Throwable]()
    try {
      source.publishBlock(block, Array.fill[Byte](64)(61))
      val initialFree = dest.urmaMetrics("urma.arenaFreeBytes")
      dest.reservePoolConsumer(block)
      val fetcher = new Thread(() => {
        try dest.fetchBlockToPool("127.0.0.1", source.port, block.toString)
        catch { case NonFatal(e) => failure.set(e) }
        finally dest.cancelPoolConsumer(block)
      }, "cleanup-race-fetcher")
      fetcher.start()
      require(started.await(5, TimeUnit.SECONDS), "remote READ did not start")
      dest.releaseCachedRemoteImportsForShuffle("cleanup-race", 1, 1)
      proceed.countDown()
      fetcher.join(5000)
      require(!fetcher.isAlive, "late remote fetch did not finish")
      require(Option(failure.get()).exists(_.getMessage.contains("shuffle retired")),
        s"late fetch was not rejected: ${failure.get()}")
      require(dest.localPoolBlock(block).isEmpty, "retired shuffle published a late destination")
      require(dest.urmaMetrics("urma.arenaFreeBytes") == initialFree,
        "late fetch did not return its destination slice")
      require(source.urmaMetrics("urma.activeLeases") == 0, "late fetch retained source lease")
      var rejected = false
      try dest.reservePoolConsumer(block)
      catch { case _: IllegalStateException => rejected = true }
      require(rejected, "new reservation was accepted after shuffle retirement")
      source.unpublishBlock(block)
      println("RM_CTP_LATE_FETCH_CLEANUP_PASS (fault-injected timing; NOT UB)")
    } finally {
      proceed.countDown()
      try dest.close() finally source.close()
    }
  }

  private def shuffleCleanupRejectsLateOwnerCommit(): Unit = {
    val service = start("cleanup-late-owner")
    val oldBlock = ScacheBlockId("cleanup-owner", 1, 1, 0, 0)
    val liveBlock = ScacheBlockId("cleanup-owner", 1, 2, 0, 0)
    try {
      val initialFree = service.urmaMetrics("urma.arenaFreeBytes")
      val pending = service.preparePoolBlock(oldBlock, 64).get
      service.retireShuffleNamespace("cleanup-owner", 1, 1)
      def retired(operation: => Any): Unit = {
        var rejected = false
        try operation
        catch { case e: IllegalStateException if e.getMessage.contains("shuffle retired") =>
          rejected = true
        }
        require(rejected, "retired shuffle accepted a late owner operation")
      }
      retired(service.commitPoolBlock(oldBlock, pending.path, pending.offset, pending.length))
      require(service.abortPoolBlock(oldBlock, pending.path, pending.offset, pending.length),
        "retired pending slice could not be aborted")
      retired(service.preparePoolBlock(oldBlock, 64))
      retired(service.publishBlock(oldBlock, Array.fill[Byte](64)(1)))
      service.publishBlock(liveBlock, Array.fill[Byte](64)(2))
      service.unpublishBlock(liveBlock)
      require(service.localPoolBlock(oldBlock).isEmpty, "late owner block became visible")
      require(service.urmaMetrics("urma.arenaFreeBytes") == initialFree,
        "late owner reservation was not reclaimed")
      println("RM_CTP_LATE_OWNER_COMMIT_REJECTED_PASS (arena lifecycle; NOT UB)")
    } finally service.close()
  }

  private def localConsumerBlocksRetry(): Unit = {
    val service = start("local-reader-protection")
    val block = ScacheBlockId("local-reader", 1, 1, 0, 0)
    val initialFree = service.urmaMetrics("urma.arenaFreeBytes")
    try {
      service.publishBlock(block, Array.fill[Byte](64)(43))
      val published = service.localPoolBlock(block).get
      val occupied = service.urmaMetrics("urma.arenaFreeBytes")
      service.reservePoolConsumer(block)
      def denied(operation: => Any): Unit = {
        var blocked = false
        try operation
        catch { case e: IllegalStateException if e.getMessage.contains("local consumers are active") =>
          blocked = true
        }
        require(blocked, "active local reader did not block slice retirement")
      }
      denied(service.preparePoolBlock(block, 64))
      denied(service.unpublishBlock(block))
      denied(service.abortPoolBlock(block, published.path, published.offset, published.length))
      require(service.localPoolBlock(block).contains(published), "reader's slice changed during retry")
      require(service.urmaMetrics("urma.arenaFreeBytes") == occupied,
        "rejected retirement changed arena capacity")
      require(service.releaseImportedPoolBlock(block))
      val retry = service.preparePoolBlock(block, 64).get
      require(service.localPoolBlock(block).isEmpty, "retry did not remove the old published slice")
      require(service.abortPoolBlock(block, retry.path, retry.offset, retry.length))
      require(service.urmaMetrics("urma.arenaFreeBytes") == initialFree,
        "aborted retry did not restore arena capacity")
      println("RM_CTP_LOCAL_CONSUMER_RETRY_GUARD_PASS (mmap lifetime; NOT UB)")
    } finally service.close()
  }

  private def closeWaitsForLocalConsumer(): Unit = {
    val service = start("local-close-protection")
    val block = ScacheBlockId("local-close", 1, 1, 0, 0)
    service.publishBlock(block, Array.fill[Byte](64)(47))
    service.reservePoolConsumer(block)
    try {
      var quarantined = false
      try service.close()
      catch { case e: IllegalStateException if e.getMessage.contains("UB_CLOSE_QUARANTINED") =>
        quarantined = true
      }
      require(quarantined, "close reclaimed a locally consumed pool slice")
      require(service.urmaMetrics("urma.registeredRegions") == 3,
        "close unregistered memory while a local reader was active")
      require(service.releaseImportedPoolBlock(block))
      service.unpublishBlock(block)
      service.close()
      println("RM_CTP_LOCAL_CONSUMER_CLOSE_GUARD_PASS (registered memory lifetime; NOT UB)")
    } finally {
      try service.close() catch { case NonFatal(_) => () }
    }
  }

  private def strictCleanupReportsActiveReader(): Unit = {
    val source = start("strict-cleanup-source")
    val dest = start("strict-cleanup-dest", strict = true)
    val block = ScacheBlockId("strict-cleanup", 1, 1, 0, 0)
    try {
      source.publishBlock(block, Array.fill[Byte](64)(51))
      dest.reservePoolConsumer(block)
      val slice = dest.fetchBlockToPool("127.0.0.1", source.port, block.toString).get
      var rejected = false
      try dest.releaseCachedRemoteImportsForShuffle("strict-cleanup", 1, 1)
      catch {
        case e: IllegalStateException if e.getMessage.contains("strict UB cleanup left") =>
          rejected = true
      }
      require(rejected, "strict shuffle cleanup silently accepted a live imported reader")
      require(dest.localPoolBlock(block).contains(slice),
        "failed strict cleanup reused a live imported slice")
      require(dest.releaseImportedPoolBlock(block), "reader release was lost after cleanup failure")
      dest.releaseCachedRemoteImportsForShuffle("strict-cleanup", 1, 1)
      source.unpublishBlock(block)
      println("RM_CTP_STRICT_CLEANUP_REPORTS_ACTIVE_READER_PASS (test copy; NOT UB)")
    } finally {
      try dest.close() finally source.close()
    }
  }

  /** Corrupt the producer epoch in a valid GRANT, after ACQUIRE created a lease. */
  private def staleGrantReleasesSource(): Unit = {
    val source = start("stale-source")
    val dest = start("stale-dest")
    val block = ScacheBlockId("stale-grant", 1, 1, 0, 0)
    source.publishBlock(block, Array.fill[Byte](64)(29))
    val initialFree = dest.urmaMetrics("urma.arenaFreeBytes")
    val proxy = new ServerSocket(0, 1, InetAddress.getLoopbackAddress)
    val corrupted = new AtomicBoolean(false)
    val released = new AtomicBoolean(false)
    val relay = new Thread(() => {
      var incoming: Socket = null
      var outgoing: Socket = null
      try {
        incoming = proxy.accept()
        outgoing = new Socket("127.0.0.1", source.port)
        val client = incoming
        val server = outgoing
        def forward(from: Socket, to: Socket, response: Boolean): Unit = {
          val input = new DataInputStream(from.getInputStream)
          val output = new DataOutputStream(to.getOutputStream)
          try {
            while (true) {
              val kind = input.readUnsignedByte()
              val version = input.readInt()
              val requestId = input.readLong()
              val length = input.readInt()
              require(length >= 0 && length <= 65536, "invalid test control frame")
              val payload = new Array[Byte](length)
              input.readFully(payload)
              if (response && kind == 3 && corrupted.compareAndSet(false, true)) {
                val nodeLength = ByteBuffer.wrap(payload, 4, 4).getInt()
                require(nodeLength > 0 && nodeLength < 8192)
                val epochAt = 8 + nodeLength
                val view = ByteBuffer.wrap(payload)
                view.putLong(epochAt, view.getLong(epochAt) + 1L)
              }
              if (!response && kind == 5) released.set(true)
              output.writeByte(kind)
              output.writeInt(version)
              output.writeLong(requestId)
              output.writeInt(length)
              output.write(payload)
              output.flush()
            }
          } catch { case NonFatal(_) => () }
        }
        val responses = new Thread(() => forward(server, client, response = true),
          "stale-grant-responses")
        responses.setDaemon(true)
        responses.start()
        forward(client, server, response = false)
      } catch { case NonFatal(_) => () }
      finally {
        if (incoming != null) incoming.close()
        if (outgoing != null) outgoing.close()
      }
    }, "stale-grant-proxy")
    relay.setDaemon(true)
    relay.start()
    try {
      var failed = false
      try dest.fetchBlockToPool("127.0.0.1", proxy.getLocalPort, block.toString)
      catch { case e: IllegalStateException if e.getMessage.contains("stale or mismatched UB descriptor") => failed = true }
      require(corrupted.get() && failed, "stale GRANT was not rejected")
      require(released.get(), "stale GRANT did not RELEASE the source lease")
      require(source.urmaMetrics("urma.activeLeases") == 0, "stale GRANT retained source lease")
      require(dest.urmaMetrics("urma.submittedBytes") == 0, "stale GRANT triggered DMA")
      require(dest.urmaMetrics("urma.arenaFreeBytes") == initialFree,
        "stale GRANT reserved a destination slice")
      source.unpublishBlock(block)
      println("RM_CTP_STALE_GRANT_RELEASE_PASS (fault-injected TCP; NOT UB)")
    } finally {
      proxy.close()
      relay.join(1000)
      try dest.close() finally source.close()
    }
  }

  /** Drop only the RELEASE request after a completed READ; destination must remain unpublished. */
  private def releaseAckFailureDoesNotPublish(): Unit = {
    val source = start("release-source")
    val dest = start("release-dest")
    val block = ScacheBlockId("release-fault", 1, 1, 0, 0)
    source.publishBlock(block, Array.fill[Byte](8193)(19))
    val initialFree = dest.urmaMetrics("urma.arenaFreeBytes")
    val proxy = new ServerSocket(0, 1, InetAddress.getLoopbackAddress)
    val dropped = new AtomicBoolean(false)
    val relay = new Thread(() => {
      var incoming: Socket = null
      var outgoing: Socket = null
      try {
        incoming = proxy.accept()
        outgoing = new Socket("127.0.0.1", source.port)
        val client = incoming
        val server = outgoing
        val responses = new Thread(() => {
          try server.getInputStream.transferTo(client.getOutputStream)
          catch { case NonFatal(_) => () }
        }, "release-fault-responses")
        responses.setDaemon(true)
        responses.start()
        val input = new DataInputStream(client.getInputStream)
        val output = new DataOutputStream(server.getOutputStream)
        var forwarding = true
        while (forwarding) {
          val kind = input.readUnsignedByte()
          val version = input.readInt()
          val requestId = input.readLong()
          val length = input.readInt()
          require(length >= 0 && length <= 65536, "invalid test control frame")
          val payload = new Array[Byte](length)
          input.readFully(payload)
          if (kind == 5) { // RELEASE; do not deliver it or its ACK.
            dropped.set(true)
            forwarding = false
          } else {
            output.writeByte(kind)
            output.writeInt(version)
            output.writeLong(requestId)
            output.writeInt(length)
            output.write(payload)
            output.flush()
          }
        }
      } catch { case NonFatal(_) => () }
      finally {
        if (incoming != null) incoming.close()
        if (outgoing != null) outgoing.close()
      }
    }, "release-fault-proxy")
    relay.setDaemon(true)
    relay.start()
    try {
      var failed = false
      try dest.fetchBlockToPool("127.0.0.1", proxy.getLocalPort, block.toString)
      catch { case NonFatal(_) => failed = true }
      require(dropped.get() && failed, "RELEASE failure was not injected and reported")
      require(dest.localPoolBlock(block).isEmpty, "failed RELEASE exposed a published destination")
      require(dest.urmaMetrics("urma.arenaFreeBytes") == initialFree,
        "failed RELEASE retained a destination slice")
      require(source.urmaMetrics("urma.activeLeases") == 1,
        "lost control connection should quarantine the source lease")
      println("RM_CTP_RELEASE_FAILURE_NO_PUBLISH_PASS (fault-injected TCP; NOT UB)")
    } finally {
      proxy.close()
      relay.join(1000)
      dest.close()
      // The source has an intentionally disconnected lease. Its close must
      // reject reclamation, while still stopping all service worker threads.
      var quarantined = false
      try source.close() catch { case _: IllegalStateException => quarantined = true }
      require(quarantined, "source with disconnected lease was reclaimed")
    }
  }

  private def allocationFailureReleasesSource(): Unit = {
    val source = start("allocation-source"); val dest = start("allocation-dest")
    val block = ScacheBlockId("allocation", 1, 1, 0, 0)
    val fillers = (0 until 3).map(i => ScacheBlockId("full", 1, 1, 0, i))
    try {
      source.publishBlock(block, Array.fill[Byte](64)(7))
      fillers.foreach(b => dest.publishBlock(b, new Array[Byte](1048576)))
      require(dest.urmaMetrics("urma.arenaFreeBytes") == 0)
      var exhausted = false
      try dest.fetchBlockToPool("127.0.0.1", source.port, block.toString)
      catch { case e: IllegalStateException if e.getMessage.contains("exhausted") => exhausted = true }
      require(exhausted, "full destination must reject the fetch")
      require(source.urmaMetrics("urma.activeLeases") == 0,
        "destination allocation failure leaked the remote source lease before any DMA")
      require(dest.urmaMetrics("urma.submittedBytes") == 0)
      fillers.foreach(dest.unpublishBlock)
      val slice = dest.fetchBlockToPool("127.0.0.1", source.port, block.toString).get
      require(slice.length == 64, "fetch retry after freeing capacity failed")
      dest.unpublishBlock(block); source.unpublishBlock(block)
      for (s <- Seq(source, dest)) require(s.urmaMetrics("urma.arenaFreeBytes") == 3L * 1048576)
      println("RM_CTP_ALLOCATION_FAILURE_RECOVERY_PASS (TCP/arena only; NOT UB)")
    } finally { try source.close() finally dest.close() }
  }

  private def failedReadCanRetry(scenario: String): Unit = {
    val source = start("fault-source-" + scenario); val dest = start("fault-dest-" + scenario)
    val block = ScacheBlockId("fault", 1, 1, 0, 0)
    val transport = field(dest, "transport").get(dest).asInstanceOf[CopyTransport]
    try {
      val bytes = Array.tabulate[Byte](8193)(i => (i * 17).toByte)
      source.publishBlock(block, bytes)
      scenario match {
        case "submit" => transport.rejectNextRead = true
        case "completion" => transport.failNextCompletion = true
        case "checksum" => transport.corruptNextRead = true
      }
      dest.reservePoolConsumer(block)
      var failed = false
      try dest.fetchBlockToPool("127.0.0.1", source.port, block.toString)
      catch { case e: IllegalStateException =>
        val expected = scenario match {
          case "submit" => "TEST_READ_REJECTED_BEFORE_POST"
          case "completion" => "URMA completion status=-1"
          case "checksum" => "CRC mismatch"
        }
        require(e.getMessage.contains(expected), s"unexpected $scenario failure: $e")
        failed = true
      }
      require(failed, s"$scenario fault was silently accepted")
      dest.cancelPoolConsumer(block)
      require(dest.localPoolBlock(block).isEmpty, "failed destination was published")
      require(dest.urmaMetrics("urma.arenaFreeBytes") == 3L * 1048576, "failed destination leaked")
      require(source.urmaMetrics("urma.activeLeases") == 0, "failed read retained source lease")
      if (scenario == "checksum") require(dest.urmaMetrics("urma.checksumErrors") == 1)
      if (scenario == "completion") require(dest.urmaMetrics("urma.failedChunks") == 1)
      dest.reservePoolConsumer(block)
      val slice = dest.fetchBlockToPool("127.0.0.1", source.port, block.toString).get
      val channel = FileChannel.open(java.nio.file.Paths.get(slice.path), StandardOpenOption.READ)
      try {
        val out = ByteBuffer.allocate(bytes.length); var offset = slice.offset
        while (out.hasRemaining) { val n = channel.read(out, offset); require(n > 0); offset += n }
        require(java.util.Arrays.equals(out.array(), bytes), "retry returned corrupt bytes")
      } finally channel.close()
      require(dest.releaseImportedPoolBlock(block)); source.unpublishBlock(block)
      require(dest.urmaMetrics("urma.arenaFreeBytes") == 3L * 1048576)
      println(s"RM_CTP_READ_FAILURE_RECOVERY_PASS scenario=$scenario (test copy; NOT UB)")
    } finally { try source.close() finally dest.close() }
  }
}
