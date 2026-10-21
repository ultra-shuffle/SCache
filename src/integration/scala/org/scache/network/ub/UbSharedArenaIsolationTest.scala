package org.scache.network.ub

import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicLong

/** Actual file-backed arenas with registration-only stubs; no JNI, device, or DMA. */
object UbSharedArenaIsolationTest {
  private final class RegistrationOnly extends UbTransport {
    private val ids = new AtomicLong(1)
    private var active = 0L
    override def capabilities(): UbTransportCapabilities = RealUbTransport.capabilitiesFor(1, 4096)
    override def localEndpoint(): Array[Byte] = throw new UnsupportedOperationException
    override def connect(raw: Array[Byte]): Unit = throw new UnsupportedOperationException
    override def registerBuffer(b: ByteBuffer): UbRegisteredBuffer = {
      val id = ids.getAndIncrement(); active += 1
      new UbRegisteredBuffer(this, b, id, b, id * 4096L, b.capacity().toLong, 1)
    }
    override def unregisterBuffer(b: UbRegisteredBuffer): Unit = {
      require(b.markUnregistered()); active -= 1
    }
    override def read(r: RemoteBuffer, o: Long, b: ByteBuffer, l: Int, n: Int): Long =
      throw new UnsupportedOperationException
    override def write(r: RemoteBuffer, o: Long, b: ByteBuffer, l: Int, n: Int): Long =
      throw new UnsupportedOperationException
    override def waitFor(id: Long, timeout: Int): Int = throw new UnsupportedOperationException
    override def metrics(): UbTransportMetrics =
      UbTransportMetrics.builder("registration-only-not-urma").registeredRegions(active).build()
    override def isClosed(): Boolean = false
    override def close(): Unit = require(active == 0)
  }
  def main(args: Array[String]): Unit = {
    for (secondEpoch <- Seq(112L, 111L)) {
      val directory = Files.createTempDirectory("ub-arena-isolation-").resolve("pool").toString
      val transport = new RegistrationOnly
      val first = new UBBlockTransferService.RegisteredArenaPool(
        transport, 4096, 1, "same-node", 111L, 60000, Some(directory))
      var second: UBBlockTransferService.RegisteredArenaPool = null
      try {
        val old = first.prepareShared("old-block", 64)
        first.sharedBuffer("old-block").put(0, 42.toByte)
        require(first.commitShared("old-block", old.path, old.offset, old.length))
        second = new UBBlockTransferService.RegisteredArenaPool(
          transport, 4096, 1, "same-node", secondEpoch, 60000, Some(directory))
        val next = second.prepareShared("new-block", 64)
        second.sharedBuffer("new-block").put(0, 99.toByte)
        require(second.commitShared("new-block", next.path, next.offset, next.length))
        require(first.sharedBuffer("old-block").get(0) == 42.toByte,
          "new arena instance overwrote an existing instance's still-live mapped bytes")
        require(old.path != next.path, "independent arena instances reused one backing file")
      } finally {
        try { if (second != null) second.close(1) } finally first.close(1)
      }
      transport.close()
    }
    println("SHARED_ARENA_INSTANCE_ISOLATION_PASS cases=2 (mapped files; registration stub; NOT UB)")
  }
}
