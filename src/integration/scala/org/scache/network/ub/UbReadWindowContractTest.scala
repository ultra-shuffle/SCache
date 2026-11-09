package org.scache.network.ub

import java.nio.ByteBuffer
import java.util.zip.CRC32
import scala.collection.mutable.ArrayBuffer

/** Deterministic window/range and bulk CRC checks; no performance claims. */
object UbReadWindowContractTest {
  def main(args: Array[String]): Unit = {
    var cases = 0
    for (size <- Seq(0, 1, 4095, 4096, 4097, 65536, 1048577, Int.MaxValue);
         window <- Seq(1, 4096, 1048576, Int.MaxValue) if size < 2000000 || window >= 1048576) {
      var expected = 0L; var calls = 0
      UBBlockTransferService.foreachReadWindow(size, window) { (offset, bytes) =>
        require(offset.toLong == expected && bytes > 0 && bytes <= window)
        require(offset.toLong + bytes <= size.toLong)
        expected += bytes; calls += 1
      }
      require(expected == size.toLong)
      require(calls.toLong == (size.toLong + window - 1) / window)
      cases += 1
    }
    for ((size, window) <- Seq((-1, 1), (1, 0), (1, -1))) {
      var rejected = false
      try UBBlockTransferService.foreachReadWindow(size, window)((_, _) => sys.error("must not call"))
      catch { case _: IllegalArgumentException => rejected = true }
      require(rejected); cases += 1
    }
    var calls = 0
    try UBBlockTransferService.foreachReadWindow(100, 10) { (_, _) =>
      calls += 1; throw new IllegalStateException("injected completion failure")
    } catch { case _: IllegalStateException => }
    require(calls == 1); cases += 1
    for (size <- Seq(0, 1, 4095, 4096, 4097, 1048577)) {
      val bytes = Array.tabulate[Byte](size)(i => (i * 37 + 19).toByte)
      val reference = new CRC32(); reference.update(bytes)
      val heap = ByteBuffer.wrap(bytes)
      val direct = ByteBuffer.allocateDirect(size); direct.put(bytes); direct.flip()
      for (buffer <- Seq(heap, direct, heap.asReadOnlyBuffer(), direct.asReadOnlyBuffer())) {
        val position = math.min(size, 3); buffer.position(position)
        require(UBBlockTransferService.crc(buffer, size) == reference.getValue)
        require(buffer.position() == position && buffer.limit() == size)
        cases += 1
      }
    }
    println(s"UB_READ_WINDOW_CONTRACT_PASS cases=$cases")
  }
}
