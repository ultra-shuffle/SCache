package org.scache.network.ub

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.net.{InetSocketAddress, ServerSocket, Socket}
import java.nio.channels.FileChannel
import java.nio.file.{Files, Paths, StandardOpenOption}
import scala.reflect.ClassTag
import org.scache.network.BlockDataManager
import org.scache.network.buffer.ManagedBuffer
import org.scache.storage.{BlockId, ScacheBlockId, StorageLevel}
import org.scache.util.ScacheConf

/** Two independent JVMs/machines, actual SCache service and real JNI only.
  * The extra TCP socket carries phase markers, never test payload.
  */
object UmqServiceNode {
  private object NoFallback extends BlockDataManager {
    override def getBlockData(id: BlockId): ManagedBuffer =
      throw new IllegalStateException("unexpected non-pool fetch: " + id)
    override def putBlockData(id: BlockId, data: ManagedBuffer, level: StorageLevel,
        tag: ClassTag[_]): Boolean = throw new IllegalStateException("unexpected non-pool put")
    override def releaseLock(id: BlockId): Unit = ()
  }
  private def pattern(n: Int, node: Int, block: Int): Array[Byte] =
    Array.tabulate[Byte](n)(i => ((i * 31 + node * 71 + block * 13) & 255).toByte)

  def main(args: Array[String]): Unit = {
    require(args.length == 9,
      "usage: A|B work-dir device eid-index priority bind-ip control-port peer-ip peer-control-port")
    require(Set("A", "B")(args(0)), "role must be A or B")
    val node = if (args(0) == "A") 0 else 1
    val peerNode = 1 - node
    val work = Paths.get(args(1)).toAbsolutePath
    // A fresh run directory prevents overwriting another run's arena files.
    Files.createDirectories(work)
    val run = Files.createTempDirectory(work, "rm-ctp-service-")
    Files.createDirectory(run.resolve("conf"))
    Files.writeString(run.resolve("conf/log4j.properties"),
      "log4j.rootLogger=INFO, console\nlog4j.appender.console=org.apache.log4j.ConsoleAppender\n" +
      "log4j.appender.console.layout=org.apache.log4j.PatternLayout\n" +
      "log4j.appender.console.layout.ConversionPattern=%d %p %c %m%n\n")
    val port = args(6).toInt; val peerPort = args(8).toInt
    require(port > 1024 && port < 65535 && peerPort > 1024 && peerPort < 65535)
    val conf = new ScacheConf(run.toString)
    Seq("scache.ub.device" -> args(2), "scache.ub.mode" -> "rm-ctp",
      "scache.ub.transport" -> "umq",
      "scache.ub.eidIndex" -> args(3), "scache.ub.priority" -> args(4),
      "spark.urma.strict" -> "true", "spark.urma.controlBindHost" -> args(5),
      "spark.urma.controlAdvertiseHost" -> args(5), "spark.urma.controlPort" -> port.toString,
      // Conservative example request size, not a universal UMDK READ/WQE limit.
      "spark.urma.chunkSize" -> "4096",
      "spark.urma.arenaBytes" -> "1048576", "spark.urma.arenaCount" -> "3",
      "scache.ub.pool.enabled" -> "true", "scache.ub.pool.path" -> run.resolve("pool").toString,
      "scache.ub.checksum.enabled" -> "true", "scache.ub.diagnostics.level" -> "summary",
      "spark.urma.transferTimeoutMs" -> "10000", "scache.ub.drainTimeoutMs" -> "2000"
    ).foreach { case (k, v) => conf.set(k, v, slient = true) }
    println(s"RM_CTP_SERVICE_RUN role=${args(0)} path=$run")
    val service = new UBBlockTransferService(conf, args(5), 1)
    var coordination: Socket = null
    var listener: ServerSocket = null
    try {
      service.init(NoFallback)
      val initial = service.urmaMetrics("urma.arenaFreeBytes")
      val sizes = Seq(64, 700000, 700001)
      def id(owner: Int, i: Int) = ScacheBlockId("rmctp", 1, 1, owner, i)
      sizes.zipWithIndex.foreach { case (n, i) => service.publishBlock(id(node, i), pattern(n, node, i)) }
      if (node == 0) {
        listener = new ServerSocket()
        listener.bind(new InetSocketAddress(args(5), port + 1))
        listener.setSoTimeout(60000)
        println(s"RM_CTP_SERVICE_READY port=$port coordination=${port + 1}")
        coordination = listener.accept()
      } else {
        coordination = new Socket()
        coordination.connect(new InetSocketAddress(args(7), peerPort + 1), 10000)
      }
      coordination.setSoTimeout(60000)
      val input = new BufferedReader(new InputStreamReader(coordination.getInputStream, "UTF-8"))
      val output = new PrintWriter(coordination.getOutputStream, true)
      def barrier(label: String): Unit = {
        output.println(label)
        require(input.readLine() == label, "peer did not complete " + label)
      }
      barrier("PUBLISHED")
      sizes.zipWithIndex.foreach { case (n, i) =>
        val block = id(peerNode, i)
        service.reservePoolConsumer(block); service.reservePoolConsumer(block)
        val slice = service.fetchBlockToPool(args(7), peerPort, block.toString).getOrElse(
          throw new IllegalStateException("missing peer block " + block))
        require(slice.length == n)
        val file = FileChannel.open(Paths.get(slice.path), StandardOpenOption.READ)
        try {
          val buffer = java.nio.ByteBuffer.allocate(n)
          var pos = slice.offset
          while (buffer.hasRemaining) {
            val count = file.read(buffer, pos)
            require(count > 0, "short local arena read")
            pos += count
          }
          require(java.util.Arrays.equals(buffer.array(), pattern(n, peerNode, i)), "payload mismatch")
        } finally file.close()
        require(service.releaseImportedPoolBlock(block), "first consumer release failed")
        require(service.localPoolBlock(block).nonEmpty, "destination freed before last consumer")
        require(service.releaseImportedPoolBlock(block), "last consumer release failed")
        require(service.localPoolBlock(block).isEmpty, "destination retained after last consumer")
        println(s"RM_CTP_BLOCK_PASS role=${args(0)} block=$block bytes=$n")
      }
      barrier("FETCHED")
      sizes.indices.foreach(i => service.unpublishBlock(id(node, i)))
      service.releaseCachedRemoteImports()
      val m = service.urmaMetrics
      m.toSeq.sortBy(_._1).foreach { case (k, v) => println(s"METRIC $k=$v") }
      require(m("urma.arenaFreeBytes") == initial, "arena capacity not restored")
      Seq("urma.arenaPublishedBlocks", "urma.activeLeases", "urma.activeScratch",
        "urma.providerOutstanding", "urma.transportInflightOperations", "urma.tcpPayloadBytes",
        "urma.nettyPayloadBytes", "urma.fallbacks", "urma.failedChunks", "urma.checksumErrors",
        "urma.generationErrors", "urma.transportFailedOperations", "urma.transportTimedOutOperations"
      ).foreach(k => require(m(k) == 0, k))
      require(m("urma.submittedBytes") == sizes.sum.toLong &&
        m("umq.rpcCalls") > 0 && m("umq.rpcFailed") == 0 &&
        m("umq.rpcCompleted") == m("umq.rpcCalls") &&
        m("umq.libraryNative") == 1 &&
        m("urma.completedBytes") == sizes.sum.toLong, "URMA byte accounting mismatch")
      barrier("QUIESCENT")
      service.close()
      println(s"UMQ_SERVICE_PASS role=${args(0)} blocks=${sizes.size} bytes=${sizes.sum}")
    } catch {
      case e: Throwable =>
        System.err.println(s"RM_CTP_SERVICE_FAIL role=${args(0)} path=$run")
        e.printStackTrace()
        try service.close() catch { case cleanup: Throwable => e.addSuppressed(cleanup) }
        throw e
    } finally {
      if (coordination != null) coordination.close()
      if (listener != null) listener.close()
    }
  }
}
