package org.scache.network.ub

/** Wire-format contracts only; never opens a device or changes host state. */
object RmCtpControlProtocolTest {
  def main(args: Array[String]): Unit = {
    val results = UBBlockTransferService.runProtocolContracts()
    results.foreach { case (name, ok) => println(s"CONTROL_CONTRACT $name=$ok") }
    require(results.nonEmpty && results.forall(_._2), "control protocol contract failed")
    println(s"RM_CTP_CONTROL_PROTOCOL_PASS cases=${results.size} (offline)")
  }
}
