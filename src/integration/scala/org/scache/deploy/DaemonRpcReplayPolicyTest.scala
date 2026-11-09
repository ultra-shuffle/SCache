package org.scache.deploy

import org.scache.network.util.{MapConfigProvider, TransportConf}

object DaemonRpcReplayPolicyTest {
  def main(args: Array[String]): Unit = {
    val stateful = Seq("get-ipc", "get-ipc-batch", "release-imported-block",
      "prepare-put-pool", "prepare-put", "prepare-put-batch", "commit-put-pool",
      "commit-put", "commit-put-batch", "abort-put-pool")
    stateful.foreach { operation =>
      assert(DaemonRpcReplayPolicy.requiresOneShot("ub", operation), operation)
      assert(DaemonRpcReplayPolicy.requiresOneShot("UB", operation), operation)
      assert(!DaemonRpcReplayPolicy.requiresOneShot("netty", operation), operation)
    }
    assert(!DaemonRpcReplayPolicy.requiresOneShot("ub", "get-shuffle-status"))
    val settings = new java.util.HashMap[String, String]()
    settings.put("scache.network.timeout", "1800s")
    settings.put("scache.rpc.askTimeout", "1800s")
    val transport = new TransportConf("rpc", new MapConfigProvider(settings))
    assert(transport.connectionTimeoutMs() == 1800000)
    println("DAEMON_REPLAY_AND_TRANSPORT_TIMEOUT_PASS operations=" + stateful.size)
  }
}
