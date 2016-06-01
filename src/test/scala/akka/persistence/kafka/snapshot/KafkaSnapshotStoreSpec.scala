package akka.persistence.kafka.snapshot

import com.typesafe.config.ConfigFactory

import akka.persistence._
import akka.persistence.SnapshotProtocol._
import akka.persistence.snapshot.SnapshotStoreSpec
import akka.persistence.kafka.KafkaCleanup
import akka.persistence.kafka.server._
import akka.testkit.TestProbe

class KafkaSnapshotStoreSpec extends SnapshotStoreSpec(ConfigFactory.parseString(
  s"""
      |akka.persistence.journal.plugin = "kafka-journal"
      |akka.persistence.snapshot-store.plugin = "kafka-snapshot-store"
      |akka.test.single-expect-default = 10s
      |kafka-snapshot-store.consumer.fetch.message.max.bytes = ${1000 * 1000 * 11}
      |kafka-snapshot-store.ignore-orphan = false
      |test-server.kafka.message.max.bytes = ${1000 * 1000 * 11}
      |test-server.kafka.replica.fetch.max.bytes = ${1000 * 1000 * 11}
      |test-server.zookeeper.dir = target/test/zookeeper
      |test-server.kafka.log.dirs = target/test/kafka
    """.stripMargin)) with KafkaCleanup {

  val systemConfig = system.settings.config
  val serverConfig = new TestServerConfig(systemConfig.getConfig("test-server"))
  val server = new TestServer(system, serverConfig)

  override protected def afterAll(): Unit = {
    server.stop()
    Thread.sleep(5000)
    super.afterAll()
  }

  override protected def beforeAll(): Unit = {
    server.start()
    super.beforeAll()
  }

  //  "A Kafka snapshot store" must {
  //    "support large snapshots" in {
  //      val senderProbe = TestProbe()
  //      val snapshot = Array.ofDim[Byte](1000 * 1000 * 2).toList
  //
  //      snapshotStore.tell(SaveSnapshot(SnapshotMetadata("large", 100), snapshot), senderProbe.ref)
  //      val metadata = senderProbe.expectMsgPF() { case SaveSnapshotSuccess(md) => md }
  //
  //      snapshotStore.tell(LoadSnapshot("large", SnapshotSelectionCriteria.Latest, Long.MaxValue), senderProbe.ref)
  //      senderProbe.expectMsg(LoadSnapshotResult(Some(SelectedSnapshot(metadata, snapshot)), Long.MaxValue))
  //    }
  //  }
}
