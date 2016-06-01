package akka.persistence.kafka.journal

import com.typesafe.config.ConfigFactory

import akka.persistence.journal.JournalSpec
import akka.persistence.kafka.KafkaCleanup
import akka.persistence.kafka.server._
import akka.persistence.CapabilityFlag
import org.apache.commons.io.FileUtils
import java.io.File

class KafkaJournalSpec extends JournalSpec(ConfigFactory.parseString(
  """
    |akka {
    |  loggers = ["akka.event.slf4j.Slf4jLogger"]
    |  loglevel = "DEBUG"
    | }
    |akka.persistence.journal.plugin = "kafka-journal"
    |akka.persistence.snapshot-store.plugin = "kafka-snapshot-store"
    |akka.test.single-expect-default = 10s
    |
    |kafka-journal.event.producer.request.required.acks = 1
    |kafka-journal.event.producer.bootstrap.servers = "localhost:9092"
    |kafka-journal.event.producer.session.timeout.ms = 30000
    |
    |kafka-journal.producer.session.timeout.ms = 30000
    |kafka-journal.producer.bootstrap.servers = "localhost:9092"
    |
    |kafka-journal.consumer.session.timeout.ms = 30000
    |kafka-journal.consumer.socket.receive.buffer.bytes = 65536
    |kafka-journal.consumer.max.partition.fetch.bytes = 1048576
    |kafka-journal.consumer.auto.offset.reset = "earliest"
    |kafka-journal.consumer.enable.auto.commit = false
    |kafka-journal.consumer.bootstrap.servers = "localhost:9092"
    |
    |test-server.zookeeper.dir = target/test/zookeeper
		|test-server.zookeeper.port = 2181
    |test-server.kafka.log.dirs = target/test/kafka
    |test-server.kafka.broker.id=0
    |test-server.kafka.port = 9092
  """.stripMargin)) {

  val systemConfig = system.settings.config

  val serverConfig = new TestServerConfig(systemConfig.getConfig("test-server"))
  val server = new TestServer(system, serverConfig)

  override protected def afterAll(): Unit = {
    server.stop()
    Thread.sleep(5000)
    super.afterAll()
  }

  def supportsRejectingNonSerializableObjects: CapabilityFlag = true

  override def supportsAtomicPersistAllOfSeveralEvents: Boolean = false

  override protected def beforeAll(): Unit = {
    server.start()
    super.beforeAll()
  }

}
