package akka.persistence.kafka.server

import java.io.File

import akka.persistence.kafka._

import com.typesafe.config._

import kafka.server._

import org.apache.curator.test.TestingServer
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import akka.event.LogSource
import akka.event.Logging
import akka.actor.ActorSystem

object TestServerConfig {
  def load(): TestServerConfig =
    load("application")

  def load(resource: String): TestServerConfig =
    new TestServerConfig(ConfigFactory.load(resource).getConfig("test-server"))
}

class TestServerConfig(config: Config) {
  object zookeeper {
    val port: Int =
      config.getInt("zookeeper.port")

    val dir: String =
      config.getString("zookeeper.dir")
  }

  val kafka: KafkaConfig =
    new KafkaConfig(configToProperties(config.getConfig("kafka"),
      Map("zookeeper.connect" -> s"localhost:${zookeeper.port}", "host.name" -> "localhost")))
}

class TestServer(system: ActorSystem, config: TestServerConfig = TestServerConfig.load()) extends LogSupport[TestServer] {
  val log = Logging(system, this)
  val zookeeper = new TestZookeeperServer(system, config)
  val kafka = new TestKafkaServer(system, config)

  def start() = {
    zookeeper.start()
    kafka.start()
  }

  def stop(): Unit = {
    kafka.stop()
    zookeeper.stop()
  }
}

class TestZookeeperServer(system: ActorSystem, config: TestServerConfig) extends LogSupport[TestZookeeperServer] {
  import config._
  val log = Logging(system, this)
  private val server: TestingServer = new TestingServer(zookeeper.port, new File(zookeeper.dir), false)

  def start() = {
    server.start()
  }

  def stop(): Unit = {
    log.info("Stopping Zookeeper...")
    server.stop()
  }
}

class TestKafkaServer(system: ActorSystem, config: TestServerConfig) extends LogSupport[TestKafkaServer] {
  val log = Logging(system, this)
  private val server: KafkaServer = new KafkaServer(config.kafka)

  val zkClient = CuratorFrameworkFactory.newClient(config.kafka.zkConnect, new ExponentialBackoffRetry(1000, 3))

  def start(): Unit = {
    log.info("Waiting for Zookeeper to start...")
    zkClient.start
    zkClient.blockUntilConnected()
    log.info("Starting Kafka...")
    server.startup()
    log.info("Waiting for Kafka to complete startup...")
    Thread.sleep(3000)
  }

  def stop(): Unit = {
    log.info("Stopping Kafka...")
    server.shutdown()
    server.awaitShutdown()
    log.info("Closing Zookeeper client...")
    zkClient.close
  }
}
