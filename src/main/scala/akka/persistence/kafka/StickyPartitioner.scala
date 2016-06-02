package akka.persistence.kafka

import kafka.utils.VerifiableProperties
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import java.util.concurrent.atomic.AtomicReference

object StickyPartitioner {
  val actorSystem: AtomicReference[Option[ActorSystem]] = new AtomicReference(None)

  def setActorSystem(system: ActorSystem) = {
    actorSystem.set(Option(system))
  }
}

class StickyPartitioner extends Partitioner {

  def close(): Unit = {}

  def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    StickyPartitioner.actorSystem.get.map { system =>
      key match {
        case "journal"  => system.settings.config.getInt("kafka-journal.partition")
        case "snapshot" => system.settings.config.getInt("kafka-snapshot-store.partition")
        case _          => 0
      }

    } getOrElse 0
  }

  def configure(configs: java.util.Map[String, _]): Unit = {}
}
