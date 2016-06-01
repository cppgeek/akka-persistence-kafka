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

  def partition(x$1: String, x$2: Any, x$3: Array[Byte], x$4: Any, x$5: Array[Byte], x$6: Cluster): Int = {
    StickyPartitioner.actorSystem.get.map {
      system => system.settings.config.getInt("kafka-journal.partition")
    } getOrElse 0
  }

  def configure(configs: java.util.Map[String, _]): Unit = {

  }
}
