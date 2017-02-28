package akka.persistence.kafka

import scala.util._
import scala.collection.JavaConverters._

import kafka.api._
import kafka.common._
import kafka.consumer._
import kafka.utils._

import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import java.util.Arrays
import org.apache.kafka.clients.consumer.InvalidOffsetException
import akka.actor.ActorLogging

object MetadataConsumer {
  object Broker {
    def toString(brokers: List[Broker]) = brokers.mkString(",")

    def fromString(asString: String): Option[Broker] = {
      Json.parseFull(asString) match {
        case Some(m) =>
          val brokerInfo = m.asInstanceOf[Map[String, Any]]
          val host = brokerInfo.get("host").get.asInstanceOf[String]
          val port = brokerInfo.get("port").get.asInstanceOf[Int]
          Some(Broker.apply(host, port))
        case None => None
      }
    }
  }

  case class Broker(host: String, port: Int) {
    override def toString = s"${host}:${port}"
  }
}

trait MetadataConsumer {

  this: ActorLogging =>

  import MetadataConsumer._

  val config: MetadataConsumerConfig

  def consumer(partition: TopicPartition): KafkaConsumer[String, Array[Byte]] = {
    val consumer = new KafkaConsumer[String, Array[Byte]](configToProperties(config.consumerConfig))
    consumer assign Arrays.asList(partition)
    consumer
  }

  //  def leaderFor(topic: String, brokers: List[Broker]): Option[Broker] =
  //    brokers match {
  //    case Nil =>
  //      throw new IllegalArgumentException("empty broker list")
  //    case Broker(host, port) :: Nil =>
  //      leaderFor(host, port, topic)
  //    case Broker(host, port) :: brokers =>
  //      Try(leaderFor(host, port, topic)) match {
  //        case Failure(e) => leaderFor(topic, brokers) // failover
  //        case Success(l) => l
  //      }
  //  }

  //  def leaderFor(host: String, port: Int, topic: String): Option[Broker] = {
  //    import config.consumerConfig._
  //    import ErrorMapping._
  //
  //    val consumer = new SimpleConsumer(host, port, socketTimeoutMs, socketReceiveBufferBytes, clientId)
  //    val request = new TopicMetadataRequest(TopicMetadataRequest.CurrentVersion, 0, clientId, List(topic))
  //    val response = try { consumer.send(request) } finally { consumer.close() }
  //    val topicMetadata = response.topicsMetadata(0)
  //
  //    try {
  //      topicMetadata.errorCode match {
  //        case LeaderNotAvailableCode => None
  //        case NoError                => topicMetadata.partitionsMetadata.filter(_.partitionId == config.partition)(0).leader.map(leader => Broker(leader.host, leader.port))
  //        case anError                => throw exceptionFor(anError)
  //      }
  //    } finally {
  //      consumer.close()
  //    }
  //  }

  def offsetFor(topic: String, partition: Int): Try[Long] = {
    val p = new TopicPartition(topic, partition)
    val c = consumer(p)
    val result = Try {
      c.seekToEnd(List(p).asJava)
      c.position(p)
    }

    log.debug("Result of offsetFor({}, {}) is {}", topic, partition, result)

    c.close

    //    result match {
    //      case Success(off)                       => Option(off)
    //      case Failure(e: InvalidOffsetException) => None
    //      case Failure(e)                         => throw e
    //    }

    result

    //    val offsetRequest = OffsetRequest(Map(TopicAndPartition(topic, partition) -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
    //    val offsetResponse = try { c.getOffsetsBefore(offsetRequest) } finally { consumer.close() }
    //    val offsetPartitionResponse = offsetResponse.partitionErrorAndOffsets(TopicAndPartition(topic, partition))
    //
    //    try {
    //      offsetPartitionResponse.error match {
    //        case NoError => offsetPartitionResponse.offsets.head
    //        case anError => throw exceptionFor(anError)
    //      }
    //    } finally {
    //      consumer.close()
    //    }
  }
}
