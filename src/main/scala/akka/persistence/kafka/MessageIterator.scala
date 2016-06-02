package akka.persistence.kafka

import kafka.api.FetchRequestBuilder
import kafka.common.ErrorMapping
import kafka.consumer._
import kafka.message._
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import java.util.Arrays
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.ConsumerRecord
import com.typesafe.config.Config
import akka.event.Logging
import akka.actor.ActorSystem
import scala.annotation.tailrec

object MessageUtil {
  def payloadBytes(m: Message): Array[Byte] = {
    val payload = m.payload
    val payloadBytes = Array.ofDim[Byte](payload.limit())

    payload.get(payloadBytes)
    payloadBytes
  }
}

class MessageIterator(topic: String, part: Int, offset: Long, consumerConfig: Config)(implicit system: ActorSystem) extends Iterator[Array[Byte]] with LogSupport[MessageIterator] {
  import ErrorMapping._

  val log = Logging(system, this)

  val consumer = initConsumer
  var iter = iterator(offset)
  var readMessages = 0
  var nextOffset = offset
  def partition = new TopicPartition(topic, part)

  def initConsumer: KafkaConsumer[String, Array[Byte]] = {
    val consumer = new KafkaConsumer[String, Array[Byte]](configToProperties(consumerConfig))
    log.debug("Assigning consumer to partition {}", partition)
    consumer assign Arrays.asList(partition)
    consumer
  }

  def iterator(offset: Long): Iterator[ConsumerRecord[String, Array[Byte]]] = {
    log.debug("Creating iterator for offset {}", offset)
    if (offset == -1)
      consumer.seekToBeginning(partition)
    else
      consumer.seek(partition, offset)

    log.debug("Polling for records...")
    val response = consumer.poll(1000)
    log.debug("Received {} messages from Kafka", response.count())

    import scala.collection.JavaConverters._

    response.iterator().asScala
  }

  def next(): Array[Byte] = {
    val record = iter.next()
    readMessages += 1
    nextOffset = record.offset + 1
    record.value()
  }

  @tailrec
  final def hasNext: Boolean =
    if (iter.hasNext) {
      true
    } else if (readMessages == 0) {
      close()
      false
    } else {
      iter = iterator(nextOffset)
      readMessages = 0
      hasNext
    }

  def close(): Unit = {
    consumer.close()
  }
}
