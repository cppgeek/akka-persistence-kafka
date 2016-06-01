package akka.persistence.kafka.journal

import akka.persistence.kafka._
import akka.persistence.kafka.MetadataConsumer.Broker

import com.typesafe.config.Config

import kafka.producer.ProducerConfig
import kafka.utils._
import java.util.Properties
import com.typesafe.config.ConfigValueFactory

class KafkaJournalConfig(config: Config) extends MetadataConsumerConfig(config) {
  val pluginDispatcher: String =
    config.getString("plugin-dispatcher")

  val writeConcurrency: Int =
    config.getInt("write-concurrency")

  val eventTopicMapper: EventTopicMapper =
    CoreUtils.createObject[EventTopicMapper](config.getString("event.producer.topic.mapper.class"))

  def journalProducerConfig: Config =
    config.getConfig("producer").
      withValue("key.serializer", ConfigValueFactory.fromAnyRef("org.apache.kafka.common.serialization.StringSerializer")).
      withValue("value.serializer", ConfigValueFactory.fromAnyRef("org.apache.kafka.common.serialization.ByteArraySerializer"))

  def eventProducerConfig: Config =
    config.getConfig("event.producer").
      withValue("key.serializer", ConfigValueFactory.fromAnyRef("org.apache.kafka.common.serialization.StringSerializer")).
      withValue("value.serializer", ConfigValueFactory.fromAnyRef("org.apache.kafka.common.serialization.ByteArraySerializer"))
}
