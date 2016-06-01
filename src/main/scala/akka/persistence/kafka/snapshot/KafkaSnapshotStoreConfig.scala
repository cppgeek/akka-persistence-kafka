package akka.persistence.kafka.snapshot

import akka.persistence.kafka._
import akka.persistence.kafka.MetadataConsumer.Broker

import com.typesafe.config.Config

import kafka.producer.ProducerConfig
import com.typesafe.config.ConfigValueFactory

class KafkaSnapshotStoreConfig(config: Config) extends MetadataConsumerConfig(config) {
  val prefix: String =
    config.getString("prefix")

  val ignoreOrphan: Boolean =
    config.getBoolean("ignore-orphan")

  def producerConfig: Config =
    config.getConfig("producer").
      withValue("key.serializer", ConfigValueFactory.fromAnyRef("org.apache.kafka.common.serialization.StringSerializer")).
      withValue("value.serializer", ConfigValueFactory.fromAnyRef("org.apache.kafka.common.serialization.ByteArraySerializer"))

  //  def producerConfig(brokers: List[Broker]): ProducerConfig =
  //    new ProducerConfig(configToProperties(config.getConfig("producer"),
  //      Map("metadata.broker.list" -> Broker.toString(brokers), "partition" -> config.getString("partition"))))
}
