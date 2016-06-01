package akka.persistence.kafka

import com.typesafe.config.Config

import kafka.utils._
import java.util.Properties
import com.typesafe.config.ConfigValueFactory

class MetadataConsumerConfig(config: Config) {
  val partition: Int =
    config.getInt("partition")

  val zookeeperConfig: ZKConfig =
    new ZKConfig(new VerifiableProperties(configToProperties(config)))

  val consumerConfig: Config =
    config.getConfig("consumer").
      withValue("group.id", ConfigValueFactory.fromAnyRef("snapshot")).
      withValue("key.deserializer", ConfigValueFactory.fromAnyRef("org.apache.kafka.common.serialization.StringDeserializer")).
      withValue("value.deserializer", ConfigValueFactory.fromAnyRef("org.apache.kafka.common.serialization.ByteArrayDeserializer"))
}
