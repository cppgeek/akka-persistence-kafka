package akka.persistence

import scala.collection.JavaConverters._

import java.util.Properties

import com.typesafe.config.Config
import akka.event.LogSource

package object kafka {

  /**
    * Used for integrating non-actor based application classes into the Akka
    * logging system.
    */
  trait LogSupport[A] {
    implicit val myLogSourceType: LogSource[A] = new LogSource[A] {
      def genString(a: A) = a.getClass.getName
    }
  }

  def journalTopic(persistenceId: String): String =
    persistenceId.replaceAll("[^\\w\\._-]", "_")

  def configToProperties(config: Config, extra: Map[String, String] = Map.empty): Properties = {
    val properties = new Properties()

    config.entrySet.asScala.foreach { entry =>
      properties.put(entry.getKey, entry.getValue.unwrapped.toString)
    }

    extra.foreach {
      case (k, v) => properties.put(k, v)
    }

    properties
  }
}
