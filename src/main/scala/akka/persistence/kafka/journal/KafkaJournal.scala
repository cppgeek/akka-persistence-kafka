package akka.persistence.kafka.journal

import scala.collection.immutable.Seq
import scala.util.{ Try, Success, Failure }

//import akka.persistence.{ PersistentId, PersistentConfirmation }
import akka.persistence.journal.AsyncWriteJournal
import akka.serialization.{ Serialization, SerializationExtension }

import kafka.producer._

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor._
import akka.pattern.ask
import akka.pattern.pipe
import akka.persistence.PersistentRepr
import akka.persistence.kafka._
import akka.persistence.kafka.MetadataConsumer.Broker
import akka.persistence.kafka.BrokerWatcher.BrokersUpdated
import akka.util.Timeout
import akka.persistence.AtomicWrite
import java.util.Properties
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import com.typesafe.config.Config
import org.apache.kafka.clients.producer.RecordMetadata
import java.util.concurrent.CompletableFuture

/**
  * Request to read the highest stored sequence number of a given persistent actor.
  *
  * @param fromSequenceNr optional hint where to start searching for the maximum sequence number.
  * @param persistenceId requesting persistent actor id.
  * @param persistentActor requesting persistent actor.
  */
case class ReadHighestSequenceNr(fromSequenceNr: Long = 1L, persistenceId: String)

/**
  * Reply message to a successful [[ReadHighestSequenceNr]] request.
  *
  * @param highestSequenceNr read highest sequence number.
  */
case class ReadHighestSequenceNrSuccess(highestSequenceNr: Long)

/**
  * Reply message to a failed [[ReadHighestSequenceNr]] request.
  *
  * @param cause failure cause.
  */
case class ReadHighestSequenceNrFailure(cause: Throwable)

class KafkaJournal extends AsyncWriteJournal with MetadataConsumer with ActorLogging {
  import context.dispatcher
  import context.system

  type Deletions = Map[String, Long]

  val serialization = SerializationExtension(context.system)
  val config = new KafkaJournalConfig(context.system.settings.config.getConfig("kafka-journal"))

  log.debug("Akka Kafka config is in...")

  override def receivePluginInternal: Receive = {

    case ReadHighestSequenceNr(fromSequenceNr, persistenceId) =>
      log.debug("Processing request for highest seq no")
      val highestTry = Try {
        readHighestSequenceNr(persistenceId, fromSequenceNr)
      }

      highestTry match {
        case Success(h) => sender ! ReadHighestSequenceNrSuccess(h)
        case Failure(e) => sender ! ReadHighestSequenceNrFailure(e)
      }

  }

  // --------------------------------------------------------------------------------------
  //  Journal writes
  // --------------------------------------------------------------------------------------

  var journalProducerConfig = config.journalProducerConfig
  var eventProducerConfig = config.eventProducerConfig

  lazy val writer: ActorRef = initWriter()
  implicit val writeTimeout = Timeout(journalProducerConfig.getLong("request.timeout.ms") milliseconds)

  // Transient deletions only to pass TCK (persistent not supported)
  var deletions: Deletions = Map.empty

  def asyncWriteMessages(messages: Seq[AtomicWrite]): Future[Seq[Try[Unit]]] = {
    log.debug("Going to write messages {}", messages.size)
    (writer ? messages).mapTo[Seq[Future[Try[Unit]]]].flatMap { x => Future.sequence(x) }
  }

  def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long): Future[Unit] =
    Future.successful(deleteMessagesTo(persistenceId, toSequenceNr))

  def deleteMessagesTo(persistenceId: String, toSequenceNr: Long): Unit =
    deletions = deletions + (persistenceId -> toSequenceNr)

  //  private def writerFor(persistenceId: String): ActorRef =
  //    writers(math.abs(persistenceId.hashCode) % config.writeConcurrency)

  private def initWriter(): ActorRef = {
    context.actorOf(Props(new KafkaJournalWriter(writerConfig)).withDispatcher(config.pluginDispatcher))
  }

  private def writerConfig = {
    KafkaJournalWriterConfig(journalProducerConfig, eventProducerConfig, config.eventTopicMapper, serialization)
  }

  // --------------------------------------------------------------------------------------
  //  Journal reads
  // --------------------------------------------------------------------------------------

  def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] =
    Future(readHighestSequenceNr(persistenceId, fromSequenceNr))

  def readHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Long = {
    val topic = journalTopic(persistenceId)
    offsetFor(topic, config.partition) match {
      case Some(off) => off
      case None      => fromSequenceNr
    }
  }

  def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: PersistentRepr => Unit): Future[Unit] = {
    val deletions = this.deletions
    Future(replayMessages(persistenceId, fromSequenceNr, toSequenceNr, max, deletions, replayCallback))
  }

  def replayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long, deletions: Deletions, callback: PersistentRepr => Unit): Unit = {
    log.debug("Replaying messages from {} to {}... ", fromSequenceNr, toSequenceNr)
    val deletedTo = deletions.getOrElse(persistenceId, 0L)

    val adjustedFrom = math.max(deletedTo + 1L, fromSequenceNr)
    val adjustedNum = toSequenceNr - adjustedFrom + 1L
    val adjustedTo = if (max < adjustedNum) adjustedFrom + max - 1L else toSequenceNr
    log.debug("Adjusted numbers: adjustedFrom {}, deletedTo {}, adjustedTo {}", adjustedFrom, deletedTo, adjustedTo)

    val iter = persistentIterator(journalTopic(persistenceId), adjustedFrom - 1L)

    iter.map(p => if (p.sequenceNr <= deletedTo) p.update(deleted = true) else p).foldLeft(adjustedFrom) {
      case (_, p) =>
        if (p.sequenceNr >= adjustedFrom && p.sequenceNr <= adjustedTo) {
          callback(p)
        }
        p.sequenceNr
    }

  }

  def persistentIterator(topic: String, offset: Long): Iterator[PersistentRepr] = {
    log.debug("Creating MessageIterator for topic {}, offset {}", topic, offset)
    val msgIter = new MessageIterator(topic, config.partition, offset, config.consumerConfig)
    msgIter.map { m =>
      serialization.deserialize(m, classOf[PersistentRepr]).get
    }
  }
}

private case class KafkaJournalWriterConfig(
  journalProducerConfig: Config,
  eventProducerConfig: Config,
  evtTopicMapper: EventTopicMapper,
  serialization: Serialization)

private case class UpdateKafkaJournalWriterConfig(config: KafkaJournalWriterConfig)

private class KafkaJournalWriter(var config: KafkaJournalWriterConfig) extends Actor with ActorLogging {

  import context.dispatcher
  var msgProducer = createMessageProducer()
  var evtProducer = createEventProducer()

  def receive = {
    case UpdateKafkaJournalWriterConfig(newConfig) =>
      msgProducer.close()
      evtProducer.close()
      config = newConfig
      msgProducer = createMessageProducer()
      evtProducer = createEventProducer()

    case messages: Seq[_] =>
      if (messages.length > 0 && messages.head.isInstanceOf[AtomicWrite])
        sender ! writeMessages(messages.asInstanceOf[Seq[AtomicWrite]])
      else
        sender ! Nil

  }

  def writeMessages(messages: Seq[AtomicWrite]): Seq[Future[Try[Unit]]] = {

    log.debug("Sending {} AtomicWrites to Kafka", messages.size)

    def batchResult(atomicBatch: Seq[Future[Try[RecordMetadata]]]): Future[Try[Unit]] =
      atomicBatch.foldLeft(Future.successful(Success(): Try[Unit])) { (acc, f) =>
        f.flatMap {
          case Failure(e) => Future.successful(Failure(e))
          case _          => acc
        }
      }

    val messageSends = for {
      m <- messages
    } yield {

      val atomicBatch = (for {
        p <- m.payload
      } yield {
        config.serialization.serialize(p) match {
          case Success(s) => msgProducer.send(new ProducerRecord[String, Array[Byte]](journalTopic(m.persistenceId), "journal", s))
          case Failure(e) => val t = new CompletableFuture(); t.completeExceptionally(e); t
        }
      }) map { x => Future { Try { x.get } } }

      // The entire batch will fail if at least one send is not successful
      batchResult(atomicBatch)

    }

    val eventSends = for {
      m <- messages
    } yield {

      val atomicBatch = (for {
        p <- m.payload
        e = Event(m.persistenceId, p.sequenceNr, p.payload)
        t <- config.evtTopicMapper.topicsFor(e)
      } yield {
        config.serialization.serialize(e) match {
          case Success(s) => evtProducer.send(new ProducerRecord(t, e.persistenceId, s))
          case Failure(e) => val t = new CompletableFuture(); t.completeExceptionally(e); t
        }

      }) map { x => Future { Try { x.get } } }

      // The entire batch will fail if at least one send is not successful
      batchResult(atomicBatch)
    }

    messageSends.zip(eventSends).map { x =>
      for {
        x1 <- x._1
        x2 <- x._2
      } yield {
        (x1, x2) match {
          case (Success(_), Success(_)) => Success(())
          case (Failure(a), _)          => Failure(a)
          case (_, Failure(a))          => Failure(a)
        }
      }
    }
  }

  override def postStop(): Unit = {
    log.debug("Stopping message and event producers...")
    msgProducer.close()
    evtProducer.close()
    super.postStop()
  }

  private def createMessageProducer() = new KafkaProducer[String, Array[Byte]](configToProperties(config.journalProducerConfig))

  private def createEventProducer() = new KafkaProducer[String, Array[Byte]](configToProperties(config.eventProducerConfig))
}
