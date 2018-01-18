package akka.persistence.kafka.snapshot

import scala.concurrent.{ Promise, Future }
import scala.concurrent.duration._
import scala.util._

import akka.actor._
import akka.pattern.{ PromiseActorRef, pipe, ask }
import akka.persistence._
import akka.persistence.JournalProtocol._
import akka.persistence.kafka._
import akka.persistence.kafka.MetadataConsumer.Broker
import akka.persistence.kafka.BrokerWatcher.BrokersUpdated
import akka.serialization.SerializationExtension
import akka.util.Timeout

import _root_.kafka.producer.{ Producer, KeyedMessage }
import akka.persistence.kafka.journal.{ ReadHighestSequenceNr, ReadHighestSequenceNrSuccess, ReadHighestSequenceNrFailure }
import akka.persistence.snapshot.SnapshotStore
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.RecordMetadata
import java.util.NoSuchElementException

/**
  * Optimized and fully async version of [[akka.persistence.snapshot.SnapshotStore]].
  */
trait KafkaSnapshotStoreEndpoint extends Actor {
  import SnapshotProtocol._
  import context.dispatcher
  import context.system

  val extension = Persistence(context.system)
  val publish = extension.settings.internal.publishPluginCommands

  def receive = {
    case LoadSnapshot(persistenceId, criteria, toSequenceNr) ⇒
      val p = sender
      loadAsync(persistenceId, criteria.limit(toSequenceNr)) map {
        sso ⇒ LoadSnapshotResult(sso, toSequenceNr)
      } recover {
        case e ⇒ LoadSnapshotResult(None, toSequenceNr)
      } pipeTo (p)
    case SaveSnapshot(metadata, snapshot) ⇒
      val p = sender
      val md = metadata.copy(timestamp = System.currentTimeMillis)
      saveAsync(md, snapshot) map {
        _ ⇒ SaveSnapshotSuccess(md)
      } recover {
        case e ⇒ SaveSnapshotFailure(metadata, e)
      } pipeTo (p)
    case d @ DeleteSnapshot(metadata) ⇒
      deleteAsync(metadata) onComplete {
        case Success(_) => if (publish) context.system.eventStream.publish(d)
        case Failure(_) =>
      }
    case d @ DeleteSnapshots(persistenceId, criteria) ⇒
      deleteAsync(persistenceId, criteria) onComplete {
        case Success(_) => if (publish) context.system.eventStream.publish(d)
        case Failure(_) =>
      }
  }

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]]
  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit]
  def deleteAsync(metadata: SnapshotMetadata): Future[Unit]
  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit]
}

class KafkaSnapshotStore extends SnapshotStore with MetadataConsumer with ActorLogging {
  import context.dispatcher
  import context.system

  type RangeDeletions = Map[String, SnapshotSelectionCriteria]
  type SingleDeletions = Map[String, List[SnapshotMetadata]]

  val serialization = SerializationExtension(context.system)
  val config = new KafkaSnapshotStoreConfig(context.system.settings.config.getConfig("kafka-snapshot-store"))
  val persistence = Persistence(system)

  override def postStop(): Unit = {
    super.postStop()
  }

  // Transient deletions only to pass TCK (persistent not supported)
  var rangeDeletions: RangeDeletions = Map.empty.withDefaultValue(SnapshotSelectionCriteria.None)
  var singleDeletions: SingleDeletions = Map.empty.withDefaultValue(Nil)

  def deleteAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Unit] = Future.successful {
    rangeDeletions += (persistenceId -> criteria)
  }

  def deleteAsync(metadata: SnapshotMetadata): Future[Unit] = Future.successful {
    singleDeletions.get(metadata.persistenceId) match {
      case Some(dels) => singleDeletions += (metadata.persistenceId -> (metadata :: dels))
      case None       => singleDeletions += (metadata.persistenceId -> List(metadata))
    }
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = Future {
    val snapshotBytes = serialization.serialize(KafkaSnapshot(metadata, snapshot)).get
    val snapshotRecord = new ProducerRecord(snapshotTopic(metadata.persistenceId), "snapshot", snapshotBytes)
    val snapshotProducer = new KafkaProducer[String, Array[Byte]](configToProperties(config.producerConfig))
    try {
      // TODO: take a producer from a pool
      val cb: Callback = new Callback {
        def onCompletion(metadata: RecordMetadata, exception: Exception) =
          log.debug("After sending snapshot to Kafka; metadata: {}, exception: {}", metadata, exception)
      }
      snapshotProducer.send(snapshotRecord, cb)
    } finally {
      snapshotProducer.close()
    }
  }

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    val singleDeletions = this.singleDeletions
    val rangeDeletions = this.rangeDeletions
    log.debug("Snapshot store, loadAsync criteria {}, singleDeletions {}, rangeDeletions {}", criteria, singleDeletions, rangeDeletions)
    for {
      highest <- if (config.ignoreOrphan) highestJournalSequenceNr(persistenceId) else Future.successful(Long.MaxValue)
      snapshot <- Future {

        val adjusted = if (config.ignoreOrphan &&
          highest < criteria.maxSequenceNr &&
          highest > 0L) {
          log.debug("loadAsync, highest = {}", highest)
          criteria.copy(maxSequenceNr = highest)
        } else {
          criteria
        }

        val topic = snapshotTopic(persistenceId)

        log.debug("In loadAsync, adjusted = {}", adjusted)

        def matcher(snapshot: KafkaSnapshot): Boolean = snapshot.matches(adjusted) &&
          !snapshot.matches(rangeDeletions(persistenceId)) &&
          !singleDeletions(persistenceId).contains(snapshot.metadata.copy(timestamp = 0L))

        load(topic, matcher).map(s => SelectedSnapshot(s.metadata, s.snapshot))
      }
    } yield snapshot
  }

  def load(topic: String, matcher: KafkaSnapshot => Boolean): Option[KafkaSnapshot] = {
    val offset = offsetFor(topic, config.partition)

    offset map { off =>
      log.debug("KafkaSnapshotStore.load offset = {}", offset)

      @annotation.tailrec
      def load(topic: String, offset: Long): Option[KafkaSnapshot] =
        if (offset < 0) None else {
          val t = trySnapshot(topic, offset)
          t match {
            case Success(s) => if (matcher(s)) Some(s) else load(topic, offset - 1)
            case Failure(e: NoSuchElementException) =>
              log.warning("Recovering from bad offset {} for topic {}", offset, topic)
              load(topic, offset - 1)
            case Failure(e) =>
              log.error(e, "Unable to load snapshot from topic {}", topic)
              None
          }
        }

      load(topic, off - 1)

    } match {
      case Success(s) => s
      case Failure(e) => throw e
    }

  }

  /**
    * Fetches the highest sequence number for `persistenceId` from the journal actor.
    */
  private def highestJournalSequenceNr(persistenceId: String): Future[Long] = {
    log.debug("Requesting highest seq Nr...")

    val journal = persistence.journalFor(null)
    log.debug("Obtained journal for requesting highest seq Nr...")
    val promise = Promise[Any]()

    val ms = config.consumerConfig.getLong("session.timeout.ms")
    implicit val timeout: Timeout = ms milliseconds

    val t = (journal ? ReadHighestSequenceNr(0L, persistenceId)).flatMap {
      case ReadHighestSequenceNrSuccess(snr) =>
        log.debug(s"ReadHighestSequenceNr succeeded with $snr")
        Future.successful(snr)
      case ReadHighestSequenceNrFailure(err) =>
        log.error("ReadHighestSequenceNr failed {}", err)
        Future.failed(err)
    }

    t.onComplete {
      case Success(snr) => log.debug("Received snr {}", snr)
      case Failure(e)   => log.error("SNR request failed {}", e)
    }
    t
  }

  private def trySnapshot(topic: String, offset: Long): Try[KafkaSnapshot] = {
    val iter = new MessageIterator(topic, config.partition, offset, config.consumerConfig)
    val result = for {
      data <- Try(iter.next)
      snap <- serialization.deserialize(data, classOf[KafkaSnapshot])
    } yield snap
    iter.close()
    result
  }

  private def snapshotTopic(persistenceId: String): String =
    s"${config.prefix}${journalTopic(persistenceId)}"
}
