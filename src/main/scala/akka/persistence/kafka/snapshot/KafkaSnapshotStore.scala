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
    val snapshotRecord = new ProducerRecord(snapshotTopic(metadata.persistenceId), "static", snapshotBytes)
    val snapshotProducer = new KafkaProducer[String, Array[Byte]](configToProperties(config.producerConfig))
    try {
      // TODO: take a producer from a pool
      snapshotProducer.send(snapshotRecord)
    } finally {
      snapshotProducer.close()
    }
  }

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    val singleDeletions = this.singleDeletions
    val rangeDeletions = this.rangeDeletions
    for {
      highest <- if (config.ignoreOrphan) highestJournalSequenceNr(persistenceId) else Future.successful(Long.MaxValue)
      adjusted = if (config.ignoreOrphan &&
        highest < criteria.maxSequenceNr &&
        highest > 0L) criteria.copy(maxSequenceNr = highest) else criteria
      snapshot <- Future {
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

    @annotation.tailrec
    def load(topic: String, offset: Long): Option[KafkaSnapshot] =
      if (offset < 0) None else {
        val s = snapshot(topic, offset)
        if (matcher(s)) Some(s) else load(topic, offset - 1)
      }

    offset flatMap { off =>
      load(topic, off - 1)
    }

  }

  /**
    * Fetches the highest sequence number for `persistenceId` from the journal actor.
    */
  private def highestJournalSequenceNr(persistenceId: String): Future[Long] = {

    val journal = Persistence.get(system).journalFor(persistenceId)
    val promise = Promise[Any]()

    val t = config.consumerConfig.getLong("session.timeout.ms")

    implicit val timeout: Timeout = t milliseconds

    (journal ? ReadHighestSequenceNr(0L, persistenceId)).flatMap {
      case ReadHighestSequenceNrSuccess(snr) => Future.successful(snr)
      case ReadHighestSequenceNrFailure(err) => Future.failed(err)
    }
  }

  private def snapshot(topic: String, offset: Long): KafkaSnapshot = {
    val iter = new MessageIterator(topic, config.partition, offset, config.consumerConfig)
    try { serialization.deserialize(iter.next(), classOf[KafkaSnapshot]).get } finally { iter.close() }
  }

  private def snapshotTopic(persistenceId: String): String =
    s"${config.prefix}${journalTopic(persistenceId)}"
}
