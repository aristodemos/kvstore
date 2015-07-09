package kvstore

import akka.actor.{Scheduler, Props, Actor, ActorRef}
import scala.concurrent.duration._


object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)
  case class SnapshotFailed(seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }
  override def postStop() = {
    println("Replicator Post Stop")
    acks.foreach(x => x._2._1 ! Replicated(x._2._2.key, x._2._2.id) )
  }

  def receive: Receive = {
    /// valueOption = None when remove
    ///             = Some(value) when insert
    /* TODO Fix scheduling strategy. */
    case Replicate(key, valueOption, id) => {
      acks = acks + (_seqCounter -> Tuple2(sender(), Replicate(key, valueOption, id)))
      replica ! Snapshot(key, valueOption, nextSeq)
      context.system.scheduler.scheduleOnce(100.millis, self, SnapshotFailed(id))
    }
    case SnapshotFailed(id) => {
      if (acks.contains(id)){
        replica ! Snapshot(acks(id)._2.key, acks(id)._2.valueOption, acks(id)._2.id)
        context.system.scheduler.scheduleOnce(100.millis, self, SnapshotFailed(id))
      }
    }
    case SnapshotAck(key, seq) => {
      /*
      Accessing Tuple2 elements:
        ActorRef = acks.get(seq).get._1
        Replicate (the message) = acks.get(seq).get._2
          use .get and parameter name to get the wanted value
      */
      acks.get(seq).get._1 ! Replicated(key, acks.get(seq).get._2.id)
      acks = acks - seq
    }

    case _ => ???
  }

}
