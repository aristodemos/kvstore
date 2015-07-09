package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class exOperationFailed(id: Long) extends OperationReply
  case class reSendPersist(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher
  import akka.event.Logging
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  val log = Logging(context.system, this)

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]

  // Store the sequence number for the Replica
  var expectedSeq = 0L
  def nextSeq = {
    val ret = expectedSeq
    expectedSeq += 1
    ret
  }

  //Peristence Actor
  val actorToPersist = context.actorOf(persistenceProps)
  // add a watch to see if persistence actor fails
  context.watch(actorToPersist)

  // Copy from the Replicator handed out code
  // map from sequence number to pair of sender and request
  // pending Persist messages
  var pendingPers = Map.empty[Long, (ActorRef, Persist)]

  // similar to above. Pending Replication from secondary replicas
  var pendingReps = Map.empty[Long, (ActorRef, Replicate, Set[ActorRef])]

  override val preStart = {
    arbiter ! Join
  }

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(k, v, id) => {
      //println(s"Insert key $k with value $v and id $id ")
      kv = kv ++ Map(k -> v)
      // propagate persistence to local Persistent Actor
      pendingPers = pendingPers + (id -> Tuple2(sender(), Persist(k, Option(v), id)))
      actorToPersist !  pendingPers.get(id).get._2
      context.system.scheduler.scheduleOnce(100.millis, self, reSendPersist(id))

      // Update all secondaries and wait for their acknowledgement
      if (replicators.nonEmpty){
        pendingReps = pendingReps + (id -> Tuple3(sender(), Replicate(k, Option(v), id), replicators))
      }
      replicators.foreach(_ ! pendingReps.get(id).get._2)
      // await for global acknowledgement
      context.system.scheduler.scheduleOnce(1000.millis, self, exOperationFailed(id))
    }
    case reSendPersist(id) => {
      //println("Pending Pers:  " +pendingPers)
      if (pendingPers.contains(id)) {
        actorToPersist ! pendingPers.get(id).get._2
        context.system.scheduler.scheduleOnce(100.millis, self, reSendPersist(id))
      }
    }
    // if an exOperationFailed message is received it means we didn't receive a global ack, thus OperationFailed
    case exOperationFailed(id) => {
      println("We have reached time Out, id: "+id)
      //println("Pending Pers:  " +pendingPers)
      //println("Pending Reps:  " +pendingReps)
      if (pendingPers.contains(id)) pendingPers.get(id).get._1 ! OperationFailed(id)
      else if (pendingReps.contains(id)) pendingReps.get(id).get._1 ! OperationFailed(id)
      //else OperationAck(id)
    }

    case Get(k, id) => {
      sender ! GetResult(k, kv.get(k), id )
      //println(s"GET key $k  and id $id ")
    }
    case Remove(k, id) => {
      kv = kv - k
      pendingPers = pendingPers + (id -> Tuple2(sender(), Persist(k, None, id)))
      actorToPersist !  pendingPers.get(id).get._2
      context.system.scheduler.scheduleOnce(100.millis, self, reSendPersist(id))
      if (replicators.nonEmpty){
        pendingReps = pendingReps + (id -> Tuple3(sender(), Replicate(k, None, id), replicators))
      }
      replicators.foreach(_ ! pendingReps.get(id).get._2)
      context.system.scheduler.scheduleOnce(1000.millis, self, exOperationFailed(id))
    }

    case Persisted(key, id) => {
      //println(s"Key $key with id: $id is now persisted from primary node")
      val toAck = pendingPers.get(id).get._1
      pendingPers = pendingPers - id
      //println("pendingPers: "+pendingPers)
      if (!pendingReps.contains(id)) {
        toAck ! OperationAck(id)
        println("Ack Sent __ id: "+id)
      }
    }

    case Replicated(key, id) => {
      println(s"Key $key with id: $id replicated after request from primary node")
      println("sender: "+sender)
      println("Pending Reps:  " +pendingReps)
      pendingReps = pendingReps + (id -> (pendingReps.get(id).get._1, pendingReps.get(id).get._2, pendingReps.get(id)
        .get._3 - sender))
      if (!pendingPers.contains(id) & pendingReps.get(id).get._3.isEmpty) {
        pendingReps.get(id).get._1 ! OperationAck(id)
        pendingReps = pendingReps - id
        println("Ack Sent")
      }
    }

    //allocate a new actor of type Replicator for the new replica when a "Replicas" msg is received
    case Replicas(nodes) => {
      //println("What to do with Replicas")
      // 0.   remove myself from the Set (I am the primary replica)
      // 1.   clear pendingPers: use filter to drop unwanted secondaries
      // 2a.  create a Replicator for each new secondary
      // 2b.  start sending snapshots to new Replicators - make a copy and filter out old replicas to find the newly
      //      joined ones so we can start copying
      //
      // newNodes are replicas that have just joined the party
      val newNodes = nodes.filterNot(_ == self) -- secondaries.keySet
      println("New nodes: " + newNodes)
      // oldNodes are replicas that no longer exist.
      // Their corresponding replicators must be shutdown
      val oldNodes = secondaries.keySet -- nodes.filterNot(_ == self)
      println("Old nodes: " + oldNodes)
      /* TODO fix this - Stop Replication to removed Replicas */
      // remove them from pendingReps - issue OperationAcks for outstanding acknowledgements
      oldNodes.foreach(x => {
        context.stop(secondaries.get(x).get)
        replicators = replicators - secondaries.get(x).get


      })
      // remove old secondaries
      //secondaries = secondaries -- oldNodes
      newNodes.foreach(x => {
        secondaries = secondaries + (x -> context.actorOf(Replicator.props(x)))
        replicators = replicators + secondaries.get(x).get
      })
      println("Secondaries: "+secondaries)
      // "Push" - Replicate kv to all new nodes
      // create a newReplicators keySet from the newNodes val
      val newReplicators = secondaries.filterKeys(newNodes.contains(_)).values
      // Update pendingReps
      // use nextSeq as id for Replicate msg
      newReplicators.foreach(nR =>
        kv.foreach(x => {
          val idN = nextSeq
          pendingReps = pendingReps + (idN -> (self, Replicate(x._1, Option(x._2), idN), Set(nR)))
          nR ! Replicate(x._1, Option(x._2), idN)
        })
      )
    }
    case Terminated(myPersActor) => println("My Persistence Actor Failed !!!")
    case x      => log.warning("Received unknown message: {} from node:{}", x, sender)
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(k, id) => {
      sender ! GetResult(k, kv.get(k), id )
    }
    case Snapshot(key, valueOption, seq) => {
      if (seq < expectedSeq) {
        sender() ! SnapshotAck(key, seq)
      }
      else if (seq > expectedSeq) {
        println("FIND ME")
      }
      else {
        if (valueOption == None) kv = kv - key
        else kv = kv ++ Map(key -> valueOption.get)
        pendingPers = pendingPers + (seq -> Tuple2(sender(), Persist(key, valueOption, seq)))
        //println(pendingPers)
        context.system.scheduler.schedule(0.milli, 100.milli, actorToPersist, pendingPers.get(seq).get._2)
        //sender() ! SnapshotAck(key, seq)
        expectedSeq = math.max(expectedSeq, seq + 1)
      }
    }
    case Persisted(key, id) => {
      println(s"Key $key with id: $id is now persisted")
      pendingPers.get(id).get._1 ! SnapshotAck(pendingPers.get(id).get._2.key, id)
      pendingPers = pendingPers - id
    }
    case Terminated(myPersActor) => println("My Persistence Actor Failed !!!")
    case _ => println("Unknown msg received in secondary")
  }

}

