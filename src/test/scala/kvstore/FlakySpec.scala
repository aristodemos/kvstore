package kvstore

/**
 * Created by mariosp on 5/6/15.
 */

import akka.testkit.TestKit
import akka.testkit.ImplicitSender
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Matchers
import org.scalatest.FunSuiteLike
import akka.actor.ActorSystem
import akka.testkit.TestProbe
import Arbiter._
import Replicator._
import org.scalactic.ConversionCheckedTripleEquals

class FlakySpec extends TestKit(ActorSystem("FlakyNodesSpec"))
with FunSuiteLike
with BeforeAndAfterAll
with Matchers
with ConversionCheckedTripleEquals
with ImplicitSender
with Tools {

  override def afterAll(): Unit = {
    system.shutdown()
  }

  test("case1: Handle Flaky Nodes") {
    val arbiter = TestProbe()
    val primary = system.actorOf(Replica.props(arbiter.ref, Persistence.props(flaky = true)), "case1-primary")
    val user = session(primary)
    val secondary = TestProbe()

    arbiter.expectMsg(Join)
    arbiter.send(primary, JoinedPrimary)

    user.setAcked("k1", "v1")
    arbiter.send(primary, Replicas(Set(primary, secondary.ref)))

    secondary.expectMsg(Snapshot("k1", Some("v1"), 0L))
    secondary.reply(SnapshotAck("k1", 0L))

    val ack1 = user.set("k1", "v2")
    secondary.expectMsg(Snapshot("k1", Some("v2"), 1L))
    secondary.reply(SnapshotAck("k1", 1L))
    user.waitAck(ack1)

    val ack2 = user.remove("k1")
    secondary.expectMsg(Snapshot("k1", None, 2L))
    secondary.reply(SnapshotAck("k1", 2L))
    user.waitAck(ack2)
  }
}