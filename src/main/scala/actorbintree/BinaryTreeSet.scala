/**
  * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
  */
package actorbintree

import actorbintree.BinaryTreeNode.CopyTo
import akka.actor._

import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef

    def id: Int

    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection */
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {

  import BinaryTreeSet._


  case class Job(requester: ActorRef, operation: Object)

  def runNext(queue: Vector[Job]): Receive =
    if (queue.isEmpty) normal
    else queue.head.operation match {
      case msg: Operation => root ! msg
        inProgress(queue)
      case GC =>
        val newRoot = createRoot
        root ! CopyTo(newRoot)
        garbageCollecting(newRoot, queue)
    }

  def enqueueJob(queue: Vector[Job], job: Job): Receive =
    inProgress(queue :+ job)

  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional (used to stash incoming operations during garbage collection)
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case Insert(requester, id, elem) => context.become(runNext(Vector(Job(requester, Insert(self, id, elem)))))
    case Remove(requester, id, elem) => context.become(runNext(Vector(Job(requester, Remove(self, id, elem)))))
    case Contains(requester, id, elem) => context.become(runNext(Vector(Job(requester, Contains(self, id, elem)))))
    case GC => context.become(runNext(Vector(Job(self, GC))))
  }

  def inProgress(queue: Vector[Job]): Receive = {
    case OperationFinished(id) =>
      val job = queue.head
      job.requester ! OperationFinished(id)
      context.become(runNext(queue.tail))
    case ContainsResult(id, result) =>
      val job = queue.head
      job.requester ! ContainsResult(id, result)
      context.become(runNext(queue.tail))
    case Insert(requester, id, elem) => context.become(inProgress(queue :+ Job(requester, Insert(self, id, elem))))
    case Remove(requester, id, elem) => context.become(inProgress(queue :+ Job(requester, Remove(self, id, elem))))
    case Contains(requester, id, elem) => context.become(inProgress(queue :+ Job(requester, Contains(self, id, elem))))
    case GC => context.become(inProgress(queue :+ Job(self, GC)))
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef, queue: Vector[Job]): Receive = {
    case Insert(requester, id, elem) => context.become(enqueueJob(queue, Job(requester, Insert(self, id, elem))))
    case Remove(requester, id, elem) => context.become(enqueueJob(queue, Job(requester, Remove(self, id, elem))))
    case Contains(requester, id, elem) => context.become(enqueueJob(queue, Job(requester, Contains(self, id, elem))))
    case CopyFinished =>
      root = newRoot
      context.become(runNext(queue.tail))
  }

}

object BinaryTreeNode {

  trait Position

  case object Left extends Position

  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)

  /**
    * Acknowledges that a copy has been completed. This message should be sent
    * from a node to its parent, when this node and all its children nodes have
    * finished being copied.
    */
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {

  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, expectedElem) =>
      if (expectedElem == elem) {
        removed = false
        requester ! OperationFinished(id)
      }
      else if (expectedElem > elem && subtrees.get(Right).isEmpty) {
        subtrees += (Right -> context.actorOf(BinaryTreeNode.props(expectedElem, false)))
        requester ! OperationFinished(id)
      }
      else if (expectedElem < elem && subtrees.get(Left).isEmpty) {
        subtrees += (Left -> context.actorOf(BinaryTreeNode.props(expectedElem, false)))
        requester ! OperationFinished(id)
      }
      else if (expectedElem > elem && subtrees.get(Right).nonEmpty)
        subtrees(Right) ! Insert(requester, id, expectedElem)

      else if (expectedElem < elem && subtrees.get(Left).nonEmpty)
        subtrees(Left) ! Insert(requester, id, expectedElem)
    //-------------------------------------------------------------------------------------------------------------

    case Contains(requester, id, expectedElem) =>
      if (expectedElem == elem && !removed)
        requester ! ContainsResult(id, true)
      else if (expectedElem > elem && subtrees.get(Right).nonEmpty)
        subtrees(Right) ! Contains(requester, id, expectedElem)
      else if (expectedElem < elem && subtrees.get(Left).nonEmpty)
        subtrees(Left) ! Contains(requester, id, expectedElem)
      else requester ! ContainsResult(id, false)


    //--------------------------------------------------------------------------------------------------------------
    case Remove(requester, id, expectedElem) =>
      if (expectedElem == elem) {
        removed = true
        requester ! OperationFinished(id)
      }
      else if (expectedElem > elem && subtrees.get(Right).nonEmpty)
        subtrees(Right) ! Remove(requester, id, expectedElem)
      else if (expectedElem < elem && subtrees.get(Left).nonEmpty)
        subtrees(Left) ! Remove(requester, id, expectedElem)
      else requester ! OperationFinished(id)
    //--------------------------------------------------------------------------------------------------------------

    case CopyTo(newRoot) =>
      if (!removed) {
        newRoot ! Insert(self, 0, elem)
        context.become(copying(Set(newRoot), false))
      }
      else {
        context.become(copying(Set(newRoot), false))
        self ! OperationFinished(0)
      }
  }


  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(_) =>
      if (subtrees.nonEmpty) {
        subtrees.foreach(_._2 ! CopyTo(expected.head))
        context.become(copying(subtrees.values.toSet, false))
      }
      else {
        context.parent ! CopyFinished
        context.stop(self)
      }
    case CopyFinished if expected.size < 2 || insertConfirmed =>
      context.parent ! CopyFinished
      context.stop(self)
    case CopyFinished if expected.size == 2 && !insertConfirmed =>
      context.become(copying(expected, true))
  }
}