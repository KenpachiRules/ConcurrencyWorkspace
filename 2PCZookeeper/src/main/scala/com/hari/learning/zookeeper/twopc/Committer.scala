package com.hari.learning.zookeeper.twopc

import org.apache.zookeeper.{ ZooKeeper, WatchedEvent, Watcher, CreateMode }
import org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE
import org.apache.zookeeper.Watcher.Event.EventType._
import scala.collection.JavaConversions._

class Committer(zkConn: String, value: String, obsNum: Int) extends Watcher {

  val zk = new ZooKeeper(zkConn, 10000, this)
  // create the 2phase transaction node.
  zk.create(Constants.PARENT_PATH, value.getBytes, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
  zk.getChildren(Constants.PARENT_PATH, this)

  override def process(event: WatchedEvent): Unit = {
    event.getType match {
      case NodeCreated => {
      }
      case NodeDeleted => {
      }
      case NodeChildrenChanged => {
        val children = zk.getChildren(Constants.PARENT_PATH, this)
        if (children.size() == obsNum) {
          // check for Transaction value.
          if (children.forall(child => Constants.TRANS_ACCEPT.equals(zk.getData(child, false, null))))
            println("Transaction Accepted ")
          else
            println("Transaction Declined")
          // delete the parent transaction node.
          val parentStat = zk.exists(Constants.PARENT_PATH, this)
          if (parentStat != null)
            zk.delete(Constants.PARENT_PATH, parentStat.getVersion)
        }
      }
      case NodeDataChanged => {}
      case None            => {}
    }
  }

}

object InitiateCommitter {

  def main(args: Array[String]): Unit = {
    // extract program args
    val zkHost = args(0)
    val zkPort = args(1).toInt
    val transValue = args(2)
    val numObservers = args(3).toInt
    val committer = new Committer(zkHost + Constants.PATH_SEP + zkPort, transValue, numObservers)
  }

}