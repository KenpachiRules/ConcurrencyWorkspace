package com.hari.learning.zookeeper.twopc

import org.apache.zookeeper.{ ZooKeeper, Watcher, WatchedEvent, CreateMode }
import org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE
import org.apache.zookeeper.data.ACL
import org.apache.zookeeper.Watcher.Event.EventType._
import java.util.concurrent.CountDownLatch

class Observers(path: String, value: String, zkConn: String, observerNum: Int) extends Watcher {
  require(path != null && !path.isEmpty())
  require(value != null && !value.isEmpty())
  require(zkConn != null && !zkConn.isEmpty())
  val zk = new ZooKeeper(zkConn, 10000, this)
  zk.exists(path, this)
  var childPath: String = ""
  val ENDED_STR = new String(Constants.TRANS_ENDED)

  override def process(event: WatchedEvent): Unit = {

    event.getType match {
      case NodeCreated => {
        if (path.equals(event.getPath)) {
          // if the path is newly created then a new a transaction request is raised.
          println("Received a consensus request from committer")
          val transValue = zk.getData(path, false, null)
          childPath = zk.create(path + Constants.PATH_SEP, if (value.equals(transValue)) {
            println(s"Transaction accepted by observer # $observerNum")
            Constants.TRANS_ACCEPT
          } else {
            println(s"Transaction declined by observer # $observerNum")
            Constants.TRANS_DECLINE
          }, OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL)
          zk.exists(path, this)
        }
      }
      case NodeDeleted         => {}
      case NodeChildrenChanged => {}
      case None                => {}
      case NodeDataChanged => {
        if (path.equals(event.getPath)) {
          val fromParent = zk.getData(path, true, null)
          if (ENDED_STR.equals(new String(zk.getData(path, true, null)))) {
            println("Committer has ended the current " +
              s"transcation and hence deleting the observer child in $observerNum")
            val childStat = zk.exists(childPath, false)
            zk.delete(childPath, childStat.getVersion)
          }
        }
      }
    }
  }
}

object InitiateObserver {

  def main(args: Array[String]): Unit = {
    val zkHost: String = args(0)
    val zkPort: Int = args(1).toInt
    val acceptableValue: String = args(2)
    val observerNum: Int = args(3).toInt
    val obs = new Observers(Constants.PARENT_PATH, acceptableValue, zkHost + ":" + zkPort, observerNum)
    val latch = new CountDownLatch(1)
    latch.await
  }

}
