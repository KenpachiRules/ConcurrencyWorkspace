package com.hari.learning.concurrency

import java.lang.Runnable
import java.util.concurrent.Executors
import scala.collection.mutable.ListBuffer
import java.util.UUID
import java.text.SimpleDateFormat
import java.util.Date

/***
 * Lamport Bakery Algorithm with a small twist , instead of processes looking for an int identifier 
 * the lock will assign it back and will be the reference for the lock , thereby removing the delegation
 * from the process and instead focus on 
 * 
 * 
 */

object LamportsBakeryAlgorithm {

  def main(args: Array[String]): Unit = {
    val lock = LamportLock()
    val executors = Executors.newFixedThreadPool(10)
    for (i <- 0 until 10) {
      Thread.sleep(300)
      executors.submit(new Process(lock, i))
    }
  }

}

case class LamportLock() {

  val processes: ListBuffer[String] = new ListBuffer()
  var seqNum: Int = 0

  def getServingOrder: String = {
    val mySeq = UUID.randomUUID().toString
    processes += mySeq
    mySeq
  }

  def requestLock(seqId: String): Unit = {
    while (seqId.equals(processes(0))) {}
  }

  def releaseLock(): Unit = {
    processes.remove(0)
  }

}

class Process(lock: LamportLock, pid: Int) extends Runnable {
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  override def run: Unit = {
    while (true) {
      Thread.sleep(100)
      val order = lock.getServingOrder
      lock.requestLock(order)
      println(s" $pid is currently being served  ${dateFormat.format(new Date(System.currentTimeMillis()))} ")
      lock.releaseLock()
    }
  }
}