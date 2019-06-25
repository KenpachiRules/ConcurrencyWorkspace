package com.hari.learning.concurrency
/**
 * Peterson Algorithm satisfies Mutual Exclusion and fair sharing of Critical Section.
 * While it works for only two processes trying to acquire lock to critical section.
 * owner always lets other process take ownership while acquireLocks controls the
 * ownership
 *
 */

import java.lang.Runnable
import java.util.concurrent.Executors
import java.text.SimpleDateFormat
import java.util.Date

case class Lock() {
  var owner = 0
  var acquireLocks: Array[Boolean] = (false :: false :: Nil).toArray
  def requireLock(pid: Int): Unit = {
    require(pid == 0 || pid == 1)
    val j = 1 - pid // the local variable is vital to this algorithm , this determines the swing in the ownership of the lock
    acquireLocks(pid) = true
    owner = j
    while (acquireLocks(j) && owner == j) {
      Thread.sleep(100)
    }
  }

  def releaseLock(pid: Int) = {
    acquireLocks(pid) = false
  }

}

class Process(pid: Int, lock: Lock) extends Runnable {
  require(pid == 1 || pid == 0)
  val format = new SimpleDateFormat("dd MMM yyyy HH:mm:ss:SSS Z");
  override def run: Unit = {
    while (true) {
      Thread.sleep(1000)
      lock.requireLock(pid)
      println(s" $pid acquired lock at " + format.format(new Date(System.currentTimeMillis())))
      lock.releaseLock(pid)
    }
  }

}

object PetersonAlgorithm {

  def main(args: Array[String]): Unit = {
    val lock = Lock()
    val p0 = new Process(0, lock)
    val p1 = new Process(1, lock)
    val processExecs = Executors.newFixedThreadPool(2)
    processExecs.submit(p0)
    processExecs.submit(p1)
  }

}