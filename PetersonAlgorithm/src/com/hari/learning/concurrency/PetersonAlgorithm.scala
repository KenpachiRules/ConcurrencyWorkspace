package com.hari.learning.concurrency

/**
 * Peterson's algorithm for mutual exclusion is the most simplistic form of operating on Atomic transactions
 * involving two consumers.
 *
 */
import java.lang.Runnable
import java.util.concurrent.Executors

object PetersonAlgorithm {

  def main(args: Array[String]): Unit = {
    val lock = new Lock()
    val p0 = new Process(0, lock)
    val p1 = new Process(1, lock)
    val executors = Executors.newFixedThreadPool(2)
    executors.submit(p0)
    executors.submit(p1)
  }

}

case class Lock() {
  var owner = 0
  var acquireLocks = Array(false, false)

  def requestLock(pid: Int): Unit = {
    val j = 1 - pid
    acquireLocks(pid) = true
    owner = j
    while (acquireLocks(j) && owner == j) {}
  }

  def releaseLock(pid: Int): Unit = {
    acquireLocks(pid) = false
  }

}

class Process(pid: Int, lock: Lock) extends Runnable {

  override def run: Unit = {
    while (true) {
      Thread.sleep(1000)
      lock.releaseLock(pid)
      println(s" $pid is the owner of the lock ")
      lock.releaseLock(pid)
    }
  }
}