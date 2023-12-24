import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooDefs, ZooKeeper}

import java.util.concurrent.Semaphore
import scala.util.Random

case class Philosopher(id:Integer,
                       hostPort:String,
                       root:String,
                       leftFork:Semaphore,
                       rightFork:Semaphore)

extends  Watcher {
  val zk = new ZooKeeper(hostPort, 3000, this)
  val mutex = new Object()
  val philosopherPath = root + "/philosopher_" + id.toString

  override def process(event: WatchedEvent): Unit = {
    mutex.synchronized {
      mutex.notify()
    }
  }


  def eat(): Unit = {
    zk.create(philosopherPath, Array.emptyByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    mutex.synchronized {
      println("Philosopher #" + id + " is hungry")
      rightFork.acquire()
      println("Philosopher #" + id + " takes right fork")
      leftFork.acquire()
      println("Philosopher #" + id + " takes left fork")
      println("Philosopher #" + id + " starts eating")
      Thread.sleep((Random.nextInt(9) + 1) * 1000)
      println("Philosopher #" + id + " stops eating")
      rightFork.release()
      println("Philosopher #" + id + " releases right fork")
      leftFork.release()
      println("Philosopher #" + id + " releases left fork")
    }
  }


  def think(): Unit = {
    println("Philosopher #" + id + " starts thinking")
    zk.delete(philosopherPath, -1)
    Thread.sleep((Random.nextInt(9) + 1) * 1000)
    println("Philosopher #" + id + " stops thinking")
  }

}
