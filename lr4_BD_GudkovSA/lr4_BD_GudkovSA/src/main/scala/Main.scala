import java.util.concurrent.Semaphore


object Main {
  def main(args: Array[String]): Unit = {
    philosophers()
    //commit()
  }

  def philosophers(): Unit = {
    println("------Philosophers lunch------")
    val hostPort = "localhost:2181"
    val root = "/philosophers"
    val places = 7
    val forks = new Array[Semaphore](places)
    for (i <- 0 until places) {
      forks(i) = new Semaphore(1)
    }
    val philosophers = new Array[Thread](places)
    for (i <- 0 until places) {
      philosophers(i) = new Thread(
        () => {
          val philosopher = Philosopher(i, hostPort, root, forks(i), forks((i + 1) % places))
          while (true) {
            philosopher.eat()
            philosopher.think()
          }
        }
      )
      philosophers(i).start()
    }
  }

  def commit(): Unit = {
    println("------Commit------")
    val hostPort = "localhost:2181"
    val root = "/commit"
    val n_workers = 7
    val workers = new Array[Thread](n_workers)

    val coordinator = Coordinator(hostPort, root, n_workers)

    val coordinator_thread = new Thread(
      () => {
        coordinator.run()
      }
    )
    coordinator_thread.start()

    for (i <- 0 until n_workers) {
      workers(i) = new Thread(
        () => {
          val worker = Worker(i, hostPort, coordinator.coordinatorPath)
          worker.run()
        }
      )
      workers(i).start()
    }
  }

}