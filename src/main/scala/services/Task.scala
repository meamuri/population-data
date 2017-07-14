package services

object Task {

  def selectUsefulRows()

  def calculateRatio(worker: Miner, saver: Keeper): Unit = {
    val data = worker.getRatio()
    saver.save(data)
  }

  def calculatePopulation(worker: Miner, saver: Keeper): Unit = {
    val data = worker.getCountiesPopulation
    saver.save(data)
  }

  def calculateTop(worker: Miner, saver: Keeper): Unit = {
    val data = worker.getTop5cities
    saver.save(data)
  }

  def calculateMillionaires(worker: Miner, saver: Keeper): Unit = {
    val data = worker.getCitiesWithMillionPopulation
    server.save(data)
  }
}
