package services

import utils.DataLoader

class Task (val loader: DataLoader) {
  val worker = new Miner(loader)
  val saver = new Keeper

  def calculateRatio(isMaleToFemaleRatio: Boolean): Unit = {
    val res = worker.getRatio(isMaleToFemaleRatio)
  }

  def calculatePopulation(): Unit = {
    val res = worker.getCountiesPopulation
  }

  def calculateTop(top: Int): Unit = {
    val res = worker.getTopCities(top)
  }

  def calculateMillionaires(level: Int): Unit = {
    val res = worker.getCitiesWithPopulationMoreThan(level)
  }
}
