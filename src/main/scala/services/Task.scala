package services

import factories.{MongoFactory, Resources}
import utils.DataLoader

class Task (val loader: DataLoader) {
  private val worker = new Miner(loader)
  private val saver = new Keeper

  def calculateRatio(): Unit = {
    val res = worker.getRatio(Resources.isRationMaleToFemale)
    saver.saveRatio(res, MongoFactory.getRatioCollection)
  }

  def calculatePopulation(): Unit = {
    val res = worker.getCountiesPopulation
    saver.savePopulation(res, MongoFactory.getPopulationCollection)
  }

  def calculateTop(): Unit = {
    val res = worker.getTopCities(Resources.getTop)
    saver.saveTop(res, MongoFactory.getTopCollection)
  }

  def calculateMillionaires(): Unit = {
    val res = worker.getCitiesWithPopulationMoreThan(Resources.getLevel)
    saver.saveMillionaires(res, MongoFactory.getMillionairesCollection)
  }
}
