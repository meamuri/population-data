package dao

import dao.PartOfPeople.PartOfPeople

/**
  * класс-представление для города
  */
case class City (
                  country: String,
                  name: String,
                  year: Int,
                  population: Double,
                  sex: Char = 'b'
                )
