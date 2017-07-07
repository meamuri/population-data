package dao

/**
  * Перечисление полов населения
  */
object PartOfPeople extends Enumeration {
  type PartOfPeople = Value
  val male, female, both = Value
}
