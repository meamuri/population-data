package dao

/**
  * Перечисление полов населения
  */
object PartOfPeople extends Enumeration {
  type PartOfPeople = Value
  val male, female, both = Value

  def strToPart(info: String): PartOfPeople = {
    info match {
      case "Male" => male
      case "Female" => female
      case _ => both
    }

  }

  def strToChar(info: String): Char = {
    info match {
      case "Male" => 'm'
      case "Female" => 'f'
      case _ => 'b'
    }
  }

}
