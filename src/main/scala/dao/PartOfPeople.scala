package dao

/**
  * Перечисление полов населения
  */
object PartOfPeople extends Enumeration {
  type PartOfPeople = Value
  val male, female, both = Value

  /**
    * Конвертация строки в одно из значений перечисления
    * @param info строка, описывающая следующее: данные о населении какого пола представлены
    * @return PartOfPeople.male если строка Male
    *         PartOfPeople.female если строка Female
    *         PartOfPeople.both если строка любая другая
    */
  def strToPart(info: String): PartOfPeople = {
    info.toLowerCase match {
      case "male" => male
      case "female" => female
      case _ => both
    }

  }

  /**
    * Конвертация строки в буквенный символ в нижнем регистре,
    * характеризующий информацию: данные о наслелении какого пола представлены
    * @param info строка, описывающая следующее: данные о населении какого пола представлены
    * @return 'm' если строка Male
    *         'f' если строка Female
    *         'b' если строка любая другая
    */
  def strToChar(info: String): Char = {
    info.toLowerCase match {
      case "male" => 'm'
      case "female" => 'f'
      case _ => 'b'
    }
  }

}
