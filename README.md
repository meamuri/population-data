# Scala application. 

Набор технологий:
* Spark
* Scala
* MongoDB

## Задача: данные о населении

По ссылке можно найти [DataSet](https://github.com/datasets/population-city)

*Внимание!* Используется Scala 2.11.8 и Spark 2.1.1

Перед запуском убедитесь, что *запущен MongoDB* (порт 27017): 

```bash
sudo service mongod start
```

### Запуск приложения

в зависимости от задачи выполните одну из команд:

```bash
$ sbt "run-main application.JobMillionaires"
$ sbt "run-main application.JobPopultaion"
$ sbt "run-main application.JobRatio"
$ sbt "run-main application.JobTop"
```

Запуская любой из модулей, можно указать специфические параметры, например:
```bash
$ sbt "run-main application.JobTop --year 2010 --path resources/data"
```
где 
* **--path**: путь, в котором искать файлы, 
* **--year**: год, за который следует отбирать записи

Для запуска тестов:
```bash
$ sbt test
```

Для просмотра полученных после работы программы коллекциях:

```bash
$ mongo
> use dsr
> show collections
> db.<collection_name>.find() 
# где <collection_name>, например, ratio
```
