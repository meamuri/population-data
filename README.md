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
$ sbt "run-main application.JobMillionaires data -1"
$ sbt "run-main application.JobPopultaion data -1"
$ sbt "run-main application.JobRatio data -1"
$ sbt "run-main application.JobTop data -1"
```

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
