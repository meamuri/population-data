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

### Запуск приложения:

```bash
sbt "run-main application.Job data -1"
```

Для просмотра полученных после работы программы коллекциях:

```bash
$ mongo
> use dsr
> show collections
> db.<collection_name>.find() # где <collection_name>, например, ratio
```
