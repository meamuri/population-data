# Scala application. 

Набор технологий:
* Spark
* Scala
* MongoDB

## Задача: данные о населении

По ссылке можно найти [DataSet](https://github.com/datasets/population-city)

*Внимание!* Используется Scala 2.11.8 и Spark 2.1.1

Перед запуском убедитесь, что *запущен MongoDB*: 

```bash
sudo service mongod start
```

Для запуска приложения:

```bash
sbt "main-run application.Job param1 param2"
```

Где param1: путь до файлов с данными CSV,
 
а param2: год, за который интересуют данные