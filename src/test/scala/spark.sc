
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

val conf = new SparkConf().setMaster("local").setAppName("Simple Application")
val sc = new SparkContext(conf)
val session = SparkSession.builder()

val abc = sc.parallelize(List("1", "2", "3"))