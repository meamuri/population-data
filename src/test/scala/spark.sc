
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.DataLoader

val conf = new SparkConf().setMaster("local").setAppName("Simple Application")
val sc = new SparkContext(conf)
val session = SparkSession.builder()
  .config(conf=conf)
  .appName("spark session")
  .getOrCreate()

val df = new DataLoader(session).loadDataWithDiffSexes()
//val actual = DataUtils.get