package utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class DataLoader(private val basePath: String, private val year: Int, private val sc: SparkContext) {

  def getYear: Int = this.year

  private val fileBoth = basePath + "/unsd-citypopulation-year-both.csv"
  private val fileDiff = basePath + "/unsd-citypopulation-year-fm.csv"

  def loadDataWithBothSexes: RDD[Map[String, String]] = loadData(isBoth = true, sc)
  def loadDataWithDiffSexes: RDD[Map[String, String]] = loadData(isBoth = false, sc)

  private def loadData(isBoth: Boolean, sparkContext: SparkContext):
  RDD[Map[String, String]] = {
    val path =  if (isBoth) { fileBoth } else { fileDiff }
    val csv = sparkContext.textFile(path)
    val all_lines = csv
      .map(line => line.split(",")
      .map(elem => elem.trim.replaceAll("\"", "").replaceAll("\\.", " ")))

    val header = all_lines.first

    val result = all_lines.map(splits => { header.zip(splits).toMap })
    result.collect().take(5).foreach(p => println(p))
    result
  }

} // ... class DataLoader
