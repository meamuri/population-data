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
    val data = csv.map(line => line.split(",").map(elem => elem.trim))
    val header = data.first

    val result = data.map(splits => { header.zip(splits).toMap })
    result
  }

} // ... class DataLoader
