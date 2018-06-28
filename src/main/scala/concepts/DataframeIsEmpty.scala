package concepts

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object DataframeIsEmpty {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("DataframeIsEmpty")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val path = "src/main/resources/person-demo.csv"
    val inputDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .load(path)

    inputDF.queryExecution.toRdd.isEmpty()
    inputDF.rdd.isEmpty()
  }
}
