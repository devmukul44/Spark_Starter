package concepts

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount-spark-core")
    val sc = new SparkContext(sparkConf)

    val textFileLineRDD: RDD[String] = sc.textFile("location-to-file")

    /** Performs Following Operations:
      * 1. Ignore Case of different words
      * 2. Removes unwanted keywords
      * 3. Split Lines into Words using array of split characters
      * 4. create tuple of (word, 1)
      *
      * note this is not a good approach if line size is very large because `wordArray` will break
      * For large lines use separate map operation to create tuple of (word, 1)
      */
    val textFileWordTupleRDD: RDD[(String, Int)] = textFileLineRDD.flatMap{ line =>
      val wordArray = line.toLowerCase.replaceAll("-", "").split(Array(',', ' '))
      wordArray.map(word => (word,1))
    }

    /** ReduceByKey vs GroupByKey
      *
      */
    val outputReduceByKeyRDD: RDD[(String, Int)] = textFileWordTupleRDD.reduceByKey(_ + _)
    val outputGroupByKeyRDD: RDD[(String, Int)] = textFileWordTupleRDD.groupByKey.map({case(x,y) => (x, y.size)})

    // Display Output ReduceByKey RDD
    outputReduceByKeyRDD.foreachPartition(partition => partition.foreach(println))
    // Display Output GroupByKey RDD
    outputGroupByKeyRDD.foreachPartition(partition => partition.foreach(println))
  }
}
