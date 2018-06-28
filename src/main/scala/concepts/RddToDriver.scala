package concepts

import org.apache.spark.{SparkConf, SparkContext}

object RddToDriver {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("CoalesceRepartition")
    val sc = new SparkContext(sparkConf)

    val parallelRDD = sc.parallelize(1 to 9)
    // Case 1: get each partition and execute
    parallelRDD.foreachPartition{ partition =>
      val list = partition.toList
      println(list)
    }
    // Case 2: use `toLocalIterator`
    // This method creates New Job for each Partition; uses sc.runJob()
    val iterator = parallelRDD.toLocalIterator
    iterator.foreach(println)

    parallelRDD.isEmpty()
  }

}
