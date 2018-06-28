package RDDfirst

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.expressions.aggregate.First
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mukul on 17/1/17.
  */

object rddtry {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("rddtry")
    val sc = new SparkContext(conf)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR);

    val ipPath = "/home/mukul/IdeaProjects/spark/src/main/resources/war-and-peace.txt"
    val readRDD = sc.textFile(ipPath)
      .cache()

    readRDD.take(10).foreach(println)

    val sampleRDD = readRDD.sample(withReplacement = false, 0.01, 123) //transformation - creates rdd
    //val z = readRDD.takeSample(withReplacement = false, 10, 123) // action - gives out string
    //y.foreach(println)
    //z.foreach(println)

    println("Total line Count: " + readRDD.count())
    println("Sample line Count: " + sampleRDD.count())

    // splitting words method 1 (2 steps)
    /*
    val splitRDD = sampleRDD.map(x => x.split(" "))
    val opRDD = splitRDD.flatMap(x => x)
    opRDD.take(10).foreach(println)
    */

    // splitting words method 2 (1 step)
    val splitRDD = readRDD.flatMap(x => x.toLowerCase().split(" "))
    splitRDD.take(10).foreach(println)

    val stopwords = List("the","is","are","a","an")
    println("Total Output word count: "+ splitRDD.count())
    val filteredRDD = splitRDD.filter(x => !stopwords.contains(x)).cache()
    println("Total filtered word count:"+ filteredRDD.count())

    println("----------------------------Complete Word count Using MAP-----------------------------------------")
    val wordUnitRDD = filteredRDD.map(x => (x,1))
    val wordCountRDD = wordUnitRDD
      .groupBy(x => x._1)
      .map( x => {
        val key = x._1
        val totalCount = x._2.size
        (key, totalCount)
      })
    wordCountRDD.foreach(println)
    wordCountRDD.persist()

    println("---------------------------- Complete Word count Using REDUCE -----------------------------------------")
    val wordCounts = filteredRDD.map(x => (x, 1)).reduceByKey( (x,y) => x + y )
    wordCounts.foreach(println)

    println("-------------------------------------Frequency Plot-----------------------------------------------")
    // Frequency Count
    //val frequencyRDD = wordCountRDD.map(x=>(x._2,1))
    val frequencyRDD = wordCountRDD.map(x=>(x._2,x._1)).sortByKey()
    val frequencyCountRDD = frequencyRDD
      .groupBy(x => x._1)
      .map(x=>{
        val key = x._1
        val keyCount = x._2.size
        (key, keyCount)
      })
    frequencyCountRDD.foreach(println)

    println("------------------------------ Saving RDD in CSV format ----------------------------------------")
    val frequencyCountCSVRDD = frequencyCountRDD.map(x => x._1 + " , "+ x._2).coalesce(1)
    frequencyCountCSVRDD.saveAsTextFile("/home/mukul/IdeaProjects/spark/src/main/resources/frequencyCountRDD-csv")

    println("------------------ most significant data (Most Frequent upto 50 Percentile ---------------------")

    val totalWords = wordCountRDD.count()
    val top50per = totalWords.toInt/2
    val sortedWordCountRDD = wordCountRDD.coalesce(1).sortBy(x => x._2, false).take(top50per)
    val finalWordCount = sc.parallelize(sortedWordCountRDD.toSeq)
    /*
    finalWordCount.foreach(println)

    finalWordCount.saveAsTextFile("/home/mukul/IdeaProjects/spark/src/main/resources/wordCountRDD-txt")
     */
    sortedWordCountRDD.foreach(println)

    finalWordCount.saveAsTextFile("/home/mukul/IdeaProjects/spark/src/main/resources/wordCountRDD-txt")

    wordCountRDD.unpersist()
    readRDD.unpersist()
    filteredRDD.unpersist()
  }
}