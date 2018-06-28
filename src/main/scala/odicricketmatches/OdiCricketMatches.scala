package odicricketmatches

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}


object OdiCricketMatches {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("odi-cricket-matches")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // Setting log level to error
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    // Use of scala curring and partial functions
    val getCsvPartialDF = getCsvDF(sqlContext)_

    // Reading DataFrames for OriginalDataSet and ContinuousDataSet
    val originalDatasetDF = getCsvPartialDF("/Users/mukuldev/Downloads/odi-cricket-matches-19712017/originalDataset.csv")
    val continuousDatasetDF = getCsvPartialDF("/Users/mukuldev/Downloads/odi-cricket-matches-19712017/ContinousDataset.csv")

    // Selecting Distinct Combination of `Ground` and `Host_Country` from ContinuousDataSet
    val continuousDatasetSelectDF = continuousDatasetDF
      .select(col("Ground"), col("Host_Country"))
      .distinct()

    // Filtering OriginalDataSet for
    val originalDatasetFilterDF = originalDatasetDF
      .filter(col("Team 1") === lit("India") || col("Team 2") === lit("India"))

    // Joining OriginalDataSet and ContinuousDataSet; to get Host_Country information
    val joinInputDF = originalDatasetFilterDF.join(broadcast(continuousDatasetSelectDF),
      lower(trim(originalDatasetFilterDF("Ground"))) === lower(trim(continuousDatasetSelectDF("Ground"))),
      "left_outer")
      .drop(continuousDatasetSelectDF("Ground"))

    // Getting Venue type with respect to India (Home, Away, Neutral)
    val venueDetailsIpDF = joinInputDF.withColumn("venue_details_india",
      when(trim(col("Host_Country")) === lit("India"), "Home")
        .when(trim(col("Host_Country")) === trim(col("Team 1")) || trim(col("Host_Country")) === trim(col("Team 2")),
          "Away")
        .otherwise("Neutral"))

    // Getting Match result with respect to India (Win, Loss, Tie, No Result)
    val matchResultIpDF = venueDetailsIpDF.withColumn("match_result_india",
      when(trim(col("Winner")) === lit("India"), "Win")
        .when(trim(col("Winner")) === lit("no result"), "No Result")
        .when(trim(col("Winner")) === lit("tied"), "Tie")
        .otherwise("Loss"))

    // Getting total Count of Input DataSet
    val datasetTotalCount: Long = matchResultIpDF.count()

    // Solution 1
    println("-- What is India’s total Win/Loss/Tie percentage? --")
    val q1Sol = matchResultIpDF.groupBy("match_result_india").
      agg(((count("match_result_india")/datasetTotalCount) * 100).alias("match_result_percent"))
    q1Sol.show()

    // Solution 2
    println(" -- What is India’s Win/Loss/Tie percentage in away and home matches? --")
    val q2Sol = matchResultIpDF.groupBy("venue_details_india","match_result_india")
      .agg(((count("match_result_india")/datasetTotalCount) * 100).alias("match_result_percent"))
      .orderBy("venue_details_india", "match_result_india")
    q2Sol.show()

    // Solution 3
    println("-- How many matches has India played against different ICC teams? --")
    val otherTeamIpDF = matchResultIpDF.withColumn("other_team",
      when(trim(col("Team 1")) === lit("India"), col("Team 2"))
        .otherwise(col("Team 1")))
    val q3Sol = otherTeamIpDF.groupBy("other_team")
      .agg(count("other_team").alias("matches_played"))
    q3Sol.show()

    // Solution 4
    println("-- How many matches India has won or lost against different teams? --")
    val q4Sol = otherTeamIpDF.groupBy("other_team", "match_result_india")
      .agg(count("match_result_india").alias("matches_played"))
      .orderBy("other_team", "match_result_india")
    q4Sol.show()

    // Solution 5
    println("-- Which are the home and away grounds where India has played most number of matches? --")
    val q5Sol = venueDetailsIpDF.groupBy("venue_details_india", "Ground")
      .agg(count("Ground").alias("count_ground"))
      .rdd.groupBy(row => row.getAs[String]("venue_details_india"))
      .map{ case(_, rowIter) =>
        rowIter.maxBy(row => row.getAs[Long]("count_ground"))
      }
    q5Sol.foreach(println)

    // Solution 6
    println("-- What has been the average Indian win or loss by Runs per year? --")
    val q6Temp = matchResultIpDF.filter(col("Margin").endsWith("runs"))
      .withColumn("run_abs", getAbsoluteRuns(col("Margin")))
      .withColumn("match_year", getMatchYear(col("Match Date")))

    val q6Sol = q6Temp.groupBy("match_year", "match_result_india")
      .agg(avg("run_abs").alias("average_runs"))
      .orderBy("match_year", "match_result_india")
    q6Sol.show()
  }

  val getAbsoluteRuns = udf((value: String)=> value.split(" ")(0).toInt)

  val getMatchYear = udf((value: String) => value.takeRight(4))

  /** Returns DataFrame for the input csv
    *
    * @param sqlContext: sqlContext
    * @param path:       absolute path of the csv file
    * @return            DataFrame
    */
  def getCsvDF(sqlContext: SQLContext)(path: String): DataFrame = {
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .load(path)
  }
}
