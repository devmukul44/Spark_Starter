package DataFrameFirst

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * Created by mukul on 17/1/17.
  */
object dftry {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("dftry")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR);

    val personDemographicCSVPath = "/home/mukul/IdeaProjects/spark/src/main/resources/person-demo.csv"
    val personDemographicReadDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .load(personDemographicCSVPath)

    val personHealthCSVPath = "/home/mukul/IdeaProjects/spark/src/main/resources/person-health.csv"
    val personHealthDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .load(personHealthCSVPath)

    personDemographicReadDF.show()
    personHealthDF.show()

    //Join
    //method 1
    val personDF1 = personDemographicReadDF
      .join(personHealthDF,
        personHealthDF("id") === personDemographicReadDF("id"),
        "left_outer"
      )
    //method 2 - mostly used
    val personDF2 = personHealthDF
      .join(broadcast(personDemographicReadDF),
        personHealthDF("id") === personDemographicReadDF("id"),
        "right_outer"
      ).drop(personHealthDF("id"))

    //personDF1.show()
    personDF2.show()
    personDF2.cache()

    val ageLessThan50DF = personDF2.filter(personDF2("age") < 50)
    ageLessThan50DF.show()

    ageLessThan50DF
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header","true")
      .save("/home/mukul/IdeaProjects/spark/src/main/resources/person-save-csv")

    println("---------------------------------Assignment------------------------------------------")
    println("--------------------------------- Insurance DataFrame ------------------------------------------")

    val personInsuranceCSVPath = "/home/mukul/IdeaProjects/spark/src/main/resources/person-insurance.csv"
    val personInsuranceDF = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header","true")
      .load(personInsuranceCSVPath)
    personInsuranceDF.show()

    val personDF3 = personDF2
      .join(broadcast(personInsuranceDF),
        personDF2("id") === personInsuranceDF("id"),
        "right_outer"
      ).drop(personDF2("id"))
    personDF3.show()

    println("----------------- Payer wise information of total valid amount insured -------------------------")
    val payerGrpSum = personInsuranceDF
      .select(personInsuranceDF("payer"), personInsuranceDF("amount"))
      .groupBy("payer")
      .agg(sum("amount"))

    payerGrpSum.show()

    payerGrpSum
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header","true")
      .save("/home/mukul/IdeaProjects/spark/src/main/resources/payer-save-csv")

    personDF2.unpersist()
  }
}
