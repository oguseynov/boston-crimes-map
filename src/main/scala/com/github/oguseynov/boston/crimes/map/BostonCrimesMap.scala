package com.github.oguseynov.boston.crimes.map


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object BostonCrimesMap extends App {

  lazy val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()

  import sparkSession.implicits._

  val offenseCodesCsv = args(0)
  val crimeCsv = args(1)
  //val output = args(2)

  lazy val crimes = readCsv(crimeCsv)

  lazy val offenseCodes = readCsv(offenseCodesCsv)

  lazy val crimesJoined = broadcast(offenseCodes).join(crimes, $"CODE" === $"OFFENSE_CODE")

  val byDistrictDesc = Window.partitionBy('DISTRICT)

  // Crimes total per district

  val totalCrimesPerDistrict = count('DISTRICT).over(byDistrictDesc).as("crimes_total")

  lazy val totalCrimesPerDistrictDataFrame = crimesJoined
    .select('DISTRICT, totalCrimesPerDistrict)
    .distinct()
    .orderBy(desc("crimes_total"))

  // Monthly median

  crimesJoined
    .groupBy('DISTRICT, 'MONTH).count().orderBy('DISTRICT)
    .createTempView("monthly")


  lazy val monthlyMedianDataFrame = sparkSession.sql(
    "select DISTRICT, percentile_approx(count, 0.5) as crimes_monthly " +
      "from monthly group by DISTRICT"
  )

  // Frequent crime types

  lazy val crimesWithTypes = crimesJoined.withColumn(
    "crime_type",
    split(col("NAME"), " - ").getItem(0)
  )


  lazy val crimesWithFrequencyDataFrame = crimesWithTypes
    .select('DISTRICT, 'crime_type)
    .groupBy('DISTRICT, 'crime_type)
    .count()

  val byDistrictOrderedByFrequencyDesc = Window.partitionBy('DISTRICT).orderBy(desc("count"))

  lazy val most3CrimeTypes = crimesWithFrequencyDataFrame
    .withColumn("rank", rank.over(byDistrictOrderedByFrequencyDesc))
    .filter($"rank" <= 3)
    .drop("rank")

  lazy val districts = crimesJoined.select("DISTRICT").distinct()
    .collect
    .toSeq

  val schema = List(
    ("DISTRICT", StringType, true),
    ("frequent_crime_types_list", StringType, true)
  )

  lazy val most3CrimeTypesConcatenated = districts
    .map(
      x => (
        x.getString(0),
        most3CrimeTypes
          .select("crime_type")
          .filter($"DISTRICT" === x(0))
          .collect
          .map(x => x.getString(0))
          .mkString(", ")
      )
    ).toDF("DISTRICT", "frequent_crime_types")

  most3CrimeTypesConcatenated.show()

  sparkSession.stop()

  def readCsv(path: String): DataFrame = sparkSession
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(path)
}
