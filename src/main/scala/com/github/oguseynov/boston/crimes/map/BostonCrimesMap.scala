package com.github.oguseynov.boston.crimes.map


import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object BostonCrimesMap extends App {

  lazy val sparkSession = SparkSession.builder().master("local[*]").getOrCreate()

  import sparkSession.implicits._

  val offenseCodesCsv = args(0)
  val crimeCsv = args(1)
  val output = args(2)

  lazy val crimes = readCsv(crimeCsv).dropDuplicates("INCIDENT_NUMBER")
  lazy val offenseCodes = readCsv(offenseCodesCsv)//.dropDuplicates("CODE")
  lazy val crimesJoined = crimes
    .join(broadcast(offenseCodes), $"CODE" === $"OFFENSE_CODE", "left")
    .dropDuplicates("INCIDENT_NUMBER")

    val byDistrictDesc = Window.partitionBy('DISTRICT)

    // Crimes total per district

    val totalCrimesPerDistrict = count('DISTRICT).over(byDistrictDesc).as("crimes_total")

    val totalCrimesPerDistrictDataFrame = crimesJoined
      .select('DISTRICT, totalCrimesPerDistrict)
      .distinct()

    // Monthly median

    crimesJoined
      .groupBy('DISTRICT, 'YEAR, 'MONTH).count().orderBy('DISTRICT)
      .createTempView("monthly")


    val monthlyMedianDataFrame = sparkSession.sql(
      "select DISTRICT, percentile_approx(count, 0.5) as crimes_monthly " +
        "from monthly group " +
        "by DISTRICT"
    )

    // Frequent crime types

    val crimesWithTypes = crimesJoined.withColumn(
      "crime_type",
      split(col("NAME"), " - ").getItem(0)
    )

    val crimesWithFrequencyDataFrame = crimesWithTypes
      .select('DISTRICT, 'crime_type)
      .groupBy('DISTRICT, 'crime_type)
      .count()

    val byDistrictOrderedByFrequencyDesc = Window.partitionBy('DISTRICT).orderBy(desc("count"))

    val most3CrimeTypes = dense_rank().over(byDistrictOrderedByFrequencyDesc).as("rank")

    val most3CrimeTypesDataFrame = crimesWithFrequencyDataFrame
      .select('DISTRICT, 'crime_type, most3CrimeTypes)
      .filter("rank <= 2")

    val most3CrimeTypesConcatenatedDataFrame = most3CrimeTypesDataFrame
      .select("DISTRICT", "crime_type")
      .groupBy("DISTRICT")
      .agg(concat_ws(" ,", collect_list("crime_type")).as("frequent_type"))

    // Average lat

    val averageLat = avg('Lat).over(byDistrictDesc).as("lat")

    val latDataFrame = crimesJoined
      .select('DISTRICT, averageLat)
      .distinct()

    // Average lng

    val averageLng = avg('Long).over(byDistrictDesc).as("lng")

    val lngDataFrame = crimesJoined
      .select('DISTRICT, averageLng)
      .distinct()

    // Join all of them

    val resultingDataFrame = totalCrimesPerDistrictDataFrame
      .join(monthlyMedianDataFrame, Seq("DISTRICT"))
      .join(most3CrimeTypesConcatenatedDataFrame, Seq("DISTRICT"))
      .join(latDataFrame, Seq("DISTRICT"))
      .join(lngDataFrame, Seq("DISTRICT"))

    // Write to parquet file

    resultingDataFrame.repartition(1).write.parquet(output)

  sparkSession.stop()

  def readCsv(path: String): DataFrame = sparkSession
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(path)
}
