package net.lirneasia

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object CalculateMobilityFeatures {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CalculateMobilityFeatures").getOrCreate()

    val host = args(0)
    val port = args(1)
    val towerDataLocation = args(2)
    val startDate = args(3)
    val endDate = args(4)
    val hdfsUrl = "hdfs://" + host + ":" + port

    // Load the filtered data
    val filteredData =
      spark
        .read
        .parquet(hdfsUrl + "/results/viren_socioeconomic_mapping/filtered_data_" + startDate + "_" + endDate)

    val towerLocations =
      spark
        .read
        .option("header", "true")
        .csv(hdfsUrl + towerDataLocation)
        .select(col("LOCATION_ID").as("TOWER_ID"), col("LATITUDE"), col("LONGITUDE"))

    // Calculate the required features
    val homeTower =
      filteredData
        .filter(col("CALL_TIME_HHMM") >= 2100 || col("CALL_TIME_HHMM") < 500)
        .groupBy(col("SUBSCRIBER_ID"), col("TOWER_ID"))
        .agg(countDistinct(col("CALL_DATE_YYYYMMDD")).as("TOWER_DAYS"))
        .withColumn(
          "TOWER_RANK",
          row_number().over(Window.partitionBy(col("SUBSCRIBER_ID")).orderBy(col("TOWER_DAYS").desc)))
        .filter(col("TOWER_RANK") === 1)
        .select(col("SUBSCRIBER_ID"), col("TOWER_ID").as("HOME_TOWER_ID"))

    def haversineDistance =
      udf((lat1: Double, lon1: Double, lat2: Double, lon2: Double) => {
        val earthRadius = 6378.137 // This is the radius at the equator in km
        val term1 = math.pow(math.sin(math.toRadians(lat2 - lat1) / 2), 2)
        val term2 = math.cos(math.toRadians(lat1)) * math.cos(math.toRadians(lat2))
        val term3 = math.pow(math.sin(math.toRadians(lon2 - lon1) / 2), 2)

        2 * earthRadius * math.asin(math.sqrt(term1 + term2 * term3))
      })

    val radiusOfGyration =
      filteredData
        .join(towerLocations, Seq("TOWER_ID"), "left")
        .withColumn("CENTER_LATITUDE", mean(col("LATITUDE")).over(Window.partitionBy(col("SUBSCRIBER_ID"))))
        .withColumn("CENTER_LONGITUDE", mean(col("LONGITUDE")).over(Window.partitionBy(col("SUBSCRIBER_ID"))))
        .withColumn(
          "DISTANCE",
          haversineDistance(col("LATITUDE"), col("LONGITUDE"), col("CENTER_LATITUDE"), col("CENTER_LONGITUDE")))
        .groupBy(col("SUBSCRIBER_ID"))
        .agg(sqrt(mean(pow(col("DISTANCE"), 2))).as("RADIUS_OF_GYRATION"))

    val uniqueTowerCount =
      filteredData
        .groupBy(col("SUBSCRIBER_ID"))
        .agg(countDistinct(col("TOWER_ID")).as("UNIQUE_TOWER_COUNT"))

    val spatialEntropy =
      filteredData
        .groupBy(col("SUBSCRIBER_ID"), col("TOWER_ID"))
        .agg(sum(col("CALL_DURATION")).as("TOWER_CALL_DURATION"))
        .withColumn(
          "TOTAL_CALL_DURATION",
          sum(col("TOWER_CALL_DURATION")).over(Window.partitionBy(col("SUBSCRIBER_ID"))))
        .withColumn("PROP_CALL_DURATION", col("TOWER_CALL_DURATION") / col("TOTAL_CALL_DURATION"))
        .join(uniqueTowerCount, Seq("SUBSCRIBER_ID"), "left")
        .withColumn(
          "SPATIAL_ENTROPY_TERM",
          lit(-1) * col("PROP_CALL_DURATION") * log(col("PROP_CALL_DURATION")) / log(col("UNIQUE_TOWER_COUNT")))
        .groupBy(col("SUBSCRIBER_ID"))
        .agg(sum(col("SPATIAL_ENTROPY_TERM")).as("SPATIAL_ENTROPY"))

    // Write the results to disk
    homeTower
      .join(radiusOfGyration, Seq("SUBSCRIBER_ID"), "inner")
      .join(uniqueTowerCount, Seq("SUBSCRIBER_ID"), "inner")
      .join(spatialEntropy, Seq("SUBSCRIBER_ID"), "inner")
      .write
      .parquet(hdfsUrl + "/results/viren_socioeconomic_mapping/mobility_features_" + startDate + "_" + endDate)
  }
}
