package net.lirneasia

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CalculateUsageFeatures {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CalculateUsageFeatures").getOrCreate()

    val host = args(0)
    val port = args(1)
    val startDate = args(2)
    val endDate = args(3)
    val hdfsUrl = "hdfs://" + host + ":" + port

    // Load the filtered data
    val filteredData =
      spark
        .read
        .parquet(hdfsUrl + "/results/viren_socioeconomic_mapping/filtered_data_" + startDate + "_" + endDate)

    // Calculate the required features
    val nighttimeFeatures =
      filteredData
        .filter(col("CALL_TIME_HHMM") >= 2100 || col("CALL_TIME_HHMM") < 500)
        .groupBy(col("SUBSCRIBER_ID"))
        .agg(
          count("*").as("NIGHTTIME_CALL_COUNT"),
          mean(col("CALL_DURATION")).as("AVG_NIGHTTIME_CALL_DURATION"))

    val incomingFeatures =
      filteredData
        .filter(col("CALL_DIRECTION") === "Incoming")
        .groupBy(col("SUBSCRIBER_ID"))
        .agg(
          count("*").as("INCOMING_CALL_COUNT"),
          mean(col("CALL_DURATION")).as("AVG_INCOMING_CALL_DURATION"))

    val generalFeatures =
      filteredData
        .groupBy(col("SUBSCRIBER_ID"))
        .agg(
          count("*").as("CALL_COUNT"),
          mean(col("CALL_DURATION")).as("AVG_CALL_DURATION"))
        .join(nighttimeFeatures, Seq("SUBSCRIBER_ID"), "left")
        .join(incomingFeatures, Seq("SUBSCRIBER_ID"), "left")

    // Write the results to disk
    filteredData
      .select(col("SUBSCRIBER_ID"))
      .distinct
      .join(generalFeatures, Seq("SUBSCRIBER_ID"), "left")
      .join(nighttimeFeatures, Seq("SUBSCRIBER_ID"), "left")
      .join(incomingFeatures, Seq("SUBSCRIBER_ID"), "left")
      .write
      .parquet(hdfsUrl + "/results/viren_socioeconomic_mapping/usage_features_" + startDate + "_" + endDate)
  }
}
