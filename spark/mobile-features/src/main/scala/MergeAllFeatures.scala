package net.lirneasia

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MergeAllFeatures {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MergeAllFeatures").getOrCreate()

    val host = args(0)
    val port = args(1)
    val startDate = args(2)
    val endDate = args(3)
    val hdfsUrl = "hdfs://" + host + ":" + port

    // Load the calculated features and fill NA values
    val usageFeatures =
      spark
        .read
        .parquet(hdfsUrl + "/results/viren_socioeconomic_mapping/usage_features_" + startDate + "_" + endDate)
        .na
        .fill(0)

    val mobilityFeatures =
      spark
        .read
        .parquet(hdfsUrl + "/results/viren_socioeconomic_mapping/mobility_features_" + startDate + "_" + endDate)
        .na
        .fill(0, Seq("RADIUS_OF_GYRATION", "UNIQUE_TOWER_COUNT", "SPATIAL_ENTROPY"))

    val socialFeatures =
      spark
        .read
        .parquet(hdfsUrl + "/results/viren_socioeconomic_mapping/social_features_" + startDate + "_" + endDate)
        .na
        .fill(0)

    // Merge the calculated features into a single dataframe and map to the voronoi cells
    val allFeatures =
      usageFeatures
        .join(mobilityFeatures, Seq("SUBSCRIBER_ID"), "left")
        .join(socialFeatures, Seq("SUBSCRIBER_ID"), "left")
        .drop(col("SUBSCRIBER_ID"))
        .filter(col("HOME_TOWER_ID").isNotNull)
        .groupBy(col("HOME_TOWER_ID"))
        .agg(
          mean(col("CALL_COUNT")).as("CALL_COUNT"),
          mean(col("AVG_CALL_DURATION")).as("AVG_CALL_DURATION"),
          mean(col("NIGHTTIME_CALL_COUNT")).as("NIGHTTIME_CALL_COUNT"),
          mean(col("AVG_NIGHTTIME_CALL_DURATION")).as("AVG_NIGHTTIME_CALL_DURATION"),
          mean(col("INCOMING_CALL_COUNT")).as("INCOMING_CALL_COUNT"),
          mean(col("AVG_INCOMING_CALL_DURATION")).as("AVG_INCOMING_CALL_DURATION"),
          mean(col("RADIUS_OF_GYRATION")).as("RADIUS_OF_GYRATION"),
          mean(col("UNIQUE_TOWER_COUNT")).as("UNIQUE_TOWER_COUNT"),
          mean(col("SPATIAL_ENTROPY")).as("SPATIAL_ENTROPY"),
          mean(col("AVG_CALL_COUNT_PER_CONTACT")).as("AVG_CALL_COUNT_PER_CONTACT"),
          mean(col("AVG_CALL_DURATION_PER_CONTACT")).as("AVG_CALL_DURATION_PER_CONTACT"),
          mean(col("CONTACT_COUNT")).as("CONTACT_COUNT"),
          mean(col("SOCIAL_ENTROPY")).as("SOCIAL_ENTROPY"))

    // Write the results to disk
    allFeatures
      .write
      .option("header", "true")
      .csv(hdfsUrl + "/results/viren_socioeconomic_mapping/all_features_" + startDate + "_" + endDate)
  }
}
