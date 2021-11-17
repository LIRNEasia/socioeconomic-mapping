package net.lirneasia

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object CalculateSocialFeatures {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CalculateSocialFeatures").getOrCreate()

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
    val interactionPerContact =
      filteredData
        .groupBy(col("SUBSCRIBER_ID"), col("OTHER_ID"))
        .agg(count("*").as("CALL_COUNT_PER_CONTACT"), sum(col("CALL_DURATION")).as("CALL_DURATION_PER_CONTACT"))
        .groupBy(col("SUBSCRIBER_ID"))
        .agg(
          mean(col("CALL_COUNT_PER_CONTACT")).as("AVG_CALL_COUNT_PER_CONTACT"),
          mean(col("CALL_DURATION_PER_CONTACT")).as("AVG_CALL_DURATION_PER_CONTACT"))

    val contactCount =
      filteredData
        .groupBy(col("SUBSCRIBER_ID"))
        .agg(countDistinct(col("OTHER_ID")).as("CONTACT_COUNT"))

    val socialEntropy =
      filteredData
        .groupBy(col("SUBSCRIBER_ID"), col("OTHER_ID"))
        .agg(sum(col("CALL_DURATION")).as("CONTACT_CALL_DURATION"))
        .withColumn(
          "TOTAL_CALL_DURATION",
          sum(col("CONTACT_CALL_DURATION")).over(Window.partitionBy(col("SUBSCRIBER_ID"))))
        .withColumn("PROP_CALL_DURATION", col("CONTACT_CALL_DURATION") / col("TOTAL_CALL_DURATION"))
        .join(contactCount, Seq("SUBSCRIBER_ID"), "left")
        .withColumn(
          "SOCIAL_ENTROPY_TERM",
          lit(-1) * col("PROP_CALL_DURATION") * log(col("PROP_CALL_DURATION")) / log(col("CONTACT_COUNT")))
        .groupBy(col("SUBSCRIBER_ID"))
        .agg(sum(col("SOCIAL_ENTROPY_TERM")).as("SOCIAL_ENTROPY"))

    // Write the results to disk
    filteredData
      .select(col("SUBSCRIBER_ID"))
      .distinct
      .join(interactionPerContact, Seq("SUBSCRIBER_ID"), "left")
      .join(contactCount, Seq("SUBSCRIBER_ID"), "left")
      .join(socialEntropy, Seq("SUBSCRIBER_ID"), "left")
      .write
      .parquet(hdfsUrl + "/results/viren_socioeconomic_mapping/social_features_" + startDate + "_" + endDate)
  }
}
