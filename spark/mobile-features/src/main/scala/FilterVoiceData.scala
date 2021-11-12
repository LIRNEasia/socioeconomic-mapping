package net.lirneasia

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object FilterVoiceData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("FilterVoiceData").getOrCreate()

    val host = args(0)
    val port = args(1)
    val voiceDataLocation = args(2)
    val towerDataLocation = args(3)
    val startDate = args(4)
    val endDate = args(5)
    val hdfsUrl = "hdfs://" + host + ":" + port

    // Load and filter the voice and tower data
    val voiceData =
      spark
        .read
        .parquet(hdfsUrl + voiceDataLocation)
        .filter(col("CALL_TIME") >= startDate + "000000" && col("CALL_TIME") < endDate + "000000")
        .select(
          col("CALL_DIRECTION_KEY"),
          col("ANUMBER"),
          col("OTHER_NUMBER"),
          col("CELL_ID"),
          col("CALL_TIME").substr(9, 4).cast(IntegerType).as("CALL_TIME_HHMM"),
          col("CALL_TIME").substr(1, 8).cast(IntegerType).as("CALL_DATE_YYYYMMDD"),
          col("DURATION").as("CALL_DURATION"))

    val towerData = spark.read.option("header", "true").csv(hdfsUrl + towerDataLocation)

    // Label the subscriber ID and other ID using the call direction key, and match the cell IDs to find the tower
    // associated with each record
    def subscriberFilter =
      udf((callDirectionKey: Integer, aNumber: String, otherNumber: String) => {
        if (callDirectionKey == 2) aNumber else otherNumber
      })

    def otherFilter =
      udf((callDirectionKey: Integer, aNumber: String, otherNumber: String) => {
        if (callDirectionKey == 1) aNumber else otherNumber
      })

    def directionFilter =
      udf((callDirectionKey: Integer) => {
        if (callDirectionKey == 1) "Incoming" else "Outgoing"
      })

    val filteredData =
      voiceData
        .withColumn("SUBSCRIBER_ID", subscriberFilter(col("CALL_DIRECTION_KEY"), col("ANUMBER"), col("OTHER_NUMBER")))
        .withColumn("OTHER_ID", otherFilter(col("CALL_DIRECTION_KEY"), col("ANUMBER"), col("OTHER_NUMBER")))
        .withColumn("CALL_DIRECTION", directionFilter(col("CALL_DIRECTION_KEY")))
        .join(towerData, col("CELL_ID") === col("cellid"))
        .select(
          col("SUBSCRIBER_ID"),
          col("OTHER_ID"),
          col("1k_cell").as("TOWER_ID"),
          col("CALL_DIRECTION"),
          col("CALL_TIME_HHMM"),
          col("CALL_DATE_YYYYMMDD"),
          col("CALL_DURATION"))

    // Write the results to disk
    filteredData
      .write
      .parquet(hdfsUrl + "/results/viren_socioeconomic_mapping/filtered_data_" + startDate + "_" + endDate)
  }
}
