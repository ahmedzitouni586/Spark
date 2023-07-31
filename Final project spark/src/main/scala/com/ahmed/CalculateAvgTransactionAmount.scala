package com.ahmed

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.avg

object CalculateAvgTransactionAmount {
  def calculateAvgTransactionAmount(data: DataFrame, segments: DataFrame): DataFrame = {
    val dataWithSegment = data.join(segments, Seq("customer_id"))
    val avgTransactionAmountDF = dataWithSegment.groupBy("segment")
      .agg(avg("amount").alias("avg_transaction_amount"))
    avgTransactionAmountDF
  }
}
