package com.ahmed

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{count, udf}

object CustomerSegmentation {
  def customerSegmentation(data: DataFrame): DataFrame = {
    val spark = data.sparkSession
    import spark.implicits._
    val customerSummary = data.groupBy("customer_id")
      .agg(
        functions.sum("amount").alias("total_transaction_amount"),
        count("*").alias("transaction_frequency")
      )
    val highValueAmountThreshold = 1000.0
    val frequentTransactionThreshold = 10
    val segmentLabelUDF = udf((totalAmount: Double, frequency: Long) => {
      if (totalAmount >= highValueAmountThreshold && frequency >= frequentTransactionThreshold) "High-Value & Frequent"
      else if (totalAmount >= highValueAmountThreshold) "High-Value"
      else if (frequency >= frequentTransactionThreshold) "Frequent"
      else "Inactive"
    })
    val segmentedCustomers = customerSummary.withColumn("segment", segmentLabelUDF($"total_transaction_amount", $"transaction_frequency"))
    segmentedCustomers.select("customer_id", "segment")
  }
}
