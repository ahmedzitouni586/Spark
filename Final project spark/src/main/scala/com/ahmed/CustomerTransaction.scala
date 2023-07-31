package com.ahmed

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count

object CustomerTransaction {
  def customerTransactionFrequency(data: DataFrame): DataFrame = {
    val transactionFrequencyDF = data.groupBy("customer_id")
      .agg(count("*").alias("transaction_frequency"))
    transactionFrequencyDF
  }
}
