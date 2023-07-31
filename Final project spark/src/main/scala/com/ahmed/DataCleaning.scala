package com.ahmed

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object DataCleaning {
  def handleMissingData(data: DataFrame): DataFrame = {
    data.filter(col("amount") > 0 && col("transaction_date").isNotNull && col("transaction_type").isNotNull && col("customer_id").isNotNull)
  }
}
