package com.ahmed

import org.apache.orc.impl.DateUtils.parseDate
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{avg, count, date_trunc, udf}

object GroupTransactions {
  def groupByTransactionDate(data: DataFrame, timeUnit: String): DataFrame = {
    val spark = data.sparkSession
    import spark.implicits._
    val parseDateUDF = udf(parseDate _)
    val dataWithTimeUnit = data.withColumn("transaction_date_parsed", parseDateUDF($"transaction_date"))
      .withColumn("transaction_date_truncated", date_trunc(timeUnit, $"transaction_date_parsed"))
    val aggregatedData = dataWithTimeUnit.groupBy("transaction_date_truncated")
      .agg(count("*").alias("transaction_count"),
        functions.sum("amount").alias("total_amount"),
        avg("amount").alias("average_amount"),
        functions.min("amount").alias("min_amount"),
        functions.max("amount").alias("max_amount"))
    aggregatedData
  }
}
