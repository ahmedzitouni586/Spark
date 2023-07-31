package com.ahmed

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{avg, col}

object StatisticsCalculating {
  def calculateBasicStatistics(data: DataFrame): Unit = {
    val totalDeposits = data.filter(col("transaction_type") === "deposit").agg(functions.sum("amount")).head().getDouble(0)
    val totalWithdrawals = data.filter(col("transaction_type") === "withdrawal").agg(functions.sum("amount")).head().getDouble(0)
    val averageTransactionAmount = data.agg(avg("amount")).head().getDouble(0)
    println(s"Total Deposits: $totalDeposits")
    println(s"Total Withdrawals: $totalWithdrawals")
    println(s"Average Transaction Amount: $averageTransactionAmount")
  }

}
