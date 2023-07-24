package com.ahmed

import org.apache.spark.rdd.RDD

object StatisticsCalculating {
  def calculateBasicStatistics(data: RDD[Transaction]): Unit = {
    val totalDeposits = data.filter(_.transaction_type == "deposit").map(_.amount).sum()
    val totalWithdrawals = data.filter(_.transaction_type == "withdrawal").map(_.amount).sum()
    val averageTransactionAmount = data.map(_.amount).mean()

    println(s"Total Deposits: $totalDeposits")
    println(s"Total Withdrawals: $totalWithdrawals")
    println(s"Average Transaction Amount: $averageTransactionAmount")
  }

}
