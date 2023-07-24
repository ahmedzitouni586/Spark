package com.ahmed

import org.apache.spark.rdd.RDD

object GroupTransactions {
  def groupByTransactionDate(data: RDD[Transaction], timeUnit: String): RDD[(String, Double)] = {
    // Implement your logic here to group transactions based on the transaction date (daily, monthly, etc.)
    // For example, you can use RDD transformations like map and reduceByKey to aggregate transactions by date
    // Return an RDD with the aggregated data, where the key is the date and the value is the total transaction amount
    data.map(transaction => (transaction.transaction_date, transaction.amount))
      .reduceByKey(_ + _)
  }
}
