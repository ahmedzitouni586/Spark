package com.ahmed

import org.apache.spark.rdd.RDD

object CustomerTransaction {
  def customerTransactionFrequency(data: RDD[Transaction]): RDD[CustomerTransactionFrequency] = {
    val transactionCounts = data.map(transaction => (transaction.customer_id, 1))
      .reduceByKey(_ + _)

    transactionCounts.map { case (customer_id, frequency) =>
      CustomerTransactionFrequency(customer_id, frequency)
    }
  }
}
