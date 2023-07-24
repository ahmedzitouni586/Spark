package com.ahmed

import org.apache.spark.rdd.RDD

object DataCleaning {
  def handleMissingData(data: RDD[Transaction]): RDD[Transaction] = {
    data.filter(transaction => transaction.amount > 0)
  }
}
