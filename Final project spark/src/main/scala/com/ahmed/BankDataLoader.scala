package com.ahmed

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.zookeeper.Transaction

object BankDataLoader {
  def loadDataFromCSV(sc: SparkContext, filename: String): RDD[Transaction] = {
    val dataRDD = sc.textFile(filename)
    // Use zipWithIndex to add an index to each line of the RDD
    val indexedDataRDD = dataRDD.zipWithIndex()

    // Filter out the line with index 0 (header line)
    val nonHeaderLinesRDD = indexedDataRDD.filter { case (_, index) => index > 0 }

    val transactionRDD = nonHeaderLinesRDD.map { case (line, _) =>
      val fields = line.split(",")
      Transaction(fields(0).toInt, fields(1), fields(2), fields(3).toDouble)
    }

    transactionRDD
  }
}
