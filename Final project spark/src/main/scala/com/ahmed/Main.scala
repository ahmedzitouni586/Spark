package com.ahmed

import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("BankDataAnalysis").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // Load data from CSV and perform data cleaning
    val dataRDD = BankDataLoader.loadDataFromCSV(sc, "input.csv")
    val cleanedDataRDD = DataCleaning.handleMissingData(dataRDD)

    // display cleanedRDD
    //val collectedData = cleanedDataRDD.collect()
    //collectedData.foreach(println)
    val basicStatistics = StatisticsCalculating.calculateBasicStatistics(cleanedDataRDD)
    val customerTransaction = CustomerTransaction.customerTransactionFrequency(cleanedDataRDD)
    val groupTransactions = GroupTransactions.groupByTransactionDate(cleanedDataRDD, "2023-07-03")
    val customerSegmentation = CustomerSegmentation.customerSegmentation(cleanedDataRDD)

  }
}
