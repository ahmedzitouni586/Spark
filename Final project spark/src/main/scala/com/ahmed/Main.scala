package com.ahmed

import com.ahmed.BankDataLoader.loadDataFromCSV
import com.ahmed.CustomerTransaction.customerTransactionFrequency
import com.ahmed.DataCleaning.handleMissingData

import com.ahmed.GroupTransactions.groupByTransactionDate
//import com.ahmed.PlotTransaction.plotTransactionTrends
import com.ahmed.StatisticsCalculating.calculateBasicStatistics
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object Main {



  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Load CSV Data")
      .master("local")
      .getOrCreate()

    val transactions = loadDataFromCSV(spark, "input.csv")

    Exploredataset.exploreDataset(transactions)
    val filteredData = handleMissingData(transactions)
    Exploredataset.exploreDataset(filteredData)
    calculateBasicStatistics(filteredData)
    println("**************************************************************")
    val transactionFrequencyDF = customerTransactionFrequency(filteredData)
    transactionFrequencyDF.show()
    println("*******************************************************************")
    val dailyData = groupByTransactionDate(filteredData, "day")
    val monthlyData = groupByTransactionDate(filteredData, "month")
    dailyData.show()
    monthlyData.show()
    //plotTransactionTrends(filteredData, "day")
  }
}
