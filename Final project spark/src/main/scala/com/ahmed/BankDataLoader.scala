package com.ahmed

import org.apache.hadoop.shaded.org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.zookeeper.Transaction
import org.json4s.DefaultFormats.dateFormat

object BankDataLoader {

  def loadDataFromCSV(spark: SparkSession, filename: String): DataFrame = {
    val dataDF = spark.read.option("header", "true").option("inferSchema", "true").csv(filename)
    dataDF
  }
}