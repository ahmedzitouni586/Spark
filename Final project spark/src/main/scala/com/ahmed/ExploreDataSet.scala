package com.ahmed

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}


object Exploredataset {
  def exploreDataset(data: DataFrame): Unit = {
    data.printSchema()
    data.show()
  }
}
