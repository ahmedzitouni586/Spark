package com.ahmed
%spark

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.sys.process._

// Import the required Python libraries for plotting



object PlotTransaction {
  def plotTransactionTrends(data: RDD[(String, Double)]): Unit = {
    // Process the RDD to get aggregated data for transaction types
    val aggregatedData = data
      .map { case (customer_id, amount) => (customer_id, if (amount >= 0) "deposit" else "withdrawal") }
      .map((_, 1))
      .reduceByKey(_ + _)
      .collect() // Collect the data to the driver

    // Convert the aggregated data into Zeppelin-specific format for visualization
    val zeppelinData = aggregatedData.map { case ((customer_id, transactionType), count) =>
      Map("customer_id" -> customer_id, "transactionType" -> transactionType, "count" -> count)
    }

    // Display the histogram using Zeppelin built-in visualization
    zeppelinData.toSeq.toDF.createOrReplaceTempView("transaction_trends")
    % table transaction_trends
  }


}
