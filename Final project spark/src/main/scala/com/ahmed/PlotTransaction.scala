package com.ahmed

import org.apache.orc.impl.DateUtils.parseDate
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{coalesce, col, date_format, date_trunc, lit, udf}

object PlotTransaction {
  /*def plotTransactionTrends(data: DataFrame, timeUnit: String): Unit = {
    val spark = data.sparkSession
    import spark.implicits._
    val parseDateUDF = udf(parseDate _)
    val dataWithTimeUnit = data.withColumn("transaction_date_parsed", parseDateUDF($"transaction_date"))
      .withColumn("transaction_date_truncated", date_trunc(timeUnit, $"transaction_date_parsed"))
    val aggregatedData = dataWithTimeUnit.groupBy("transaction_date_truncated", "transaction_type")
      .agg(functions.sum("amount").alias("transaction_amount"))
    val pivotedData = aggregatedData.groupBy("transaction_date_truncated")
      .pivot("transaction_type", Seq("deposit", "withdrawal"))
      .agg(coalesce(functions.sum("transaction_amount"), lit(0.0)))
    val timeColumn = timeUnit match {
      case "day" => $"transaction_date_truncated".cast("string")
      case "month" => date_format($"transaction_date_truncated", "yyyy-MM")
    }
    val depositSeries = pivotedData.select(timeColumn, col("deposit"))
      .withColumnRenamed("deposit", "Deposits")
      .orderBy(timeColumn)
      .collect()
      .map(row => (row.getAs[String](0), row.getAs[Double]("Deposits")))
    val withdrawalSeries = pivotedData.select(timeColumn, col("withdrawal"))
      .withColumnRenamed("withdrawal", "Withdrawals")
      .orderBy(timeColumn)
      .collect()
      .map(row => (row.getAs[String](0), row.getAs[Double]("Withdrawals")))
    val dataset = new DefaultCategoryDataset()
    depositSeries.foreach { case (date, depositAmount) =>
      dataset.addValue(depositAmount, "Deposits", date)
    }
    withdrawalSeries.foreach { case (date, withdrawalAmount) =>
      dataset.addValue(withdrawalAmount, "Withdrawals", date)
    }
    val chart = ChartFactory.createLineChart("Transaction Trends", "Date", "Amount", dataset, PlotOrientation.VERTICAL, true, true, false)
    val frame = new JFrame("Transaction Trends")
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
    frame.add(new ChartPanel(chart))
    frame.pack()
    frame.setVisible(true)
  }*/
}
