package com.ahmed

import org.apache.hadoop.shaded.org.apache.commons.net.ntp.TimeStamp

case class Transaction(customer_id: Int, transaction_date: TimeStamp, transaction_type: String, amount: Double)

