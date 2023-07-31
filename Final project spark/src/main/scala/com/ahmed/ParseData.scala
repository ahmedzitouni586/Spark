package com.ahmed

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object ParseData {
  def parseDate(dateStr: String): LocalDate = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss]")
    LocalDate.parse(dateStr, formatter)
  }
}
