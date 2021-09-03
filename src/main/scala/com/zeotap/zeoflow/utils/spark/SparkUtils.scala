package com.zeotap.zeoflow.utils.spark

import org.apache.spark.sql.DataFrame

object SparkUtils {
  def moveTablesToContext(inputTables: Map[String, DataFrame]): Map[String, DataFrame] = {
    inputTables.foreach(kv => kv._2.createOrReplaceTempView(kv._1))
    inputTables
  }
}
