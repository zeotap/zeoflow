package com.zeotap.zeoflow.spark.utils

import org.apache.spark.sql.DataFrame

object SparkUtils {

  implicit class SparkContextExt(tables: Map[String, DataFrame]) {
    def moveTablesToContext: Map[String, DataFrame] = {
      tables.foreach(kv => kv._2.createOrReplaceTempView(kv._1))
      tables
    }
  }
}
