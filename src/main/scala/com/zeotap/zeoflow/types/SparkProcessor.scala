package com.zeotap.zeoflow.types

import org.apache.spark.sql.SparkSession

class SparkProcessor(implicit spark: SparkSession) extends Processor {

  override def process(inputTableNames: List[String], outputTableNames: List[String]): Unit = {}

}
