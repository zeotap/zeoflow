package com.zeotap.zeoflow.types

import com.zeotap.zeoflow.utils.spark.SparkUtils.moveTablesToContext
import org.apache.spark.sql.{DataFrame, SparkSession}

case class SparkQuery(query: String, name: String)(implicit spark: SparkSession) extends Query[DataFrame](query, name) {
  override def preprocess(inputTables: Map[String, DataFrame]): Map[String, DataFrame] = moveTablesToContext(inputTables)
  override def process(inputTables: Map[String, DataFrame], cache: Map[String, Any]): Map[String, DataFrame] = Map(name -> spark.sql(query))
  override def postprocess(inputTables: Map[String, DataFrame]): Map[String, DataFrame] = moveTablesToContext(inputTables)
}
