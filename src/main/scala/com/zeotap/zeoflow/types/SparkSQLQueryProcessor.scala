package com.zeotap.zeoflow.types

import com.zeotap.zeoflow.utils.spark.SparkUtils.SparkContextExt
import org.apache.spark.sql.{DataFrame, SparkSession}

final case class SparkSQLQueryProcessor(name: String, query: String)(implicit spark: SparkSession) extends SQLQueryProcessor[DataFrame](name, query) {
  override def preprocess(inputTables: Map[String, DataFrame]): Map[String, DataFrame] = inputTables.moveTablesToContext
  override def process(inputTables: Map[String, DataFrame], readOnlyGlobalCache: Map[String, Any]): Map[String, DataFrame] = Map(name -> spark.sql(query))
  override def postprocess(outputTables: Map[String, DataFrame]): Map[String, DataFrame] = outputTables.moveTablesToContext
}
