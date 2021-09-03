package com.zeotap.zeoflow.types

import com.zeotap.zeoflow.utils.spark.SparkUtils.moveTablesToContext
import org.apache.spark.sql.DataFrame

class SparkProcessor extends Processor[DataFrame] {
  override def preprocess(inputTables: Map[String, DataFrame]): Map[String, DataFrame] = moveTablesToContext(inputTables)
  override def process(inputTables: Map[String, DataFrame], cache: Map[String, Any]): Map[String, DataFrame] = Map()
  override def postprocess(inputTables: Map[String, DataFrame]): Map[String, DataFrame] = moveTablesToContext(inputTables)
}
