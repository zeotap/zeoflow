package com.zeotap.zeoflow.spark.types

import com.zeotap.zeoflow.common.types.Transformation
import com.zeotap.zeoflow.spark.utils.SparkUtils.SparkContextExt
import org.apache.spark.sql.DataFrame

abstract class SparkProcessor extends Transformation[DataFrame] {
  def preprocess(inputTables: Map[String, DataFrame]): Map[String, DataFrame] = inputTables.moveTablesToContext
  def postprocess(outputTables: Map[String, DataFrame]): Map[String, DataFrame] = outputTables.moveTablesToContext
}
