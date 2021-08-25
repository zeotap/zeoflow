package com.zeotap.zeoflow.types

import org.apache.spark.sql.DataFrame

case class SparkProcessorTransformation(processor: Processor[DataFrame], inputTableNames: Map[String,DataFrame]) extends Transformation[DataFrame]{
  override def transform(): Map[String, DataFrame] = processor.process(inputTableNames)
}
