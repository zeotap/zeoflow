package com.zeotap.zeoflow.spark.types

import com.zeotap.sink.spark.writer.SparkWriter
import com.zeotap.zeoflow.common.types.SinkBuilder
import org.apache.spark.sql.DataFrame

final case class SparkSinkBuilder(tableName: String, sinkWriter: SparkWriter) extends SinkBuilder[DataFrame] {
  override def build(inputTables: Map[String, DataFrame]): Unit = sinkWriter.buildUnsafe(inputTables(tableName))
}
