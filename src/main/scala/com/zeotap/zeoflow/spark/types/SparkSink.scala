package com.zeotap.zeoflow.spark.types

import com.zeotap.data.io.sink.spark.writer.SparkWriter
import com.zeotap.zeoflow.common.types.Sink
import org.apache.spark.sql.DataFrame

final case class SparkSink(tableName: String, sparkWriter: SparkWriter) extends Sink[DataFrame] {
  override def write(inputTables: Map[String, DataFrame]): Unit = sparkWriter.buildUnsafe(inputTables(tableName))
}
