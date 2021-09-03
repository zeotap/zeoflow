package com.zeotap.zeoflow.dsl

import com.zeotap.sink.spark.writer.SparkWriter
import org.apache.spark.sql.DataFrame

trait SinkBuilder[A] {
  def build(inputTables: Map[String, A]): Unit
}

case class SparkSinkBuilder(sinkWriter: SparkWriter, tableName: String) extends SinkBuilder[DataFrame] {
  override def build(inputTables: Map[String, DataFrame]): Unit = sinkWriter.buildUnsafe(inputTables(tableName))
}
