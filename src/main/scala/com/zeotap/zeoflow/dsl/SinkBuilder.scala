package com.zeotap.zeoflow.dsl

import com.zeotap.sink.spark.writer.SparkWriter
import org.apache.spark.sql.DataFrame

sealed trait SinkBuilder[A] {
  def build(inputTables: Map[String, A]): Unit
}

final case class SparkSinkBuilder(tableName: String, sinkWriter: SparkWriter) extends SinkBuilder[DataFrame] {
  override def build(inputTables: Map[String, DataFrame]): Unit = sinkWriter.buildUnsafe(inputTables(tableName))
}
