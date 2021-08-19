package com.zeotap.zeoflow.dsl

import com.zeotap.sink.spark.writer.SparkWriter
import org.apache.spark.sql.SparkSession

trait SinkBuilder {

  def build(): Unit

}

case class SparkSinkBuilder(sinkWriter: SparkWriter, tableName: String)(implicit spark: SparkSession) extends SinkBuilder {

  override def build(): Unit = sinkWriter.buildUnsafe(spark.table(tableName))

}
