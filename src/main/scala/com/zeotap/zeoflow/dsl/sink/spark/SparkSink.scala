package com.zeotap.zeoflow.dsl.sink.spark

import com.zeotap.sink.spark.writer.FSSparkWriter
import com.zeotap.zeoflow.dsl.sink.Sink
import org.apache.spark.sql.SparkSession

case class SparkSink(sinkWriter: FSSparkWriter, tableName: String)(implicit spark: SparkSession) extends Sink {
  override def write(): Unit = sinkWriter.buildUnsafe(spark.table(tableName))
}