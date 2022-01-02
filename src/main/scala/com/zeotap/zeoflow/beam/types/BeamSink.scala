package com.zeotap.zeoflow.beam.types

import com.zeotap.data.io.sink.beam.writer.BeamWriter
import com.zeotap.zeoflow.common.types.Sink
import org.apache.beam.sdk.values.{PCollection, Row}

final case class BeamSink(tableName: String, beamWriter: BeamWriter) extends Sink[PCollection[Row]] {
  override def write(inputTables: Map[String, PCollection[Row]]): Unit = beamWriter.buildUnsafe(inputTables(tableName))
}
