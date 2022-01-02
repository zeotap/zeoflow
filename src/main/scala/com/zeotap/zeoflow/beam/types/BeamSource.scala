package com.zeotap.zeoflow.beam.types

import com.zeotap.data.io.source.beam.loader.BeamLoader
import com.zeotap.zeoflow.common.types.Source
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.{PCollection, Row}

final case class BeamSource(tableName: String, beamLoader: BeamLoader)(implicit beam: Pipeline) extends Source[PCollection[Row]] {
  override def load(): (String, PCollection[Row]) = (tableName, beamLoader.buildUnsafe)
}
