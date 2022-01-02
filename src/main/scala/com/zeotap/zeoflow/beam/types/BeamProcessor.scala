package com.zeotap.zeoflow.beam.types

import com.zeotap.zeoflow.common.types.Transformation
import org.apache.beam.sdk.values.{PCollection, Row}

abstract class BeamProcessor extends Transformation[PCollection[Row]] {
  def preprocess(inputTables: Map[String, PCollection[Row]]): Map[String, PCollection[Row]] = inputTables
  def postprocess(outputTables: Map[String, PCollection[Row]]): Map[String, PCollection[Row]] = outputTables
}
