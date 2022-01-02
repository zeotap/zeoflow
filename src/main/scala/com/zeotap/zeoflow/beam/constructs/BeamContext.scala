package com.zeotap.zeoflow.beam.constructs

import com.zeotap.zeoflow.beam.types.BeamUDF
import org.apache.beam.sdk.values.{PCollection, Row}

case class BeamContext(pCollections: Map[String, PCollection[Row]], udfs: List[BeamUDF])
