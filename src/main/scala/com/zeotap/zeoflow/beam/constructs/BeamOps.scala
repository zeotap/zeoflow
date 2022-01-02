package com.zeotap.zeoflow.beam.constructs

import com.zeotap.zeoflow.beam.types.BeamUDF
import com.zeotap.zeoflow.common.types.{Sink, Source, Transformation}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.{PCollection, Row}

object BeamOps {

  implicit class BeamExt(beam: Pipeline) {

    def loadSources(context: BeamContext, sources: List[Source[PCollection[Row]]]): BeamContext = BeamContext(context.pCollections ++ sources.map(source => source.load()).toMap, context.udfs)

    def loadUserDefinedFunctions(context: BeamContext, udfs: List[BeamUDF]): BeamContext = BeamContext(context.pCollections, context.udfs ++ udfs)

    def runTransformations(context: BeamContext, transformations: List[Transformation[PCollection[Row]]]): BeamContext = {
      val inputTables = context.pCollections
      BeamContext(transformations.foldLeft(inputTables) { (accMap, transformation) =>
        accMap ++ transformation.postprocess(transformation.process(transformation.preprocess(accMap), Map("udfs" -> context.udfs)))
      }, context.udfs)
    }

    def writeToSinks(context: BeamContext, sinks: List[Sink[PCollection[Row]]]): Unit = sinks.foreach(sink => sink.write(context.pCollections))

  }

}
