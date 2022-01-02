package com.zeotap.zeoflow.beam.interpreters

import cats.arrow.FunctionK
import cats.data.State
import com.zeotap.zeoflow.beam.constructs.BeamContext
import com.zeotap.zeoflow.beam.constructs.BeamOps.BeamExt
import com.zeotap.zeoflow.beam.types.BeamUDF
import com.zeotap.zeoflow.common.dsl.FlowDSL
import com.zeotap.zeoflow.common.dsl.FlowDSL.{LoadSources, LoadUserDefinedFunctions, RunTransformations, WriteToSinks}
import com.zeotap.zeoflow.common.types.{Sink, Source, Transformation}
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.{PCollection, Row}

object BeamInterpreters {

  type BeamFlow[A] = State[BeamContext, A]

  def beamFlowInterpreter()(implicit beam: Pipeline): FunctionK[FlowDSL, BeamFlow] = new FunctionK[FlowDSL, BeamFlow] {
    override def apply[A](feature: FlowDSL[A]): BeamFlow[A] = State {
      context =>
        feature match {
          case LoadSources(sources) => (beam.loadSources(context, sources.asInstanceOf[List[Source[PCollection[Row]]]]), context.asInstanceOf[A])
          case LoadUserDefinedFunctions(udfs) => (beam.loadUserDefinedFunctions(context, udfs.asInstanceOf[List[BeamUDF]]), context.asInstanceOf[A])
          case RunTransformations(transformations) => (beam.runTransformations(context, transformations.asInstanceOf[List[Transformation[PCollection[Row]]]]), context.asInstanceOf[A])
          case WriteToSinks(sinks) => (context, beam.writeToSinks(context, sinks.asInstanceOf[List[Sink[PCollection[Row]]]]).asInstanceOf[A])
        }
    }
  }
}
