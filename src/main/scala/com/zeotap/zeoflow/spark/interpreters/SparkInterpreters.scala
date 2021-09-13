package com.zeotap.zeoflow.spark.interpreters

import cats.arrow.FunctionK
import cats.data.State
import com.zeotap.zeoflow.common.dsl.FlowDSL
import com.zeotap.zeoflow.common.dsl.FlowDSL.{LoadSources, LoadUserDefinedFunctions, RunTransformations, WriteToSinks}
import com.zeotap.zeoflow.common.types.{Sink, Source, Transformation}
import com.zeotap.zeoflow.spark.constructs.SparkOps.SparkExt
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkInterpreters {

  type SparkFlow[A] = State[Map[String, DataFrame], A]

  def sparkFlowInterpreter()(implicit spark: SparkSession): FunctionK[FlowDSL, SparkFlow] = new FunctionK[FlowDSL, SparkFlow] {
    override def apply[A](feature: FlowDSL[A]): SparkFlow[A] = State {
      context =>
        feature match {
          case LoadSources(sources) => (spark.loadSources(sources.asInstanceOf[List[Source[DataFrame]]]), context.asInstanceOf[A])
          case LoadUserDefinedFunctions(udfs) => (context, spark.loadUserDefinedFunctions(udfs).asInstanceOf[A])
          case RunTransformations(transformations) => (spark.runTransformations(context, transformations.asInstanceOf[List[Transformation[DataFrame]]]), context.asInstanceOf[A])
          case WriteToSinks(sinks) => (context, spark.writeToSinks(context, sinks.asInstanceOf[List[Sink[DataFrame]]]).asInstanceOf[A])
        }
    }
  }
}
