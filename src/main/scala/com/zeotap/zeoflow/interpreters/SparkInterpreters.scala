package com.zeotap.zeoflow.interpreters

import cats.arrow.FunctionK
import cats.data.State
import com.zeotap.zeoflow.constructs.SparkOps.SparkExt
import com.zeotap.zeoflow.dsl.FlowDSL._
import com.zeotap.zeoflow.dsl.{FlowDSL, SinkBuilder, SourceBuilder}
import com.zeotap.zeoflow.types.Transformation
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkInterpreters {

  type SparkFlow[A] = State[Map[String, DataFrame], A]

  def sparkFlowInterpreter()(implicit spark: SparkSession): FunctionK[FlowDSL, SparkFlow] = new FunctionK[FlowDSL, SparkFlow] {
    override def apply[A](feature: FlowDSL[A]): SparkFlow[A] = State {
      context => feature match {
        case LoadSources(sources) => (spark.loadSources(sources.asInstanceOf[List[SourceBuilder[DataFrame]]]), context.asInstanceOf[A])
        case LoadUserDefinedFunctions(udfs) => (context, spark.loadUserDefinedFunctions(udfs).asInstanceOf[A])
        case RunTransformations(transformations) => (spark.runTransformations(context, transformations.asInstanceOf[List[Transformation[DataFrame]]]), context.asInstanceOf[A])
        case WriteToSinks(sinks) => (context, spark.writeToSinks(context, sinks.asInstanceOf[List[SinkBuilder[DataFrame]]]).asInstanceOf[A])
      }
    }
  }
}
